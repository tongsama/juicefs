/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

const compactionGCHintCapacity = 1024

// compactionGC isolates obsolete-slice hints from the legacy deletion transport.
// Hints are advisory: durable dead references remain until physical deletion succeeds.
// Neither hint submission nor dispatcher shutdown acquires dSliceMu.
type compactionGC struct {
	hints  chan Slice
	stop   chan struct{}
	done   chan struct{}
	once   sync.Once
	retire func(uint64, uint32) error
	events map[string]prometheus.Counter
}

// newCompactionGC starts a bounded dispatcher with optional local retirement.
// Retirement callbacks must perform local work only and preserve upload cancellation ordering.
func newCompactionGC(output chan<- Slice, canceled <-chan struct{}, capacity int, retire ...func(uint64, uint32) error) *compactionGC {
	var fn func(uint64, uint32) error
	if len(retire) > 0 {
		fn = retire[0]
	}
	return newCompactionGCWithEvents(output, canceled, capacity, fn, nil)
}

// newCompactionGCWithEvents prepares fixed event counters before publishing a dispatcher.
// Counter children are cached so foreground overflow reporting takes no metric-vector lock.
func newCompactionGCWithEvents(output chan<- Slice, canceled <-chan struct{}, capacity int, retire func(uint64, uint32) error, metrics *prometheus.CounterVec) *compactionGC {
	g := &compactionGC{hints: make(chan Slice, capacity), stop: make(chan struct{}), done: make(chan struct{}), retire: retire}
	if metrics != nil {
		g.events = make(map[string]prometheus.Counter)
		for _, event := range []string{"hint_overflow", "local_error", "local_retire_success", "remote_deferred", "remote_enqueued"} {
			g.events[event] = metrics.WithLabelValues(event)
		}
	}
	go g.dispatch(output, canceled)
	return g
}

// record counts fixed lifecycle outcomes without per-event logging or metadata reads.
func (g *compactionGC) record(event string) {
	if counter := g.events[event]; counter != nil {
		counter.Inc()
	}
}

// dispatch retires local staging before attempting a nonblocking remote enqueue.
// A full remote transport never prevents the next hint's local retirement; durable
// dead references retain remote intent until the existing recovery scan retries it.
func (g *compactionGC) dispatch(output chan<- Slice, canceled <-chan struct{}) {
	defer close(g.done)
	for {
		select {
		case <-g.stop:
			return
		case <-canceled:
			return
		case s := <-g.hints:
			select {
			case <-g.stop:
				return
			case <-canceled:
				return
			default:
			}
			if g.retire != nil {
				if err := g.retire(s.Id, s.Size); err != nil {
					g.record("local_error")
					if logger.IsLevelEnabled(logrus.DebugLevel) {
						logger.Debugf("compaction GC local retirement slice=%d size=%d err=%v; retaining durable marker", s.Id, s.Size, err)
					}
					continue
				}
				g.record("local_retire_success")
				if logger.IsLevelEnabled(logrus.DebugLevel) {
					logger.Debugf("compaction GC local retirement slice=%d size=%d err=<nil>", s.Id, s.Size)
				}
			}
			select {
			case <-g.stop:
				return
			case <-canceled:
				return
			case output <- s:
				g.record("remote_enqueued")
			default:
				g.record("remote_deferred")
				if logger.IsLevelEnabled(logrus.DebugLevel) {
					logger.Debugf("compaction GC remote deferred slice=%d size=%d err=transport_full; retaining durable marker", s.Id, s.Size)
				}
			}
		}
	}
}

// notify submits a hint without waiting, retaining durable intent on overflow or shutdown.
func (g *compactionGC) notify(id uint64, size uint32) {
	select {
	case <-g.stop:
		return
	default:
	}
	select {
	case <-g.stop:
	case g.hints <- Slice{Id: id, Size: size}:
	default:
		g.record("hint_overflow")
	}
}

// close joins the dispatcher before its captured deletion channel can be closed.
func (g *compactionGC) close() {
	g.once.Do(func() { close(g.stop) })
	<-g.done
}

// startCompactionGC attaches an optional dispatcher after deletion workers start.
// The caller serializes lifecycle transitions; producers use an atomic snapshot.
func (m *baseMeta) startCompactionGC() {
	if m.conf.CompactionGCMode != "deferred" || m.conf.ValidateCompactionGC() != nil || m.dslices == nil || m.compactionGC.Load() != nil {
		return
	}
	m.compactionGC.Store(newCompactionGCWithEvents(m.dslices, m.sessCtx.Done(), compactionGCHintCapacity, m.retireCompactionSlice, m.compactionGCEvents))
}

// enqueueCompactionDelete handles only obsolete slices after their reference decrement commits.
// A missing or stopped dispatcher leaves recovery to the existing durable marker scan.
func (m *baseMeta) enqueueCompactionDelete(id uint64, size uint32) {
	if m.conf.CompactionGCMode != "deferred" || m.conf.ValidateCompactionGC() != nil {
		m.deleteSlice(id, size)
		return
	}
	if id == 0 {
		return
	}
	if g := m.compactionGC.Load(); g != nil {
		g.notify(id, size)
	}
}

// retireCompactionSlice invokes an optional local-only adapter after committed retirement.
// An absent callback preserves compatibility with metadata-only users and older adapters.
func (m *baseMeta) retireCompactionSlice(id uint64, size uint32) error {
	m.msgCallbacks.Lock()
	cb := m.msgCallbacks.callbacks[RetireSlice]
	m.msgCallbacks.Unlock()
	if cb == nil {
		return nil
	}
	return cb(id, size)
}
