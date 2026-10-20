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
	"fmt"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Bounds of the kaz compaction policy. The default keeps the upstream behavior:
// a read requests compaction from 5 slices. The upper bound stays below
// maxSlices (2500, a variable in base.go), where writes compact synchronously;
// TestKazCompactionPolicyConfig pins the relation.
//
// A cooldown that held compaction of busy chunks was tried and removed: it let
// small staged slices upload before compaction could retire them, which raised
// the total PUT count (docs/inspection/vm_write_hotspot/2026-10-14).
const (
	kazCompactionDefaultReadMin = 5
	kazCompactionMaxReadMin     = 2499
)

// ValidateKazCompactionPolicy rejects a read minimum outside its documented range.
// Zero means "use the default", so Go callers that never set it keep upstream behavior.
func (c *Config) ValidateKazCompactionPolicy() error {
	if v := c.KazCompactionReadMin; v != 0 && (v < kazCompactionDefaultReadMin || v > kazCompactionMaxReadMin) {
		return fmt.Errorf("invalid kaz-compaction-read-min %d (want %d..%d)", v, kazCompactionDefaultReadMin, kazCompactionMaxReadMin)
	}
	return nil
}

// kazCompactionReadMin returns the effective read minimum.
func (c *Config) kazCompactionReadMin() int {
	if c.KazCompactionReadMin == 0 {
		return kazCompactionDefaultReadMin
	}
	return c.KazCompactionReadMin
}

// kazCompactionPolicyEnabled reports whether the read minimum changes the upstream behavior.
func (c *Config) kazCompactionPolicyEnabled() bool {
	return c.kazCompactionReadMin() != kazCompactionDefaultReadMin
}

// kazCompactionMetrics groups the observations exported for the kaz compaction policy.
type kazCompactionMetrics struct {
	events    *prometheus.CounterVec
	queueWait prometheus.Histogram
	slices    prometheus.Histogram
	block     *prometheus.CounterVec // block compaction outcomes
	amount    *prometheus.CounterVec // block compaction amounts
}

// newKazCompactionMetrics creates unregistered collectors; register attaches them.
func newKazCompactionMetrics() *kazCompactionMetrics {
	return &kazCompactionMetrics{
		block: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_compaction_block_events_total",
			Help: "Kaz block compaction outcomes: noop, partial, full, wasted, error, fallback_upstream, fallback_block, delta_noop, delta_tier, delta_merge, delta_fold, delta_prune, delta_block_fallback, delta_hidden_full.",
		}, []string{"event"}),
		amount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_compaction_block_amount_total",
			Help: "Kaz block compaction amounts: blocks_written, masks, removed (records dropped), delta_bytes, delta_records, delta_fold_blocks.",
		}, []string{"kind"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_compaction_policy_events_total",
			Help: "Decisions of the kaz compaction policy on read-triggered requests: read_below_min, pass, delta_below_trigger.",
		}, []string{"event"}),
		queueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kaz_compaction_queue_wait_seconds",
			Help:    "Time from queuing a chunk in the priority compaction scheduler to the start of its compaction.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 18),
		}),
		slices: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kaz_compaction_scheduled_slices",
			Help:    "Slice count recorded in the priority scheduler hint when its compaction starts.",
			Buckets: []float64{5, 10, 20, 50, 100, 200, 350, 500, 700, 1000, 1500, 2000, 2500},
		}),
	}
}

// register attaches the collectors to reg.
func (k *kazCompactionMetrics) register(reg prometheus.Registerer) {
	reg.MustRegister(k.events, k.queueWait, k.slices, k.block, k.amount)
}

// observeStart records one compaction start taken from the priority scheduler.
func (k *kazCompactionMetrics) observeStart(wait time.Duration, count int) {
	k.queueWait.Observe(wait.Seconds())
	k.slices.Observe(float64(count))
}

// compactionPolicy filters read-triggered compaction requests: when Read reloads
// a chunk's slice list from the engine (an open-file chunk cache miss), the
// request for that 64 MiB chunk is queued only if the chunk has at least readMin
// slice records (raw records, not the merged visible pieces). Queuing does not
// run compaction at once; the scheduler runs one chunk per inode at a time.
// Commit-triggered requests (compactionWanted) never pass through it. It never touches metadata: dropping a request
// only leaves more slices in a chunk until a commit or a later read asks again.
type compactionPolicy struct {
	readMin int
	forward func(inode Ino, chunk uint32, count, tier int)
	metrics *kazCompactionMetrics
}

// newCompactionPolicy builds a read-request filter that forwards accepted requests.
func newCompactionPolicy(readMin int, forward func(inode Ino, chunk uint32, count, tier int), metrics *kazCompactionMetrics) *compactionPolicy {
	return &compactionPolicy{readMin: readMin, forward: forward, metrics: metrics}
}

// event counts one decision when metrics are attached.
func (p *compactionPolicy) event(name string) {
	if p.metrics != nil {
		p.metrics.events.WithLabelValues(name).Inc()
	}
}

// requestFromRead drops a read-triggered request below the read minimum and forwards the rest.
func (p *compactionPolicy) requestFromRead(inode Ino, chunk uint32, count, tier int) {
	if count < p.readMin {
		p.event("read_below_min")
		return
	}
	p.event("pass")
	p.forward(inode, chunk, count, tier)
}

// startCompactionPolicy attaches the kaz policy when the read minimum differs from
// upstream, and logs once whether it is on.
func (m *baseMeta) startCompactionPolicy() {
	if m.conf.ReadOnly || m.kazCompact.Load() != nil {
		return
	}
	if err := m.conf.ValidateKazCompactionPolicy(); err != nil {
		logger.Warnf("%s; keeping the upstream compaction triggers", err)
		return
	}
	if !m.conf.kazCompactionPolicyEnabled() {
		logger.Infof("kaz compaction policy: off (upstream compaction triggers)")
		return
	}
	readMin := m.conf.kazCompactionReadMin()
	if m.kazCompact.CompareAndSwap(nil, newCompactionPolicy(readMin, m.requestBackgroundCompaction, m.kazCompactMetrics)) {
		logger.Infof("kaz compaction policy: read-min=%d", readMin)
	}
}

// stopCompactionPolicy detaches the policy; later read requests take the upstream path.
func (m *baseMeta) stopCompactionPolicy() {
	m.kazCompact.Store(nil)
}

// requestCompactionFromRead routes a read-triggered request through the policy, if any.
//
// In delta mode, for a chunk whose base count this client remembers, the request
// is dropped (event delta_below_trigger) unless at least kazDeltaTrigger records
// were added since the last plan; the count passed on is that number. Such a
// request replaces the read-min check, which is meant for chunks that hold few
// records. The event is counted even when no policy is attached.
func (m *baseMeta) requestCompactionFromRead(inode Ino, chunk uint32, count, tier int) {
	if n, ok := m.kazDeltaReadCount(inode, chunk, count); ok {
		if n < kazDeltaTrigger {
			if m.kazCompactMetrics != nil {
				m.kazCompactMetrics.events.WithLabelValues("delta_below_trigger").Inc()
			}
			return
		}
		if p := m.kazCompact.Load(); p != nil {
			p.event("pass")
		}
		m.requestBackgroundCompaction(inode, chunk, n, tier)
		return
	}
	if p := m.kazCompact.Load(); p != nil {
		p.requestFromRead(inode, chunk, count, tier)
		return
	}
	m.requestBackgroundCompaction(inode, chunk, count, tier)
}
