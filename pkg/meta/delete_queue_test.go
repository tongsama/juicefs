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
	"bytes"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fullDeleteQueue creates the production queue, stops its consumer, and fills it
// to capacity so enqueue backpressure can be exercised without deleting objects.
func fullDeleteQueue(t *testing.T) *baseMeta {
	t.Helper()
	conf := DefaultConf()
	conf.MaxDeletes = 1
	m := &baseMeta{conf: conf, sessCtx: Background()}
	m.startDeleteSliceTasks()
	if cap(m.dslices) != 10240 {
		t.Fatalf("queue capacity = %d, want 10240 for one worker", cap(m.dslices))
	}
	// No slices have been submitted: cancellation can stop the real worker
	// without requiring a metadata backend or an object deletion callback.
	m.sessCtx.Cancel()
	m.dSliceWG.Wait()
	m.sessWG.Wait()
	m.sessCtx = Background()
	t.Cleanup(func() {
		m.sessCtx.Cancel()
		m.stopDeleteSliceTasks()
	})
	for i := 0; i < cap(m.dslices); i++ {
		m.dslices <- Slice{Id: uint64(i + 1), Size: 4096}
	}
	return m
}

// deleteQueueWriteObserver preserves the real backend while exposing the
// transaction completion that precedes synchronous compaction waiting.
type deleteQueueWriteObserver struct {
	engine
	committed chan struct{}
	once      sync.Once
}

// doWrite signals only after the real metadata transaction reaches maxSlices.
func (e *deleteQueueWriteObserver) doWrite(ctx Context, inode Ino, indx uint32, off uint32, s Slice, mtime time.Time, n *int, delta *dirStat, attr *Attr) syscall.Errno {
	st := e.engine.doWrite(ctx, inode, indx, off, s, mtime, n, delta, attr)
	if st == 0 && *n == maxSlices {
		e.once.Do(func() { close(e.committed) })
	}
	return st
}

// waitSynchronousCompaction observes the real Write goroutine sleeping in
// compactChunk, rather than inferring entry from a fixed delay.
func waitSynchronousCompaction(t *testing.T, done <-chan struct{}) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	buf := make([]byte, 1<<20)
	for {
		select {
		case <-done:
			t.Fatal("Write completed before the background compaction was released")
		case <-deadline.C:
			t.Fatal("Write did not enter synchronous compaction waiting")
		case <-ticker.C:
			n := runtime.Stack(buf, true)
			for _, stack := range bytes.Split(buf[:n], []byte("\n\n")) {
				if bytes.Contains(stack, []byte("[sleep]")) &&
					bytes.Contains(stack, []byte(".(*baseMeta).compactChunk(")) &&
					bytes.Contains(stack, []byte(".(*baseMeta).Write(")) {
					return
				}
			}
		}
	}
}

// TestWriteWaitsForCompactionDeleteQueue exercises the real MemKV transaction,
// compaction replacement, deletion backpressure, and synchronous Write gate.
func TestWriteWaitsForCompactionDeleteQueue(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 1
	meta, err := newKVMeta("memkv", t.Name(), conf)
	if err != nil {
		t.Fatal(err)
	}
	m := meta.(*kvMeta)
	// MemKV may import a shared format fixture regardless of its address. Reset
	// only this client's in-memory state so the test owns its format completely.
	if err := m.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := m.Init(testFormat(), false); err != nil {
		t.Fatal(err)
	}
	// This metadata-only test simulates object creation like testCompaction.
	m.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	m.sessCtx = Background()
	observer := &deleteQueueWriteObserver{engine: m, committed: make(chan struct{})}
	m.en = observer
	queue := make(chan Slice, 1)
	queue <- Slice{Id: 999999, Size: 4096}
	m.dslices = queue
	bgDone, writeDone := make(chan struct{}), make(chan struct{})
	var bgStarted, writeStarted bool
	var drainOnce sync.Once
	drainDone := make(chan struct{})
	startDrain := func() {
		drainOnce.Do(func() {
			go func() {
				for range queue {
				}
				close(drainDone)
			}()
		})
	}
	t.Cleanup(func() {
		m.sessCtx.Cancel()
		startDrain()
		if bgStarted {
			waitDeleteQueueDone(t, bgDone)
		}
		if writeStarted {
			waitDeleteQueueDone(t, writeDone)
		}
		m.stopDeleteSliceTasks()
		waitDeleteQueueDone(t, drainDone)
		if err := m.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	ctx := Background()
	var inode Ino
	var attr Attr
	if st := m.Create(ctx, RootInode, "queue-wait", 0600, 0, 0, &inode, &attr); st != 0 {
		t.Fatal(st)
	}
	// Direct backend writes deliberately bypass automatic compaction.
	appendRaw := func() Slice {
		var id uint64
		if st := m.NewSlice(ctx, &id); st != 0 {
			t.Fatal(st)
		}
		s := Slice{Id: id, Size: 4096, Len: 4096}
		var n int
		var delta dirStat
		if st := m.doWrite(ctx, inode, 0, 0, s, time.Now(), &n, &delta, &attr); st != 0 {
			t.Fatal(st)
		}
		return s
	}
	appendRaw()
	appendRaw()
	bgStarted = true
	go func() {
		m.compactChunk(inode, 0, false, false, 0)
		close(bgDone)
	}()
	waitDeleteQueueBlocked(t, m.baseMeta, bgDone)
	ss, st := m.doRead(ctx, inode, 0)
	if st != 0 || len(ss) != 1 {
		t.Fatalf("background replacement not committed: slices=%d errno=%s", len(ss), st)
	}
	m.Lock()
	active := m.compacting[uint64(inode)]
	m.Unlock()
	if !active {
		t.Fatal("background compaction flag disappeared during deletion wait")
	}
	for i := 1; i < maxSlices-1; i++ {
		appendRaw()
	}
	var finalID uint64
	if st := m.NewSlice(ctx, &finalID); st != 0 {
		t.Fatal(st)
	}
	final := Slice{Id: finalID, Size: 4096, Len: 4096}
	var writeErr syscall.Errno
	writeStarted = true
	go func() {
		writeErr = m.Write(ctx, inode, 0, 0, final, time.Now())
		close(writeDone)
	}()
	waitDeleteQueueDone(t, observer.committed)
	waitSynchronousCompaction(t, writeDone)
	ss, st = m.doRead(ctx, inode, 0)
	if st != 0 || len(ss) != maxSlices || ss[len(ss)-1].id != finalID {
		t.Fatalf("waiting Write transaction not committed: slices=%d errno=%s", len(ss), st)
	}
	startDrain()
	waitDeleteQueueDone(t, bgDone)
	waitDeleteQueueDone(t, writeDone)
	if writeErr != 0 {
		t.Fatalf("Write failed after deletion queue progressed: %s", writeErr)
	}
	ss, st = m.doRead(ctx, inode, 0)
	visible := buildSlice(ss)
	if st != 0 || len(ss) >= maxSlices || len(visible) != 1 || visible[0] != final {
		t.Fatalf("final metadata changed: raw=%d visible=%+v errno=%s", len(ss), visible, st)
	}
}

// waitDeleteQueueBlocked waits for deleteSlice to own the enqueue mutex. With a
// full queue and no consumer, that ownership proves the call cannot complete.
func waitDeleteQueueBlocked(t *testing.T, m *baseMeta, done <-chan struct{}) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()
	for {
		select {
		case <-done:
			t.Fatal("deleteSlice returned while its queue was full")
		case <-timeout.C:
			t.Fatal("deleteSlice did not enter the enqueue critical section")
		case <-ticker.C:
			if !m.dSliceMu.TryLock() {
				return
			}
			m.dSliceMu.Unlock()
		}
	}
}

// waitDeleteQueueDone bounds test failures without imposing a production timeout.
func waitDeleteQueueDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("deleteSlice did not resume")
	}
}

// TestDeleteSliceQueueBackpressure verifies a full queue preserves every queued
// request and resumes the blocked enqueue only after a slot becomes available.
func TestDeleteSliceQueueBackpressure(t *testing.T) {
	m := fullDeleteQueue(t)
	pending := Slice{Id: uint64(cap(m.dslices) + 1), Size: 8192}
	done := make(chan struct{})
	go func() {
		m.deleteSlice(pending.Id, pending.Size)
		close(done)
	}()
	waitDeleteQueueBlocked(t, m, done)
	if got := <-m.dslices; got.Id != 1 || got.Size != 4096 {
		t.Fatalf("oldest queued slice changed: %+v", got)
	}
	waitDeleteQueueDone(t, done)
	for i := 2; i <= cap(m.dslices); i++ {
		if got := <-m.dslices; got.Id != uint64(i) || got.Size != 4096 {
			t.Fatalf("queued slice %d changed: %+v", i, got)
		}
	}
	if got := <-m.dslices; got != pending {
		t.Fatalf("resumed enqueue = %+v, want %+v", got, pending)
	}
	if len(m.dslices) != 0 {
		t.Fatal("unexpected extra deletion requests")
	}
}

// TestDeleteSliceQueueCancel verifies session shutdown releases a full-queue
// producer without overwriting queued requests or retaining the enqueue mutex.
func TestDeleteSliceQueueCancel(t *testing.T) {
	m := fullDeleteQueue(t)
	done := make(chan struct{})
	go func() {
		m.deleteSlice(uint64(cap(m.dslices)+1), 8192)
		close(done)
	}()
	waitDeleteQueueBlocked(t, m, done)
	m.sessCtx.Cancel()
	waitDeleteQueueDone(t, done)
	if !m.dSliceMu.TryLock() {
		t.Fatal("session cancellation retained the enqueue mutex")
	}
	m.dSliceMu.Unlock()
	for i := 1; i <= cap(m.dslices); i++ {
		if got := <-m.dslices; got.Id != uint64(i) || got.Size != 4096 {
			t.Fatalf("queued slice %d changed on cancellation: %+v", i, got)
		}
	}
	if len(m.dslices) != 0 {
		t.Fatal("canceled producer unexpectedly added a deletion request")
	}
}
