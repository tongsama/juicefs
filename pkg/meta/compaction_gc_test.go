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
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// TestDeferredCompactionBypassesDeleteTransport proves real committed metadata does not wait on the legacy mutex.
func TestDeferredCompactionBypassesDeleteTransport(t *testing.T) {
	conf := testConfig()
	conf.MaxDeletes = 1
	conf.NoBGJob = false
	conf.CompactionGCMode = "deferred"
	mm, err := newKVMeta("memkv", t.Name(), conf)
	if err != nil {
		t.Fatal(err)
	}
	m := mm.(*kvMeta)
	if err = m.Reset(); err != nil {
		t.Fatal(err)
	}
	if err = m.Init(testFormat(), false); err != nil {
		t.Fatal(err)
	}
	m.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	m.sessCtx = Background()
	m.dslices = make(chan Slice, 1)
	m.dslices <- Slice{Id: 999999, Size: 4096}
	// Production initializes the dispatcher with its existing deletion workers.
	m.startCompactionGC()
	var inode Ino
	var attr Attr
	if st := m.Create(Background(), RootInode, "deferred", 0600, 0, 0, &inode, &attr); st != 0 {
		t.Fatal(st)
	}
	for i := 0; i < 2; i++ {
		var id uint64
		if st := m.NewSlice(Background(), &id); st != 0 {
			t.Fatal(st)
		}
		var n int
		var d dirStat
		if st := m.doWrite(Background(), inode, 0, 0, Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), &n, &d, &attr); st != 0 {
			t.Fatal(st)
		}
	}
	m.dSliceMu.Lock()
	done := make(chan struct{})
	go func() { m.compactChunk(inode, 0, false, false, 0); close(done) }()
	select {
	case <-done:
	case <-time.After(250 * time.Millisecond):
		m.dSliceMu.Unlock()
		m.sessCtx.Cancel()
		<-done
		t.Fatal("deferred compaction waited on deletion transport mutex")
	}
	m.dSliceMu.Unlock()
	ss, st := m.doRead(Background(), inode, 0)
	if st != 0 || len(ss) != 1 {
		t.Fatalf("replacement not committed: %d %s", len(ss), st)
	}
	m.sessCtx.Cancel()
	m.stopDeleteSliceTasks()
	_ = m.Shutdown()
}

// TestCompactionGCValidation rejects modes that cannot recover lost volatile hints.
func TestCompactionGCValidation(t *testing.T) {
	for _, tc := range []struct {
		mode                  string
		deletes               int
		noBG, readOnly, valid bool
	}{
		{"", 0, true, false, true}, {"legacy", -1, true, false, true}, {"deferred", 1, false, false, true},
		{"deferred", 0, false, false, false}, {"deferred", -1, false, false, false},
		{"deferred", 1, true, false, false}, {"deferred", 1, false, true, false}, {"unknown", 1, false, false, false},
	} {
		c := &Config{CompactionGCMode: tc.mode, MaxDeletes: tc.deletes, NoBGJob: tc.noBG, ReadOnly: tc.readOnly}
		if got := c.ValidateCompactionGC() == nil; got != tc.valid {
			t.Fatalf("%+v valid=%v", tc, got)
		}
		if !tc.valid {
			c.SelfCheck()
			if c.CompactionGCMode != "legacy" {
				t.Fatal("invalid setting did not fall back")
			}
		}
	}
}

// TestCompactionGCBoundedShutdown checks bounded overflow and cancellation under a saturated worker queue.
func TestCompactionGCBoundedShutdown(t *testing.T) {
	output := make(chan Slice, 1)
	output <- Slice{Id: 100}
	ctx := Background()
	g := newCompactionGC(output, ctx.Done(), 2)
	for i := 0; i < 10000; i++ {
		g.notify(uint64(i+1), 4096)
	}
	if len(g.hints) > 2 {
		t.Fatal("unbounded hint queue")
	}
	done := make(chan struct{})
	go func() { g.close(); close(done) }()
	waitDeleteQueueDone(t, done)
	g.notify(20000, 4096)
	g.close()
	if s := <-output; s.Id != 100 {
		t.Fatal("overwrote deletion worker queue")
	}
	ctx.Cancel()
}

// gcTestBackend adapts each backend to the common real transaction/recovery contract.
type gcTestBackend struct {
	*baseMeta
	meta Meta
}

// newGCTestBackend creates only local isolated metadata; Redis requires an explicit test URL.
func newGCTestBackend(t *testing.T, backend string) gcTestBackend {
	t.Helper()
	conf := testConfig()
	conf.MaxDeletes = 1
	conf.CompactionGCMode = "deferred"
	var mm Meta
	var err error
	switch backend {
	case "memkv":
		mm, err = newKVMeta("memkv", t.Name(), conf)
	case "sqlite":
		mm, err = newSQLMeta("sqlite3", filepath.Join(t.TempDir(), "gc.db"), conf)
	case "redis":
		addr := os.Getenv("JUICEFS_TEST_COMPACTION_GC_REDIS")
		if addr == "" {
			t.Skip("set JUICEFS_TEST_COMPACTION_GC_REDIS to an isolated Redis address/database")
		}
		conn, probeErr := net.DialTimeout("tcp", strings.SplitN(addr, "/", 2)[0], time.Second)
		if probeErr != nil {
			t.Fatalf("isolated Redis unavailable: %v", probeErr)
		}
		_ = conn.Close()
		mm, err = newRedisMeta("redis", addr, conf)
	}
	if err != nil {
		t.Fatal(err)
	}
	var b *baseMeta
	switch m := mm.(type) {
	case *kvMeta:
		b = m.baseMeta
	case *dbMeta:
		b = m.baseMeta
	case *redisMeta:
		b = m.baseMeta
	}
	if err = mm.Reset(); err != nil {
		t.Fatal(err)
	}
	if err = mm.Init(testFormat(), false); err != nil {
		t.Fatal(err)
	}
	b.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	b.sessCtx = Background()
	b.dslices = make(chan Slice, 1)
	b.dslices <- Slice{Id: 999999, Size: 4096}
	b.startCompactionGC()
	t.Cleanup(func() {
		b.sessCtx.Cancel()
		b.stopDeleteSliceTasks()
		if err := mm.Shutdown(); err != nil {
			t.Error(err)
		}
	})
	return gcTestBackend{b, mm}
}

// createGCTestFile adds two raw slices without activating ordinary compaction scheduling.
func createGCTestFile(t *testing.T, b gcTestBackend, name string) (Ino, []uint64) {
	t.Helper()
	var inode Ino
	var attr Attr
	if st := b.Create(Background(), RootInode, name, 0600, 0, 0, &inode, &attr); st != 0 {
		t.Fatal(st)
	}
	var ids []uint64
	for i := 0; i < 2; i++ {
		var id uint64
		if st := b.NewSlice(Background(), &id); st != 0 {
			t.Fatal(st)
		}
		var n int
		var d dirStat
		if st := b.en.doWrite(Background(), inode, 0, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), &n, &d, &attr); st != 0 {
			t.Fatal(st)
		}
		ids = append(ids, id)
	}
	return inode, ids
}

// TestCompactionGCRecoveryParity verifies lost hints and deletion errors keep durable intent on all backends.
func TestCompactionGCRecoveryParity(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			b := newGCTestBackend(t, backend)
			inode, ids := createGCTestFile(t, b, "recovery")
			g := b.compactionGC.Load()
			// Saturate hints while the physical deletion queue is full. These IDs never
			// reach object deletion, so no synthetic durable metadata is required.
			for i := 0; i < compactionGCHintCapacity+100; i++ {
				g.notify(uint64(100000+i), 4096)
			}
			b.dSliceMu.Lock()
			done := make(chan struct{})
			go func() { b.compactChunk(inode, 0, false, false, 0); close(done) }()
			select {
			case <-done:
				b.dSliceMu.Unlock()
			case <-time.After(time.Second):
				b.dSliceMu.Unlock()
				b.sessCtx.Cancel()
				waitDeleteQueueDone(t, done)
				t.Fatal("committed compaction waited on saturated transport mutex")
			}
			ss, st := b.en.doRead(Background(), inode, 0)
			if st != 0 || len(ss) != 1 {
				t.Fatal("compaction did not commit under overflow")
			}
			b.stopDeleteSliceTasks()
			deleted := make(map[uint64]int)
			fail := true
			b.OnMsg(DeleteSlice, func(args ...interface{}) error {
				id := args[0].(uint64)
				deleted[id]++
				if fail {
					return fmt.Errorf("injected delete failure")
				}
				return nil
			})
			var count uint64
			if err := b.en.doCleanupSlices(Background(), &count); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 1 {
					t.Fatalf("durable marker for %d not recovered after overflow: %v", id, deleted)
				}
			}
			fail = false
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 2 {
					t.Fatalf("failed deletion lost marker for %d: %v", id, deleted)
				}
			}
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 2 {
					t.Fatalf("successful deletion retained marker for %d", id)
				}
			}
			// Shutdown also drops hints only after durable reference decrements commit.
			inode, ids = createGCTestFile(t, b, "after-shutdown")
			b.compactChunk(inode, 0, false, false, 0)
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 1 {
					t.Fatalf("shutdown lost marker for %d", id)
				}
			}
		})
	}
}

// TestCompactionGCSharedReferencesParity verifies copied live slices are never submitted for deletion.
func TestCompactionGCSharedReferencesParity(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			b := newGCTestBackend(t, backend)
			var localMu sync.Mutex
			localIDs := make(map[uint64]bool)
			const sentinelID = uint64(1 << 63)
			localBarrier := make(chan struct{})
			b.OnMsg(RetireSlice, func(args ...interface{}) error {
				localMu.Lock()
				id := args[0].(uint64)
				localIDs[id] = true
				localMu.Unlock()
				if id == sentinelID {
					close(localBarrier)
				}
				return nil
			})
			src, ids := createGCTestFile(t, b, "source")
			var dst Ino
			var attr Attr
			if st := b.Create(Background(), RootInode, "copy", 0600, 0, 0, &dst, &attr); st != 0 {
				t.Fatal(st)
			}
			var copied, length uint64
			if st := b.meta.CopyFileRange(Background(), src, 0, dst, 0, 8192, 0, &copied, &length); st != 0 || copied != 8192 {
				t.Fatalf("copy: %s %d", st, copied)
			}
			b.compactChunk(src, 0, false, false, 0)
			// The single dispatcher processes hints in FIFO order. Wait for a local
			// sentinel callback before stopping it, so an incorrectly submitted live
			// slice cannot disappear in shutdown and make this assertion falsely pass.
			b.compactionGC.Load().notify(sentinelID, 4096)
			waitDeleteQueueDone(t, localBarrier)
			b.stopDeleteSliceTasks()
			localMu.Lock()
			for _, id := range ids {
				if localIDs[id] {
					localMu.Unlock()
					t.Fatalf("locally retired shared live slice %d", id)
				}
			}
			localMu.Unlock()
			deleted := make(map[uint64]bool)
			b.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted[args[0].(uint64)] = true; return nil })
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] {
					t.Fatalf("deleted shared live slice %d", id)
				}
			}
			ss, st := b.en.doRead(Background(), dst, 0)
			if st != 0 || len(ss) != 2 || ss[0].id != ids[0] || ss[1].id != ids[1] {
				t.Fatal("copy references changed")
			}
			b.compactChunk(dst, 0, false, false, 0)
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if !deleted[id] {
					t.Fatalf("last reference marker not recovered for %d", id)
				}
			}
		})
	}
}

// TestCompactionGCDispatch preserves the slice identity handed to existing workers.
func TestCompactionGCDispatch(t *testing.T) {
	output := make(chan Slice, 1)
	ctx := Background()
	g := newCompactionGC(output, ctx.Done(), 2)
	defer g.close()
	g.notify(17, 8192)
	select {
	case s := <-output:
		if s != (Slice{Id: 17, Size: 8192}) {
			t.Fatalf("dispatched %+v", s)
		}
	case <-time.After(time.Second):
		t.Fatal("hint not dispatched")
	}
	ctx.Cancel()
	waitDeleteQueueDone(t, g.done)
}

// TestCompactionGCConcurrentClose ensures producers never race a closed hint channel.
func TestCompactionGCConcurrentClose(t *testing.T) {
	output := make(chan Slice, 1)
	output <- Slice{Id: 999}
	ctx := Background()
	g := newCompactionGC(output, ctx.Done(), 2)
	var producers sync.WaitGroup
	for i := 0; i < 8; i++ {
		producers.Add(1)
		go func() {
			defer producers.Done()
			for j := 0; j < 1000; j++ {
				g.notify(uint64(j+1), 4096)
			}
		}()
	}
	g.close()
	producers.Wait()
	ctx.Cancel()
	if s := <-output; s.Id != 999 {
		t.Fatal("shutdown changed worker queue")
	}
}

// TestCompactionGCLocalRetirePastFullRemoteQueue proves later local work proceeds while remote transport is saturated.
func TestCompactionGCLocalRetirePastFullRemoteQueue(t *testing.T) {
	output := make(chan Slice, 1)
	output <- Slice{Id: 999}
	ctx := Background()
	retired := make(chan uint64, 2)
	g := newCompactionGC(output, ctx.Done(), 2, func(id uint64, size uint32) error { retired <- id; return nil })
	defer g.close()
	defer ctx.Cancel()
	g.notify(1, 4096)
	g.notify(2, 8192)
	for _, want := range []uint64{1, 2} {
		select {
		case got := <-retired:
			if got != want {
				t.Fatalf("local retirement order=%d want=%d", got, want)
			}
		case <-time.After(250 * time.Millisecond):
			t.Fatal("remote saturation blocked subsequent local retirement")
		}
	}
	if got := <-output; got.Id != 999 {
		t.Fatal("retirement overwrote remote queue")
	}
}

// gcEventCount reads one fixed counter without requiring the production registry.
func gcEventCount(t *testing.T, g *compactionGC, event string) float64 {
	t.Helper()
	metric := &dto.Metric{}
	if err := g.events[event].Write(metric); err != nil {
		t.Fatal(err)
	}
	return metric.GetCounter().GetValue()
}

// waitGCEvent bounds eventual dispatcher counter observations without adding production waits.
func waitGCEvent(t *testing.T, g *compactionGC, event string, want float64) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for gcEventCount(t, g, event) != want {
		if time.Now().After(deadline) {
			t.Fatalf("%s=%v want=%v", event, gcEventCount(t, g, event), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestCompactionGCLocalRetireMetrics verifies overflow, local errors and remote deferral are observable.
func TestCompactionGCLocalRetireMetrics(t *testing.T) {
	output := make(chan Slice, 1)
	output <- Slice{Id: 999}
	ctx := Background()
	entered, release := make(chan struct{}), make(chan struct{})
	counters := prometheus.NewCounterVec(prometheus.CounterOpts{Name: "test_gc_events", Help: "test phase counters"}, []string{"event"})
	g := newCompactionGCWithEvents(output, ctx.Done(), 2, func(id uint64, size uint32) error {
		if id == 1 {
			close(entered)
			<-release
			return fmt.Errorf("local staging busy")
		}
		return nil
	}, counters)
	defer g.close()
	defer ctx.Cancel()
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	g.notify(1, 4096)
	waitDeleteQueueDone(t, entered)
	g.notify(2, 4096)
	g.notify(3, 4096)
	g.notify(4, 4096)
	if got := gcEventCount(t, g, "hint_overflow"); got != 1 {
		t.Fatalf("overflow=%v", got)
	}
	unblock()
	waitGCEvent(t, g, "local_error", 1)
	waitGCEvent(t, g, "local_retire_success", 2)
	waitGCEvent(t, g, "remote_deferred", 2)
	if got := gcEventCount(t, g, "remote_enqueued"); got != 0 {
		t.Fatalf("enqueued despite saturated queue: %v", got)
	}
	if got := <-output; got.Id != 999 {
		t.Fatal("changed existing physical work")
	}
}

// TestCompactionGCLocalErrorRecoveryParity preserves markers without remote enqueue when local retirement fails.
func TestCompactionGCLocalErrorRecoveryParity(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			b := newGCTestBackend(t, backend)
			queue := b.dslices
			<-queue // remote slots are available: errors, rather than saturation, must prevent handoff.
			failed := make(chan uint64, 2)
			b.OnMsg(RetireSlice, func(args ...interface{}) error {
				failed <- args[0].(uint64)
				return fmt.Errorf("local retirement busy")
			})
			inode, ids := createGCTestFile(t, b, "local-error")
			b.compactChunk(inode, 0, false, false, 0)
			for range ids {
				select {
				case <-failed:
				case <-time.After(time.Second):
					t.Fatal("local retirement not attempted")
				}
			}
			b.stopDeleteSliceTasks()
			if len(queue) != 0 {
				t.Fatal("local error allowed remote DELETE handoff")
			}
			deleted := make(map[uint64]int)
			fail := true
			b.OnMsg(DeleteSlice, func(args ...interface{}) error {
				deleted[args[0].(uint64)]++
				if fail {
					return fmt.Errorf("retry still busy")
				}
				return nil
			})
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 1 {
					t.Fatalf("lost durable marker for %d", id)
				}
			}
			fail = false
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 2 {
					t.Fatalf("lost retry marker for %d", id)
				}
			}
			if err := b.en.doCleanupSlices(Background(), nil); err != nil {
				t.Fatal(err)
			}
			for _, id := range ids {
				if deleted[id] != 2 {
					t.Fatalf("successful delete retained marker for %d", id)
				}
			}
		})
	}
}

// TestCompactionGCLegacySkipsRetireCallback keeps the opt-in local phase out of legacy compaction.
func TestCompactionGCLegacySkipsRetireCallback(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			b := newGCTestBackend(t, backend)
			b.stopDeleteSliceTasks()
			b.conf.CompactionGCMode = "legacy"
			localCalls := 0
			b.OnMsg(RetireSlice, func(args ...interface{}) error { localCalls++; return fmt.Errorf("must not run in legacy") })
			deleted := make(map[uint64]int)
			b.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted[args[0].(uint64)]++; return nil })
			inode, ids := createGCTestFile(t, b, "legacy-retire")
			b.compactChunk(inode, 0, false, false, 0)
			if localCalls != 0 {
				t.Fatal("legacy invoked optional retirement callback")
			}
			for _, id := range ids {
				if deleted[id] != 1 {
					t.Fatalf("legacy did not delete obsolete slice %d", id)
				}
			}
		})
	}
}
