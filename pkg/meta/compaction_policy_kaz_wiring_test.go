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
	"errors"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"
)

// errPolicyTestCompaction makes CompactChunk abort without changing metadata;
// "not found" keeps compactChunk from logging a warning.
var errPolicyTestCompaction = errors.New("kaz policy test: not found")

// newPolicyTestBackend returns a backend whose compaction callback only signals,
// configured by mutate before the policy and scheduler start.
func newPolicyTestBackend(t *testing.T, backend string, mutate func(*Config)) (gcTestBackend, chan struct{}) {
	t.Helper()
	b := newGCTestBackend(t, backend)
	b.conf.CompactionScheduler = "priority"
	mutate(b.conf)
	dispatched := make(chan struct{}, 8192)
	b.OnMsg(CompactChunk, func(...interface{}) error {
		dispatched <- struct{}{}
		return errPolicyTestCompaction
	})
	b.startPriorityCompactor()
	t.Cleanup(b.stopPriorityCompactor)
	return b, dispatched
}

// rawSlices appends slices to chunk 0 without the commit hooks, from offset index start,
// and drops the open-file chunk cache as a real commit would.
func rawSlices(t *testing.T, b gcTestBackend, inode Ino, start, n int) {
	t.Helper()
	var attr Attr
	for i := start; i < start+n; i++ {
		var id uint64
		if st := b.NewSlice(Background(), &id); st != 0 {
			t.Fatal(st)
		}
		var count int
		var delta dirStat
		if st := b.en.doWrite(Background(), inode, 0, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), &count, &delta, &attr); st != 0 {
			t.Fatal(st)
		}
	}
	b.of.InvalidateChunk(inode, 0)
}

// writeSlices appends slices to chunk 0 through the public Write path, so afterWrite runs.
func writeSlices(t *testing.T, b gcTestBackend, inode Ino, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		var id uint64
		if st := b.NewSlice(Background(), &id); st != 0 {
			t.Fatal(st)
		}
		if st := b.Write(Background(), inode, 0, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now()); st != 0 {
			t.Fatal(st)
		}
	}
}

// expectDispatch waits for one compaction callback.
func expectDispatch(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("compaction was not dispatched")
	}
}

// expectNoDispatch fails if a compaction callback arrives within a short window.
func expectNoDispatch(t *testing.T, ch chan struct{}) {
	t.Helper()
	select {
	case <-ch:
		t.Fatal("compaction was dispatched")
	case <-time.After(300 * time.Millisecond):
	}
}

// TestKazCompactionPolicyDefaultsUnchanged keeps the upstream read trigger when no knob is set.
func TestKazCompactionPolicyDefaultsUnchanged(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b, dispatched := newPolicyTestBackend(t, backend, func(*Config) {})
			if b.kazCompact.Load() != nil {
				t.Fatal("policy attached with default settings")
			}
			inode, _ := createGCTestFile(t, b, "defaults")
			rawSlices(t, b, inode, 2, 4) // 6 slices
			var s []Slice
			if st := b.Read(Background(), inode, 0, &s); st != 0 {
				t.Fatal(st)
			}
			expectDispatch(t, dispatched)
		})
	}
}

// TestKazCompactionPolicyReadMinWiring drops read requests below the minimum on real engines.
func TestKazCompactionPolicyReadMinWiring(t *testing.T) {
	for _, backend := range []string{"memkv", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			b, dispatched := newPolicyTestBackend(t, backend, func(c *Config) { c.KazCompactionReadMin = 100 })
			inode, _ := createGCTestFile(t, b, "readmin")
			rawSlices(t, b, inode, 2, 4) // 6 slices
			var s []Slice
			if st := b.Read(Background(), inode, 0, &s); st != 0 {
				t.Fatal(st)
			}
			expectNoDispatch(t, dispatched)
			rawSlices(t, b, inode, 6, 94) // 100 slices
			if st := b.Read(Background(), inode, 0, &s); st != 0 {
				t.Fatal(st)
			}
			expectDispatch(t, dispatched)
		})
	}
}

// TestKazCompactionPolicyRestart replaces the policy when the session restarts in the same process.
func TestKazCompactionPolicyRestart(t *testing.T) {
	b, _ := newPolicyTestBackend(t, "memkv", func(c *Config) { c.KazCompactionReadMin = 100 })
	old := b.kazCompact.Load()
	if old == nil {
		t.Fatal("policy not attached")
	}
	b.stopPriorityCompactor()
	if b.kazCompact.Load() != nil {
		t.Fatal("policy still attached after stop")
	}
	b.startPriorityCompactor()
	if p := b.kazCompact.Load(); p == nil || p == old {
		t.Fatal("restart did not attach a new policy")
	}
}

// TestKazCompactionSchedulerMetrics records start observations from the attached priority scheduler.
func TestKazCompactionSchedulerMetrics(t *testing.T) {
	b, dispatched := newPolicyTestBackend(t, "memkv", func(*Config) {})
	inode, _ := createGCTestFile(t, b, "metrics")
	rawSlices(t, b, inode, 2, 4)
	var s []Slice
	if st := b.Read(Background(), inode, 0, &s); st != 0 {
		t.Fatal(st)
	}
	expectDispatch(t, dispatched)
	var m dto.Metric
	if err := b.kazCompactMetrics.slices.Write(&m); err != nil {
		t.Fatal(err)
	}
	if m.GetHistogram().GetSampleCount() == 0 {
		t.Fatal("scheduled slices histogram is empty")
	}
}

// TestKazCompactionPolicyCommitBypassesPolicy leaves commit-triggered requests on the upstream path.
func TestKazCompactionPolicyCommitBypassesPolicy(t *testing.T) {
	b, dispatched := newPolicyTestBackend(t, "memkv", func(c *Config) { c.KazCompactionReadMin = 100 })
	inode, _ := createGCTestFile(t, b, "commit")
	writeSlices(t, b, inode, 2, 97) // 99 slices: crosses compactionWanted
	expectDispatch(t, dispatched)
	if got := policyEvents(b.kazCompactMetrics, "pass"); got != 0 {
		t.Fatalf("commit request went through the policy: pass=%v", got)
	}
}
