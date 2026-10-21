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
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// kazDeltaTriggerWrite commits one 8-byte record of chunk 0 through the
// public Write path, so that afterWrite and its trigger hook run.
func kazDeltaTriggerWrite(t *testing.T, e *kazBlockEnv, inode Ino, i int) {
	t.Helper()
	var id uint64
	require.Zero(t, e.NewSlice(Background(), &id))
	off := uint32(i%3+2)*kazEB + uint32(i/3)*100
	require.Zero(t, e.Write(Background(), inode, 0, off, Slice{Id: id, Size: 8, Len: 8}, time.Now()))
}

// kazDeltaBaseOne writes a base slice, runs one compaction (a noop) and checks
// that the base count 1 is remembered.
func kazDeltaBaseOne(t *testing.T, e *kazBlockEnv) Ino {
	t.Helper()
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	kazRun(t, e, inode)
	n, ok := e.kazDeltaBases.get(inode, 0)
	require.True(t, ok)
	require.Equal(t, 1, n)
	return inode
}

// TestKazDeltaWanted is the decision table of kazCompactionWanted.
func TestKazDeltaWanted(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.kazDeltaBases.set(7, 0, 400)
	cases := []struct {
		name      string
		delta     bool
		base      bool
		prev, now int
		want      bool
		count     int
	}{
		{"off x99", false, true, 98, 99, true, 99},
		{"below trigger", true, true, 497, 498, false, 98},
		{"at trigger", true, true, 498, 499, true, 99},
		{"no base over 350", true, false, 400, 401, true, 401},
		{"no base x99", true, false, 98, 99, true, 99},
		{"no base quiet", true, false, 120, 121, false, 121},
		{"maxSlices with base", true, true, 2499, 2500, true, 2500},
		{"maxSlices no base", true, false, 2499, 2500, true, 2500},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.delta {
				e.conf.KazCompactionMode = KazCompactionModeDelta
			} else {
				e.conf.KazCompactionMode = KazCompactionModeBlock
			}
			ino := Ino(7)
			if !c.base {
				ino = 8
			}
			want, count := e.kazCompactionWanted(ino, 0, c.prev, c.now)
			require.Equal(t, c.want, want)
			require.Equal(t, c.count, count)
		})
	}
}

// TestKazDeltaWantedForgetsShrunk drops a base count above the record count
// (the chunk shrank after a delete or truncate) and uses the usual trigger.
func TestKazDeltaWantedForgetsShrunk(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.kazDeltaBases.set(7, 0, 400)
	want, count := e.kazCompactionWanted(7, 0, 119, 120)
	require.False(t, want)
	require.Equal(t, 120, count)
	_, ok := e.kazDeltaBases.get(7, 0)
	require.False(t, ok)
}

// TestKazDeltaWriteTriggerThroughWrite checks that Write requests compaction
// only after 99 records were added since the plan, not at the old x99 counts.
func TestKazDeltaWriteTriggerThroughWrite(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	inode := kazDeltaBaseOne(t, e)
	for i := 0; i < kazDeltaTrigger-1; i++ {
		kazDeltaTriggerWrite(t, e, inode, i) // record 99 is the old x99 trigger
	}
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, e.store.callCount())
	kazDeltaTriggerWrite(t, e, inode, kazDeltaTrigger-1)
	require.Eventually(t, func() bool { return e.store.callCount() > 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestKazDeltaReadTrigger checks that a read-triggered request of a chunk with
// a remembered base is dropped below 99 new records and passes at 99, with a
// read-min policy attached.
func TestKazDeltaReadTrigger(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.KazCompactionReadMin = 100
	e.startCompactionPolicy()
	t.Cleanup(e.stopCompactionPolicy)
	require.NotNil(t, e.kazCompact.Load())
	inode := kazDeltaBaseOne(t, e)
	kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
	e.requestCompactionFromRead(inode, 0, 1+kazDeltaTrigger-1, 0)
	require.Equal(t, float64(1), testutilEvent(e, "delta_below_trigger"))
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, e.store.callCount())
	e.requestCompactionFromRead(inode, 0, 1+kazDeltaTrigger, 0)
	require.Eventually(t, func() bool { return e.store.callCount() > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, float64(1), testutilEvent(e, "delta_below_trigger"))
}

// TestKazDeltaReadTriggerNoPolicy drops a below-trigger read request even when
// no policy is attached (default read-min), and counts it.
func TestKazDeltaReadTriggerNoPolicy(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	require.Nil(t, e.kazCompact.Load())
	inode := kazDeltaBaseOne(t, e)
	kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
	e.requestCompactionFromRead(inode, 0, 1+kazDeltaTrigger-1, 0)
	require.Equal(t, float64(1), testutilEvent(e, "delta_below_trigger"))
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, e.store.callCount())
}

// TestKazDeltaSchedulerCount checks that the priority scheduler hint carries
// the records added since the plan, not the total.
func TestKazDeltaSchedulerCount(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.CompactionScheduler = "priority"
	e.startPriorityCompactor()
	t.Cleanup(e.stopPriorityCompactor)
	var mu sync.Mutex
	var counts []int
	e.priorityCompactor.Load().setObserver(func(_ time.Duration, count int) {
		mu.Lock()
		counts = append(counts, count)
		mu.Unlock()
	})
	inode := kazDeltaBaseOne(t, e)
	for i := 0; i < kazDeltaTrigger; i++ {
		kazDeltaTriggerWrite(t, e, inode, i)
	}
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(counts) == 1 && counts[0] == kazDeltaTrigger
	}, 5*time.Second, 10*time.Millisecond)
}

// testutilEvent reads one kaz_compaction_policy_events_total counter.
func testutilEvent(e *kazBlockEnv, event string) float64 {
	return testutil.ToFloat64(e.kazCompactMetrics.events.WithLabelValues(event))
}

// kazDeltaRuns sums the block compaction events that count one run of a chunk.
func kazDeltaRuns(e *kazBlockEnv) float64 {
	var n float64
	for _, ev := range []string{"delta_noop", "noop", "partial", "full", "delta_tier", "delta_merge", "delta_fold", "delta_prune", "delta_block_fallback", "delta_hidden_full"} {
		n += kazBlockEventCount(e, ev)
	}
	return n
}

// TestKazDeltaNoopDoesNotRepeat checks that after a noop compaction of a chunk
// with many records, later writes below 99 new records request nothing and the
// 99th asks again: one compaction per 99 records, not one per write. (The noop
// here has no new records; the give-up with new records is covered by
// TestKazDeltaGivenUpBase.)
func TestKazDeltaNoopDoesNotRepeat(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	inode := e.file(t)
	e.write(t, inode, 0, 400*kazEB)
	for i := 0; i < kazDeltaTrigger; i++ { // each record crosses a block boundary
		e.write(t, inode, uint32(i)*3*kazEB+2*kazEB+1874, 5668)
	}
	kazRun(t, e, inode)
	require.Equal(t, float64(1), kazBlockEventCount(e, "delta_noop"))
	n, ok := e.kazDeltaBases.get(inode, 0)
	require.True(t, ok)
	require.Equal(t, kazDeltaTrigger+1, n)
	for i := 0; i < kazDeltaTrigger-1; i++ {
		kazDeltaTriggerWrite(t, e, inode, i)
	}
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, float64(1), kazDeltaRuns(e))
	kazDeltaTriggerWrite(t, e, inode, kazDeltaTrigger-1)
	require.Eventually(t, func() bool { return kazDeltaRuns(e) > 1 }, 5*time.Second, 10*time.Millisecond)
}

// TestKazDeltaTriggerKnob30 checks that --kaz-compaction-delta-trigger 30 makes
// both the write hook and the read hook fire after 30 new records, not 99.
func TestKazDeltaTriggerKnob30(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.KazCompactionDeltaTrigger = 30
	inode := kazDeltaBaseOne(t, e)
	for i := 0; i < 29; i++ {
		kazDeltaTriggerWrite(t, e, inode, i)
	}
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, e.store.callCount())
	kazDeltaTriggerWrite(t, e, inode, 29)
	require.Eventually(t, func() bool { return e.store.callCount() > 0 }, 5*time.Second, 10*time.Millisecond)

	r := newKazDeltaEnv(t, "sqlite")
	r.conf.KazCompactionDeltaTrigger = 30
	ino := kazDeltaBaseOne(t, r)
	kazDeltaSmallWrites(t, r, ino, []uint32{2, 5, 11}, 30, 0)
	r.requestCompactionFromRead(ino, 0, 1+29, 0)
	require.Equal(t, float64(1), testutilEvent(r, "delta_below_trigger"))
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, 0, r.store.callCount())
	r.requestCompactionFromRead(ino, 0, 1+30, 0)
	require.Eventually(t, func() bool { return r.store.callCount() > 0 }, 5*time.Second, 10*time.Millisecond)
}

// TestKazDeltaTriggerKnob200 checks that trigger 200 asks for nothing before 200
// new records in the write and read hooks and asks at 200 (the read request
// passes the policy).
func TestKazDeltaTriggerKnob200(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.KazCompactionDeltaTrigger = 200
	e.conf.KazCompactionReadMin = 100
	e.startCompactionPolicy()
	t.Cleanup(e.stopCompactionPolicy)
	require.NotNil(t, e.kazCompact.Load())
	e.kazDeltaBases.set(7, 0, 400)
	want, n := e.kazCompactionWanted(7, 0, 598, 599)
	require.False(t, want)
	require.Equal(t, 199, n)
	want, n = e.kazCompactionWanted(7, 0, 599, 600)
	require.True(t, want)
	require.Equal(t, 200, n)
	// The upstream x99 count is not a trigger while a base is remembered.
	want, _ = e.kazCompactionWanted(7, 0, 498, 499)
	require.False(t, want)

	// A real chunk with a remembered base of 400 (its one record compacts to a noop).
	inode := kazDeltaBaseOne(t, e)
	e.kazDeltaBases.set(inode, 0, 400)
	e.requestCompactionFromRead(inode, 0, 599, 0)
	require.Equal(t, float64(1), testutilEvent(e, "delta_below_trigger"))
	require.Zero(t, testutilEvent(e, "pass"))
	e.requestCompactionFromRead(inode, 0, 600, 0)
	require.Equal(t, float64(1), testutilEvent(e, "delta_below_trigger"))
	require.Equal(t, float64(1), testutilEvent(e, "pass"))
}

// TestKazDeltaTriggerTooLargeRejected checks that max-records 800 with trigger
// 200 (800+200+50 > 1000 records per compaction read) is rejected by validation
// and SelfCheck drops delta mode before mounting, so kazDeltaUsable never sees it.
func TestKazDeltaTriggerTooLargeRejected(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.KazCompactionDeltaMaxRecords = 800
	e.conf.KazCompactionDeltaTrigger = 200
	require.Error(t, e.conf.ValidateKazBlockCompaction())
	_, limit, need, ok := e.kazDeltaUsable()
	require.False(t, ok)
	require.Equal(t, 1050, need)
	require.Less(t, limit, need)
	e.conf.SelfCheck()
	require.False(t, e.conf.kazDeltaEnabled())
	require.False(t, e.kazDeltaActive())
}
