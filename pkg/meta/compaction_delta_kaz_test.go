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
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// kazDeltaBackends lists the engines that serve delta mode; redis needs
// JUICEFS_TEST_COMPACTION_GC_REDIS.
var kazDeltaBackends = []string{"sqlite", "redis"}

// newKazDeltaEnv returns a 4 KiB-block env in delta mode with a 16-byte gap
// (the 16 KiB default scaled to the env's block size, Ruling 2).
func newKazDeltaEnv(t *testing.T, backend string) *kazBlockEnv {
	t.Helper()
	e := newKazBlockEnv(t, backend, 4)
	e.conf.KazCompactionMode = KazCompactionModeDelta
	e.conf.KazCompactionDeltaGap = 16
	return e
}

// kazDeltaAmount reads one block-compaction amount counter.
func kazDeltaAmount(e *kazBlockEnv, kind string) float64 {
	return testutil.ToFloat64(e.kazCompactMetrics.amount.WithLabelValues(kind))
}

// kazDeltaSmallWrites appends n 8-byte records spread over blocks, 100 bytes
// apart inside each block (shifted by shift), and returns their slice ids.
func kazDeltaSmallWrites(t *testing.T, e *kazBlockEnv, inode Ino, blocks []uint32, n int, shift uint32) []uint64 {
	t.Helper()
	var ids []uint64
	for i := 0; i < n; i++ {
		b := blocks[i%len(blocks)]
		k := uint32(i / len(blocks))
		ids = append(ids, e.write(t, inode, b*kazEB+k*100+shift, 8))
	}
	return ids
}

// kazTierRecords returns the indexes of the records pointing to slice id.
func kazTierRecords(ss []*slice, id uint64) []int {
	var idx []int
	for i, s := range ss {
		if s.id == id {
			idx = append(idx, i)
		}
	}
	return idx
}

// kazDropRecords removes the records at idx through the replacer in one
// replacement and returns the records left.
func kazDropRecords(t *testing.T, r kazChunkReplacer, inode Ino, cur []*slice, idx []int) []*slice {
	t.Helper()
	drop := make(map[int]bool)
	for _, i := range idx {
		drop[i] = true
	}
	var rest, removed []*slice
	for i, s := range cur {
		if drop[i] {
			removed = append(removed, s)
		} else {
			rest = append(rest, s)
		}
	}
	require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(cur), kazMarshal(rest), removed, nil, nil, 0))
	return rest
}

// kazDeltaTierSetup writes a 16-block base and 99 small writes over blocks 2, 5
// and 11, runs one delta compaction and returns the inode, the tier slice id and
// the records after it. It checks the view and the single PUT.
func kazDeltaTierSetup(t *testing.T, e *kazBlockEnv) (Ino, uint64, []*slice) {
	t.Helper()
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
	before := e.store.view(e.records(t, inode), 16*kazEB)
	after := kazRun(t, e, inode)
	require.Equal(t, before, e.store.view(after, 16*kazEB))
	require.Equal(t, 1, e.store.callCount())
	tier := e.store.callIDs()[0]
	return inode, tier, after
}

// TestKazDeltaCompactionTier packs 99 small writes into one tier slice with one
// PUT; the slice outlives the removal of one of its records.
func TestKazDeltaCompactionTier(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			inode, tier, after := kazDeltaTierSetup(t, e)
			idx := kazTierRecords(after, tier)
			n := len(idx)
			require.Equal(t, kazDeltaTrigger, n)
			require.Len(t, after, 1+n)
			require.Equal(t, float64(n), kazDeltaAmount(e, "delta_records"))
			require.Equal(t, float64(n*8), kazDeltaAmount(e, "delta_bytes"))
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_tier"))
			require.Zero(t, kazDeltaAmount(e, "blocks_written"))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[tier])

			kazDropRecords(t, e.en.(kazChunkReplacer), inode, after, idx[:1])
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[tier])
		})
	}
}

// TestKazDeltaReplaceDropsSeveral removes several records of one tier slice in
// one replacement: no deletion while records remain, exactly one when the last
// ones go together (the engines' notification dedup path).
func TestKazDeltaReplaceDropsSeveral(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			r := e.en.(kazChunkReplacer)
			inode, tier, cur := kazDeltaTierSetup(t, e)
			cur = kazDropRecords(t, r, inode, cur, kazTierRecords(cur, tier)[:3])
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[tier])
			cur = kazDropRecords(t, r, inode, cur, kazTierRecords(cur, tier))
			require.Len(t, cur, 1)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[tier])
		})
	}
}

// TestKazDeltaPartialRelease drops only the tier record a later write hides,
// keeping the tier slice, and frees the slice once when all its records are hidden.
func TestKazDeltaPartialRelease(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			inode, tier, after := kazDeltaTierSetup(t, e)
			first := after[kazTierRecords(after, tier)[0]]
			cover := e.write(t, inode, first.pos, first.len) // hides exactly one tier record
			before := e.store.view(e.records(t, inode), 16*kazEB)
			again := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(again, 16*kazEB))
			require.Len(t, kazTierRecords(again, tier), kazDeltaTrigger-1)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[tier])
			require.Equal(t, 1, e.deletedIDs()[cover])
			tier2 := e.store.callIDs()[1] // the covering write moved into a second tier
			require.Len(t, kazTierRecords(again, tier2), 1)

			// A large write over blocks 2..11 hides every tier record: a prune with
			// no upload frees both tier slices once.
			e.write(t, inode, 2*kazEB, 10*kazEB)
			before = e.store.view(e.records(t, inode), 16*kazEB)
			last := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(last, 16*kazEB))
			require.Len(t, last, 2)
			require.Equal(t, 2, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_prune"))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[tier])
			require.Equal(t, 1, e.deletedIDs()[tier2])
		})
	}
}

// TestKazDeltaPruneTrash commits a prune (no new slice) under trash: the
// dropped record is kept for the trash under a fresh delayed key, then freed.
func TestKazDeltaPruneTrash(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			e.kazTestUpdateFormat(func(f *Format) { f.TrashDays = 1 })
			inode := e.file(t)
			old := e.write(t, inode, 3*kazEB, kazEB)
			e.write(t, inode, 3*kazEB, kazEB)
			after := kazRun(t, e, inode)
			require.Len(t, after, 1)
			require.Zero(t, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_prune"))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Empty(t, e.deletedIDs())
			_, err := e.en.doCleanupDelayedSlices(Background(), time.Now().Add(time.Hour).Unix())
			require.NoError(t, err)
			require.Equal(t, map[uint64]int{old: 1}, e.deletedIDs())
		})
	}
}

// TestKazDeltaCompactionConcurrentCommit: a replacement that changes the chunk
// head during the PUT wastes the compaction (tier slice freed, base count kept);
// a write appended during the PUT stays on top of the committed tier.
func TestKazDeltaCompactionConcurrentCommit(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend+"/head", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			r := e.en.(kazChunkReplacer)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger-1, 0)
			kazRun(t, e, inode) // noop: remembers 1
			n, ok := e.kazDeltaBases.get(inode, 0)
			require.True(t, ok)
			require.Equal(t, 1, n)
			e.write(t, inode, 7*kazEB+1, 8)
			var swapErr error
			e.store.during = func() {
				ss, st := e.en.doRead(Background(), inode, 0)
				if st != 0 {
					swapErr = st
					return
				}
				var x uint64
				if st := e.NewSlice(Background(), &x); st != 0 {
					swapErr = st
					return
				}
				if st := r.doKazReplaceChunk(inode, 0, kazMarshal(ss[:1]), marshalSlice(0, x, 16*kazEB, 0, 16*kazEB), ss[:1], []kazCreated{{Slice{Id: x, Size: 16 * kazEB, Len: 16 * kazEB}, 1}}, nil, 0); st != 0 {
					swapErr = st
				}
			}
			before := len(e.records(t, inode))
			after := kazRun(t, e, inode)
			require.NoError(t, swapErr)
			require.Len(t, after, before)
			require.Equal(t, float64(1), kazBlockEventCount(e, "wasted"))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[e.store.callIDs()[0]])
			n, ok = e.kazDeltaBases.get(inode, 0)
			require.True(t, ok)
			require.Equal(t, 1, n)
		})
		t.Run(backend+"/append", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
			origin := e.records(t, inode)
			var late uint64
			var lateErr error
			e.store.during = func() {
				if st := e.NewSlice(Background(), &late); st != 0 {
					lateErr = st
					return
				}
				var n int
				var d dirStat
				var attr Attr
				if st := e.en.doWrite(Background(), inode, 0, 5*kazEB, Slice{Id: late, Size: 100, Len: 100}, time.Now(), &n, &d, &attr); st != 0 {
					lateErr = st
				}
			}
			after := kazRun(t, e, inode)
			require.NoError(t, lateErr)
			require.Equal(t, late, after[len(after)-1].id)
			want := e.store.view(append(origin, &slice{pos: 5 * kazEB, id: late, size: 100, len: 100}), 16*kazEB)
			require.Equal(t, want, e.store.view(after, 16*kazEB))
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_tier"))
			n, ok := e.kazDeltaBases.get(inode, 0)
			require.True(t, ok)
			require.Equal(t, len(after)-1, n) // the late record stays counted as new
		})
	}
}

// TestKazDeltaBaseUpdated remembers len(ss) minus the new records on a normal noop and
// the records left (kept + outs) after a commit.
func TestKazDeltaBaseUpdated(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			_, ok := e.kazDeltaBases.get(inode, 0)
			require.False(t, ok)
			kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger-1, 0)
			kazRun(t, e, inode)
			n, ok := e.kazDeltaBases.get(inode, 0)
			require.True(t, ok)
			require.Equal(t, 1, n)
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_noop"))
			e.write(t, inode, 7*kazEB+1, 8)
			after := kazRun(t, e, inode)
			n, ok = e.kazDeltaBases.get(inode, 0)
			require.True(t, ok)
			require.Equal(t, len(after), n)
			require.Equal(t, 1+kazDeltaTrigger, n)
		})
	}
}

// TestKazDeltaBaseTable checks get/set/forget and dropping the oldest entry at the cap.
func TestKazDeltaBaseTable(t *testing.T) {
	var tb kazDeltaBaseTable
	_, ok := tb.get(1, 0)
	require.False(t, ok)
	tb.set(1, 0, 5)
	tb.set(1, 1, 6)
	n, ok := tb.get(1, 0)
	require.True(t, ok)
	require.Equal(t, 5, n)
	n, _ = tb.get(1, 1)
	require.Equal(t, 6, n)
	tb.forget(1, 0)
	_, ok = tb.get(1, 0)
	require.False(t, ok)
	for i := 0; i < kazDeltaBaseCap; i++ {
		tb.set(Ino(100+i), 0, i)
	}
	_, ok = tb.get(1, 1) // the oldest entry made room
	require.False(t, ok)
	n, ok = tb.get(100, 0)
	require.True(t, ok)
	require.Zero(t, n)
	tb.set(100, 0, 7) // a refresh makes 100 the newest
	tb.set(1, 2, 1)
	_, ok = tb.get(101, 0)
	require.False(t, ok)
	n, ok = tb.get(100, 0)
	require.True(t, ok)
	require.Equal(t, 7, n)
}

// TestKazDeltaFallbackLimit turns delta off when an engine reads too few records
// per compaction (maxCompactSlices 100 as for etcd).
func TestKazDeltaFallbackLimit(t *testing.T) {
	e := newKazBlockEnv(t, "sqlite", 1024) // 1 MiB blocks keep block mode usable (limit 36)
	e.conf.KazCompactionMode = KazCompactionModeDelta
	require.True(t, e.kazDeltaActive())
	old := maxCompactSlices
	maxCompactSlices = 100
	t.Cleanup(func() { maxCompactSlices = old })
	require.False(t, e.kazDeltaActive())
	require.NotNil(t, e.kazBlockReplacer())
}

// TestKazDeltaFallbackTKV runs block compaction on a TKV engine in delta mode
// and counts the fallback.
func TestKazDeltaFallbackTKV(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	e.conf.KazCompactionMode = KazCompactionModeDelta
	require.False(t, e.kazDeltaActive())
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+1, 10)
	e.write(t, inode, 9*kazEB+1, 10)
	e.backgroundCompactChunk(inode, 0, 0)
	after := e.records(t, inode)
	require.Len(t, after, 3)
	for _, s := range after[1:] {
		require.Equal(t, kazEB, s.len)
	}
	require.Equal(t, 2, e.store.callCount())
	require.Equal(t, float64(1), kazBlockEventCount(e, "fallback_block"))
	require.Equal(t, float64(1), kazBlockEventCount(e, "partial"))
}

// TestKazDeltaMinFileSize plans block compaction for files below
// delta-min-file-size and delta compaction once the file is long enough.
func TestKazDeltaMinFileSize(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			e.conf.KazCompactionDeltaMinFileSize = 1 << 20
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB) // 64 KiB file
			kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
			before := e.store.view(e.records(t, inode), 16*kazEB)
			after := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(after, 16*kazEB))
			require.Equal(t, 3, e.store.callCount()) // blocks 2, 5 and 11
			require.Equal(t, float64(1), kazBlockEventCount(e, "partial"))
			_, ok := e.kazDeltaBases.get(inode, 0)
			require.False(t, ok)

			var attr Attr
			require.Zero(t, e.Truncate(Background(), inode, 0, 2<<20, &attr, true))
			kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 3)
			before = e.store.view(e.records(t, inode), 16*kazEB)
			after = kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(after, 16*kazEB))
			require.Equal(t, 4, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_tier"))
		})
	}
}

// TestKazDeltaForcedIsBlock: juicefs compact leaves a delta chunk block-shaped
// with no hidden data and frees the tier slice once.
func TestKazDeltaForcedIsBlock(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			inode, tier, after := kazDeltaTierSetup(t, e)
			before := e.store.view(after, 16*kazEB)
			require.Zero(t, e.meta.Compact(Background(), inode, 1, func() {}, func() {}))
			forced := e.records(t, inode)
			require.Equal(t, before, e.store.view(forced, 16*kazEB))
			require.Len(t, forced, 16)
			for i, s := range forced {
				require.Equal(t, uint32(i)*kazEB, s.pos)
				require.Equal(t, kazEB, s.len)
				require.Equal(t, s.size, s.len)
			}
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[tier])
		})
	}
}

// TestKazDeltaLogs checks the delta DEBUG line (mode, tiers, delta records,
// tier bytes, rewritten blocks), one block-object line per object and the
// slow-compaction line marked mode=delta.
func TestKazDeltaLogs(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.DebugLevel)
	e := newKazDeltaEnv(t, "sqlite")
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	kazDeltaSmallWrites(t, e, inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
	e.store.during = func() { time.Sleep(1100 * time.Millisecond) }
	kazRun(t, e, inode)
	require.True(t, hasLog(hook, logrus.DebugLevel, "compaction block inode=", "mode=delta-tier", "tiers=1", "delta_records=99", "delta_bytes=792", "blocks=0"))
	require.True(t, hasLog(hook, logrus.WarnLevel, "slow compaction", "mode=delta"))
	count := map[string]int{}
	for _, en := range hook.AllEntries() {
		for _, f := range []string{"phase=read", "phase=object", "phase=done", "compaction block-object", "compaction block inode"} {
			if strings.Contains(en.Message, f) {
				count[f]++
			}
		}
	}
	require.Equal(t, map[string]int{"phase=read": 1, "phase=object": 1, "phase=done": 1, "compaction block-object": 1, "compaction block inode": 1}, count)
}

// TestKazDeltaModeLog states delta at startup when usable, and warns when it
// falls back to block compaction.
func TestKazDeltaModeLog(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.InfoLevel)
	e := newKazDeltaEnv(t, "sqlite")
	e.logKazCompactionMode()
	require.True(t, hasLog(hook, logrus.InfoLevel, "kaz compaction mode: delta (tiers 8, max-records 500, gap 16, min-file-size 0, trigger 99, block-ratio 0.5)"))
	m := newKazBlockEnv(t, "memkv", 4)
	m.conf.KazCompactionMode = KazCompactionModeDelta
	hook.Reset()
	m.logKazCompactionMode()
	require.True(t, hasLog(hook, logrus.WarnLevel, "kaz compaction mode: delta is not usable here", "using block compaction"))
	require.True(t, hasLog(hook, logrus.InfoLevel, "kaz compaction mode: block"))
}

// TestKazDeltaReasonAmounts commits one delta compaction that rewrites one block
// for the block ratio (block 3) and one with no base below (block 18, past the
// end of the base) and checks the per-reason amounts and the DEBUG line.
func TestKazDeltaReasonAmounts(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		t.Run(backend, func(t *testing.T) {
			hook := capturePolicyLogs(t, logrus.DebugLevel)
			e := newKazDeltaEnv(t, backend)
			e.conf.KazCompactionDeltaTrigger = 10
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			for _, off := range []uint32{10, 1000, 2000} { // 2100 bytes of block 3
				e.write(t, inode, 3*kazEB+off, 700)
			}
			e.write(t, inode, 18*kazEB+10, 8)
			kazDeltaSmallWrites(t, e, inode, []uint32{5, 9}, 6, 0) // 48 delta bytes
			before := e.store.view(e.records(t, inode), 19*kazEB)
			after := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(after, 19*kazEB))
			require.Equal(t, float64(1), kazBlockEventCount(e, "delta_tier"))
			require.Equal(t, float64(2), kazDeltaAmount(e, "blocks_written"))
			require.Equal(t, float64(1), kazDeltaAmount(e, "delta_blocks_ratio"))
			require.Equal(t, float64(1), kazDeltaAmount(e, "delta_blocks_nobase"))
			require.Zero(t, kazDeltaAmount(e, "delta_fold_blocks"))
			require.Zero(t, kazDeltaAmount(e, "delta_blocks_whole"))
			require.Zero(t, kazDeltaAmount(e, "delta_merge_bytes"))
			require.Equal(t, float64(48), kazDeltaAmount(e, "delta_bytes"))
			require.True(t, hasLog(hook, logrus.DebugLevel, "compaction block inode=", "mode=delta-tier", "blocks=2",
				"fold_blocks=0", "ratio_blocks=1", "nobase_blocks=1", "whole_blocks=0", "fallback_blocks=0", "merge_bytes=0"))
			var reasons float64
			for _, k := range []string{"delta_blocks_ratio", "delta_blocks_nobase", "delta_fold_blocks", "delta_blocks_whole", "delta_blocks_fallback"} {
				reasons += kazDeltaAmount(e, k)
			}
			require.Equal(t, kazDeltaAmount(e, "blocks_written"), reasons)
		})
	}
}

// TestKazDeltaWholeAmount counts the blocks of a whole rewrite in delta mode
// (delta_hidden_full) as delta_blocks_whole.
func TestKazDeltaWholeAmount(t *testing.T) {
	e := newKazDeltaEnv(t, "sqlite")
	e.conf.KazCompactionDeltaTrigger = 10
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB) // three stacked bases: 2/3 hidden
	e.write(t, inode, 1, 16*kazEB-1)
	e.write(t, inode, 2, 16*kazEB-2)
	kazDeltaSmallWrites(t, e, inode, []uint32{5, 9}, 10, 0)
	kazRun(t, e, inode)
	require.Equal(t, float64(1), kazBlockEventCount(e, "delta_hidden_full"))
	require.Equal(t, float64(16), kazDeltaAmount(e, "delta_blocks_whole"))
	require.Equal(t, float64(16), kazDeltaAmount(e, "blocks_written"))
	require.Zero(t, kazDeltaAmount(e, "delta_blocks_ratio"))
}
