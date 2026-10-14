package meta

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

const kazEB = uint32(4096) // block size of the env (4 KiB)

// kazRun runs one block compaction of chunk 0 and returns the chunk's records.
func kazRun(t *testing.T, e *kazBlockEnv, inode Ino) []*slice {
	t.Helper()
	e.compactChunkBlocks(e.en.(kazChunkReplacer), inode, 0, 0)
	return e.records(t, inode)
}

// kazBlockEventCount reads one block-compaction event counter.
func kazBlockEventCount(e *kazBlockEnv, event string) float64 {
	return testutil.ToFloat64(e.kazCompactMetrics.block.WithLabelValues(event))
}

// TestKazBlockCompactionPartial rewrites only touched blocks, keeps the view and
// releases exactly the dropped slices, then rewrites a changed block again.
func TestKazBlockCompactionPartial(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			base := e.write(t, inode, 0, 16*kazEB)
			small := []uint64{
				e.write(t, inode, 2*kazEB+100, 300),
				e.write(t, inode, 3*kazEB+10, 20),
				e.write(t, inode, 9*kazEB, 50),
				e.write(t, inode, 2*kazEB+200, 10),
			}
			e.zero(t, inode, 9*kazEB+100, 10)
			before := e.store.view(e.records(t, inode), 16*kazEB)
			after := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(after, 16*kazEB))
			require.Equal(t, 3, e.store.callCount())
			require.Len(t, after, 4)
			require.Equal(t, base, after[0].id)
			for i, b := range []uint32{2, 3, 9} {
				require.Equal(t, b*kazEB, after[i+1].pos)
				require.Equal(t, kazEB, after[i+1].len)
			}
			require.Equal(t, float64(1), kazBlockEventCount(e, "partial"))
			require.Equal(t, float64(3), testutil.ToFloat64(e.kazCompactMetrics.amount.WithLabelValues("blocks_written")))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			want := map[uint64]int{}
			for _, id := range small {
				want[id] = 1
			}
			require.Equal(t, want, e.deletedIDs())

			// A new write in block 2 replaces that block's record; others stay.
			e.write(t, inode, 2*kazEB+7, 3)
			before = e.store.view(e.records(t, inode), 16*kazEB)
			again := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(again, 16*kazEB))
			require.Equal(t, 4, e.store.callCount())
			require.Equal(t, []uint64{base, after[2].id, after[3].id}, kazIDs(again[:3]))
		})
	}
}

// TestKazBlockCompactionFull rewrites every block once most of the base is hidden.
func TestKazBlockCompactionFull(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			base := e.write(t, inode, 0, 16*kazEB)
			for i := uint32(0); i < 9; i++ {
				e.write(t, inode, i*kazEB+1, 10)
			}
			before := e.store.view(e.records(t, inode), 16*kazEB)
			after := kazRun(t, e, inode)
			require.Equal(t, before, e.store.view(after, 16*kazEB))
			require.Len(t, after, 16)
			require.Equal(t, 16, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "full"))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[base])

			// Nothing small is left: the next run writes nothing.
			require.Equal(t, kazIDs(after), kazIDs(kazRun(t, e, inode)))
			require.Equal(t, 16, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "noop"))
		})
	}
}

// TestKazBlockCompactionWriteFailure leaves metadata untouched and deletes the
// slices already written when one block fails.
func TestKazBlockCompactionWriteFailure(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			for _, b := range []uint32{1, 4, 7} {
				e.write(t, inode, b*kazEB+3, 30)
			}
			var failed atomic.Uint64 // blocks are written concurrently
			e.store.fail = func(id uint64) error {
				if failed.CompareAndSwap(0, id) {
					return errors.New("injected upload failure")
				}
				return nil
			}
			before := kazIDs(e.records(t, inode))
			require.Equal(t, before, kazIDs(kazRun(t, e, inode)))
			written := e.store.callIDs()
			require.Len(t, written, 2)
			deleted := e.deletedIDs()
			for _, id := range written {
				require.Equal(t, 1, deleted[id])
			}
			require.Zero(t, deleted[failed.Load()])
			require.Equal(t, float64(1), kazBlockEventCount(e, "error"))
		})
	}
}

// TestKazBlockCompactionConcurrentCommit keeps a record committed during the
// compaction on top of the new block records.
func TestKazBlockCompactionConcurrentCommit(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			e.write(t, inode, 5*kazEB+1, 10)
			e.write(t, inode, 6*kazEB+1, 10)
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
		})
	}
}

// TestKazBlockCompactionLimit compacts only the leading records when the chunk
// exceeds the engine's limit and keeps the rest on top.
func TestKazBlockCompactionLimit(t *testing.T) {
	old := maxCompactSlices
	maxCompactSlices = 100 // as for etcd
	t.Cleanup(func() { maxCompactSlices = old })
	e := newKazBlockEnv(t, "memkv", 1024) // 1 MiB blocks: 64 per chunk, limit 36
	inode := e.file(t)
	for i := uint32(0); i < 50; i++ {
		e.write(t, inode, (i%3)<<20+i*100, 50)
	}
	before := e.store.view(e.records(t, inode), 3<<20)
	after := kazRun(t, e, inode)
	require.Equal(t, before, e.store.view(after, 3<<20))
	require.Len(t, after, 3+14)
}

// TestKazBlockCompactionTrash records dropped slices for the trash instead of releasing them.
func TestKazBlockCompactionTrash(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			e.Lock()
			e.fmt.TrashDays = 1
			e.Unlock()
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			s1 := e.write(t, inode, kazEB+1, 10)
			s2 := e.write(t, inode, 8*kazEB+1, 10)
			kazRun(t, e, inode)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Empty(t, e.deletedIDs())
			_, err := e.en.doCleanupDelayedSlices(Background(), time.Now().Add(time.Hour).Unix())
			require.NoError(t, err)
			require.Equal(t, map[uint64]int{s1: 1, s2: 1}, e.deletedIDs())
		})
	}
}

// TestKazBlockCompactLimit checks the record limit for the engines' transaction sizes.
func TestKazBlockCompactLimit(t *testing.T) {
	old := maxCompactSlices
	t.Cleanup(func() { maxCompactSlices = old })
	_, ok := kazBlockCompactLimit(0)
	require.False(t, ok)
	maxCompactSlices = 100
	limit, ok := kazBlockCompactLimit(1 << 20)
	require.True(t, ok)
	require.Equal(t, 36, limit)
	_, ok = kazBlockCompactLimit(4096)
	require.False(t, ok)
	maxCompactSlices = 1000
	limit, ok = kazBlockCompactLimit(4096)
	require.True(t, ok)
	require.Equal(t, 1000, limit)
}

// TestKazBlockCompactionUnusableBlockSize leaves the chunk and the store untouched.
func TestKazBlockCompactionUnusableBlockSize(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			e.write(t, inode, kazEB+1, 10)
			before := kazIDs(e.records(t, inode))
			e.Lock()
			e.fmt.BlockSize = 0
			e.Unlock()
			require.Equal(t, before, kazIDs(kazRun(t, e, inode)))
			require.Zero(t, e.store.callCount())
			require.Equal(t, float64(1), kazBlockEventCount(e, "error"))
		})
	}
}

// TestKazBackgroundCompactionDefaultIsUpstream merges a chunk into one slice
// when the block mode is not selected.
func TestKazBackgroundCompactionDefaultIsUpstream(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	e.conf.KazCompactionMode = ""
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+1, 10)
	e.write(t, inode, 9*kazEB+1, 10)
	e.backgroundCompactChunk(inode, 0, 0)
	require.Len(t, e.records(t, inode), 1)
	require.Equal(t, 1, e.store.callCount())
	require.Zero(t, kazBlockEventCount(e, "partial"))
}

// TestKazBackgroundCompactionBlockMode takes the block path when selected.
func TestKazBackgroundCompactionBlockMode(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+1, 10)
	e.write(t, inode, 9*kazEB+1, 10)
	e.backgroundCompactChunk(inode, 0, 0)
	require.Len(t, e.records(t, inode), 3)
	require.Equal(t, 2, e.store.callCount())
}

// TestKazBackgroundCompactionFallsBack uses upstream compaction when the block
// size cannot split a chunk.
func TestKazBackgroundCompactionFallsBack(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 0)
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+1, 10)
	e.backgroundCompactChunk(inode, 0, 0)
	require.Len(t, e.records(t, inode), 1)
	require.Equal(t, float64(1), kazBlockEventCount(e, "fallback_upstream"))
}

// TestKazBlockCompactionThroughScheduler reaches the block path from a request
// to the priority scheduler.
func TestKazBlockCompactionThroughScheduler(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	e.conf.CompactionScheduler = "priority"
	e.startPriorityCompactor()
	t.Cleanup(e.stopPriorityCompactor)
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+1, 10)
	e.write(t, inode, 9*kazEB+1, 10)
	e.requestBackgroundCompaction(inode, 0, 3, 0)
	// The chunk already holds 3 records before compaction, so wait on the
	// compaction's own writes (callCount is mutex-guarded; no require inside).
	require.Eventually(t, func() bool { return e.store.callCount() == 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		ss, st := e.en.doRead(Background(), inode, 0)
		return st == 0 && len(ss) == 3 && kazBlockEventCount(e, "partial") > 0
	}, 5*time.Second, 10*time.Millisecond)
}

// TestKazBlockCompactionLogs checks one phase=read, phase=object and phase=done
// line per compaction (as upstream, so analysis scripts count runs) and one
// block-object line per block written.
func TestKazBlockCompactionLogs(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.DebugLevel)
	e := newKazBlockEnv(t, "memkv", 4)
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+100, 300)
	e.write(t, inode, 9*kazEB, 50)
	kazRun(t, e, inode)
	count := map[string]int{}
	for _, en := range hook.AllEntries() {
		for _, f := range []string{"phase=read", "phase=object", "phase=done", "compaction block-object", "compaction block inode"} {
			if strings.Contains(en.Message, f) {
				count[f]++
			}
		}
	}
	require.Equal(t, map[string]int{"phase=read": 1, "phase=object": 1, "phase=done": 1, "compaction block-object": e.store.callCount(), "compaction block inode": 1}, count)
	require.Equal(t, 2, e.store.callCount())
}

// TestKazBlockCompactionRetiresRemoved reaches the deferred retire path.
func TestKazBlockCompactionRetiresRemoved(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			var mu sync.Mutex
			retired := map[uint64]bool{}
			e.OnMsg(RetireSlice, func(args ...interface{}) error {
				mu.Lock()
				retired[args[0].(uint64)] = true
				mu.Unlock()
				return nil
			})
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			s1 := e.write(t, inode, 2*kazEB+1, 10)
			s2 := e.write(t, inode, 9*kazEB+1, 10)
			kazRun(t, e, inode)
			require.Eventually(t, func() bool {
				mu.Lock()
				defer mu.Unlock()
				return retired[s1] && retired[s2]
			}, 5*time.Second, 10*time.Millisecond)
		})
	}
}

// TestKazBlockCompactionSmallMaxSlicesNoRepeat rewrites a chunk whole once
// under a record limit below the block count, and then writes nothing more
// until new data arrives.
func TestKazBlockCompactionSmallMaxSlicesNoRepeat(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			e.conf.KazCompactionBlockMaxSlices = 10
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			for b := uint32(0); b < 10; b++ { // base + 10 touched blocks > 10 records: whole rewrite
				e.write(t, inode, b*kazEB+1, 10)
			}
			after := kazRun(t, e, inode)
			require.Len(t, after, 16)
			require.Equal(t, 16, e.store.callCount())
			for i := 0; i < 3; i++ {
				require.Equal(t, kazIDs(after), kazIDs(kazRun(t, e, inode)))
			}
			require.Equal(t, 16, e.store.callCount())
			require.Equal(t, float64(3), kazBlockEventCount(e, "noop"))
		})
	}
}

// TestKazBlockCompactionModeLogWarnsSmallMaxSlices warns at startup when the
// record limit is below blocks per chunk + 1.
func TestKazBlockCompactionModeLogWarnsSmallMaxSlices(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.InfoLevel)
	e := newKazBlockEnv(t, "memkv", 4096) // 4 MiB blocks: 16 per chunk
	e.conf.KazCompactionBlockMaxSlices = 10
	e.logKazCompactionMode()
	require.True(t, hasLog(hook, logrus.WarnLevel, "max-slices 10", "17"))
	e.conf.KazCompactionBlockMaxSlices = 0
	hook.Reset()
	e.logKazCompactionMode()
	require.False(t, hasLog(hook, logrus.WarnLevel, "max-slices"))
}

// TestKazForcedCompaction makes juicefs compact (Meta.Compact) leave block-shaped
// chunks with no hidden data in block mode, and write nothing the second time.
func TestKazForcedCompaction(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			e.write(t, inode, 0, 16*kazEB)
			for _, b := range []uint32{2, 3, 9} {
				e.write(t, inode, b*kazEB+5, 10)
			}
			before := e.store.view(e.records(t, inode), 16*kazEB)
			require.Zero(t, e.meta.Compact(Background(), inode, 1, func() {}, func() {}))
			after := e.records(t, inode)
			require.Equal(t, before, e.store.view(after, 16*kazEB))
			require.Len(t, after, 16)
			for i, s := range after {
				require.Equal(t, uint32(i)*kazEB, s.pos)
				require.Equal(t, kazEB, s.len)
			}
			require.Equal(t, 16, e.store.callCount())
			require.Zero(t, e.meta.Compact(Background(), inode, 1, func() {}, func() {}))
			require.Equal(t, kazIDs(after), kazIDs(e.records(t, inode)))
			require.Equal(t, 16, e.store.callCount())
		})
	}
}

// TestKazForcedCompactionAll does the same through CompactAll (juicefs gc --compact).
func TestKazForcedCompactionAll(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+5, 10)
	require.Zero(t, e.meta.CompactAll(Background(), 1, utils.NewProgress(true).AddCountBar("", 0)))
	require.Len(t, e.records(t, inode), 16)
}

// TestKazForcedCompactionUpstreamMode keeps upstream juicefs compact (one slice) by default.
func TestKazForcedCompactionUpstreamMode(t *testing.T) {
	e := newKazBlockEnv(t, "memkv", 4)
	e.conf.KazCompactionMode = ""
	inode := e.file(t)
	e.write(t, inode, 0, 16*kazEB)
	e.write(t, inode, 2*kazEB+5, 10)
	require.Zero(t, e.meta.Compact(Background(), inode, 1, func() {}, func() {}))
	require.Len(t, e.records(t, inode), 1)
}
