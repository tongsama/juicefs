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

package chunk

import (
	"math/rand"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// kazTestBlock is the block size (bytes) used by the kaz 2Q index tests.
const kazTestBlock = 100

// kazTestStart is the fixed clock origin used by the kaz 2Q index tests.
const kazTestStart uint32 = 1_000_000

// newKazTestIndex builds a kaz2QIndex with a controllable clock and a capacity
// of the given number of kazTestBlock blocks. It returns the index and a pointer
// to the fake clock.
func newKazTestIndex(t *testing.T, blocks int64, ghostRatio float64) (*kaz2QIndex, *uint32) {
	t.Helper()
	conf := &Config{
		CacheEviction:          EvictionKaz2Q,
		KazCacheProbationRatio: 0.4,
		KazCachePromoteDelay:   10 * time.Minute,
		KazCacheGhostRatio:     ghostRatio,
	}
	clock := kazTestStart
	p := newKaz2QIndex(conf)
	p.now = func() uint32 { return clock }
	p.kazSetCapacity(blocks * kazTestBlock)
	return p, &clock
}

// kazKey returns a test cache key with the given id.
func kazKey(id uint64) cacheKey { return cacheKey{id: id, indx: 0, size: kazTestBlock} }

// kazPromote adds key and makes it protected by reading it after the promote delay.
func kazPromote(t *testing.T, p *kaz2QIndex, clock *uint32, id uint64) {
	t.Helper()
	p.add(kazKey(id), cacheItem{size: kazTestBlock, atime: *clock})
	*clock += 600
	require.NotNil(t, p.get(kazKey(id)))
	seg, ok := p.kazSegmentOf(kazKey(id))
	require.True(t, ok)
	require.Equal(t, kazProtected, seg)
}

// kazNextEvicted pulls exactly one item out of a fresh evictionIter.
func kazNextEvicted(p *kaz2QIndex) (cacheKey, bool) {
	var got cacheKey
	var found bool
	for k := range p.evictionIter() {
		got, found = k, true
		break
	}
	return got, found
}

// TestKaz2QPromoteAfterDelay checks that reads inside the correlation period do
// not promote, and a read after it does.
func TestKaz2QPromoteAfterDelay(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	k := kazKey(1)
	p.add(k, cacheItem{size: kazTestBlock, atime: *clock})
	require.NotNil(t, p.get(k))
	seg, ok := p.kazSegmentOf(k)
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	*clock += 300
	require.NotNil(t, p.get(k))
	seg, _ = p.kazSegmentOf(k)
	require.Equal(t, kazProbation, seg)
	*clock += 600
	it := p.get(k)
	require.NotNil(t, it)
	require.Equal(t, *clock, it.atime)
	seg, _ = p.kazSegmentOf(k)
	require.Equal(t, kazProtected, seg)
	require.Nil(t, p.get(kazKey(99)))
}

// TestKaz2QExistingAddDoesNotPromote checks that re-adding an existing key only
// updates its size and keeps the segment.
func TestKaz2QExistingAddDoesNotPromote(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	k := kazKey(1)
	p.add(k, cacheItem{size: kazTestBlock, atime: *clock})
	*clock += 3600
	p.add(k, cacheItem{size: kazTestBlock + 1, atime: *clock})
	seg, _ := p.kazSegmentOf(k)
	require.Equal(t, kazProbation, seg)
	it := p.kazPeek(k)
	require.Equal(t, int32(kazTestBlock+1), it.size)
	require.Equal(t, kazTestStart, it.atime)

	kazPromote(t, p, clock, 2)
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: *clock})
	seg, _ = p.kazSegmentOf(kazKey(2))
	require.Equal(t, kazProtected, seg)
}

// TestKaz2QEvictProbationFirst checks that probation items are evicted first,
// oldest order first.
func TestKaz2QEvictProbationFirst(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	kazPromote(t, p, clock, 100)
	kazPromote(t, p, clock, 101)
	for i := uint64(1); i <= 8; i++ {
		*clock++
		p.add(kazKey(i), cacheItem{size: kazTestBlock, atime: *clock})
	}
	require.Equal(t, 10, p.len())
	for i := uint64(1); i <= 8; i++ {
		k, ok := kazNextEvicted(p)
		require.True(t, ok)
		require.Equal(t, kazKey(i), k)
		_, ok = p.kazSegmentOf(k)
		require.False(t, ok)
	}
	k, ok := kazNextEvicted(p)
	require.True(t, ok)
	require.Equal(t, kazKey(100), k)
}

// TestKaz2QDemoteOverflow checks that an over-quota protected segment demotes
// its oldest item to the newest end of probation instead of evicting it.
func TestKaz2QDemoteOverflow(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	m := newKaz2QMetrics()
	p.kazSetMetrics(m)
	for i := uint64(100); i < 107; i++ { // 7 protected > 10*0.6
		kazPromote(t, p, clock, i)
	}
	for i := uint64(1); i <= 3; i++ {
		*clock++
		p.add(kazKey(i), cacheItem{size: kazTestBlock, atime: *clock})
	}
	k, ok := kazNextEvicted(p)
	require.True(t, ok)
	require.Equal(t, kazKey(1), k)
	seg, ok := p.kazSegmentOf(kazKey(100))
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	require.Equal(t, 1.0, testutil.ToFloat64(m.events.WithLabelValues("demote")))
	require.Equal(t, 1.0, testutil.ToFloat64(m.events.WithLabelValues("evict_probation")))
	require.Equal(t, 7.0, testutil.ToFloat64(m.events.WithLabelValues("promote")))
	require.Equal(t, 6.0, testutil.ToFloat64(m.blocks.WithLabelValues("protected")))
	require.Equal(t, 3.0, testutil.ToFloat64(m.blocks.WithLabelValues("probation")))
	require.Equal(t, 300.0, testutil.ToFloat64(m.bytes.WithLabelValues("probation")))
	// the remaining probation items go before the demoted one
	k, _ = kazNextEvicted(p)
	require.Equal(t, kazKey(2), k)
	k, _ = kazNextEvicted(p)
	require.Equal(t, kazKey(3), k)
	k, _ = kazNextEvicted(p)
	require.Equal(t, kazKey(100), k)
}

// TestKaz2QGhostPromotesOnReadd checks that an evicted key re-added later goes
// straight to protected, and that the ghost list forgets oldest keys at its cap.
func TestKaz2QGhostPromotesOnReadd(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 1) // ghost cap = 10 blocks
	m := newKaz2QMetrics()
	p.kazSetMetrics(m)
	for i := uint64(1); i <= 12; i++ {
		*clock++
		p.add(kazKey(i), cacheItem{size: kazTestBlock, atime: *clock})
	}
	for range p.evictionIter() {
	}
	require.Equal(t, 0, p.len())
	// keys 1 and 2 were forgotten, 3..12 are remembered
	p.add(kazKey(1), cacheItem{size: kazTestBlock, atime: *clock})
	seg, _ := p.kazSegmentOf(kazKey(1))
	require.Equal(t, kazProbation, seg)
	p.add(kazKey(3), cacheItem{size: kazTestBlock, atime: *clock})
	seg, _ = p.kazSegmentOf(kazKey(3))
	require.Equal(t, kazProtected, seg)
	require.Equal(t, 1.0, testutil.ToFloat64(m.events.WithLabelValues("ghost_hit")))
	// the ghost entry is consumed
	p.remove(kazKey(3), false)
	p.add(kazKey(3), cacheItem{size: kazTestBlock, atime: *clock})
	seg, _ = p.kazSegmentOf(kazKey(3))
	require.Equal(t, kazProbation, seg)
	// remove does not feed the ghost list
	p.remove(kazKey(1), false)
	p.add(kazKey(1), cacheItem{size: kazTestBlock, atime: *clock})
	seg, _ = p.kazSegmentOf(kazKey(1))
	require.Equal(t, kazProbation, seg)

	// ghostRatio 0 disables the list
	q, qclock := newKazTestIndex(t, 10, 0)
	q.add(kazKey(1), cacheItem{size: kazTestBlock, atime: *qclock})
	for range q.evictionIter() {
	}
	q.add(kazKey(1), cacheItem{size: kazTestBlock, atime: *qclock})
	seg, _ = q.kazSegmentOf(kazKey(1))
	require.Equal(t, kazProbation, seg)
}

// TestKaz2QStagingNeverEvicted checks that staging items are never yielded,
// survive remove(staging=false), and join probation after upload.
func TestKaz2QStagingNeverEvicted(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	m := newKaz2QMetrics()
	p.kazSetMetrics(m)
	s := kazKey(50)
	p.add(s, cacheItem{size: -kazTestBlock, atime: *clock})
	p.add(kazKey(1), cacheItem{size: kazTestBlock, atime: *clock})
	require.Equal(t, 2, p.len())
	require.Equal(t, 1.0, testutil.ToFloat64(m.blocks.WithLabelValues("probation")))
	*clock += 3600
	require.NotNil(t, p.get(s)) // staging hits never promote
	seg, ok := p.kazSegmentOf(s)
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	require.Equal(t, 1.0, testutil.ToFloat64(m.hits.WithLabelValues("write")))
	require.Nil(t, p.remove(s, false))
	var evicted []cacheKey
	for k := range p.evictionIter() {
		evicted = append(evicted, k)
	}
	require.Equal(t, []cacheKey{kazKey(1)}, evicted)
	require.Equal(t, 1, p.len())

	// upload completes: becomes a probation block with inserted = order = now
	p.add(s, cacheItem{size: kazTestBlock, atime: *clock})
	require.Equal(t, *clock, p.keys[s].inserted)
	require.Equal(t, 1.0, testutil.ToFloat64(m.blocks.WithLabelValues("probation")))
	k, ok := kazNextEvicted(p)
	require.True(t, ok)
	require.Equal(t, s, k)

	// staging removal must be explicit
	p.add(s, cacheItem{size: -kazTestBlock, atime: *clock})
	require.NotNil(t, p.remove(s, true))
	require.Equal(t, 0, p.len())
}

// TestKaz2QEvictionTerminates checks that eviction drains everything when only
// protected items remain, and with a tiny capacity.
func TestKaz2QEvictionTerminates(t *testing.T) {
	for _, capBlocks := range []int64{10, 0} {
		p, clock := newKazTestIndex(t, capBlocks, 4)
		for i := uint64(1); i <= 5; i++ {
			kazPromote(t, p, clock, i)
		}
		n := 0
		for range p.evictionIter() {
			n++
		}
		require.Equal(t, 5, n)
		require.Equal(t, 0, p.len())
	}
	p, clock := newKazTestIndex(t, 0, 4)
	p.kazSetCapacity(1)
	for i := uint64(1); i <= 3; i++ {
		kazPromote(t, p, clock, i)
		p.add(kazKey(10+i), cacheItem{size: kazTestBlock, atime: *clock})
	}
	n := 0
	for range p.evictionIter() {
		n++
	}
	require.Equal(t, 6, n)
	require.Equal(t, 0, p.len())
}

// TestKaz2QResetKeepsSegments checks that reset keeps per-key state for keys
// re-added by the next scan, and that the snapshot answers peekAtime.
func TestKaz2QResetKeepsSegments(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	kazPromote(t, p, clock, 1)
	*clock += 5
	p.get(kazKey(1))
	want := *p.keys[kazKey(1)]
	*clock += 100
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: kazTestStart - 3600}) // legacy

	snap := p.reset()
	require.Equal(t, 0, p.len())
	require.Equal(t, want.atime, snap.peekAtime(kazKey(1)))
	require.Equal(t, uint32(0), snap.peekAtime(kazKey(9)))

	*clock += 100
	p.add(kazKey(1), cacheItem{size: kazTestBlock, atime: kazTestStart})
	got := p.keys[kazKey(1)]
	require.Equal(t, kazProtected, got.seg)
	require.Equal(t, want.inserted, got.inserted)
	require.Equal(t, want.origin, got.origin)
	require.Equal(t, want.hits, got.hits)
	require.Equal(t, uint8(2), got.hits)
	require.Equal(t, want.atime, got.atime)

	// a second reset drops the restore entries that were not re-added
	p.reset()
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: *clock})
	require.Equal(t, kazOriginFill, p.keys[kazKey(2)].origin)
	require.Equal(t, 1, p.len())

	// a restore loaded from the state file is merged into the next reset
	// (current items win); key 1 is still in restore from the last reset
	p.restore[kazKey(7)] = kazSaved{seg: kazProtected, atime: 7, inserted: 7, order: 7}
	p.restoreFromFile = true
	p.reset()
	require.False(t, p.restoreFromFile)
	require.Contains(t, p.restore, kazKey(7))
	require.Contains(t, p.restore, kazKey(1))
	require.Contains(t, p.restore, kazKey(2))
	require.Equal(t, kazOriginFill, p.restore[kazKey(2)].origin)
	p.reset()
	require.NotContains(t, p.restore, kazKey(7))

	// reset drops expectations older than 10 minutes
	p.kazExpect(kazKey(30), kazProtected, *clock)
	*clock += 601
	p.kazExpect(kazKey(31), kazProtected, *clock)
	p.reset()
	require.NotContains(t, p.expect, kazKey(30))
	require.Contains(t, p.expect, kazKey(31))
}

// TestKaz2QPlacement checks kazPlacement over protected, probation, staging and
// missing keys.
func TestKaz2QPlacement(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	kazPromote(t, p, clock, 1) // protected, atime = start+600
	*clock += 10
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: *clock}) // probation
	*clock += 10
	p.add(kazKey(3), cacheItem{size: -kazTestBlock, atime: *clock}) // staging

	seg, at, ok := p.kazPlacement([]cacheKey{kazKey(1), kazKey(2), kazKey(9)})
	require.True(t, ok)
	require.Equal(t, kazProtected, seg)
	require.Equal(t, kazTestStart+610, at)

	seg, at, ok = p.kazPlacement([]cacheKey{kazKey(2), kazKey(3)})
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	require.Equal(t, kazTestStart+620, at)

	seg, at, ok = p.kazPlacement([]cacheKey{kazKey(3)})
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	require.Equal(t, kazTestStart+620, at)

	_, _, ok = p.kazPlacement([]cacheKey{kazKey(8), kazKey(9)})
	require.False(t, ok)
	_, _, ok = p.kazPlacement(nil)
	require.False(t, ok)

	// expect places a compaction output with the inherited segment and atime
	p.kazExpect(kazKey(20), kazProtected, kazTestStart+5)
	p.add(kazKey(20), cacheItem{size: kazTestBlock, atime: *clock})
	it := p.keys[kazKey(20)]
	require.Equal(t, kazProtected, it.seg)
	require.Equal(t, kazTestStart+5, it.atime)
	require.Equal(t, kazOriginCompaction, it.origin)
}

// kazVerify checks that the heaps, positions, and segment accounting of the
// index agree with its key map.
func kazVerify(t *testing.T, p *kaz2QIndex) {
	t.Helper()
	var used, counts [3]int64
	for k, it := range p.keys {
		require.Equal(t, k, it.key)
		if it.size <= 0 {
			require.Equal(t, notInLru, it.pos)
			continue
		}
		h := p.heapOf(it.seg)
		require.Less(t, it.pos, h.Len())
		require.Same(t, it, h.items[it.pos])
		used[it.seg] += int64(it.size)
		counts[it.seg]++
	}
	require.Equal(t, used, p.used)
	require.Equal(t, counts, p.counts)
	require.Equal(t, int(counts[kazProbation]), p.prob.Len())
	require.Equal(t, int(counts[kazProtected]), p.prot.Len())
	for _, h := range []*kazHeap{&p.prob, &p.prot} {
		for i := 1; i < h.Len(); i++ {
			require.False(t, h.Less(i, (i-1)/2), "heap property")
		}
	}
	var gb int64
	for el := p.ghost.Front(); el != nil; el = el.Next() {
		gb += el.Value.(kazGhost).size
	}
	require.Equal(t, gb, p.ghostBytes)
	require.Equal(t, p.ghost.Len(), len(p.ghostKeys))
}

// TestKaz2QRandomOpsConsistent runs random operations and checks the index
// invariants after each one.
func TestKaz2QRandomOpsConsistent(t *testing.T) {
	p, clock := newKazTestIndex(t, 20, 2)
	p.kazSetMetrics(newKaz2QMetrics())
	r := rand.New(rand.NewSource(1))
	for i := 0; i < 5000; i++ {
		k := kazKey(uint64(r.Intn(60)))
		*clock += uint32(r.Intn(120))
		switch r.Intn(7) {
		case 0, 1:
			size := int32(1 + r.Intn(kazTestBlock))
			if r.Intn(4) == 0 {
				size = -size
			}
			p.add(k, cacheItem{size: size, atime: *clock})
		case 2, 3:
			p.get(k)
		case 4:
			p.remove(k, r.Intn(2) == 0)
		case 5:
			n := r.Intn(4)
			for range p.evictionIter() {
				if n--; n < 0 {
					break
				}
			}
		case 6:
			if r.Intn(20) == 0 {
				p.reset()
			}
		}
		kazVerify(t, p)
	}
}

// TestKaz2QExpectPrunedWithoutReset checks that expectations older than
// kazExpectTTL are dropped while new ones are registered, even when reset()
// never runs (cache-scan-interval 0 or negative).
func TestKaz2QExpectPrunedWithoutReset(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	const n = 2 * kazExpectPruneMin
	for i := uint64(0); i < n; i++ {
		p.kazExpect(kazKey(i), kazProtected, *clock)
	}
	*clock += kazExpectTTL + 1
	for i := uint64(n); i < 2*n; i++ {
		p.kazExpect(kazKey(i), kazProtected, *clock)
	}
	for i := uint64(0); i < n; i++ {
		require.NotContains(t, p.expect, kazKey(i), "expired expectation %d kept", i)
	}
	for i := uint64(n); i < 2*n; i++ {
		require.Contains(t, p.expect, kazKey(i))
	}
	require.LessOrEqual(t, len(p.expect), int(n))
}

// TestKaz2QRestoreKeepsNewerDiskAtime checks that a key restored after a
// rescan (or from the state file) keeps the newer of the saved atime and the
// atime found on disk.
func TestKaz2QRestoreKeepsNewerDiskAtime(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	kazPromote(t, p, clock, 1)
	saved := p.keys[kazKey(1)].atime
	p.reset()
	disk := saved + 300
	p.add(kazKey(1), cacheItem{size: kazTestBlock, atime: disk})
	it := p.keys[kazKey(1)]
	require.Equal(t, kazProtected, it.seg)
	require.Equal(t, disk, it.atime)
	require.Equal(t, disk, p.peekAtime(kazKey(1)))
	kazVerify(t, p)

	// an older disk atime does not lower the saved one
	p.restore[kazKey(2)] = kazSaved{seg: kazProtected, atime: saved, inserted: saved, order: saved}
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: saved - 100})
	require.Equal(t, saved, p.keys[kazKey(2)].atime)
	kazVerify(t, p)
}

// TestKaz2QPlacementDuringRescan checks that kazPlacement still sees source
// keys that reset() moved into restore and that are not re-added yet.
func TestKaz2QPlacementDuringRescan(t *testing.T) {
	p, clock := newKazTestIndex(t, 10, 4)
	kazPromote(t, p, clock, 1)
	protAtime := p.keys[kazKey(1)].atime
	*clock += 10
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: *clock})
	probAtime := p.keys[kazKey(2)].atime
	p.reset()
	require.Equal(t, 0, p.len())

	seg, at, ok := p.kazPlacement([]cacheKey{kazKey(1)})
	require.True(t, ok)
	require.Equal(t, kazProtected, seg)
	require.Equal(t, protAtime, at)

	seg, at, ok = p.kazPlacement([]cacheKey{kazKey(2), kazKey(9)})
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
	require.Equal(t, probAtime, at)

	// a re-added key is answered from the index, not from restore
	p.add(kazKey(2), cacheItem{size: kazTestBlock, atime: probAtime})
	seg, _, ok = p.kazPlacement([]cacheKey{kazKey(2)})
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)
}
