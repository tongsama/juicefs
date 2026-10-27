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
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// kazStoreBlock is the logical size of the blocks used by the store tests.
const kazStoreBlock = 1024

// kazStoreKey returns the cache key string of block id, laid out as
// getPathFromKey does so that cleanupFull removes the same file.
func kazStoreKey(id uint64) string {
	return fmt.Sprintf("chunks/0/0/%d_0_%d", id, kazStoreBlock)
}

// newKazTestStore builds a kaz-2q kazDiskCache without background goroutines.
// newTestCacheStore bypasses kazNewDiskCache, so the helper attaches the index
// itself with the store's effective capacity, as kazNewDiskCache does after
// setLimitByFreeRatio (that path is covered by TestKaz2QStoreAttachEffectiveCapacity).
// It fixes the index clock to the returned pointer. m may be shared by
// several stores.
func newKazTestStore(t *testing.T, m *cacheManagerMetrics) (*kazDiskCache, *kaz2QIndex, *uint32) {
	t.Helper()
	conf := defaultConf
	conf.CacheEviction = EvictionKaz2Q
	conf.KazCacheProbationRatio = 0.4
	conf.KazCachePromoteDelay = 0
	conf.KazCacheGhostRatio = 4
	conf.FreeSpace = 0
	conf.CacheScanInterval = -1
	s := newTestCacheStore(t.TempDir(), &conf, nil)
	s.m = m
	s.kazAttach2Q(s.capacity)
	idx, ok := s.keys.(*kaz2QIndex)
	require.True(t, ok, "kaz-2q store must use kaz2QIndex")
	clock := kazTestStart
	idx.now = func() uint32 { return clock }
	return s, idx, &clock
}

// newKazTestMetrics returns initialized (unregistered) cache manager metrics.
func newKazTestMetrics() *cacheManagerMetrics {
	m := new(cacheManagerMetrics)
	m.initMetrics()
	return m
}

// kazWriteBlock writes a block file of kazStoreBlock bytes at the cache path of key.
func kazWriteBlock(t *testing.T, s *kazDiskCache, key string) {
	t.Helper()
	path := s.cachePath(key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	require.NoError(t, os.WriteFile(path, make([]byte, kazStoreBlock), 0600))
}

// kazLoad reads key through the store (a user hit) and closes the reader.
func kazLoad(t *testing.T, s *kazDiskCache, key string) {
	t.Helper()
	rc, err := s.load(key)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
}

// kazSeg returns the segment of key in the store index, failing if it is absent.
func kazSeg(t *testing.T, s *kazDiskCache, idx *kaz2QIndex, key string) kazSegment {
	t.Helper()
	s.Lock()
	defer s.Unlock()
	seg, ok := idx.kazSegmentOf(s.getCacheKey(key))
	require.True(t, ok, "key %s not in index", key)
	return seg
}

// kazHas reports whether key is in the store index.
func kazHas(s *kazDiskCache, idx *kaz2QIndex, key string) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := idx.kazSegmentOf(s.getCacheKey(key))
	return ok
}

// kazAtime returns the in-memory atime of key.
func kazAtime(s *kazDiskCache, key string) uint32 {
	s.Lock()
	defer s.Unlock()
	return s.keys.peekAtime(s.getCacheKey(key))
}

// kazCleanup runs cleanupFull with the store lock held, as its callers do.
func kazCleanup(s *kazDiskCache) {
	s.Lock()
	s.cleanupFull()
	s.Unlock()
}

// TestKaz2QStoreLoadPromotes checks that a user read through load promotes a
// block once the correlation period (0 here) has passed.
func TestKaz2QStoreLoadPromotes(t *testing.T) {
	s, idx, clock := newKazTestStore(t, newKazTestMetrics())
	key := kazStoreKey(1)
	kazWriteBlock(t, s, key)
	s.add(key, kazStoreBlock, *clock)
	require.Equal(t, kazProbation, kazSeg(t, s, idx, key))
	*clock += 5
	kazLoad(t, s, key)
	require.Equal(t, kazProtected, kazSeg(t, s, idx, key))
}

// TestKaz2QStoreReaddKeepsSegment checks that storing an existing key again
// (cache() after a GET, add() from flush) and an upload completion neither
// count as a read nor change the atime; the protected block stays protected.
func TestKaz2QStoreReaddKeepsSegment(t *testing.T) {
	s, idx, clock := newKazTestStore(t, newKazTestMetrics())
	key := kazStoreKey(1)
	kazWriteBlock(t, s, key)
	s.add(key, kazStoreBlock, *clock)
	*clock += 5
	kazLoad(t, s, key)
	require.Equal(t, kazProtected, kazSeg(t, s, idx, key))
	atime := kazAtime(s, key)
	require.Equal(t, kazTestStart+5, atime)

	// cache() of an existing key: nothing queued, no touch.
	*clock += 100
	p := NewPage(make([]byte, kazStoreBlock))
	defer p.Release()
	s.cache(key, p, false, false)
	s.Lock()
	require.Empty(t, s.pages, "existing key must not be queued again")
	s.Unlock()
	require.Equal(t, kazProtected, kazSeg(t, s, idx, key))
	require.Equal(t, atime, kazAtime(s, key))

	// add() of an existing positive key (e.g. flush): no touch.
	*clock += 100
	s.add(key, kazStoreBlock, *clock)
	require.Equal(t, kazProtected, kazSeg(t, s, idx, key))
	require.Equal(t, atime, kazAtime(s, key))
	require.Equal(t, int64(kazStoreBlock+4096), s.used)

	// staging -> uploaded: the upload completion is not a read; atime kept.
	staged := kazStoreKey(2)
	stagedAtime := *clock
	s.add(staged, -kazStoreBlock, stagedAtime)
	*clock += 100
	s.uploaded(staged, kazStoreBlock)
	require.Equal(t, kazProbation, kazSeg(t, s, idx, staged))
	require.Equal(t, stagedAtime, kazAtime(s, staged))
	require.Equal(t, kazProtected, kazSeg(t, s, idx, key))
	require.Equal(t, int64(2*(kazStoreBlock+4096)), s.used)
}

// TestKaz2QStoreScanKeepsSegments checks that a rescan keeps the segments of
// protected and probation blocks.
func TestKaz2QStoreScanKeepsSegments(t *testing.T) {
	s, idx, clock := newKazTestStore(t, newKazTestMetrics())
	prot := []string{kazStoreKey(1), kazStoreKey(2)}
	prob := []string{kazStoreKey(3), kazStoreKey(4)}
	for _, k := range append(append([]string{}, prot...), prob...) {
		kazWriteBlock(t, s, k)
		s.add(k, kazStoreBlock, *clock)
	}
	*clock += 5
	for _, k := range prot {
		kazLoad(t, s, k)
	}
	s.scanCached(false)
	for _, k := range prot {
		require.Equal(t, kazProtected, kazSeg(t, s, idx, k), "key %s", k)
	}
	for _, k := range prob {
		require.Equal(t, kazProbation, kazSeg(t, s, idx, k), "key %s", k)
	}
	require.Equal(t, int64(4*(kazStoreBlock+4096)), s.used)
}

// TestKaz2QStoreFreeSpaceEvictsAll checks that a capacity cleanup evicts
// probation first and keeps protected, and a free-space shortage then evicts
// protected blocks as well.
func TestKaz2QStoreFreeSpaceEvictsAll(t *testing.T) {
	s, idx, clock := newKazTestStore(t, newKazTestMetrics())
	prot := []string{kazStoreKey(1), kazStoreKey(2)}
	prob := []string{kazStoreKey(3), kazStoreKey(4)}
	for _, k := range append(append([]string{}, prot...), prob...) {
		kazWriteBlock(t, s, k)
		s.add(k, kazStoreBlock, *clock)
	}
	*clock += 5
	for _, k := range prot {
		kazLoad(t, s, k)
	}
	require.Equal(t, int64(4*(kazStoreBlock+4096)), s.used)

	// Capacity cleanup: goal 11400 bytes forces two evictions.
	s.capacity = 12000
	kazCleanup(s)
	for _, k := range prob {
		require.False(t, kazHas(s, idx, k), "probation %s should be evicted first", k)
	}
	for _, k := range prot {
		require.True(t, kazHas(s, idx, k), "protected %s should survive", k)
	}
	require.Equal(t, int64(2*(kazStoreBlock+4096)), s.used)

	// Free-space shortage: everything evictable goes.
	s.capacity = 1 << 30
	s.freeRatio = 0.9999
	kazCleanup(s)
	for _, k := range prot {
		require.False(t, kazHas(s, idx, k), "protected %s should be evicted on free-space shortage", k)
		_, err := os.Stat(s.cachePath(k))
		require.True(t, os.IsNotExist(err), "file of %s should be removed", k)
	}
	require.Equal(t, int64(0), s.used)
}

// TestKaz2QStoreStagingSafe checks that a staged block survives cleanupFull
// and is not counted in used.
func TestKaz2QStoreStagingSafe(t *testing.T) {
	s, idx, clock := newKazTestStore(t, newKazTestMetrics())
	staged := kazStoreKey(1)
	_, err := s.stage(staged, make([]byte, kazStoreBlock), 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), s.used)
	other := kazStoreKey(2)
	kazWriteBlock(t, s, other)
	s.add(other, kazStoreBlock, *clock)

	s.freeRatio = 0.9999
	kazCleanup(s)
	require.True(t, kazHas(s, idx, staged), "staged block must not be evicted")
	require.False(t, kazHas(s, idx, other))
	_, err = os.Stat(s.cachePath(staged))
	require.NoError(t, err)
	_, err = os.Stat(s.stagePath(staged))
	require.NoError(t, err)
	require.Equal(t, int64(0), s.used)
	s.Lock()
	it := idx.kazPeek(s.getCacheKey(staged))
	s.Unlock()
	require.NotNil(t, it)
	require.Less(t, it.size, int32(0))
}

// TestKaz2QMetrics checks the promote / evict_probation counters and that the
// segment gauges, shared by two stores of one manager, sum their segments.
func TestKaz2QMetrics(t *testing.T) {
	m := newKazTestMetrics()
	require.NotNil(t, m.kaz2Q)
	a, _, clockA := newKazTestStore(t, m)
	b, _, clockB := newKazTestStore(t, m)

	ka := kazStoreKey(1)
	kazWriteBlock(t, a, ka)
	a.add(ka, kazStoreBlock, *clockA)
	*clockA += 5
	kazLoad(t, a, ka) // promote in store a
	for _, id := range []uint64{2, 3} {
		k := kazStoreKey(id)
		kazWriteBlock(t, b, k)
		b.add(k, kazStoreBlock, *clockB)
	}
	km := m.kaz2Q
	require.Equal(t, 1.0, testutil.ToFloat64(km.events.WithLabelValues("promote")))
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("protected")))
	require.Equal(t, float64(kazStoreBlock), testutil.ToFloat64(km.bytes.WithLabelValues("protected")))
	require.Equal(t, 2.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
	require.Equal(t, float64(2*kazStoreBlock), testutil.ToFloat64(km.bytes.WithLabelValues("probation")))
	require.Equal(t, 1.0, testutil.ToFloat64(km.hits.WithLabelValues("fill")))

	// Evict one probation block of store b by capacity.
	b.capacity = 6000 // goal 5700: one block (5120) may stay
	kazCleanup(b)
	require.Equal(t, 1.0, testutil.ToFloat64(km.events.WithLabelValues("evict_probation")))
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
	require.Equal(t, float64(kazStoreBlock), testutil.ToFloat64(km.bytes.WithLabelValues("probation")))
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("protected")))

	// A rescan of store a must not disturb the sum either.
	a.scanCached(false)
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("protected")))
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
}

// TestKaz2QMetricsRegister checks that the kaz collectors are registered with
// a registerer and that a nil registerer is a no-op.
func TestKaz2QMetricsRegister(t *testing.T) {
	require.NotPanics(t, func() { _ = newCacheManagerMetrics(nil) })
	reg := prometheus.NewRegistry()
	m := newCacheManagerMetrics(reg)
	m.kaz2Q.events.WithLabelValues("promote").Inc()
	m.kaz2Q.hits.WithLabelValues("fill").Inc()
	m.kaz2Q.blocks.WithLabelValues("probation").Set(1)
	m.kaz2Q.bytes.WithLabelValues("probation").Set(1)
	m.kaz2Q.bytesByOrigin.WithLabelValues("probation", "fill").Set(1)
	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, n := range []string{"kaz_cache_2q_blocks", "kaz_cache_2q_bytes", "kaz_cache_2q_events_total", "kaz_cache_2q_hits_total",
		"kaz_cache_2q_ghost_blocks", "kaz_cache_2q_ghost_bytes", "kaz_cache_2q_bytes_by_origin"} {
		require.True(t, names[n], "metric %s not registered", n)
	}
}

// TestKaz2QStoreAttachEffectiveCapacity checks that kazNewDiskCache sizes the 2Q
// index from the capacity left by setLimitByFreeRatio, not from cacheSize.
func TestKaz2QStoreAttachEffectiveCapacity(t *testing.T) {
	conf := defaultConf
	conf.CacheEviction = EvictionKaz2Q
	conf.CacheScanInterval = -1
	conf.FreeSpace = 0.1
	const cacheSize = int64(1) << 60 // larger than any disk: the free-ratio limit must lower it
	// Like TestSetlimitByFreeRatio, the store goroutines are left running and
	// the directory is removed best effort: shutdownStore writes s.state
	// without the lock that available() lacks too, which -race reports.
	dir, err := os.MkdirTemp("", "kaz-attach-")
	require.NoError(t, err)
	defer os.RemoveAll(dir)
	s := kazNewDiskCache(newKazTestMetrics(), dir, cacheSize, 0, 1, &conf, nil)
	idx, ok := s.keys.(*kaz2QIndex)
	require.True(t, ok)
	s.Lock()
	defer s.Unlock()
	require.Less(t, s.capacity, cacheSize, "setLimitByFreeRatio should lower the capacity")
	require.Equal(t, s.capacity, idx.capacity)
}

// TestKaz2QMetricsReattach checks that setting the metrics again withdraws
// the share published before, so the shared gauges are not double counted.
func TestKaz2QMetricsReattach(t *testing.T) {
	m := newKazTestMetrics()
	s, _, clock := newKazTestStore(t, m)
	s.add(kazStoreKey(1), kazStoreBlock, *clock)
	km := m.kaz2Q
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
	s.kazAttach2Q(s.capacity)
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
	require.Equal(t, float64(kazStoreBlock), testutil.ToFloat64(km.bytes.WithLabelValues("probation")))
}

// TestKaz2QStoreExistNoTouch checks that an existence check (warmup --check,
// FillCache) does not count as a read under kaz-2q: a probation block stays
// in probation with its atime, even with a promote delay of 0. Under lru the
// check still updates atime as upstream does.
func TestKaz2QStoreExistNoTouch(t *testing.T) {
	s, idx, clock := newKazTestStore(t, nil)
	key := kazStoreKey(1)
	kazWriteBlock(t, s, key)
	s.add(key, kazStoreBlock, *clock)
	*clock += 5
	ok, err := s.exist(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, kazProbation, kazSeg(t, s, idx, key))
	require.Equal(t, kazTestStart, kazAtime(s, key))

	conf := defaultConf
	conf.CacheEviction = EvictionLRU
	conf.CacheScanInterval = -1
	lru := newTestCacheStore(t.TempDir(), &conf, nil)
	kazWriteBlock(t, lru, key)
	lru.add(key, kazStoreBlock, 1000)
	ok, err = lru.exist(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Greater(t, kazAtime(lru, key), uint32(1000), "lru exist() should touch atime as before")
}

// kazGhostGauges returns the evicted-key list gauges (blocks, bytes).
func kazGhostGauges(m *cacheManagerMetrics) (float64, float64) {
	return testutil.ToFloat64(m.kaz2Q.ghostBlocks), testutil.ToFloat64(m.kaz2Q.ghostBytes)
}

// kazGhostOf returns the evicted-key list size of an index under the store lock.
func kazGhostOf(s *kazDiskCache, idx *kaz2QIndex) (int, int64) {
	s.Lock()
	defer s.Unlock()
	return idx.ghost.Len(), idx.ghostBytes
}

// TestKaz2QGhostMetrics checks that the evicted-key list gauges follow
// evictions, ghost hits and trimming, sum over the stores of a manager and
// are not double counted when the metrics are attached again.
func TestKaz2QGhostMetrics(t *testing.T) {
	m := newKazTestMetrics()
	a, ia, clockA := newKazTestStore(t, m)
	b, ib, clockB := newKazTestStore(t, m)
	blocks, bytes := kazGhostGauges(m)
	require.Zero(t, blocks)
	require.Zero(t, bytes)

	for _, id := range []uint64{1, 2, 3} {
		k := kazStoreKey(id)
		kazWriteBlock(t, a, k)
		a.add(k, kazStoreBlock, *clockA)
		kazWriteBlock(t, b, k)
		b.add(k, kazStoreBlock, *clockB)
	}
	// evict two blocks of each store into the evicted-key lists
	for _, s := range []*kazDiskCache{a, b} {
		s.capacity = 6000
		kazCleanup(s)
	}
	na, ba := kazGhostOf(a, ia)
	nb, bb := kazGhostOf(b, ib)
	require.Equal(t, 2, na)
	require.Equal(t, 2, nb)
	blocks, bytes = kazGhostGauges(m)
	require.Equal(t, float64(na+nb), blocks)
	require.Equal(t, float64(ba+bb), bytes)

	// a ghost hit takes the key out of the list
	a.Lock()
	var ghostKey cacheKey
	for k := range ia.ghostKeys {
		ghostKey = k
		break
	}
	a.Unlock()
	a.Lock()
	ia.add(ghostKey, cacheItem{size: kazStoreBlock, atime: *clockA})
	a.Unlock()
	na, ba = kazGhostOf(a, ia)
	require.Equal(t, 1, na)
	blocks, bytes = kazGhostGauges(m)
	require.Equal(t, float64(na+nb), blocks)
	require.Equal(t, float64(ba+bb), bytes)

	// shrinking the capacity trims the list of store b
	b.Lock()
	ib.kazSetCapacity(1)
	b.Unlock()
	nb, bb = kazGhostOf(b, ib)
	require.Zero(t, nb)
	blocks, bytes = kazGhostGauges(m)
	require.Equal(t, float64(na+nb), blocks)
	require.Equal(t, float64(ba+bb), bytes)

	// attaching the metrics again does not double count
	a.Lock()
	ia.kazSetMetrics(m.kaz2Q)
	a.Unlock()
	blocks, bytes = kazGhostGauges(m)
	require.Equal(t, float64(na+nb), blocks)
	require.Equal(t, float64(ba+bb), bytes)
}

// TestKaz2QStoreAtimeNotLost is the kaz-2q counterpart of TestAtimeNotLost.
// An existence check does not touch atime under kaz-2q, so the upstream
// test's premise ("exist touches atime") does not hold; instead this checks
// that a rescan keeps the in-memory atime when it is newer than the file's,
// and takes the file's atime when that is newer (read outside the index).
func TestKaz2QStoreAtimeNotLost(t *testing.T) {
	s, idx, _ := newKazTestStore(t, nil)
	key := kazStoreKey(1)
	kazWriteBlock(t, s, key)
	old := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(s.cachePath(key), old, old))
	mem := uint32(old.Unix()) + 100
	s.add(key, kazStoreBlock, mem)
	ok, err := s.exist(key)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, mem, kazAtime(s, key))
	s.scanCached(false)
	require.Equal(t, mem, kazAtime(s, key))
	require.Equal(t, kazProbation, kazSeg(t, s, idx, key))

	newer := old.Add(500 * time.Second)
	require.NoError(t, os.Chtimes(s.cachePath(key), newer, newer))
	s.scanCached(false)
	require.Equal(t, uint32(newer.Unix()), kazAtime(s, key))
	require.Equal(t, kazProbation, kazSeg(t, s, idx, key))
}

// kazCheckBytesByOrigin checks that, per segment, the bytes_by_origin gauges
// sum to the segment bytes gauge and to the bytes the indexes account.
func kazCheckBytesByOrigin(t *testing.T, m *kaz2QMetrics, idx ...*kaz2QIndex) {
	t.Helper()
	for _, s := range []kazSegment{kazProbation, kazProtected} {
		var sum, want float64
		for o := kazOrigin(0); o < kazOriginCount; o++ {
			sum += testutil.ToFloat64(m.bytesByOrigin.WithLabelValues(s.String(), o.String()))
		}
		for _, p := range idx {
			want += float64(p.used[s])
		}
		require.Equal(t, want, testutil.ToFloat64(m.bytes.WithLabelValues(s.String())), "segment %s", s)
		require.Equal(t, want, sum, "bytes_by_origin of segment %s", s)
	}
}

// TestKaz2QBytesByOrigin runs random operations on two indexes sharing one set
// of collectors and checks that the per-origin byte gauges stay consistent
// with the per-segment byte gauge after each operation (add, get, staging
// upload, compaction output, demotion and eviction, remove, reset and re-add,
// and re-attaching the metrics).
func TestKaz2QBytesByOrigin(t *testing.T) {
	m := newKaz2QMetrics()
	p, pclock := newKazTestIndex(t, 20, 2)
	q, qclock := newKazTestIndex(t, 20, 2)
	p.kazSetMetrics(m)
	q.kazSetMetrics(m)
	idx := []*kaz2QIndex{p, q}
	clocks := []*uint32{pclock, qclock}
	r := rand.New(rand.NewSource(2))
	var seen [3][kazOriginCount]bool // segment and origin pairs that held bytes at some step
	for i := 0; i < 5000; i++ {
		n := r.Intn(2)
		x, clock := idx[n], clocks[n]
		k := kazKey(uint64(r.Intn(60)))
		*clock += uint32(r.Intn(120))
		switch r.Intn(9) {
		case 0, 1:
			size := int32(1 + r.Intn(kazTestBlock))
			if r.Intn(4) == 0 {
				size = -size // staging; a later positive add completes the upload
			}
			atime := *clock
			if r.Intn(3) == 0 {
				atime -= 3600 // legacy
			}
			x.add(k, cacheItem{size: size, atime: atime})
		case 2:
			seg := kazProbation
			if r.Intn(2) == 0 {
				seg = kazProtected
			}
			x.kazExpect(k, seg, *clock, false)
			x.add(k, cacheItem{size: kazTestBlock, atime: *clock})
		case 3, 4:
			x.get(k)
		case 5:
			x.remove(k, r.Intn(2) == 0)
		case 6:
			c := r.Intn(4)
			for range x.evictionIter() {
				if c--; c < 0 {
					break
				}
			}
		case 7:
			if r.Intn(20) == 0 {
				x.reset()
			}
		case 8:
			if r.Intn(50) == 0 {
				x.kazSetMetrics(m)
			}
		}
		kazVerify(t, x)
		kazCheckBytesByOrigin(t, m, p, q)
		for _, s := range []kazSegment{kazProbation, kazProtected} {
			for o := kazOrigin(0); o < kazOriginCount; o++ {
				seen[s][o] = seen[s][o] || x.usedByOrigin[s][o] > 0
			}
		}
	}
	require.NotZero(t, testutil.ToFloat64(m.events.WithLabelValues("demote")))
	require.NotZero(t, testutil.ToFloat64(m.events.WithLabelValues("promote")))
	for o := kazOrigin(0); o < kazOriginCount; o++ {
		require.True(t, seen[kazProbation][o], "origin %s never held probation bytes", o)
		require.True(t, seen[kazProtected][o], "origin %s never held protected bytes", o)
	}
	// staging is never counted
	p.add(kazKey(1000), cacheItem{size: -kazTestBlock, atime: *pclock})
	kazCheckBytesByOrigin(t, m, p, q)
	// withdrawing one index leaves the share of the other
	p.kazSetMetrics(nil)
	kazCheckBytesByOrigin(t, m, q)
}

// kazUnreadOf returns the bytes of the protected, evictable items of p without
// the read mark, computed from its key map.
func kazUnreadOf(p *kaz2QIndex) int64 {
	var n int64
	for _, it := range p.keys {
		if it.size > 0 && it.seg == kazProtected && !it.read {
			n += int64(it.size)
		}
	}
	return n
}

// TestKaz2QUnreadGaugeConsistent runs random operations on two indexes (one
// with --kaz-cache-demote-unread-first) sharing one set of collectors and
// checks after each one that the heaps and accounting hold (kazVerify), that
// no probation item has the read mark, and that the protected-unread gauge
// equals the unread protected bytes of both indexes (add, get, promotion,
// demotion, eviction, ghost hit, compaction output, reset and re-add,
// re-attaching the metrics).
func TestKaz2QUnreadGaugeConsistent(t *testing.T) {
	m := newKaz2QMetrics()
	p, pclock := newKazTestIndexB(t, 20, 2, true)
	q, qclock := newKazTestIndexB(t, 20, 2, false)
	p.kazSetMetrics(m)
	q.kazSetMetrics(m)
	idx := []*kaz2QIndex{p, q}
	clocks := []*uint32{pclock, qclock}
	gauge := func() float64 { return testutil.ToFloat64(m.protectedUnread) }
	r := rand.New(rand.NewSource(3))
	for i := 0; i < 5000; i++ {
		n := r.Intn(2)
		x, clock := idx[n], clocks[n]
		k := kazKey(uint64(r.Intn(60)))
		*clock += uint32(r.Intn(240))
		switch r.Intn(9) {
		case 0, 1:
			size := int32(1 + r.Intn(kazTestBlock))
			if r.Intn(4) == 0 {
				size = -size
			}
			x.add(k, cacheItem{size: size, atime: *clock})
		case 2:
			seg := kazProbation
			if r.Intn(2) == 0 {
				seg = kazProtected
			}
			x.kazExpect(k, seg, *clock, r.Intn(2) == 0)
			x.add(k, cacheItem{size: kazTestBlock, atime: *clock})
		case 3, 4:
			x.get(k)
		case 5:
			x.remove(k, r.Intn(2) == 0)
		case 6:
			c := r.Intn(4)
			for range x.evictionIter() {
				if c--; c < 0 {
					break
				}
			}
		case 7:
			if r.Intn(20) == 0 {
				x.reset()
			}
		case 8:
			if r.Intn(50) == 0 {
				x.kazSetMetrics(m)
			}
		}
		kazVerify(t, x)
		for _, it := range x.keys {
			if it.seg == kazProbation {
				require.False(t, it.read, "probation item %v has the read mark", it.key)
			}
		}
		require.Equal(t, kazUnreadOf(x), x.usedUnread)
		require.Equal(t, float64(kazUnreadOf(p)+kazUnreadOf(q)), gauge())
	}
	ev := func(name string) float64 { return testutil.ToFloat64(m.events.WithLabelValues(name)) }
	require.NotZero(t, ev("demote_unread"))
	require.NotZero(t, ev("demote_read"))
	require.NotZero(t, ev("ghost_hit"))
	require.Equal(t, ev("demote"), ev("demote_unread")+ev("demote_read"))
	// withdrawing one index leaves the share of the other
	p.kazSetMetrics(nil)
	require.Equal(t, float64(kazUnreadOf(q)), gauge())
}
