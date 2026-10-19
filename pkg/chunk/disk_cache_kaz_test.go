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
	"os"
	"path/filepath"
	"testing"

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

// newKazTestStore builds a kaz-2q cacheStore without background goroutines.
// newTestCacheStore bypasses newCacheStore, so the helper attaches the index
// itself with the store's effective capacity, as newCacheStore does after
// setLimitByFreeRatio (that path is covered by TestKaz2QStoreAttachEffectiveCapacity).
// It fixes the index clock to the returned pointer. m may be shared by
// several stores.
func newKazTestStore(t *testing.T, m *cacheManagerMetrics) (*cacheStore, *kaz2QIndex, *uint32) {
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
	s.kazAttach(s.capacity)
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
func kazWriteBlock(t *testing.T, s *cacheStore, key string) {
	t.Helper()
	path := s.cachePath(key)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	require.NoError(t, os.WriteFile(path, make([]byte, kazStoreBlock), 0600))
}

// kazLoad reads key through the store (a user hit) and closes the reader.
func kazLoad(t *testing.T, s *cacheStore, key string) {
	t.Helper()
	rc, err := s.load(key)
	require.NoError(t, err)
	require.NoError(t, rc.Close())
}

// kazSeg returns the segment of key in the store index, failing if it is absent.
func kazSeg(t *testing.T, s *cacheStore, idx *kaz2QIndex, key string) kazSegment {
	t.Helper()
	s.Lock()
	defer s.Unlock()
	seg, ok := idx.kazSegmentOf(s.getCacheKey(key))
	require.True(t, ok, "key %s not in index", key)
	return seg
}

// kazHas reports whether key is in the store index.
func kazHas(s *cacheStore, idx *kaz2QIndex, key string) bool {
	s.Lock()
	defer s.Unlock()
	_, ok := idx.kazSegmentOf(s.getCacheKey(key))
	return ok
}

// kazAtime returns the in-memory atime of key.
func kazAtime(s *cacheStore, key string) uint32 {
	s.Lock()
	defer s.Unlock()
	return s.keys.peekAtime(s.getCacheKey(key))
}

// kazCleanup runs cleanupFull with the store lock held, as its callers do.
func kazCleanup(s *cacheStore) {
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
	mfs, err := reg.Gather()
	require.NoError(t, err)
	names := map[string]bool{}
	for _, mf := range mfs {
		names[mf.GetName()] = true
	}
	for _, n := range []string{"kaz_cache_2q_blocks", "kaz_cache_2q_bytes", "kaz_cache_2q_events_total", "kaz_cache_2q_hits_total"} {
		require.True(t, names[n], "metric %s not registered", n)
	}
}

// TestKaz2QStoreAttachEffectiveCapacity checks that newCacheStore sizes the 2Q
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
	s := newCacheStore(newKazTestMetrics(), dir, cacheSize, 0, 1, &conf, nil)
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
	s.kazAttach(s.capacity)
	require.Equal(t, 1.0, testutil.ToFloat64(km.blocks.WithLabelValues("probation")))
	require.Equal(t, float64(kazStoreBlock), testutil.ToFloat64(km.bytes.WithLabelValues("probation")))
}
