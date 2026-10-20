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
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// kazCompSize is the length of the single-block slices used by the compaction
// read tests; it is smaller than BlockSize so that the block is cacheable.
const kazCompSize = 64 << 10

// kazCompFixture is a cachedStore over memory object storage for the
// compaction read tests, with the object (and optionally the cached file) of
// each slice prepared before the store starts.
type kazCompFixture struct {
	store *cachedStore
	cs    *cacheStore // nil for the memory cache
	data  map[uint64][]byte
	keys  map[uint64]string
}

// kazCompKey returns the object key of the only block of slice id of size bytes.
func kazCompKey(conf Config, id uint64, size int) string {
	return (&rSlice{id: id, length: size, store: &cachedStore{conf: conf}}).key(0)
}

// newKazCompFixture puts kazCompSize bytes for every id into memory object
// storage and, when cached is set, the same bytes as the cached file in a
// temporary cache dir, then starts a cachedStore with eviction and waits for
// the first scan. dir "memory" selects the memory cache instead. The
// tests avoid calling cacheStore.cache on a disk store: it reads rawFull
// without the lock, racing with checkFreeSpace under -race (upstream). The
// prefetcher is replaced by one without workers so that queued keys can be
// checked deterministically. Goroutines are left running (shutdownStore races
// under -race).
func newKazCompFixture(t *testing.T, eviction, dir string, cached bool, ids ...uint64) *kazCompFixture {
	t.Helper()
	return newKazCompFixtureConf(t, eviction, dir, cached, nil, ids...)
}

// newKazCompFixtureConf is newKazCompFixture with mutate (if not nil) applied
// to the config before the store starts.
func newKazCompFixtureConf(t *testing.T, eviction, dir string, cached bool, mutate func(*Config), ids ...uint64) *kazCompFixture {
	t.Helper()
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.CacheDir = dir
	if dir == "" {
		conf.CacheDir = t.TempDir()
	}
	conf.CacheEviction = eviction
	conf.KazCacheProbationRatio = 0.4
	conf.KazCachePromoteDelay = 0
	conf.KazCacheGhostRatio = 4
	conf.KazCacheStateInterval = time.Hour
	conf.FreeSpace = 0
	conf.Prefetch = 1
	if mutate != nil {
		mutate(&conf)
	}
	f := &kazCompFixture{data: map[uint64][]byte{}, keys: map[uint64]string{}}
	for _, id := range ids {
		data := bytes.Repeat([]byte{byte(id)}, kazCompSize)
		key := kazCompKey(conf, id, kazCompSize)
		require.NoError(t, mem.Put(context.Background(), key, bytes.NewReader(data)))
		if cached {
			path := filepath.Join(conf.CacheDir, cacheDir, key)
			require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
			require.NoError(t, os.WriteFile(path, data, 0600))
		}
		f.data[id], f.keys[id] = data, key
	}
	f.store = NewCachedStore(mem, conf, nil).(*cachedStore)
	f.store.fetcher = newPrefetcher(0, func(string) {})
	if m, ok := f.store.bcache.(*cacheManager); ok {
		require.Len(t, m.stores, 1)
		f.cs = m.stores[0]
		require.Eventually(t, func() bool {
			f.cs.Lock()
			defer f.cs.Unlock()
			return f.cs.scanned
		}, 5*time.Second, 10*time.Millisecond, "first scan must finish")
	}
	return f
}

// cached reports whether the block of slice id is cached in a disk store
// (pending in memory or in the index).
func (f *kazCompFixture) cached(id uint64) bool {
	f.cs.Lock()
	defer f.cs.Unlock()
	if _, ok := f.cs.pages[f.keys[id]]; ok {
		return true
	}
	return kazLookupNoTouch(f.cs.keys, f.cs.getCacheKey(f.keys[id])) != nil
}

// read reads [off, off+n) of slice id through a new reader with ctx and
// returns the bytes read.
func (f *kazCompFixture) read(t *testing.T, ctx context.Context, id uint64, off, n int) []byte {
	t.Helper()
	p := NewOffPage(n)
	defer p.Release()
	got, err := f.store.NewReader(id, kazCompSize).ReadAt(ctx, p, off)
	require.NoError(t, err)
	require.Equal(t, n, got)
	return append([]byte(nil), p.Data[:got]...)
}

// prefetchQueued reports whether the block of slice id was queued for prefetch.
func (f *kazCompFixture) prefetchQueued(id uint64) bool {
	f.store.fetcher.Lock()
	defer f.store.fetcher.Unlock()
	return f.store.fetcher.busy[f.keys[id]]
}

// TestKazCompactionReadNoPromote checks that a compaction read hit neither
// promotes the block nor updates its atime, while a user read promotes it.
func TestKazCompactionReadNoPromote(t *testing.T) {
	f := newKazCompFixture(t, EvictionKaz2Q, "", true, 11)
	cs, data := f.cs, f.data[11]
	idx := cs.keys.(*kaz2QIndex)
	var clock atomic.Uint32
	clock.Store(uint32(time.Now().Unix()) + 100)
	cs.Lock()
	idx.now = func() uint32 { return clock.Load() }
	k := cs.getCacheKey(f.keys[11])
	seg, ok := idx.kazSegmentOf(k)
	before := idx.peekAtime(k)
	cs.Unlock()
	require.True(t, ok)
	require.Equal(t, kazProbation, seg)

	clock.Add(10)
	cctx := KazCompactionReadContext(context.Background())
	require.Equal(t, data, f.read(t, cctx, 11, 0, kazCompSize))
	require.Equal(t, data[100:200], f.read(t, cctx, 11, 100, 100))
	cs.Lock()
	seg, _ = idx.kazSegmentOf(k)
	after := idx.peekAtime(k)
	cs.Unlock()
	require.Equal(t, kazProbation, seg, "compaction read must not promote")
	require.Equal(t, before, after, "compaction read must not update atime")

	require.Equal(t, data, f.read(t, context.Background(), 11, 0, kazCompSize))
	cs.Lock()
	seg, _ = idx.kazSegmentOf(k)
	after = idx.peekAtime(k)
	cs.Unlock()
	require.Equal(t, kazProtected, seg, "user read must promote")
	require.Equal(t, clock.Load(), after)
}

// TestKazCompactionReadMissNotCached checks that a compaction read of a block
// removed from the cache returns the data without caching it, through the
// range path (no prefetch) and the full-block path (also with FinishCanceledGet).
func TestKazCompactionReadMissNotCached(t *testing.T) {
	for _, finish := range []bool{false, true} {
		f := newKazCompFixture(t, EvictionKaz2Q, "", true, 12)
		f.store.conf.FinishCanceledGet = finish
		data := f.data[12]
		require.True(t, f.cached(12))
		f.store.bcache.remove(f.keys[12], false)
		require.False(t, f.cached(12))

		cctx := KazCompactionReadContext(context.Background())
		require.Equal(t, data[1000:1100], f.read(t, cctx, 12, 1000, 100))
		require.False(t, f.prefetchQueued(12), "compaction read must not prefetch")
		require.Equal(t, data, f.read(t, cctx, 12, 0, kazCompSize))
		time.Sleep(100 * time.Millisecond) // a detached fetch would have cached it by now
		require.False(t, f.cached(12), "compaction read must not cache (finish=%v)", finish)

		// A user range read of the same block still queues the prefetch.
		require.Equal(t, data[1000:1100], f.read(t, context.Background(), 12, 1000, 100))
		require.True(t, f.prefetchQueued(12))
	}
}

// TestKazCompactionReadOtherPolicies checks that the compaction mark does
// nothing under lru: a hit updates atime, a range miss queues the prefetch
// and a miss is cached. The miss is cached into the memory cache, because
// caching into a disk store races with checkFreeSpace under -race (upstream).
func TestKazCompactionReadOtherPolicies(t *testing.T) {
	cctx := KazCompactionReadContext(context.Background())
	f := newKazCompFixture(t, EvictionLRU, "", true, 13)
	require.False(t, f.store.kazCompactionRead(cctx))
	cs, data := f.cs, f.data[13]
	k := cs.getCacheKey(f.keys[13])
	cs.Lock()
	cs.keys.(*lruEviction).keys[k].atime = 1
	cs.Unlock()
	require.Equal(t, data, f.read(t, cctx, 13, 0, kazCompSize))
	cs.Lock()
	atime := cs.keys.peekAtime(k)
	cs.Unlock()
	require.Greater(t, atime, uint32(1), "lru hit must update atime")

	f.store.bcache.remove(f.keys[13], false)
	require.Equal(t, data[1000:1100], f.read(t, cctx, 13, 1000, 100))
	require.True(t, f.prefetchQueued(13), "lru range miss must prefetch")

	m := newKazCompFixture(t, EvictionLRU, "memory", false, 14)
	require.False(t, m.store.kazCompactionRead(cctx))
	_, err := m.store.bcache.load(m.keys[14])
	require.Error(t, err)
	require.Equal(t, m.data[14], m.read(t, cctx, 14, 0, kazCompSize))
	r, err := m.store.bcache.load(m.keys[14])
	require.NoError(t, err, "lru miss must be cached")
	require.NoError(t, r.Close())
}

// TestKazCompactionReadSharedWithUser checks that concurrent compaction and
// user reads of the same block both return correct data, for a disk-cached
// block under kaz-2q and for an uncached block whose download is shared
// between readers that disagree on caching (memory cache; see
// newKazCompFixture for why not a disk store).
func TestKazCompactionReadSharedWithUser(t *testing.T) {
	disk := newKazCompFixture(t, EvictionKaz2Q, "", true, 15)
	mem := newKazCompFixture(t, EvictionKaz2Q, "memory", false, 16)
	require.True(t, mem.store.kazCompactionRead(KazCompactionReadContext(context.Background())))
	for _, c := range []struct {
		f  *kazCompFixture
		id uint64
	}{{disk, 15}, {mem, 16}} {
		cctx := KazCompactionReadContext(context.Background())
		var wg sync.WaitGroup
		results := make([][]byte, 8)
		errs := make([]error, 8)
		for i := range results {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				rctx := context.Background()
				if i%2 == 0 {
					rctx = cctx
				}
				p := NewOffPage(kazCompSize)
				defer p.Release()
				n, err := c.f.store.NewReader(c.id, kazCompSize).ReadAt(rctx, p, 0)
				errs[i] = err
				results[i] = append([]byte(nil), p.Data[:n]...)
			}(i)
		}
		wg.Wait()
		for i := range results {
			require.NoError(t, errs[i])
			require.Equal(t, c.f.data[c.id], results[i], "slice %d reader %d", c.id, i)
		}
	}
}

// kazWriteCompactionOutput writes data as the single-block compaction output
// slice id with the given source blocks (synchronous upload, like
// vfs.Compact) and returns its block key.
func (f *kazCompFixture) kazWriteCompactionOutput(t *testing.T, id uint64, data []byte, srcs []KazBlockRef) string {
	t.Helper()
	w := f.store.NewWriter(id, 0)
	w.(*wSlice).KazSetCompactionSources(func(indx int) []KazBlockRef {
		require.Equal(t, 0, indx)
		return srcs
	})
	w.SetWriteback(false)
	_, err := w.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, w.Finish(len(data)))
	return kazCompKey(f.store.conf, id, len(data))
}

// waitState polls KazCacheState until the key has a segment (it was flushed
// into the index) and returns that segment.
func (f *kazCompFixture) waitState(t *testing.T, key string) string {
	t.Helper()
	var seg string
	require.Eventually(t, func() bool {
		seg, _ = f.store.KazCacheState(key)
		return seg != ""
	}, 5*time.Second, 10*time.Millisecond, "output %s must be cached", key)
	return seg
}

// kazEvents returns the kaz-2q event counter of the fixture's cache manager.
func (f *kazCompFixture) kazEvents(name string) float64 {
	return testutil.ToFloat64(f.cs.m.kaz2Q.events.WithLabelValues(name))
}

// TestKazCompactionOutputInherits checks that under kaz-2q a compaction output
// takes the segment and the newest user-read atime of its source blocks, and
// is not cached when no source block is cached.
func TestKazCompactionOutputInherits(t *testing.T) {
	f := newKazCompFixture(t, EvictionKaz2Q, "", true, 1, 2)
	cs := f.cs
	idx := cs.keys.(*kaz2QIndex)
	future := uint32(time.Now().Unix()) + 100000
	cs.Lock()
	orig := idx.now
	idx.now = func() uint32 { return future }
	cs.Unlock()
	f.read(t, context.Background(), 1, 0, 100) // user read: promote slice 1, atime = future
	cs.Lock()
	idx.now = orig
	k1, k2 := cs.getCacheKey(f.keys[1]), cs.getCacheKey(f.keys[2])
	seg1, _ := idx.kazSegmentOf(k1)
	seg2, _ := idx.kazSegmentOf(k2)
	atime2 := idx.peekAtime(k2)
	cs.Unlock()
	require.Equal(t, kazProtected, seg1)
	require.Equal(t, kazProbation, seg2)
	require.Less(t, atime2, future)
	ref := func(id uint64) KazBlockRef { return KazBlockRef{ID: id, SliceSize: kazCompSize, Indx: 0} }
	out := bytes.Repeat([]byte{0xAB}, kazCompSize)

	// Protected and probation sources: protected, with the newest atime.
	key := f.kazWriteCompactionOutput(t, 101, out, []KazBlockRef{ref(2), ref(1)})
	require.Equal(t, "protected", f.waitState(t, key))
	cs.Lock()
	it := idx.keys[cs.getCacheKey(key)]
	cs.Unlock()
	require.Equal(t, future, it.atime, "output must inherit the newest source atime")
	require.Equal(t, kazOriginCompaction, it.origin)

	// Probation source only: probation, with its atime.
	key = f.kazWriteCompactionOutput(t, 102, out, []KazBlockRef{ref(2)})
	require.Equal(t, "probation", f.waitState(t, key))
	cs.Lock()
	require.Equal(t, atime2, idx.peekAtime(cs.getCacheKey(key)))
	cs.Unlock()
	require.Equal(t, 2.0, f.kazEvents("compaction_output_cached"))

	// No cached source: the output is not cached.
	key = f.kazWriteCompactionOutput(t, 103, out, []KazBlockRef{ref(9)})
	time.Sleep(100 * time.Millisecond)
	seg, cached := f.store.KazCacheState(key)
	require.False(t, cached, "output without a cached source must not be cached")
	require.Equal(t, "", seg)
	require.Equal(t, 1.0, f.kazEvents("compaction_output_skipped"))

	// The data is uploaded in every case (read without caching a miss).
	for _, id := range []uint64{101, 102, 103} {
		p := NewOffPage(kazCompSize)
		n, err := f.store.NewReader(id, kazCompSize).ReadAt(KazCompactionReadContext(context.Background()), p, 0)
		require.NoError(t, err)
		require.Equal(t, out, p.Data[:n])
		p.Release()
	}
}

// TestKazCompactionOutputOtherPolicies checks that the compaction sources are
// ignored under lru: the output is cached as before for a block smaller than
// BlockSize, and for a full block with CacheLargeWrite.
func TestKazCompactionOutputOtherPolicies(t *testing.T) {
	f := newKazCompFixture(t, EvictionLRU, "", true, 1)
	w := f.store.NewWriter(201, 0).(*wSlice)
	w.KazSetCompactionSources(func(int) []KazBlockRef { return nil })
	require.Nil(t, w.kazSources, "sources must be ignored under lru")
	key := f.kazWriteCompactionOutput(t, 202, bytes.Repeat([]byte{1}, kazCompSize), nil)
	require.Eventually(t, func() bool { _, ok := f.store.KazCacheState(key); return ok },
		5*time.Second, 10*time.Millisecond, "lru must cache a small output")

	f.store.conf.CacheLargeWrite = true
	full := bytes.Repeat([]byte{2}, f.store.conf.BlockSize)
	key = f.kazWriteCompactionOutput(t, 203, full, nil)
	require.Eventually(t, func() bool { _, ok := f.store.KazCacheState(key); return ok },
		5*time.Second, 10*time.Millisecond, "lru with CacheLargeWrite must cache a full output")
}

// TestKazCompactionPlacementModes checks the segment of a compaction output
// under each --kaz-cache-compaction-placement value: inherit takes protected
// from any protected source, probation always uses probation, and replace
// uses protected only when a protected source block was copied whole. In
// every mode an output without a cached source is not cached and the output
// takes the newest atime of its sources.
func TestKazCompactionPlacementModes(t *testing.T) {
	for _, tc := range []struct {
		mode           string
		partial, whole string // segment with the protected source partial / whole
		probWhole      string // segment with only a whole probation source
	}{
		{KazPlacementInherit, "protected", "protected", "probation"},
		{KazPlacementProbation, "probation", "probation", "probation"},
		{KazPlacementReplace, "probation", "protected", "probation"},
	} {
		t.Run(tc.mode, func(t *testing.T) {
			f := newKazCompFixtureConf(t, EvictionKaz2Q, "", true,
				func(c *Config) { c.KazCacheCompactionPlacement = tc.mode }, 1, 2)
			cs := f.cs
			idx := cs.keys.(*kaz2QIndex)
			future := uint32(time.Now().Unix()) + 100000
			cs.Lock()
			orig := idx.now
			idx.now = func() uint32 { return future }
			cs.Unlock()
			f.read(t, context.Background(), 1, 0, 100) // user read: promote slice 1, atime = future
			cs.Lock()
			idx.now = orig
			seg1, _ := idx.kazSegmentOf(cs.getCacheKey(f.keys[1]))
			seg2, _ := idx.kazSegmentOf(cs.getCacheKey(f.keys[2]))
			atime2 := idx.peekAtime(cs.getCacheKey(f.keys[2]))
			cs.Unlock()
			require.Equal(t, kazProtected, seg1)
			require.Equal(t, kazProbation, seg2)
			ref := func(id uint64, whole bool) KazBlockRef {
				return KazBlockRef{ID: id, SliceSize: kazCompSize, Indx: 0, Whole: whole}
			}
			out := bytes.Repeat([]byte{0xCD}, kazCompSize)
			// atimeOf returns the atime of the cached output key.
			atimeOf := func(key string) uint32 {
				cs.Lock()
				defer cs.Unlock()
				return idx.keys[cs.getCacheKey(key)].atime
			}

			key := f.kazWriteCompactionOutput(t, 101, out, []KazBlockRef{ref(1, false), ref(2, true)})
			require.Equal(t, tc.partial, f.waitState(t, key), "protected source copied partially")
			require.Equal(t, future, atimeOf(key), "output must inherit the newest source atime")

			key = f.kazWriteCompactionOutput(t, 102, out, []KazBlockRef{ref(2, false), ref(1, true)})
			require.Equal(t, tc.whole, f.waitState(t, key), "protected source copied whole")
			require.Equal(t, future, atimeOf(key), "output must inherit the newest source atime")

			key = f.kazWriteCompactionOutput(t, 103, out, []KazBlockRef{ref(2, true)})
			require.Equal(t, tc.probWhole, f.waitState(t, key), "probation source only")
			require.Equal(t, atime2, atimeOf(key))

			key = f.kazWriteCompactionOutput(t, 104, out, []KazBlockRef{ref(9, true)})
			time.Sleep(100 * time.Millisecond)
			_, cached := f.store.KazCacheState(key)
			require.False(t, cached, "output without a cached source must not be cached")
			require.Equal(t, 3.0, f.kazEvents("compaction_output_cached"))
			require.Equal(t, 1.0, f.kazEvents("compaction_output_skipped"))
		})
	}
}

// TestKazCompactionPlacementShare checks --kaz-cache-compaction-placement share:
// an output block goes to protected when the bytes copied from protected
// source blocks reach the share ratio of its non-hole bytes, else to the
// newest end of probation; no protected source or a zero denominator (holes
// only) gives probation, no cached source skips caching, and the output
// takes the newest atime of its sources.
func TestKazCompactionPlacementShare(t *testing.T) {
	const mib = 1 << 20
	for _, tc := range []struct {
		ratio   float64
		mixed   string // 3 MiB from a protected source + 1 MiB from a probation source
		protAll string // 4 MiB from a protected source
	}{
		{0.5, "protected", "protected"},
		{0.8, "probation", "protected"},
		{1, "probation", "protected"},
	} {
		t.Run(fmt.Sprint(tc.ratio), func(t *testing.T) {
			f := newKazCompFixtureConf(t, EvictionKaz2Q, "", true, func(c *Config) {
				c.KazCacheCompactionPlacement = KazPlacementShare
				c.KazCacheCompactionPlacementShareRatio = tc.ratio
			}, 1, 2)
			cs := f.cs
			idx := cs.keys.(*kaz2QIndex)
			future := uint32(time.Now().Unix()) + 100000
			cs.Lock()
			orig := idx.now
			idx.now = func() uint32 { return future }
			cs.Unlock()
			f.read(t, context.Background(), 1, 0, 100) // user read: promote slice 1, atime = future
			cs.Lock()
			idx.now = orig
			seg1, _ := idx.kazSegmentOf(cs.getCacheKey(f.keys[1]))
			seg2, _ := idx.kazSegmentOf(cs.getCacheKey(f.keys[2]))
			atime2 := idx.peekAtime(cs.getCacheKey(f.keys[2]))
			cs.Unlock()
			require.Equal(t, kazProtected, seg1)
			require.Equal(t, kazProbation, seg2)
			// ref builds a source ref copying n bytes into an output block of out non-hole bytes.
			ref := func(id uint64, n, out uint32) KazBlockRef {
				return KazBlockRef{ID: id, SliceSize: kazCompSize, Indx: 0, Bytes: n, OutBytes: out}
			}
			data := bytes.Repeat([]byte{0xEF}, kazCompSize)
			// itemOf returns the index item of the cached output key.
			itemOf := func(key string) kazItem {
				cs.Lock()
				defer cs.Unlock()
				return *idx.keys[cs.getCacheKey(key)]
			}

			before := uint32(time.Now().Unix())
			key := f.kazWriteCompactionOutput(t, 101, data, []KazBlockRef{ref(1, 3*mib, 4*mib), ref(2, mib, 4*mib)})
			require.Equal(t, tc.mixed, f.waitState(t, key), "3 of 4 MiB from protected")
			it := itemOf(key)
			require.Equal(t, future, it.atime, "output must inherit the newest source atime")
			if tc.mixed == "probation" {
				require.GreaterOrEqual(t, it.order, before, "probation output must enter the newest end")
			}

			key = f.kazWriteCompactionOutput(t, 102, data, []KazBlockRef{ref(1, 4*mib, 4*mib)})
			require.Equal(t, tc.protAll, f.waitState(t, key), "4 of 4 MiB from protected")
			require.Equal(t, future, itemOf(key).atime)

			key = f.kazWriteCompactionOutput(t, 103, data, []KazBlockRef{ref(2, 4*mib, 4*mib)})
			require.Equal(t, "probation", f.waitState(t, key), "no protected source")
			require.Equal(t, atime2, itemOf(key).atime)

			key = f.kazWriteCompactionOutput(t, 104, data, []KazBlockRef{ref(1, 0, 0)})
			require.Equal(t, "probation", f.waitState(t, key), "zero denominator (holes only)")

			key = f.kazWriteCompactionOutput(t, 105, data, []KazBlockRef{ref(9, 4*mib, 4*mib)})
			time.Sleep(100 * time.Millisecond)
			_, cached := f.store.KazCacheState(key)
			require.False(t, cached, "output without a cached source must not be cached")
			require.Equal(t, 4.0, f.kazEvents("compaction_output_cached"))
			require.Equal(t, 1.0, f.kazEvents("compaction_output_skipped"))
		})
	}
}

// TestKaz2QReadMarkCompaction checks that kazCompactionPlacement carries the
// read mark into a compaction output placed in protected only when a protected
// source has it, and never into an output placed in probation. The outputs
// are added to the index directly (not written through the disk cache).
func TestKaz2QReadMarkCompaction(t *testing.T) {
	f := newKazCompFixture(t, EvictionKaz2Q, "", true, 1, 2, 3)
	cs := f.cs
	m := f.store.bcache.(*cacheManager)
	idx := cs.keys.(*kaz2QIndex)
	f.read(t, context.Background(), 1, 0, 100) // user read: promote slice 1 with the mark
	cs.Lock()
	k1, k3 := cs.getCacheKey(f.keys[1]), cs.getCacheKey(f.keys[3])
	require.Equal(t, kazProtected, idx.keys[k1].seg)
	require.True(t, idx.keys[k1].read)
	it := idx.keys[k3] // slice 3: protected without the mark
	idx.unlink(it)
	it.seg = kazProtected
	idx.link(it)
	cs.Unlock()

	// place registers the output of id from srcs (under placement mode) and adds it.
	place := func(id uint64, mode string, srcs ...uint64) kazItem {
		cs.Lock()
		idx.placement = mode
		cs.Unlock()
		out := kazCompKey(f.store.conf, id, kazCompSize)
		var ps []kazPlacementSource
		for _, s := range srcs {
			ps = append(ps, kazPlacementSource{key: f.keys[s], whole: true, bytes: kazCompSize})
		}
		require.True(t, m.kazCompactionPlacement(out, kazCompSize, ps))
		cs.Lock()
		defer cs.Unlock()
		k := cs.getCacheKey(out)
		idx.add(k, cacheItem{size: kazCompSize, atime: idx.now()})
		kazVerify(t, idx)
		return *idx.keys[k]
	}
	o := place(101, KazPlacementInherit, 3, 1, 2)
	require.Equal(t, kazProtected, o.seg)
	require.True(t, o.read, "a protected source has the mark")
	o = place(102, KazPlacementInherit, 3, 2)
	require.Equal(t, kazProtected, o.seg)
	require.False(t, o.read, "the protected source has no mark")
	o = place(103, KazPlacementProbation, 1, 3)
	require.Equal(t, kazProbation, o.seg)
	require.False(t, o.read, "a probation output has no mark")
	o = place(104, KazPlacementShare, 1)
	require.Equal(t, kazProtected, o.seg)
	require.True(t, o.read)
}

// TestKazCompactionHotAge checks --kaz-cache-compaction-hot-age: a protected
// source that is itself a compaction output cached less than hot-age ago (a
// young output) is not counted as a protected source by inherit, replace or
// share, and does not pass on its read mark; an output left without another
// protected source goes to the newest end of probation and counts
// compaction_output_hot (only when the exclusion changed protected into
// probation). Old compaction outputs and fill / legacy sources are not
// affected by their age, hot-age 0 keeps the old placement, and sources kept
// only in restore (after reset) are judged by their saved origin and inserted.
// The outputs are added to the index directly (not written through cache()).
func TestKazCompactionHotAge(t *testing.T) {
	const mib = 1 << 20
	const T = uint32(2_000_000_000) // fixed clock
	f := newKazCompFixture(t, EvictionKaz2Q, "", true, 1, 2, 3, 4, 5)
	cs := f.cs
	m := f.store.bcache.(*cacheManager)
	idx := cs.keys.(*kaz2QIndex)
	require.Equal(t, 0.0, f.kazEvents("compaction_output_hot"))

	// set gives slice id the segment, origin, inserted time and read mark.
	set := func(id uint64, seg kazSegment, origin kazOrigin, inserted uint32, read bool) {
		it := idx.keys[cs.getCacheKey(f.keys[id])]
		idx.unlink(it)
		it.seg, it.origin, it.inserted, it.read = seg, origin, inserted, read
		idx.link(it)
	}
	cs.Lock()
	idx.now = func() uint32 { return T }
	idx.hotAge = 1800
	set(1, kazProtected, kazOriginCompaction, T-60, true)    // young output, marked
	set(2, kazProtected, kazOriginCompaction, T-3600, false) // old output
	set(3, kazProtected, kazOriginFill, T-60, false)         // fill: age does not matter
	set(4, kazProtected, kazOriginLegacy, T-60, false)       // legacy: age does not matter
	set(5, kazProbation, kazOriginFill, T-60, false)         // probation
	kazVerify(t, idx)
	cs.Unlock()

	type src struct {
		id    uint64
		whole bool
		bytes uint32
	}
	next := uint64(100)
	// place registers the output of srcs (under placement mode, with outBytes
	// non-hole bytes) and adds it; it returns the added item.
	place := func(mode string, outBytes uint32, srcs ...src) kazItem {
		cs.Lock()
		idx.placement = mode
		cs.Unlock()
		next++
		out := kazCompKey(f.store.conf, next, kazCompSize)
		var ps []kazPlacementSource
		for _, s := range srcs {
			ps = append(ps, kazPlacementSource{key: f.keys[s.id], whole: s.whole, bytes: s.bytes})
		}
		require.True(t, m.kazCompactionPlacement(out, outBytes, ps))
		cs.Lock()
		defer cs.Unlock()
		k := cs.getCacheKey(out)
		idx.add(k, cacheItem{size: kazCompSize, atime: T})
		kazVerify(t, idx)
		return *idx.keys[k]
	}
	all := func(id uint64) src { return src{id, true, 4 * mib} }
	hot := func() float64 { return f.kazEvents("compaction_output_hot") }

	// inherit
	o := place(KazPlacementInherit, 4*mib, all(1))
	require.Equal(t, kazProbation, o.seg, "young protected source only")
	require.False(t, o.read)
	require.Equal(t, T, o.order, "probation output enters the newest end")
	require.Equal(t, T, o.inserted)
	require.Equal(t, 1.0, hot())
	o = place(KazPlacementInherit, 4*mib, all(1), all(5))
	require.Equal(t, kazProbation, o.seg, "young protected + probation source")
	require.Equal(t, 2.0, hot())
	o = place(KazPlacementInherit, 4*mib, all(2))
	require.Equal(t, kazProtected, o.seg, "old compaction output stays protected")
	require.Equal(t, T, o.inserted, "a protected output records when it entered")
	o = place(KazPlacementInherit, 4*mib, all(3))
	require.Equal(t, kazProtected, o.seg, "fill source is not judged by age")
	o = place(KazPlacementInherit, 4*mib, all(4))
	require.Equal(t, kazProtected, o.seg, "legacy source is not judged by age")
	o = place(KazPlacementInherit, 4*mib, all(1), all(2))
	require.Equal(t, kazProtected, o.seg, "an old protected source remains")
	require.False(t, o.read, "the young source's read mark is not inherited")
	require.Equal(t, 2.0, hot(), "protected decision unchanged")
	o = place(KazPlacementProbation, 4*mib, all(1))
	require.Equal(t, kazProbation, o.seg)
	require.Equal(t, 2.0, hot(), "probation mode is probation regardless of age")

	// replace
	o = place(KazPlacementReplace, 4*mib, all(1))
	require.Equal(t, kazProbation, o.seg, "young whole source")
	require.Equal(t, 3.0, hot())
	o = place(KazPlacementReplace, 4*mib, all(1), src{2, false, mib})
	require.Equal(t, kazProbation, o.seg, "young whole + old partial")
	require.Equal(t, 4.0, hot())
	o = place(KazPlacementReplace, 4*mib, all(2))
	require.Equal(t, kazProtected, o.seg, "old whole source")
	require.Equal(t, 4.0, hot())

	// share (ratio 0.5): only the old protected bytes count
	o = place(KazPlacementShare, 4*mib, src{1, false, 3 * mib}, src{2, false, mib})
	require.Equal(t, kazProbation, o.seg, "1 of 4 MiB from an old protected source")
	require.Equal(t, 5.0, hot())
	o = place(KazPlacementShare, 4*mib, src{1, false, mib}, src{2, false, 3 * mib})
	require.Equal(t, kazProtected, o.seg, "3 of 4 MiB from an old protected source")
	require.False(t, o.read, "the young source's read mark is not inherited")
	o = place(KazPlacementShare, 4*mib, src{1, false, mib}, src{5, false, 3 * mib})
	require.Equal(t, kazProbation, o.seg, "probation without the exclusion as well")
	require.Equal(t, 5.0, hot(), "not counted: protected was never the decision")

	// hot-age 0: the old placement, with the young source's mark
	cs.Lock()
	idx.hotAge = 0
	cs.Unlock()
	o = place(KazPlacementInherit, 4*mib, all(1))
	require.Equal(t, kazProtected, o.seg)
	require.True(t, o.read)
	require.Equal(t, 5.0, hot())

	// sources only in restore after reset keep their saved origin and inserted
	cs.Lock()
	idx.hotAge = 1800
	idx.reset()
	_, inIndex := idx.keys[cs.getCacheKey(f.keys[1])]
	require.False(t, inIndex)
	cs.Unlock()
	o = place(KazPlacementInherit, 4*mib, all(1))
	require.Equal(t, kazProbation, o.seg, "young source from restore")
	require.Equal(t, 6.0, hot())
	o = place(KazPlacementInherit, 4*mib, all(2))
	require.Equal(t, kazProtected, o.seg, "old source from restore")
	require.Equal(t, 6.0, hot())
	// the source becomes old as time passes
	cs.Lock()
	idx.now = func() uint32 { return T + 1800 }
	cs.Unlock()
	o = place(KazPlacementInherit, 4*mib, all(1))
	require.Equal(t, kazProtected, o.seg, "hot-age reached")
	require.True(t, o.read)
	require.Equal(t, 6.0, hot())
}
