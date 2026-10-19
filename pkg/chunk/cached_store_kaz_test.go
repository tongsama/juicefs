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
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
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
