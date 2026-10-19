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

package vfs

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// TestKazCompactionSourcesMapping checks the source blocks of each output
// block: pieces start at the cumulative Len, holes (Id 0) have no source, and
// a piece maps to every source block its slice range covers.
func TestKazCompactionSourcesMapping(t *testing.T) {
	const mib = 1 << 20
	const bs = 4 * mib
	ref := func(id uint64, size, indx uint32) chunk.KazBlockRef {
		return chunk.KazBlockRef{ID: id, SliceSize: size, Indx: indx}
	}
	// whole marks ref as fully copied into the output block (see TestKazCompactionSourcesWhole).
	whole := func(r chunk.KazBlockRef) chunk.KazBlockRef { r.Whole = true; return r }
	// A covers the output [0, 5MiB) from its slice range [2MiB, 7MiB).
	f := kazCompactionSources([]meta.Slice{
		{Id: 10, Size: 8 * mib, Off: 2 * mib, Len: 5 * mib},
		{Id: 0, Len: mib},
		{Id: 20, Size: mib, Off: 0, Len: mib},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 8*mib, 0), ref(10, 8*mib, 1)}, f(0))
	require.Equal(t, []chunk.KazBlockRef{ref(10, 8*mib, 1), whole(ref(20, mib, 0))}, f(1))
	require.Empty(t, f(2))

	// A covers only the first output block [0, 4MiB) from [2MiB, 6MiB).
	f = kazCompactionSources([]meta.Slice{
		{Id: 10, Size: 8 * mib, Off: 2 * mib, Len: 4 * mib},
		{Id: 0, Len: mib},
		{Id: 20, Size: mib, Off: 0, Len: mib},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 8*mib, 0), ref(10, 8*mib, 1)}, f(0))
	require.Equal(t, []chunk.KazBlockRef{whole(ref(20, mib, 0))}, f(1))

	// Two pieces of the same source block are listed once.
	f = kazCompactionSources([]meta.Slice{
		{Id: 30, Size: mib, Off: 0, Len: 1000},
		{Id: 31, Size: 100, Off: 0, Len: 100},
		{Id: 30, Size: mib, Off: 1100, Len: 1000},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(30, mib, 0), whole(ref(31, 100, 0))}, f(0))
}

// TestKazCompactionSourcesWhole checks Whole: a source block is whole in an
// output block when the pieces copied into that output block cover its full
// range [0, min(bs, SliceSize-Indx*bs)), summed across pieces.
func TestKazCompactionSourcesWhole(t *testing.T) {
	const mib = 1 << 20
	const bs = 4 * mib
	ref := func(id uint64, size, indx uint32, w bool) chunk.KazBlockRef {
		return chunk.KazBlockRef{ID: id, SliceSize: size, Indx: indx, Whole: w}
	}
	// One piece copies a whole source block.
	f := kazCompactionSources([]meta.Slice{{Id: 10, Size: 8 * mib, Off: 0, Len: 4 * mib}}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 8*mib, 0, true)}, f(0))

	// Two pieces of the same source block cover it together.
	f = kazCompactionSources([]meta.Slice{
		{Id: 10, Size: 4 * mib, Off: mib, Len: 3 * mib},
		{Id: 10, Size: 4 * mib, Off: 0, Len: mib},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 4*mib, 0, true)}, f(0))

	// Only one of the two pieces lands in this output block: not whole.
	f = kazCompactionSources([]meta.Slice{
		{Id: 10, Size: 4 * mib, Off: mib, Len: 3 * mib},
		{Id: 0, Len: mib},
		{Id: 10, Size: 4 * mib, Off: 0, Len: mib},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 4*mib, 0, false)}, f(0))
	require.Equal(t, []chunk.KazBlockRef{ref(10, 4*mib, 0, false)}, f(1))

	// A piece straddling source blocks 0 and 1 covers neither fully.
	f = kazCompactionSources([]meta.Slice{{Id: 10, Size: 8 * mib, Off: 2 * mib, Len: 4 * mib}}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 8*mib, 0, false), ref(10, 8*mib, 1, false)}, f(0))

	// The short last block of a slice is whole when all of its 1 MiB is copied.
	f = kazCompactionSources([]meta.Slice{{Id: 10, Size: 5 * mib, Off: 4 * mib, Len: mib}}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 5*mib, 1, true)}, f(0))

	// Overlapping pieces are merged as a union, not summed: [0,3)+[1,3) is not whole.
	f = kazCompactionSources([]meta.Slice{
		{Id: 10, Size: 4 * mib, Off: 0, Len: 3 * mib},
		{Id: 10, Size: 4 * mib, Off: mib, Len: mib},
	}, bs)
	require.Equal(t, []chunk.KazBlockRef{ref(10, 4*mib, 0, false)}, f(0))
}

// kazCacheStater is the diagnostic interface of the chunk store under test.
type kazCacheStater interface {
	KazCacheState(key string) (segment string, cached bool)
	Retire(id uint64, length int) error
}

// kazTestKey returns the block key of a slice (no hash prefix).
func kazTestKey(id uint64, indx, size int) string {
	return fmt.Sprintf("chunks/%d/%d/%d_%d_%d", id/1000/1000, id/1000, id, indx, size)
}

// TestKazCompactInheritsCache writes slices into a kaz-2q disk cache, promotes
// one slice by a user read and runs vfs.Compact: each output block takes the
// segment of its source blocks (also for a single-block output shaped like a
// kaz block compaction), an output without a cached source is not cached,
// and retiring the sources removes them from the cache.
func TestKazCompactInheritsCache(t *testing.T) {
	const bs = 256 << 10
	const slen = bs + bs/2
	cconf := chunk.Config{
		BlockSize: bs, Compress: "lz4", MaxUpload: 2, MaxDownload: 200, MaxRetries: 1,
		BufferSize: 30 << 20, CacheSize: 64 << 20, CacheDir: t.TempDir(), CacheMode: 0600,
		CacheChecksum: chunk.CsNone, CacheScanInterval: time.Hour, CacheLargeWrite: true,
		CacheEviction: chunk.EvictionKaz2Q, KazCacheProbationRatio: 0.4,
		KazCacheGhostRatio: 4, KazCacheStateInterval: time.Hour,
		PutTimeout: 10 * time.Second, GetTimeout: 10 * time.Second,
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	store := chunk.NewCachedStore(blob, cconf, nil)
	st := store.(kazCacheStater)

	// waitSeg polls until key is in the index and returns its segment.
	waitSeg := func(key string) string {
		var seg string
		require.Eventually(t, func() bool {
			seg, _ = st.KazCacheState(key)
			return seg != ""
		}, 5*time.Second, 10*time.Millisecond, "%s must be cached", key)
		return seg
	}
	data := map[uint64][]byte{}
	for _, id := range []uint64{1, 2, 9} {
		data[id] = bytes.Repeat([]byte{byte(id)}, slen)
		w := store.NewWriter(id, 0)
		w.SetWriteback(false)
		_, err := w.WriteAt(data[id], 0)
		require.NoError(t, err)
		require.NoError(t, w.Finish(slen))
		require.Equal(t, "probation", waitSeg(kazTestKey(id, 0, bs)))
		require.Equal(t, "probation", waitSeg(kazTestKey(id, 1, bs/2)))
	}
	// A user read of slice 1 promotes both its blocks (no correlation period).
	p := chunk.NewOffPage(slen)
	_, err := store.NewReader(1, slen).ReadAt(context.Background(), p, 0)
	p.Release()
	require.NoError(t, err)
	require.Equal(t, "protected", waitSeg(kazTestKey(1, 0, bs)))
	require.Equal(t, "protected", waitSeg(kazTestKey(1, 1, bs/2)))
	// Slice 9 is only in object storage.
	require.NoError(t, st.Retire(9, slen))
	_, cached := st.KazCacheState(kazTestKey(9, 0, bs))
	require.False(t, cached)

	// Multi-block output 3 = slice 1 + slice 2: blocks [1.0], [1.1, 2.0], [2.0, 2.1].
	require.NoError(t, Compact(cconf, store, []meta.Slice{
		{Id: 1, Size: slen, Len: slen}, {Id: 2, Size: slen, Len: slen},
	}, 3, 0))
	require.Equal(t, "protected", waitSeg(kazTestKey(3, 0, bs)))
	require.Equal(t, "protected", waitSeg(kazTestKey(3, 1, bs)))
	require.Equal(t, "probation", waitSeg(kazTestKey(3, 2, bs)))

	// Single-block outputs as made by kaz block compaction (pieces of block 1).
	require.NoError(t, Compact(cconf, store, []meta.Slice{
		{Id: 1, Size: slen, Off: bs, Len: bs / 4}, {Id: 0, Len: bs / 4},
	}, 7, 0))
	require.Equal(t, "protected", waitSeg(kazTestKey(7, 0, bs/2)))
	require.NoError(t, Compact(cconf, store, []meta.Slice{
		{Id: 2, Size: slen, Off: bs, Len: bs / 4}, {Id: 9, Size: slen, Off: bs, Len: bs / 4},
	}, 8, 0))
	require.Equal(t, "probation", waitSeg(kazTestKey(8, 0, bs/2)))
	require.NoError(t, Compact(cconf, store, []meta.Slice{
		{Id: 9, Size: slen, Off: bs, Len: bs / 2},
	}, 10, 0))
	time.Sleep(100 * time.Millisecond)
	_, cached = st.KazCacheState(kazTestKey(10, 0, bs/2))
	require.False(t, cached, "output without a cached source must not be cached")

	// The outputs hold the compacted data.
	p = chunk.NewOffPage(2 * slen)
	n, err := store.NewReader(3, 2*slen).ReadAt(context.Background(), p, 0)
	require.NoError(t, err)
	require.Equal(t, append(append([]byte(nil), data[1]...), data[2]...), p.Data[:n])
	p.Release()

	// Retiring the sources removes their blocks from the cache.
	for _, id := range []uint64{1, 2} {
		require.NoError(t, st.Retire(id, slen))
		for indx, size := range []int{bs, bs / 2} {
			_, cached := st.KazCacheState(kazTestKey(id, indx, size))
			require.False(t, cached, "retired block %d_%d must leave the cache", id, indx)
		}
	}
}
