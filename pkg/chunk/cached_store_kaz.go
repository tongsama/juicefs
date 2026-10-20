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

import "context"

// KazFlushCacheState saves the kaz-2q state file of every disk cache store
// (called at a normal umount). It does nothing for memory caches and other
// eviction policies; failures are only logged and counted.
func (store *cachedStore) KazFlushCacheState() {
	if m, ok := store.bcache.(interface{ kazFlushState() }); ok {
		m.kazFlushState()
	}
}

// kazCompactionReadKey is the context key that marks a read as made by
// compaction (see KazCompactionReadContext).
type kazCompactionReadKey struct{}

// KazCompactionReadContext marks ctx as a compaction read. Under kaz-2q such
// a read does not count as a user read: a cache hit neither updates atime nor
// promotes the block, and a miss is neither cached nor prefetched. Other
// eviction policies ignore the mark.
func KazCompactionReadContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, kazCompactionReadKey{}, true)
}

// kazCompactionRead reports whether ctx is marked as a compaction read and the
// store uses kaz-2q, so that lru/2-random/none behave exactly as before.
func (store *cachedStore) kazCompactionRead(ctx context.Context) bool {
	if store.conf.CacheEviction != EvictionKaz2Q {
		return false
	}
	marked, _ := ctx.Value(kazCompactionReadKey{}).(bool)
	return marked
}

// kazLoadForRead opens the cached block key for rSlice.ReadAt. A compaction
// read under kaz-2q uses the no-touch lookup when the block cache provides one;
// every other read uses bcache.load as before.
func (store *cachedStore) kazLoadForRead(ctx context.Context, key string) (ReadCloser, error) {
	if store.kazCompactionRead(ctx) {
		if m, ok := store.bcache.(interface {
			kazLoadNoTouch(key string) (ReadCloser, error)
		}); ok {
			return m.kazLoadNoTouch(key)
		}
	}
	return store.bcache.load(key)
}

// KazBlockRef names one block of a source slice of a compaction: the slice id,
// its full size (to build the block key) and the block index. Whole reports
// whether the full range of that source block was copied into the output
// block it is listed for (used by the replace placement). Bytes is the size
// of the union of the byte ranges of that source block copied into the output
// block, and OutBytes the non-hole bytes of the output block (the same in
// every ref of one output block); the share placement compares the Bytes of
// protected sources with OutBytes.
type KazBlockRef struct {
	ID        uint64
	SliceSize uint32
	Indx      uint32
	Whole     bool
	Bytes     uint32
	OutBytes  uint32
}

// KazSetCompactionSources marks the slice as a compaction output whose block
// indx is made from the source blocks f(indx). Under kaz-2q the synchronous
// upload then places each output block like its sources (see
// kazCacheCompactionOutput); under any other eviction policy it does nothing,
// so the upstream caching rule applies unchanged.
func (s *wSlice) KazSetCompactionSources(f func(indx int) []KazBlockRef) {
	if s.store.conf.CacheEviction != EvictionKaz2Q {
		return
	}
	s.kazSources = f
}

// kazCacheCompactionOutput replaces the upstream caching rule for a block of
// a compaction output under kaz-2q (it takes precedence over CacheLargeWrite
// and the small-block rule): the block is cached only when at least one of
// its source blocks is cached, with the newest user-read atime of the
// sources, in the segment chosen by --kaz-cache-compaction-placement (see
// cacheManager.kazCompactionPlacement). Like the upstream branch, cache() takes its own page
// reference and the caller releases block afterwards in both cases. A block
// cache without kaz placement (memory cache) keeps the upstream rule.
func (store *cachedStore) kazCacheCompactionOutput(key string, block *Page, sources func(int) []KazBlockRef) {
	placer, ok := store.bcache.(interface {
		kazCompactionPlacement(outKey string, outBytes uint32, srcs []kazPlacementSource) bool
	})
	if !ok {
		if len(block.Data) < store.conf.BlockSize || store.conf.CacheLargeWrite {
			store.bcache.cache(key, block, false, false)
		}
		return
	}
	// getCacheKey only parses the key, it does not use the store.
	indx := (*cacheStore)(nil).getCacheKey(key).indx
	refs := sources(int(indx))
	srcs := make([]kazPlacementSource, 0, len(refs))
	var outBytes uint32 // the same in every ref; max guards hand-made refs
	for _, ref := range refs {
		srcs = append(srcs, kazPlacementSource{
			key:   sliceForRead(ref.ID, int(ref.SliceSize), store).key(int(ref.Indx)),
			whole: ref.Whole,
			bytes: ref.Bytes,
		})
		outBytes = max(outBytes, ref.OutBytes)
	}
	if placer.kazCompactionPlacement(key, outBytes, srcs) {
		store.bcache.cache(key, block, false, false)
	}
}

// KazCacheState reports, for diagnostics and tests, whether the block key is
// in the local cache and its kaz-2q segment ("probation" or "protected"). It
// never counts as a read (no atime update, no promotion). A block that is
// still queued to be written to disk is reported as cached with segment ""
// (its segment is decided when it enters the index), so callers wait for a
// non-empty segment. Other eviction policies report segment "".
func (store *cachedStore) KazCacheState(key string) (segment string, cached bool) {
	if m, ok := store.bcache.(interface {
		kazCacheState(key string) (string, bool)
	}); ok {
		return m.kazCacheState(key)
	}
	_, cached = store.bcache.exist(key)
	return "", cached
}

// kazWholeRead reports whether a read under ctx of a block of bs bytes should
// fetch the whole object (and cache it) instead of a range, as configured by
// Config.KazWholeReadMax (delta compaction mode; 0 disables). It holds only
// when the fetched block is kept: not when the block is not cacheable
// (shouldCache) and not for a compaction read under kaz-2q, which is not
// cached; otherwise each small read would GET the whole object again.
func (store *cachedStore) kazWholeRead(ctx context.Context, bs int) bool {
	return store.conf.KazWholeReadMax > 0 && bs <= store.conf.KazWholeReadMax &&
		store.shouldCache(bs) && !store.kazCompactionRead(ctx)
}
