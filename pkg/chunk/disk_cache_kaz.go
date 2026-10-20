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
	"errors"
	"os"
)

// kazPeeker is implemented by key indexes that can look up a key without
// counting it as a user read (the kaz-2q index).
type kazPeeker interface {
	kazPeek(key cacheKey) *cacheItem
}

// kazLookupNoTouch looks up k for an existence check or a size update (storing
// a block again, an upload completion). The kaz-2q index answers without
// side effects so that these are not counted as reads; any other index is
// asked with get(k) exactly as before.
func kazLookupNoTouch(keys KeyIndex, k cacheKey) *cacheItem {
	if p, ok := keys.(kazPeeker); ok {
		return p.kazPeek(k)
	}
	return keys.get(k)
}

// kazAttach gives a kaz-2q index the effective store capacity (after the
// free-ratio limit; it sizes the protected limit and the evicted-key list
// cap) and the shared metrics, imports the state file (so that the first
// scan restores the segments) and starts the periodic state saver. It does
// nothing for other eviction policies. It must run before the store
// goroutines start.
func (cache *cacheStore) kazAttach(capacity int64) {
	idx, ok := cache.keys.(*kaz2QIndex)
	if !ok {
		return
	}
	idx.kazSetCapacity(capacity)
	if cache.m != nil {
		idx.kazSetMetrics(cache.m.kaz2Q)
	}
	cache.kazLoadState(idx)
	go cache.kazSaveLoop(idx.stateInterval)
}

// kazLoadNoTouch is load for a compaction read: it opens the cached block key
// exactly as load does (pending pages, the scanned check, removing an index
// entry whose file cannot be opened) but looks the key up without counting
// it as a user read, so that neither atime nor the 2Q segment changes.
func (cache *cacheStore) kazLoadNoTouch(key string) (ReadCloser, error) {
	cache.Lock()
	defer cache.Unlock()
	if p, ok := cache.pages[key]; ok {
		return NewPageReader(p), nil
	}
	k := cache.getCacheKey(key)
	if cache.scanned && kazLookupNoTouch(cache.keys, k) == nil {
		return nil, errNotCached
	}
	cache.Unlock()

	var f *cacheFile
	var err error
	err = cache.checkErr(func() error {
		f, err = openCacheFile(cache.cachePath(key), parseObjOrigSize(key), cache.checksum)
		if err != nil && !os.IsNotExist(err) {
			logger.Warnf("Open cache file %s failed: %s", cache.cachePath(key), err)
		}
		return err
	})

	cache.Lock()
	if err != nil {
		if it := cache.keys.remove(k, false); it != nil {
			cache.used -= int64(it.size + 4096)
		}
	}
	return f, err
}

// kazLoadNoTouch is cacheManager.load for a compaction read: the same store
// routing and legacy-store fallback, using cacheStore.kazLoadNoTouch.
func (m *cacheManager) kazLoadNoTouch(key string) (ReadCloser, error) {
	store := m.getStore(key)
	if store == nil {
		return nil, errors.New("no available cache dir")
	}
	r, err := store.kazLoadNoTouch(key)
	if err == errNotCached {
		legacy := m.getStoreLegacy(key)
		if legacy != store && legacy != nil {
			r, err = legacy.kazLoadNoTouch(key)
		}
	}
	return r, err
}

// kazPlacementSource is one source block of a compaction output block given
// to kazCompactionPlacement: its block key, whether its full range was copied
// into the output block (KazBlockRef.Whole) and how many of its bytes were
// copied into it (KazBlockRef.Bytes, a union across pieces).
type kazPlacementSource struct {
	key   string
	whole bool
	bytes uint32
}

// kazCompactionPlacement decides whether the compaction output block outKey
// is cached under kaz-2q, from its source blocks srcs: each source store is
// asked separately (one store lock at a time) with kazPlacement, and when any
// source is cached the placement is registered with kazExpect in the output
// store, to be consumed by the add after cache() flushes the block. The
// segment follows the output store's placement mode (from
// Config.KazCacheCompactionPlacement): inherit = protected if any cached
// source is protected; probation = always probation; replace = protected
// only if a protected source has whole set; share = protected if the bytes of
// the protected sources are at least the share ratio of outBytes (the
// non-hole bytes of the output block; 0 gives probation). In every mode the
// atime is the newest among the cached sources, and an output placed in
// protected gets the read mark when a protected source has it (an output in
// probation never does). It returns whether the caller should
// cache the output and counts compaction_output_cached or
// compaction_output_skipped. Sources that are only pending in memory, or
// only in a legacy store, are not considered.
func (m *cacheManager) kazCompactionPlacement(outKey string, outBytes uint32, srcs []kazPlacementSource) bool {
	var atime uint32
	var protectedBytes uint64 // bytes copied from protected sources (share)
	found, anyProtected, wholeProtected := false, false, false
	protectedRead := false // some protected source has the read mark
	for _, src := range srcs {
		s := m.getStore(src.key)
		if s == nil {
			continue
		}
		s.Lock()
		if idx, ok := s.keys.(*kaz2QIndex); ok {
			if sg, at, read, ok := idx.kazPlacement([]cacheKey{s.getCacheKey(src.key)}); ok {
				if sg == kazProtected {
					anyProtected = true
					protectedRead = protectedRead || read
					wholeProtected = wholeProtected || src.whole
					protectedBytes += uint64(src.bytes)
				}
				atime = max(atime, at)
				found = true
			}
		}
		s.Unlock()
	}
	out := m.getStore(outKey)
	placed := false
	if found && out != nil {
		out.Lock()
		if idx, ok := out.keys.(*kaz2QIndex); ok {
			seg := kazProbation
			switch idx.placement {
			case KazPlacementProbation:
			case KazPlacementReplace:
				if wholeProtected {
					seg = kazProtected
				}
			case KazPlacementShare:
				if kazShareProtected(protectedBytes, outBytes, idx.shareRatio) {
					seg = kazProtected
				}
			default: // inherit
				if anyProtected {
					seg = kazProtected
				}
			}
			idx.kazExpect(out.getCacheKey(outKey), seg, atime, seg == kazProtected && protectedRead)
			placed = true
		}
		out.Unlock()
	}
	if km := m.getMetrics().kaz2Q; km != nil {
		if placed {
			km.events.WithLabelValues("compaction_output_cached").Inc()
		} else {
			km.events.WithLabelValues("compaction_output_skipped").Inc()
		}
	}
	return placed
}

// kazShareProtected reports whether the share placement puts an output block
// in protected: protectedBytes (copied from protected sources) is at least
// ratio of outBytes (its non-hole bytes). A zero outBytes gives false
// (probation), since there is no source data to weigh.
func kazShareProtected(protectedBytes uint64, outBytes uint32, ratio float64) bool {
	if outBytes == 0 {
		return false
	}
	return float64(protectedBytes) >= ratio*float64(outBytes)
}

// kazCacheState reports whether key is cached in its store and its kaz-2q
// segment, without counting a read (see cachedStore.KazCacheState). A key
// only pending in memory is cached with segment ""; under other eviction
// policies the segment is "" and presence is taken from peekAtime.
func (m *cacheManager) kazCacheState(key string) (string, bool) {
	s := m.getStore(key)
	if s == nil {
		return "", false
	}
	s.Lock()
	defer s.Unlock()
	k := s.getCacheKey(key)
	if idx, ok := s.keys.(*kaz2QIndex); ok {
		if seg, ok := idx.kazSegmentOf(k); ok {
			return seg.String(), true
		}
	} else if s.keys.peekAtime(k) != 0 {
		return "", true
	}
	_, pending := s.pages[key]
	return "", pending
}
