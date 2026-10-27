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
	"path/filepath"
	"strings"
	"time"
)

// kazDiskCache and kazNewDiskCache are the names kaz code uses for the local
// disk cache store, so that upstream's kazDiskCache -> diskCache rename only
// needs to change these two definitions.
type kazDiskCache = cacheStore

var kazNewDiskCache = newCacheStore

// flushPageOpt is a copy of upstream's flushPage with kaz's durable handling added.
// With durable set, the data is synced before the rename publishes the name, and the
// rename plus the directory chain are synced afterwards, so an OS crash cannot leave a
// published block with missing contents or lose its name. When upstream changes
// flushPage, apply the same fix here.
func (cache *kazDiskCache) flushPageOpt(path string, data []byte, dropCache bool, tierID uint8, durable bool) (err error) {
	if !cache.available() {
		return errCacheDown
	}

	start := time.Now()
	cache.m.cacheWrites.Add(1)
	cache.m.cacheWriteBytes.Add(float64(len(data)))
	defer func() {
		cache.m.cacheWriteHist.Observe(time.Since(start).Seconds())
	}()
	dir := filepath.Dir(path)
	cache.createDir(dir)
	tmp := path + ".tmp"

	var f *os.File
	err = cache.checkErr(func() error {
		f, err = os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE, cache.mode)
		return err
	})
	if err != nil {
		logger.Warnf("Can't create cache file %s: %s", tmp, err)
		return err
	}

	defer func() {
		if err != nil {
			_ = cache.removeFile(tmp)
		}
	}()

	if err = cache.writeFile(f, data); err != nil {
		logger.Warnf("Write to cache file %s failed: %s", tmp, err)
		_ = f.Close()
		return
	}
	if cache.checksum != CsNone {
		if err = cache.writeFile(f, checksum(data)); err != nil {
			logger.Warnf("Write checksum to cache file %s failed: %s", tmp, err)
			_ = f.Close()
			return
		}
	}
	if tierID != 0 {
		// only staged file has a footer
		footer := stageFooter{Tier: tierID}
		var fData []byte
		fData, err = (&footer).marshal(cache.checksum != CsNone)
		if err != nil {
			logger.Warnf("Marshal stage footer for cache file %s failed: %s", tmp, err)
			_ = f.Close()
			return
		}
		if err = cache.writeFile(f, fData); err != nil {
			logger.Warnf("Write stage footer to cache file %s failed: %s", tmp, err)
			_ = f.Close()
			return
		}
	}
	if durable {
		if err = cache.checkErr(func() error { return syncStagingFile(f) }); err != nil {
			logger.Warnf("Sync cache file %s failed: %s", tmp, err)
			_ = f.Close()
			return
		}
	}
	if dropCache {
		dropOSCache(f)
	}
	if err = cache.closeFile(f); err != nil {
		logger.Warnf("Close cache file %s failed: %s", tmp, err)
		return
	}
	if err = cache.renameFile(tmp, path); err != nil {
		logger.Warnf("Rename cache file %s -> %s failed: %s", tmp, path, err)
		return
	}
	if durable {
		if err = cache.syncDirs(dir); err != nil {
			logger.Warnf("Sync directory of cache file %s failed: %s", path, err)
			// The name may not survive a crash; withdraw it so the caller falls back to a direct upload.
			_ = cache.removeFile(path)
		}
	}
	return
}

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
func (cache *kazDiskCache) kazAttach(capacity int64) {
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
func (cache *kazDiskCache) kazLoadNoTouch(key string) (ReadCloser, error) {
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
// routing and legacy-store fallback, using kazDiskCache.kazLoadNoTouch.
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
// asked separately (one store lock at a time) with kazPlacementHot, and when
// any source is cached the placement is registered with kazExpect in the
// output store, to be consumed by the add after cache() flushes the block. The
// segment follows the output store's placement mode (from
// Config.KazCacheCompactionPlacement, see kazPlacementSegment). In every mode
// the atime is the newest among the cached sources, and an output placed in
// protected gets the read mark when a protected source has it (an output in
// probation never does). With --kaz-cache-compaction-hot-age a young
// protected source (a compaction output cached less than hot-age ago) is not
// a protected source: it adds no protected bytes, no whole protected copy and
// no read mark; when that turns the decision from protected into probation,
// compaction_output_hot is counted. It returns whether the caller should
// cache the output and counts compaction_output_cached or
// compaction_output_skipped. Sources that are only pending in memory, or
// only in a legacy store, are not considered.
func (m *cacheManager) kazCompactionPlacement(outKey string, outBytes uint32, srcs []kazPlacementSource) bool {
	var atime uint32
	// prot counts the protected sources; all also counts the young ones, to
	// tell whether excluding them changed the decision.
	var prot, all kazProtectedSources
	found := false
	protectedRead := false // some (not young) protected source has the read mark
	for _, src := range srcs {
		s := m.getStore(src.key)
		if s == nil {
			continue
		}
		s.Lock()
		if idx, ok := s.keys.(*kaz2QIndex); ok {
			if sg, at, read, hot, ok := idx.kazPlacementHot([]cacheKey{s.getCacheKey(src.key)}); ok {
				if sg == kazProtected {
					prot.add(src)
					all.add(src)
					protectedRead = protectedRead || read
				} else if hot {
					all.add(src)
				}
				atime = max(atime, at)
				found = true
			}
		}
		s.Unlock()
	}
	out := m.getStore(outKey)
	placed, hot := false, false
	if found && out != nil {
		out.Lock()
		if idx, ok := out.keys.(*kaz2QIndex); ok {
			seg := kazPlacementSegment(idx.placement, prot, outBytes, idx.shareRatio)
			hot = seg == kazProbation && kazPlacementSegment(idx.placement, all, outBytes, idx.shareRatio) == kazProtected
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
		if hot {
			km.events.WithLabelValues("compaction_output_hot").Inc()
		}
	}
	return placed
}

// kazProtectedSources sums the protected source blocks of a compaction output
// for kazPlacementSegment.
type kazProtectedSources struct {
	any   bool   // some source is protected
	whole bool   // some protected source was copied whole
	bytes uint64 // bytes copied from protected sources (share)
}

// add counts src as a protected source.
func (p *kazProtectedSources) add(src kazPlacementSource) {
	p.any = true
	p.whole = p.whole || src.whole
	p.bytes += uint64(src.bytes)
}

// kazPlacementSegment returns the segment of a compaction output under the
// placement mode from its protected sources prot: inherit = protected if any
// source is protected; probation = always probation; replace = protected only
// if a protected source was copied whole; share = protected if the protected
// bytes are at least ratio of outBytes (the non-hole bytes of the output
// block; 0 gives probation).
func kazPlacementSegment(mode string, prot kazProtectedSources, outBytes uint32, ratio float64) kazSegment {
	var protected bool
	switch mode {
	case KazPlacementProbation:
	case KazPlacementReplace:
		protected = prot.whole
	case KazPlacementShare:
		protected = kazShareProtected(prot.bytes, outBytes, ratio)
	default: // inherit
		protected = prot.any
	}
	if protected {
		return kazProtected
	}
	return kazProbation
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

// syncDirs persists the entry renamed into dir, then the entry of dir and each ancestor below cache.dir in
// its parent, stopping at the first dir this process already made durable. A dir is recorded only after its
// whole chain is synced, so a concurrent writer never skips a parent sync that is still in progress.
func (cache *kazDiskCache) syncDirs(dir string) error {
	if err := cache.checkErr(func() error { return syncStagingDir(dir) }); err != nil {
		return err
	}
	root := filepath.Clean(cache.dir)
	var chain []string
	cache.durableMu.Lock()
	for d := filepath.Clean(dir); d != root && strings.HasPrefix(d, root+string(filepath.Separator)); d = filepath.Dir(d) {
		if _, ok := cache.durableDirs[d]; ok {
			break
		}
		chain = append(chain, d)
	}
	cache.durableMu.Unlock()
	for _, d := range chain {
		parent := filepath.Dir(d)
		if err := cache.checkErr(func() error { return syncStagingDir(parent) }); err != nil {
			return err
		}
	}
	cache.durableMu.Lock()
	for _, d := range chain {
		cache.durableDirs[d] = struct{}{}
	}
	cache.durableMu.Unlock()
	return nil
}

// forgetDurableDir drops dir after it is removed, so a recreated dir is synced into its parent again.
func (cache *kazDiskCache) forgetDurableDir(dir string) {
	cache.durableMu.Lock()
	delete(cache.durableDirs, filepath.Clean(dir))
	cache.durableMu.Unlock()
}

// removeLocal returns unlink failures and retries staged paths even after index removal.
func (cache *kazDiskCache) removeLocal(key string, staging bool) error {
	cache.Lock()
	delete(cache.pages, key)
	path := cache.cachePath(key)
	k := cache.getCacheKey(key)
	if it := cache.keys.remove(k, staging); it != nil {
		if it.size > 0 {
			cache.used -= int64(it.size + 4096)
		}
	} else if !staging {
		path = ""
	}
	cache.Unlock()
	var err error
	if path != "" {
		if e := cache.removeFile(path); e != nil && !os.IsNotExist(e) {
			err = e
		}
	}
	// Stage files may survive a previous unlink failure even when the cache index is gone.
	if staging {
		err = errors.Join(err, cache.removeStage(key))
	}
	return err
}

// retire returns local cache/stage cleanup errors for durably obsolete slices.
func (m *cacheManager) retire(key string) error {
	store := m.getStore(key)
	if store == nil {
		return errCacheDown
	}
	return store.removeLocal(key, true)
}

// syncStagingFile and syncStagingDir make staged writeback blocks durable; tests replace them to observe ordering.
var (
	syncStagingFile = fdatasyncFile
	syncStagingDir  = fsyncDir
)
