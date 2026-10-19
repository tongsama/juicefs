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
