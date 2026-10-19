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
// cap) and the shared metrics. It does nothing for other eviction policies.
// It must run before the store goroutines start.
func (cache *cacheStore) kazAttach(capacity int64) {
	idx, ok := cache.keys.(*kaz2QIndex)
	if !ok {
		return
	}
	idx.kazSetCapacity(capacity)
	if cache.m != nil {
		idx.kazSetMetrics(cache.m.kaz2Q)
	}
}
