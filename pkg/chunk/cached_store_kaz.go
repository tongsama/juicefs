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
