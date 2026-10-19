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

// KazFlushCacheState saves the kaz-2q state file of every disk cache store
// (called at a normal umount). It does nothing for memory caches and other
// eviction policies; failures are only logged and counted.
func (store *cachedStore) KazFlushCacheState() {
	if m, ok := store.bcache.(interface{ kazFlushState() }); ok {
		m.kazFlushState()
	}
}
