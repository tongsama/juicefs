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

import "time"

// This file holds the kaz staging-pack state of pending items.

// kazStagingState tells whether a pending staged block may still be uploaded by itself.
type kazStagingState uint8

const (
	// kazStagingWait: the item is uploaded individually as usual.
	kazStagingWait kazStagingState = iota
	// kazStagingClaimed: a packer owns the item; the single-object upload must not run.
	kazStagingClaimed
	// kazStagingPacked: the item's data is already stored inside a pack object.
	kazStagingPacked
)

// kazPendingInfo is the kaz-only part of pendingItem. It is read and written
// only while holding cachedStore.pendingMutex.
type kazPendingInfo struct {
	state      kazStagingState
	putStarted bool // a single-object PUT of this key has started at least once
	local      bool // the item was created by this mount's writer (not by a restart scan)
}

// kazAddLocalStaging registers a staging file written by this mount and marks the
// item local only when no pending item existed before the call.
func (store *cachedStore) kazAddLocalStaging(key, path string, added time.Time) {
	store.pendingMutex.Lock()
	existed := store.pendingKeys[key] != nil
	store.pendingMutex.Unlock()
	store.addDelayedStaging(key, path, added, false)
	if existed {
		return
	}
	store.pendingMutex.Lock()
	if item := store.pendingKeys[key]; item != nil {
		item.kaz.local = true
	}
	store.pendingMutex.Unlock()
}

// kazUnuploadedMax bounds the set of retired keys waiting for their DELETE to be skipped.
const kazUnuploadedMax = 1 << 20

// kazMarkUnuploaded records key as never PUT when the setting is on and the pending
// item is local with no PUT started. The caller holds pendingMutex and must call it
// before removing the item. It reports whether the key was added to the set.
func (store *cachedStore) kazMarkUnuploaded(key string) bool {
	if !store.conf.KazSkipUnuploadedDelete {
		return false
	}
	item := store.pendingKeys[key]
	if item == nil || !item.kaz.local || item.kaz.putStarted {
		return false
	}
	if _, ok := store.kazUnuploaded[key]; ok || len(store.kazUnuploaded) >= kazUnuploadedMax {
		return false
	}
	if store.kazUnuploaded == nil {
		store.kazUnuploaded = make(map[string]struct{})
	}
	store.kazUnuploaded[key] = struct{}{}
	return true
}

// kazUnmarkUnuploaded takes back marks made by a Retire that did not fully succeed.
func (store *cachedStore) kazUnmarkUnuploaded(keys []string) {
	if len(keys) == 0 {
		return
	}
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	for _, key := range keys {
		delete(store.kazUnuploaded, key)
	}
}

// kazForgetUnuploaded drops the mark of key because a PUT of it is starting. The
// caller holds pendingMutex.
func (store *cachedStore) kazForgetUnuploaded(key string) {
	delete(store.kazUnuploaded, key)
}

// kazTakeUnuploaded removes key from the never-PUT set and reports whether it was
// there, i.e. whether the object DELETE can be skipped.
func (store *cachedStore) kazTakeUnuploaded(key string) bool {
	store.pendingMutex.Lock()
	_, ok := store.kazUnuploaded[key]
	delete(store.kazUnuploaded, key)
	store.pendingMutex.Unlock()
	if ok {
		store.kazDeleteSkipped.Inc()
	}
	return ok
}
