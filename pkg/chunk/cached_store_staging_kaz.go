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
	"time"
)

// This file holds the kaz staging-upload bookkeeping and slice retirement of cachedStore.
// They keep durable GC intent consistent with in-flight staging writes and uploads.

// beginStageWrite keeps a late local stage callback visible to durable GC retirement.
func (store *cachedStore) beginStageWrite(key string) {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	if store.activeStageWrites == nil {
		store.activeStageWrites = make(map[string]int)
	}
	store.activeStageWrites[key]++
}

// endStageWrite drops tracking only after a late callback's local cleanup has completed.
func (store *cachedStore) endStageWrite(key string) {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	if store.activeStageWrites[key] <= 1 {
		delete(store.activeStageWrites, key)
	} else {
		store.activeStageWrites[key]--
	}
}

// beginStagingUpload registers actual I/O before retirement can remove its pending entry.
// The caller owns a currentUpload slot before calling this method.
func (store *cachedStore) beginStagingUpload(key string) *pendingItem {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	item := store.pendingKeys[key]
	if item != nil {
		if item.kaz.state != kazStagingWait {
			// Claimed or packed items are not uploaded individually.
			item.uploading.Store(false)
			return nil
		}
		item.kaz.putStarted = true
		store.kazForgetUnuploaded(key)
		if store.activeStaging == nil {
			store.activeStaging = make(map[string]int)
		}
		store.activeStaging[key]++
	}
	return item
}

// beginImmediateStaging publishes pending and active state before a writeback writer acknowledges staging.
func (store *cachedStore) beginImmediateStaging(key, path string, added time.Time) *pendingItem {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	item := store.pendingKeys[key]
	if item == nil {
		item = &pendingItem{key: key, fpath: path, ts: added}
		store.pendingKeys[key] = item
	}
	if !item.uploading.CompareAndSwap(false, true) {
		return nil
	}
	item.kaz.putStarted = true
	store.kazForgetUnuploaded(key)
	if store.activeStaging == nil {
		store.activeStaging = make(map[string]int)
	}
	store.activeStaging[key]++
	return item
}

// endStagingUpload releases bounded active bookkeeping after upload and cleanup have finished.
func (store *cachedStore) endStagingUpload(key string, item *pendingItem) {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	if store.activeStaging[key] <= 1 {
		delete(store.activeStaging, key)
	} else {
		store.activeStaging[key]--
	}
	item.uploading.Store(false)
}

// isPendingItem distinguishes the current upload from a canceled older generation.
func (store *cachedStore) isPendingItem(key string, item *pendingItem) bool {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	return store.pendingKeys[key] == item
}

// finishStagingUpload keeps abandoned cleanup inside the active-I/O guard.
func (store *cachedStore) finishStagingUpload(key string, item *pendingItem, blen int) {
	if !store.isPendingItem(key, item) {
		err := store.delete(key)
		logger.Infof("Key %s is not needed, abandoned, err: %v", key, err)
		return
	}
	store.bcache.uploaded(key, blen)
	store.pendingMutex.Lock()
	if store.pendingKeys[key] == item {
		delete(store.pendingKeys, key)
	}
	store.pendingMutex.Unlock()
	if err := store.bcache.removeStage(key); err != nil {
		logger.Warnf("failed to remove stage %s after upload: %s", key, err)
	}
}

// ErrSliceUploadInFlight means persisted GC intent must remain until staging writes/uploads finish.
var ErrSliceUploadInFlight = errors.New("slice has an in-flight staging upload")

// retireCache cancels cache/stage files without object storage I/O and propagates unlink failures.
func (store *cachedStore) retireCache(key string) error {
	if cache, ok := store.bcache.(interface{ retire(string) error }); ok {
		return cache.retire(key)
	}
	store.bcache.remove(key, true)
	return store.bcache.removeStage(key)
}

// Retire cancels local data only for slice references already durably retired by metadata.
// An in-flight error is retryable: the caller must retain its durable dead marker and
// must not hand physical deletion off as completed while a PUT can recreate the object.
func (store *cachedStore) Retire(id uint64, length int) error {
	keys := kazSliceKeys(id, length, store)
	var err error
	busy := false
	var marked []string
	first := true
	cancel := func() {
		store.pendingMutex.Lock()
		defer store.pendingMutex.Unlock()
		for _, key := range keys {
			if first {
				// kaz: remember keys whose PUT never started, so the later DELETE can be skipped.
				if store.kazMarkUnuploaded(key) {
					marked = append(marked, key)
				}
			}
			delete(store.pendingKeys, key)
			if store.activeStaging[key] > 0 || store.activeStageWrites[key] > 0 {
				busy = true
			}
		}
	}
	cancel()
	first = false
	for _, key := range keys {
		err = errors.Join(err, store.retireCache(key))
	}
	// A scanner can register and start between initial cancellation and local unlink.
	// Rechecking after unlink sees that I/O; later admission rejects the absent stage file.
	cancel()
	if busy {
		err = errors.Join(err, ErrSliceUploadInFlight)
	}
	// kaz: keep the DELETE (safe side) unless every unlink succeeded and nothing is in flight.
	if err != nil {
		store.kazUnmarkUnuploaded(marked)
	}
	return err
}
