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
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
)

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
	if !store.conf.KazSkipUnuploadedDelete {
		// the local mark is only read by kazMarkUnuploaded (skip-delete)
		store.addDelayedStaging(key, path, added, false)
		return
	}
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
	if !store.conf.KazSkipUnuploadedDelete {
		return false
	}
	store.pendingMutex.Lock()
	_, ok := store.kazUnuploaded[key]
	delete(store.kazUnuploaded, key)
	store.pendingMutex.Unlock()
	if ok {
		store.kazDeleteSkipped.Inc()
	}
	return ok
}

// KazStagedRef names a single-block slice offered to the staging packer.
type KazStagedRef struct {
	ID     uint64
	Length int
}

// KazStagedData is the staged content of a claimed slice, with its storage tier.
type KazStagedData struct {
	ID   uint64
	Data []byte
	Tier uint8
}

// KazSetStagingPacker registers the packer called from uploadStagingFile for
// small single-block slices. It returns true when it took over the slice.
func (store *cachedStore) KazSetStagingPacker(p func(id uint64, length int) bool) {
	if p == nil {
		store.kazPacker.Store(nil)
		return
	}
	store.kazPacker.Store(&p)
}

// KazSetStagingForget registers f, called with the slice id when the staged
// block of a small single-block slice leaves the pending set (upload completed
// or retired), so the vfs pack index can drop it.
func (store *cachedStore) KazSetStagingForget(f func(id uint64)) {
	if f == nil {
		store.kazForget.Store(nil)
		return
	}
	store.kazForget.Store(&f)
}

// kazForgetStaged tells the registered forget callback that key left the
// pending set when it is block 0 of a single-block slice. The caller must not
// hold pendingMutex.
func (store *cachedStore) kazForgetStaged(key string) {
	f := store.kazForget.Load()
	if f == nil {
		return
	}
	if id, length, ok := kazParseSingleBlockKey(key); ok && length < store.conf.BlockSize {
		(*f)(id)
	}
}

// KazStagedPending reports, for each ref, whether its staged block is pending,
// waiting and never PUT, i.e. could still be packed. It takes pendingMutex once.
func (store *cachedStore) KazStagedPending(refs []KazStagedRef) []bool {
	res := make([]bool, len(refs))
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	for i, ref := range refs {
		if ref.Length <= 0 || ref.Length >= store.conf.BlockSize {
			continue
		}
		item := store.pendingKeys[sliceForRead(ref.ID, ref.Length, store).key(0)]
		res[i] = item != nil && item.kaz.state == kazStagingWait && !item.kaz.putStarted
	}
	return res
}

// KazStagingPackEvent adds n to kaz_staging_pack_total{event}.
func (store *cachedStore) KazStagingPackEvent(event string, n int) {
	if store.kazPackTotal != nil && n > 0 {
		store.kazPackTotal.WithLabelValues(event).Add(float64(n))
	}
}

// kazParseSingleBlockKey extracts id and length from a key "<...>/<id>_0_<length>"
// and reports false for any other block number or format.
func kazParseSingleBlockKey(key string) (id uint64, length int, ok bool) {
	name := key[strings.LastIndexByte(key, '/')+1:]
	parts := strings.Split(name, "_")
	if len(parts) != 3 || parts[1] != "0" {
		return 0, 0, false
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return 0, 0, false
	}
	l, err := strconv.Atoi(parts[2])
	if err != nil || l <= 0 {
		return 0, 0, false
	}
	return id, l, true
}

// kazTryPack offers a small single-block key to the registered packer. It does
// nothing outside the upload window or when the item is not a plain waiting one.
// The packer's result is ignored: the caller always continues into
// beginStagingUpload, which decides by the item's state (packed/claimed: no PUT).
func (store *cachedStore) kazTryPack(key string) {
	p := store.kazPacker.Load()
	if p == nil || !store.canUpload() {
		return
	}
	id, length, ok := kazParseSingleBlockKey(key)
	if !ok || length >= store.conf.BlockSize {
		return
	}
	store.pendingMutex.Lock()
	item := store.pendingKeys[key]
	eligible := item != nil && item.kaz.state == kazStagingWait && !item.kaz.putStarted && store.activeStaging[key] == 0
	store.pendingMutex.Unlock()
	if eligible {
		(*p)(id, length)
	}
}

// KazPutStaged puts the single-block object of slice id (length len(data), at most
// BlockSize) synchronously and caches it for reads. It does not take a currentUpload
// slot, so a packer running inside an uploader cannot deadlock on MaxUpload.
func (store *cachedStore) KazPutStaged(id uint64, data []byte, tier uint8) error {
	if len(data) == 0 || len(data) > store.conf.BlockSize {
		return fmt.Errorf("invalid staged block length %d for slice %d", len(data), id)
	}
	key := sliceForRead(id, len(data), store).key(0)
	block := NewOffPage(len(data))
	copy(block.Data, data)
	ctx := context.WithValue(context.Background(), object.TierKey{}, tier)
	// Populate the read cache the way upload does for a small synchronous block (the
	// cache takes its own reference), then PUT with the 3-try budget of uploadStagingFile:
	// a stalled object store must not hold the uploader, its claims and the chunk for minutes.
	store.bcache.cache(key, block, false, false)
	return store.upload(ctx, key, block, nil)
}

// KazClaimStaged claims the staged blocks of refs for packing. Items that are
// waiting, never PUT and not busy become claimed (even when queued for upload or
// still inside the upload delay) and their data is read from the staging file.
// Ids without a pending item are returned in absent; other ids are in neither.
// A returned slice may have been retired concurrently; the caller must drop ids
// that are not in the chunk records. Every claimed id must later be released:
// kazClaimedKeys entries are cleared only by KazReleaseStaged.
func (store *cachedStore) KazClaimStaged(refs []KazStagedRef) (claimed []KazStagedData, absent []uint64) {
	type claim struct {
		ref  KazStagedRef
		key  string
		item *pendingItem
		path string
	}
	var taken []claim
	store.pendingMutex.Lock()
	for _, ref := range refs {
		if ref.Length <= 0 || ref.Length >= store.conf.BlockSize {
			continue
		}
		key := sliceForRead(ref.ID, ref.Length, store).key(0)
		item := store.pendingKeys[key]
		if item == nil {
			absent = append(absent, ref.ID)
			continue
		}
		if item.kaz.state != kazStagingWait || item.kaz.putStarted || store.activeStaging[key] > 0 {
			continue
		}
		item.kaz.state = kazStagingClaimed
		if store.kazClaimedKeys == nil {
			store.kazClaimedKeys = make(map[uint64]string)
		}
		store.kazClaimedKeys[ref.ID] = key
		taken = append(taken, claim{ref, key, item, item.fpath})
	}
	store.pendingMutex.Unlock()

	var failed []claim
	for _, c := range taken {
		data, tier, err := store.kazReadStaged(c.path, c.ref.Length)
		if err != nil {
			logger.Warnf("claim staged %s: %s", c.key, err)
			failed = append(failed, c)
			continue
		}
		claimed = append(claimed, KazStagedData{ID: c.ref.ID, Data: data, Tier: tier})
	}
	if len(failed) > 0 {
		store.pendingMutex.Lock()
		for _, c := range failed {
			if store.pendingKeys[c.key] == c.item && c.item.kaz.state == kazStagingClaimed {
				c.item.kaz.state = kazStagingWait
			}
			delete(store.kazClaimedKeys, c.ref.ID)
		}
		store.pendingMutex.Unlock()
	}
	return claimed, absent
}

// kazReadStaged reads a staging file the same way uploadStagingFile does and
// returns the block data with the tier stored in its footer.
func (store *cachedStore) kazReadStaged(path string, blen int) ([]byte, uint8, error) {
	f, err := openCacheFile(path, blen, store.conf.CacheChecksum)
	if err != nil {
		return nil, 0, err
	}
	defer f.Close()
	data := make([]byte, blen)
	if _, err = f.ReadAt(data, 0); err != nil {
		return nil, 0, err
	}
	tier := uint8(0)
	footer := &stageFooter{}
	if ferr := footer.unmarshal(f); ferr != nil {
		logger.Warnf("Parse stage footer of %s failed, use default tier: %s", path, ferr)
	} else {
		tier = footer.Tier
	}
	return data, tier, nil
}

// KazReleaseStaged ends a claim. Packed ids become packed (never PUT singly);
// the others return to wait and are queued for upload unless already queued.
func (store *cachedStore) KazReleaseStaged(ids []uint64, packed map[uint64]bool) {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	for _, id := range ids {
		key, ok := store.kazClaimedKeys[id]
		if !ok {
			continue
		}
		delete(store.kazClaimedKeys, id)
		item := store.pendingKeys[key]
		if item == nil || item.kaz.state != kazStagingClaimed {
			continue
		}
		if packed[id] {
			item.kaz.state = kazStagingPacked
			continue
		}
		item.kaz.state = kazStagingWait
		if item.uploading.CompareAndSwap(false, true) {
			select {
			case store.pendingCh <- item:
			default:
				item.uploading.Store(false)
			}
		}
	}
}
