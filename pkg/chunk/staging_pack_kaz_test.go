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
	"os"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// TestKazPendingPutMark checks that both PUT entry points mark the item as PUT-started.
func TestKazPendingPutMark(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	item := store.beginStagingUpload(key)
	require.NotNil(t, item)
	store.pendingMutex.Lock()
	require.True(t, item.kaz.putStarted)
	store.pendingMutex.Unlock()
	store.endStagingUpload(key, item)

	imm := store.beginImmediateStaging("imm-key", "/nonexistent", time.Now())
	require.NotNil(t, imm)
	store.pendingMutex.Lock()
	require.True(t, imm.kaz.putStarted)
	store.pendingMutex.Unlock()
}

// TestKazPendingLocalMark checks that only an item newly created by the writer is marked local.
func TestKazPendingLocalMark(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	path1, err := store.bcache.stage("k1", []byte("a"), 0)
	require.NoError(t, err)
	store.kazAddLocalStaging("k1", path1, time.Now())
	store.pendingMutex.Lock()
	require.True(t, store.pendingKeys["k1"].kaz.local)
	store.pendingMutex.Unlock()

	path2, err := store.bcache.stage("k2", []byte("b"), 0)
	require.NoError(t, err)
	store.addDelayedStaging("k2", path2, time.Now(), false)
	store.kazAddLocalStaging("k2", path2, time.Now())
	store.pendingMutex.Lock()
	require.False(t, store.pendingKeys["k2"].kaz.local)
	store.pendingMutex.Unlock()
}

// TestKazClaimedNotUploaded checks that a claimed item is neither PUT nor scanned.
func TestKazClaimedNotUploaded(t *testing.T) {
	store, _, _ := newRetireFixture(t, 0)
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	store.pendingMutex.Lock()
	item := store.pendingKeys[key]
	item.kaz.state = kazStagingClaimed
	store.pendingMutex.Unlock()
	// drain anything queued by admission with delay 0
	for len(store.pendingCh) > 0 {
		<-store.pendingCh
	}
	item.uploading.Store(false)

	require.Nil(t, store.beginStagingUpload(key))
	require.False(t, item.uploading.Load())
	store.scanDelayedStaging()
	require.Empty(t, store.pendingCh)
	require.False(t, item.uploading.Load())
}

// kazLocalStagedSlice stages a block as the writer does and returns its key and staging path.
func kazLocalStagedSlice(t *testing.T, store *cachedStore, id uint64, data []byte) (string, string) {
	t.Helper()
	key := sliceForRead(id, len(data), store).key(0)
	path, err := store.bcache.stage(key, data, 0)
	require.NoError(t, err)
	store.kazAddLocalStaging(key, path, time.Now())
	return key, path
}

// TestKazSkipDeleteUnuploaded checks that a never-PUT local block is removed without DELETE.
func TestKazSkipDeleteUnuploaded(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key, path := kazLocalStagedSlice(t, store, 1, []byte("aaa"))
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 0, objects.deleteCount(key))
	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err))
	require.Equal(t, float64(1), testutil.ToFloat64(store.kazDeleteSkipped))
}

// TestKazSkipDeleteDisabled checks that the setting off keeps the DELETE.
func TestKazSkipDeleteDisabled(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	key, _ := kazLocalStagedSlice(t, store, 1, []byte("aaa"))
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 1, objects.deleteCount(key))
}

// TestKazSkipDeleteAfterPutStarted checks that a block whose PUT started is deleted.
func TestKazSkipDeleteAfterPutStarted(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key, _ := kazLocalStagedSlice(t, store, 1, []byte("aaa"))
	item := store.beginStagingUpload(key)
	require.NotNil(t, item)
	store.endStagingUpload(key, item)
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 1, objects.deleteCount(key))
}

// TestKazSkipDeleteRaceWithUploader checks that no object is left when upload and Remove race.
func TestKazSkipDeleteRaceWithUploader(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	for i := 0; i < 200; i++ {
		id := uint64(100 + i)
		key, path := kazLocalStagedSlice(t, store, id, []byte("aaa"))
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); store.uploadStagingFile(key, path) }()
		go func() {
			defer wg.Done()
			for store.Remove(id, 3) != nil {
				time.Sleep(time.Millisecond)
			}
		}()
		wg.Wait()
		_, err := objects.ObjectStorage.Head(context.Background(), key)
		require.Error(t, err, "object %s left behind", key)
	}
}

// TestKazSkipDeleteScanItem checks that a restart-scan item is always deleted.
func TestKazSkipDeleteScanItem(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 1, objects.deleteCount(key))
}

// TestKazSkipDeleteImmediate checks that an immediate-staging item is always deleted.
func TestKazSkipDeleteImmediate(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key := sliceForRead(1, 3, store).key(0)
	item := store.beginImmediateStaging(key, "/nonexistent", time.Now())
	require.NotNil(t, item)
	store.endStagingUpload(key, item)
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 1, objects.deleteCount(key))
}

// TestKazSkipDeleteDeferredTwoRetires checks the deferred GC flow: Retire, then Remove.
func TestKazSkipDeleteDeferredTwoRetires(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key, _ := kazLocalStagedSlice(t, store, 1, []byte("aaa"))
	require.NoError(t, store.Retire(1, 3))
	require.NoError(t, store.Remove(1, 3))
	require.Equal(t, 0, objects.deleteCount(key))
}

// TestKazSkipDeleteBusyNotRecorded checks that an in-flight upload keeps the DELETE.
func TestKazSkipDeleteBusyNotRecorded(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	store.conf.KazSkipUnuploadedDelete = true
	key, _ := kazLocalStagedSlice(t, store, 1, []byte("aaa"))
	store.pendingMutex.Lock()
	store.activeStaging = map[string]int{key: 1}
	store.pendingMutex.Unlock()
	require.ErrorIs(t, store.Retire(1, 3), ErrSliceUploadInFlight)
	store.pendingMutex.Lock()
	_, ok := store.kazUnuploaded[key]
	store.pendingMutex.Unlock()
	require.False(t, ok)
}

// kazStateOf returns the kaz state of the pending item of key under pendingMutex.
func kazStateOf(store *cachedStore, key string) (kazStagingState, bool) {
	store.pendingMutex.Lock()
	defer store.pendingMutex.Unlock()
	item := store.pendingKeys[key]
	if item == nil {
		return 0, false
	}
	return item.kaz.state, true
}

// TestKazClaimQueuedItem checks that an item already in the upload channel can be claimed.
func TestKazClaimQueuedItem(t *testing.T) {
	store, _, _ := newRetireFixture(t, 0)
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	require.Len(t, store.pendingCh, 1)
	claimed, absent := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}, {ID: 2, Length: 3}})
	require.Len(t, claimed, 1)
	require.Equal(t, uint64(1), claimed[0].ID)
	require.Equal(t, []byte("aaa"), claimed[0].Data)
	require.Equal(t, []uint64{2}, absent)
	st, _ := kazStateOf(store, key)
	require.Equal(t, kazStagingClaimed, st)
}

// TestKazClaimSkipsPutStarted checks that an item whose PUT started is neither claimed nor absent.
func TestKazClaimSkipsPutStarted(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	store.pendingMutex.Lock()
	store.pendingKeys[key].kaz.putStarted = true
	store.pendingMutex.Unlock()
	claimed, absent := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}})
	require.Empty(t, claimed)
	require.Empty(t, absent)
}

// TestKazReleaseRequeuesOnce checks that release queues only items that are not already queued.
func TestKazReleaseRequeuesOnce(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	k1 := stageRetireSlice(t, store, 1, []byte("aaa"))
	k2 := stageRetireSlice(t, store, 2, []byte("bbb"))
	store.pendingMutex.Lock()
	store.pendingKeys[k2].uploading.Store(true) // as if already in the channel
	store.pendingMutex.Unlock()
	claimed, _ := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}, {ID: 2, Length: 3}})
	require.Len(t, claimed, 2)
	require.Empty(t, store.pendingCh)
	store.KazReleaseStaged([]uint64{1, 2, 99}, nil)
	require.Len(t, store.pendingCh, 1)
	require.Equal(t, k1, (<-store.pendingCh).key)
	st, _ := kazStateOf(store, k2)
	require.Equal(t, kazStagingWait, st)
}

// TestKazReleasePacked checks that a packed item is never PUT by the uploader.
func TestKazReleasePacked(t *testing.T) {
	store, _, objects := newRetireFixture(t, 0)
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	claimed, _ := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}})
	require.Len(t, claimed, 1)
	store.KazReleaseStaged([]uint64{1}, map[uint64]bool{1: true})
	for len(store.pendingCh) > 0 {
		item := <-store.pendingCh
		store.uploadStagingFile(item.key, item.fpath)
	}
	store.uploadStagingFile(key, "")
	require.Equal(t, int64(0), objects.putCalls.Load())
	st, _ := kazStateOf(store, key)
	require.Equal(t, kazStagingPacked, st)
}

// TestKazTryPackHook checks when the packer is consulted from uploadStagingFile.
func TestKazTryPackHook(t *testing.T) {
	store, _, objects := newRetireFixture(t, time.Hour)
	var calls []KazStagedRef
	ret := true
	store.KazSetStagingPacker(func(id uint64, length int) bool {
		calls = append(calls, KazStagedRef{ID: id, Length: length})
		return ret
	})
	key := stageRetireSlice(t, store, 1, []byte("aaa"))
	store.uploadStagingFile(key, "")
	require.Equal(t, int64(0), objects.putCalls.Load())
	require.Equal(t, []KazStagedRef{{ID: 1, Length: 3}}, calls)

	ret = false
	key = stageRetireSlice(t, store, 2, []byte("bbb"))
	store.pendingMutex.Lock()
	path := store.pendingKeys[key].fpath
	store.pendingMutex.Unlock()
	store.uploadStagingFile(key, path)
	require.Equal(t, int64(1), objects.putCalls.Load())

	// block number 1 and a full block are never offered to the packer.
	calls = nil
	for i, ln := range []int{4096 + 3, 4096} {
		id := uint64(10 + i)
		r := sliceForRead(id, ln, store)
		k := r.key(r.index(ln - 1))
		if ln == 4096 {
			k = r.key(0)
		}
		data := make([]byte, parseObjOrigSize(k))
		p, err := store.bcache.stage(k, data, 0)
		require.NoError(t, err)
		store.addDelayedStaging(k, p, time.Now(), false)
		store.uploadStagingFile(k, p)
	}
	require.Empty(t, calls)
}

// TestKazClaimedRetireSucceeds checks that Retire is not blocked by a claimed item.
func TestKazClaimedRetireSucceeds(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	stageRetireSlice(t, store, 1, []byte("aaa"))
	claimed, _ := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}})
	require.Len(t, claimed, 1)
	require.NoError(t, store.Retire(1, 3))
}

// TestKazClaimIgnoresDelay checks that an item still inside the upload delay can be claimed.
func TestKazClaimIgnoresDelay(t *testing.T) {
	store, _, _ := newRetireFixture(t, 60*time.Second)
	stageRetireSlice(t, store, 1, []byte("aaa"))
	require.Empty(t, store.pendingCh)
	claimed, absent := store.KazClaimStaged([]KazStagedRef{{ID: 1, Length: 3}})
	require.Len(t, claimed, 1)
	require.Empty(t, absent)
}
