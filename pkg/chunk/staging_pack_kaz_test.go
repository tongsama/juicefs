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
	"testing"
	"time"

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
