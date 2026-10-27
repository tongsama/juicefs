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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/davies/groupcache/consistenthash"
	"github.com/juicedata/juicefs/pkg/compress"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/stretchr/testify/require"
)

// retireStorage gates real memory object requests to expose upload/delete ordering.
type retireStorage struct {
	object.ObjectStorage
	putKey, deleteKey                              string
	putEntered, putGate, deleteEntered, deleteGate chan struct{}
	putCalls                                       atomic.Int64
	putOnce, deleteOnce                            sync.Once
	deleteFail                                     atomic.Bool
	putFail                                        atomic.Bool
	mu                                             sync.Mutex
	deletes                                        map[string]int
}

// Put blocks only the selected request and then preserves actual object bytes.
func (s *retireStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	s.putCalls.Add(1)
	if key == s.putKey {
		s.putOnce.Do(func() { close(s.putEntered) })
		select {
		case <-s.putGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.putFail.Load() {
		return errors.New("injected PUT failure")
	}
	return s.ObjectStorage.Put(ctx, key, in, getters...)
}

// Delete counts requests and optionally blocks or fails an obsolete object's deletion.
func (s *retireStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	s.mu.Lock()
	s.deletes[key]++
	s.mu.Unlock()
	if key == s.deleteKey {
		s.deleteOnce.Do(func() { close(s.deleteEntered) })
		select {
		case <-s.deleteGate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if s.deleteFail.Load() {
		return errors.New("injected DELETE failure")
	}
	return s.ObjectStorage.Delete(ctx, key, getters...)
}

// deleteCount snapshots the remote deletion count for one key.
func (s *retireStorage) deleteCount(key string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.deletes[key]
}

// newRetireFixture uses actual disk staging/checksums without unrelated disk-maintenance goroutines.
func newRetireFixture(t *testing.T, delay time.Duration) (*cachedStore, *cacheStore, *retireStorage) {
	t.Helper()
	blob, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	objectStore := &retireStorage{ObjectStorage: blob, deletes: make(map[string]int)}
	conf := Config{BlockSize: 4096, CacheDir: t.TempDir(), CacheSize: 1 << 20, CacheMode: 0600, CacheChecksum: "extend", CacheEviction: "lru", Compress: "lz4", MaxUpload: 2, MaxDownload: 2, BufferSize: 1 << 20, PutTimeout: time.Second, GetTimeout: time.Second, Writeback: true, WritebackThresholdSize: 4097, UploadDelay: delay}
	metrics := newCacheManagerMetrics(nil)
	index, err := NewKeyIndex(&conf)
	require.NoError(t, err)
	disk := &cacheStore{id: "retire-test", dir: conf.CacheDir + string(filepath.Separator), mode: 0600, capacity: int64(conf.CacheSize), maxItems: 1000, checksum: conf.CacheChecksum, keys: index, pages: make(map[string]*Page), m: metrics, opTs: make(map[time.Duration]func() error), scanned: true}
	disk.state = newDCState(dcNormal, disk)
	manager := &cacheManager{consistentMap: consistenthash.New(100, nil), storeMap: map[string]*cacheStore{disk.id: disk}, stores: []*cacheStore{disk}, metrics: metrics}
	manager.consistentMap.Add(disk.id)
	store := &cachedStore{storage: objectStore, conf: conf, bcache: manager, compressor: compress.NewCompressor(conf.Compress), currentUpload: make(chan struct{}, conf.MaxUpload), currentDownload: make(chan struct{}, conf.MaxDownload), pendingKeys: make(map[string]*pendingItem), pendingCh: make(chan *pendingItem, 100), group: NewController()}
	store.initMetrics()
	return store, disk, objectStore
}

// stageRetireSlice stores a checksum-protected live slice through the real disk staging code.
func stageRetireSlice(t *testing.T, store *cachedStore, id uint64, data []byte) string {
	t.Helper()
	key := sliceForRead(id, len(data), store).key(0)
	path, err := store.bcache.stage(key, data, 0)
	require.NoError(t, err)
	store.addDelayedStaging(key, path, time.Now(), false)
	return key
}

// TestRetireIndependentOfRemoteDelete cancels another obsolete local stage without waiting for remote DELETE.
func TestRetireIndependentOfRemoteDelete(t *testing.T) {
	store, disk, objects := newRetireFixture(t, time.Hour)
	a := stageRetireSlice(t, store, 1, []byte("aaa"))
	b := stageRetireSlice(t, store, 2, []byte("bbb"))
	live := stageRetireSlice(t, store, 3, []byte("live"))
	objects.deleteKey = a
	objects.deleteEntered = make(chan struct{})
	objects.deleteGate = make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(objects.deleteGate) })
	removed := make(chan error, 1)
	go func() { removed <- store.Remove(1, 3) }()
	select {
	case <-objects.deleteEntered:
	case <-time.After(time.Second):
		t.Fatal("remote delete did not start")
	}
	done := make(chan error, 1)
	go func() { done <- store.Retire(2, 3) }()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("local retirement waited on unrelated DELETE")
	}
	require.False(t, store.isPendingValid(b))
	_, err := os.Stat(disk.stagePath(b))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(disk.cachePath(b))
	require.True(t, os.IsNotExist(err))
	require.Zero(t, objects.deleteCount(b), "Retire must not issue remote DELETE")
	page := NewPage(make([]byte, 4))
	defer page.Release()
	n, err := store.NewReader(3, 4).ReadAt(context.Background(), page, 0)
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Equal(t, "live", string(page.Data))
	require.True(t, store.isPendingValid(live))
	release.Do(func() { close(objects.deleteGate) })
	require.NoError(t, <-removed)
}

// TestRetireUploadInFlight retains durable GC responsibility until an abandoned PUT is done.
func TestRetireUploadInFlight(t *testing.T) {
	for _, immediate := range []bool{false, true} {
		t.Run(stringMode(immediate), func(t *testing.T) {
			delay := time.Hour
			if immediate {
				delay = 0
			}
			store, disk, objects := newRetireFixture(t, delay)
			data := []byte("inflight")
			key := sliceForRead(7, len(data), store).key(0)
			objects.putKey = key
			objects.putEntered = make(chan struct{})
			objects.putGate = make(chan struct{})
			objects.deleteFail.Store(true)
			var release sync.Once
			defer release.Do(func() { close(objects.putGate) })
			uploadDone := make(chan struct{})
			if immediate {
				writer := store.NewWriter(7, 0)
				_, err := writer.WriteAt(data, 0)
				require.NoError(t, err)
				require.NoError(t, writer.Finish(len(data)))
			} else {
				stageRetireSlice(t, store, 7, data)
				go func() { store.uploadStagingFile(key, disk.stagePath(key)); close(uploadDone) }()
			}
			select {
			case <-objects.putEntered:
			case <-time.After(time.Second):
				t.Fatal("staging PUT did not start")
			}
			require.ErrorIs(t, store.Retire(7, len(data)), ErrSliceUploadInFlight, "in-flight PUT must keep the metadata marker alive")
			require.False(t, store.isPendingValid(key))
			_, err := os.Stat(disk.stagePath(key))
			require.True(t, os.IsNotExist(err))
			require.ErrorIs(t, store.Remove(7, len(data)), ErrSliceUploadInFlight, "physical deletion must not clear intent before a PUT finishes")
			require.Zero(t, objects.deleteCount(key))
			release.Do(func() { close(objects.putGate) })
			if !immediate {
				select {
				case <-uploadDone:
				case <-time.After(time.Second):
					t.Fatal("upload cleanup did not finish")
				}
			}
			require.Eventually(t, func() bool { return objects.deleteCount(key) > 0 }, time.Second, time.Millisecond, "abandoned successful PUT still attempts cleanup")
			// The failed abandoned DELETE leaves the object, but retrying persisted GC can delete it.
			require.Eventually(t, func() bool { return store.Retire(7, len(data)) == nil }, time.Second, time.Millisecond)
			objects.deleteFail.Store(false)
			require.NoError(t, store.Remove(7, len(data)))
			_, err = objects.Head(context.Background(), key)
			require.Error(t, err)
		})
	}
}

// stringMode gives names to the two production writeback upload paths.
func stringMode(immediate bool) string {
	if immediate {
		return "immediate"
	}
	return "queued"
}

// TestRetireStageRemovalError keeps cleanup failures visible and retryable after index eviction.
func TestRetireStageRemovalError(t *testing.T) {
	store, disk, _ := newRetireFixture(t, time.Hour)
	key := stageRetireSlice(t, store, 9, []byte("checksum"))
	path := disk.stagePath(key)
	staged, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Greater(t, len(staged), len("checksum"))
	require.NoError(t, os.Remove(path))
	require.NoError(t, os.Mkdir(path, 0700))
	require.NoError(t, os.WriteFile(filepath.Join(path, "blocker"), []byte("x"), 0600))
	require.Error(t, store.Retire(9, len("checksum")), "filesystem unlink errors must not be hidden")
	require.NoError(t, os.RemoveAll(path))
	require.NoError(t, os.WriteFile(path, staged, 0600))
	require.NoError(t, store.Retire(9, len("checksum")))
	_, err = os.Stat(path)
	require.True(t, os.IsNotExist(err), "retry removes stage despite missing cache index")
}

// TestRetireIdempotentMemory verifies optional local cancellation works with memory-only caches too.
func TestRetireIdempotentMemory(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	store.bcache = newMemStore(&store.conf, newCacheManagerMetrics(nil))
	p := NewPage(bytes.Repeat([]byte{1}, 10))
	defer p.Release()
	key := sliceForRead(11, 10, store).key(0)
	store.bcache.cache(key, p, true, false)
	require.NoError(t, store.Retire(11, 10))
	require.NoError(t, store.Retire(11, 10))
	require.NoError(t, store.Retire(0, 0))
}

// TestRetireQueuedUploadCanceled proves a queued callback cannot PUT after its pending entry was canceled.
func TestRetireQueuedUploadCanceled(t *testing.T) {
	store, disk, objects := newRetireFixture(t, time.Hour)
	key := stageRetireSlice(t, store, 19, []byte("queued"))
	path := disk.stagePath(key)
	require.NoError(t, store.Retire(19, 6))
	store.uploadStagingFile(key, path)
	require.Zero(t, objects.putCalls.Load())
	require.False(t, store.isPendingValid(key))
	_, err := objects.Head(context.Background(), key)
	require.Error(t, err)
	require.False(t, store.addDelayedStaging(key, path, time.Now(), true), "a stale scanner snapshot cannot resurrect the removed stage")
	require.False(t, store.isPendingValid(key))
}

// delayedRetireCache gates the real stage operation while preserving all real cache methods.
type delayedRetireCache struct {
	CacheManager
	entered, gate chan struct{}
}

// stage delays only publication, then executes real checksum/staging I/O.
func (c *delayedRetireCache) stage(key string, data []byte, tier uint8) (string, error) {
	close(c.entered)
	<-c.gate
	return c.CacheManager.stage(key, data, tier)
}

// retire delegates real local cleanup with its error result.
func (c *delayedRetireCache) retire(key string) error {
	return c.CacheManager.(interface{ retire(string) error }).retire(key)
}

// TestRetireLateStageCallback retains GC intent across a timed-out stage and direct-upload fallback.
func TestRetireLateStageCallback(t *testing.T) {
	store, disk, objects := newRetireFixture(t, time.Hour)
	store.conf.PutTimeout = 20 * time.Millisecond
	gate := &delayedRetireCache{CacheManager: store.bcache, entered: make(chan struct{}), gate: make(chan struct{})}
	store.bcache = gate
	var release sync.Once
	defer release.Do(func() { close(gate.gate) })
	writer := store.NewWriter(23, 0)
	data := []byte("late stage")
	_, err := writer.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Finish(len(data)), "direct-upload fallback still acknowledges successful object persistence")
	require.Equal(t, int64(1), objects.putCalls.Load())
	key := sliceForRead(23, len(data), store).key(0)
	require.ErrorIs(t, store.Retire(23, len(data)), ErrSliceUploadInFlight, "callback can still publish local bytes")
	require.Zero(t, objects.deleteCount(key))
	release.Do(func() { close(gate.gate) })
	require.Eventually(t, func() bool { return store.Retire(23, len(data)) == nil }, time.Second, time.Millisecond)
	_, err = os.Stat(disk.stagePath(key))
	require.True(t, os.IsNotExist(err))
	_, err = os.Stat(disk.cachePath(key))
	require.True(t, os.IsNotExist(err))
	require.NoError(t, store.Remove(23, len(data)))
	_, err = objects.Head(context.Background(), key)
	require.Error(t, err)
}

// retiringGateCache pauses actual local unlink while a scanner takes an earlier stage snapshot.
type retiringGateCache struct {
	CacheManager
	entered, gate chan struct{}
	once          sync.Once
}

// retire delays one actual local unlink, preserving the real disk cleanup implementation.
func (c *retiringGateCache) retire(key string) error {
	c.once.Do(func() { close(c.entered); <-c.gate })
	return c.CacheManager.(interface{ retire(string) error }).retire(key)
}

// TestRetireScannerDuringUnlink detects I/O admitted after the first cancellation but before unlink.
func TestRetireScannerDuringUnlink(t *testing.T) {
	store, disk, objects := newRetireFixture(t, time.Hour)
	key := stageRetireSlice(t, store, 31, []byte("scanner"))
	path := disk.stagePath(key)
	gate := &retiringGateCache{CacheManager: store.bcache, entered: make(chan struct{}), gate: make(chan struct{})}
	store.bcache = gate
	objects.putKey = key
	objects.putEntered = make(chan struct{})
	objects.putGate = make(chan struct{})
	var releaseLocal, releasePut sync.Once
	defer releaseLocal.Do(func() { close(gate.gate) })
	defer releasePut.Do(func() { close(objects.putGate) })
	retired := make(chan error, 1)
	go func() { retired <- store.Retire(31, 7) }()
	<-gate.entered
	require.True(t, store.addDelayedStaging(key, path, time.Now(), true))
	uploaded := make(chan struct{})
	go func() { store.uploadStagingFile(key, path); close(uploaded) }()
	select {
	case <-objects.putEntered:
	case <-time.After(time.Second):
		t.Fatal("scanner upload did not enter")
	}
	releaseLocal.Do(func() { close(gate.gate) })
	require.ErrorIs(t, <-retired, ErrSliceUploadInFlight)
	require.Zero(t, objects.deleteCount(key))
	releasePut.Do(func() { close(objects.putGate) })
	<-uploaded
	require.NoError(t, store.Remove(31, 7))
}

// TestRetireFailedImmediateUploadDoesNotRequeue cancels retry intent without resurrecting a retired stage.
func TestRetireFailedImmediateUploadDoesNotRequeue(t *testing.T) {
	store, disk, objects := newRetireFixture(t, 0)
	data := []byte("failed")
	key := sliceForRead(41, len(data), store).key(0)
	objects.putKey = key
	objects.putEntered = make(chan struct{})
	objects.putGate = make(chan struct{})
	objects.putFail.Store(true)
	var release sync.Once
	defer release.Do(func() { close(objects.putGate) })
	writer := store.NewWriter(41, 0)
	_, err := writer.WriteAt(data, 0)
	require.NoError(t, err)
	require.NoError(t, writer.Finish(len(data)))
	<-objects.putEntered
	require.ErrorIs(t, store.Retire(41, len(data)), ErrSliceUploadInFlight)
	release.Do(func() { close(objects.putGate) })
	require.Eventually(t, func() bool { return store.Retire(41, len(data)) == nil }, 7*time.Second, 10*time.Millisecond)
	require.False(t, store.isPendingValid(key))
	_, err = os.Stat(disk.stagePath(key))
	require.True(t, os.IsNotExist(err))
	_, err = objects.Head(context.Background(), key)
	require.Error(t, err)
	require.Equal(t, int64(3), objects.putCalls.Load(), "the existing staging upload retry limit is retained")
}

// TestRetireStagingStatErrorRetainsIntent does not mistake inaccessible staging paths for absent files.
func TestRetireStagingStatErrorRetainsIntent(t *testing.T) {
	store, _, _ := newRetireFixture(t, time.Hour)
	regular := filepath.Join(t.TempDir(), "regular")
	require.NoError(t, os.WriteFile(regular, []byte("x"), 0600))
	path := filepath.Join(regular, "child")
	_, err := os.Stat(path)
	require.Error(t, err)
	require.False(t, os.IsNotExist(err))
	key := sliceForRead(53, 5, store).key(0)
	store.addDelayedStaging(key, path, time.Now(), false)
	require.True(t, store.isPendingValid(key), "unknown filesystem errors retain retry intent")
}
