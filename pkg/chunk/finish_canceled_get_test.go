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
	"sync"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// gateStorage wraps an object storage and holds every Get until release is closed,
// so tests can cancel a reader while its GET is in flight.
type gateStorage struct {
	object.ObjectStorage
	mu       sync.Mutex
	calls    map[string]int
	started  chan string
	release  chan struct{}
	canceled chan string
}

// newGateStorage creates a gateStorage around inner with an unreleased gate.
func newGateStorage(inner object.ObjectStorage) *gateStorage {
	return &gateStorage{
		ObjectStorage: inner,
		calls:         make(map[string]int),
		started:       make(chan string, 16),
		release:       make(chan struct{}),
		canceled:      make(chan string, 16),
	}
}

// Get records the call, then waits for the gate or for the request context to end.
func (g *gateStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	g.mu.Lock()
	g.calls[key]++
	g.mu.Unlock()
	g.started <- key
	select {
	case <-g.release:
		return g.ObjectStorage.Get(ctx, key, off, limit, getters...)
	case <-ctx.Done():
		g.canceled <- key
		return nil, ctx.Err()
	}
}

// callCount returns how many times Get was called for key.
func (g *gateStorage) callCount(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls[key]
}

// finishCanceledFixture writes one full zstd block for each slice id into a shared
// in-memory bucket, and returns a reading store over a gated view of that bucket.
func finishCanceledFixture(t *testing.T, finish bool, maxDownload int, ids ...uint64) (*cachedStore, *gateStorage, []byte) {
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	conf := defaultConf
	conf.Compress = "zstd"
	conf.CacheDir = t.TempDir()
	conf.CacheFullBlock = true
	conf.MaxDownload = maxDownload
	conf.GetTimeout = 30 * time.Second
	data := bytes.Repeat([]byte("0123456789abcdef"), conf.BlockSize/16)
	writer := NewCachedStore(mem, conf, nil)
	for _, id := range ids {
		w := writer.NewWriter(id, 0)
		_, err := w.WriteAt(data, 0)
		require.NoError(t, err)
		require.NoError(t, w.Finish(len(data)))
	}

	conf.CacheDir = t.TempDir()
	conf.FinishCanceledGet = finish
	gate := newGateStorage(mem)
	store := NewCachedStore(gate, conf, nil).(*cachedStore)
	return store, gate, data
}

// blockKey returns the object key of the first block of slice id.
func blockKey(store *cachedStore, id uint64, length int) string {
	return store.NewReader(id, length).(*rSlice).key(0)
}

type readResult struct {
	n   int
	err error
	buf []byte
}

// startRead reads 4 KiB at offset 4 KiB of slice id in the background.
func startRead(ctx context.Context, store *cachedStore, id uint64, length int) chan readResult {
	ch := make(chan readResult, 1)
	go func() {
		p := NewOffPage(4096)
		defer p.Release()
		n, err := store.NewReader(id, length).ReadAt(ctx, p, 4096)
		ch <- readResult{n, err, append([]byte(nil), p.Data[:n]...)}
	}()
	return ch
}

// waitResult waits for a background read with a timeout.
func waitResult(t *testing.T, ch chan readResult) readResult {
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("read did not return")
		return readResult{}
	}
}

// waitStarted waits until the gated storage reports a Get for key.
func waitStarted(t *testing.T, gate *gateStorage, key string) {
	select {
	case k := <-gate.started:
		require.Equal(t, key, k)
	case <-time.After(5 * time.Second):
		t.Fatalf("Get for %s did not start", key)
	}
}

// waitCached polls the block cache until key is present.
func waitCached(t *testing.T, store *cachedStore, key string) {
	require.Eventually(t, func() bool {
		r, err := store.bcache.load(key)
		if err != nil {
			return false
		}
		_ = r.Close()
		return true
	}, 5*time.Second, 10*time.Millisecond)
}

func TestFinishCanceledGetCachesBlock(t *testing.T) {
	store, gate, data := finishCanceledFixture(t, true, 4, 1)
	key := blockKey(store, 1, len(data))
	cctx, cancel := context.WithCancel(context.Background())
	res := startRead(cctx, store, 1, len(data))
	waitStarted(t, gate, key)

	cancel()
	r := waitResult(t, res)
	require.True(t, errors.Is(r.err, context.Canceled), "canceled reader must return promptly: %v", r.err)

	close(gate.release)
	waitCached(t, store, key)
	require.Equal(t, 1.0, testutil.ToFloat64(store.canceledGetFinished))

	r = waitResult(t, startRead(context.Background(), store, 1, len(data)))
	require.NoError(t, r.err)
	require.Equal(t, data[4096:8192], r.buf)
	require.Equal(t, 1, gate.callCount(key), "cached block must not be fetched again")
}

func TestFinishCanceledGetSharesResultWithWaiter(t *testing.T) {
	store, gate, data := finishCanceledFixture(t, true, 4, 1)
	key := blockKey(store, 1, len(data))
	cctx, cancel := context.WithCancel(context.Background())
	first := startRead(cctx, store, 1, len(data))
	waitStarted(t, gate, key)
	second := startRead(context.Background(), store, 1, len(data))
	time.Sleep(50 * time.Millisecond) // let the second reader join the in-flight fetch

	cancel()
	require.True(t, errors.Is(waitResult(t, first).err, context.Canceled))
	close(gate.release)

	r := waitResult(t, second)
	require.NoError(t, r.err, "a waiter must not inherit the first reader's cancellation")
	require.Equal(t, data[4096:8192], r.buf)
	require.Equal(t, 1, gate.callCount(key))
}

func TestFinishCanceledGetSkipsFetchNotStarted(t *testing.T) {
	store, gate, data := finishCanceledFixture(t, true, 1, 1, 2)
	key1 := blockKey(store, 1, len(data))
	key2 := blockKey(store, 2, len(data))
	busy := startRead(context.Background(), store, 1, len(data))
	waitStarted(t, gate, key1) // holds the only download slot

	cctx, cancel := context.WithCancel(context.Background())
	waiting := startRead(cctx, store, 2, len(data))
	time.Sleep(50 * time.Millisecond)
	cancel()
	require.True(t, errors.Is(waitResult(t, waiting).err, context.Canceled))

	close(gate.release)
	require.NoError(t, waitResult(t, busy).err)
	time.Sleep(50 * time.Millisecond)
	require.Equal(t, 0, gate.callCount(key2), "a fetch canceled before it started must not be issued")
}

func TestFinishCanceledGetLimit(t *testing.T) {
	// max-downloads 2 allows one detached fetch; the second canceled fetch is aborted.
	store, gate, data := finishCanceledFixture(t, true, 2, 1, 2)
	key1 := blockKey(store, 1, len(data))
	key2 := blockKey(store, 2, len(data))
	ctx1, cancel1 := context.WithCancel(context.Background())
	ctx2, cancel2 := context.WithCancel(context.Background())
	r1 := startRead(ctx1, store, 1, len(data))
	waitStarted(t, gate, key1)
	r2 := startRead(ctx2, store, 2, len(data))
	waitStarted(t, gate, key2)

	cancel1()
	require.True(t, errors.Is(waitResult(t, r1).err, context.Canceled))
	cancel2()
	require.True(t, errors.Is(waitResult(t, r2).err, context.Canceled))
	select {
	case k := <-gate.canceled:
		require.Equal(t, key2, k, "only the fetch over the limit is aborted")
	case <-time.After(5 * time.Second):
		t.Fatal("fetch over the limit was not aborted")
	}
	require.Equal(t, 1.0, testutil.ToFloat64(store.canceledGetDropped))

	close(gate.release)
	waitCached(t, store, key1)
	_, err := store.bcache.load(key2)
	require.Error(t, err)
}

func TestFinishCanceledGetDisabled(t *testing.T) {
	store, gate, data := finishCanceledFixture(t, false, 4, 1)
	key := blockKey(store, 1, len(data))
	cctx, cancel := context.WithCancel(context.Background())
	res := startRead(cctx, store, 1, len(data))
	waitStarted(t, gate, key)

	cancel()
	require.True(t, errors.Is(waitResult(t, res).err, context.Canceled))
	select {
	case k := <-gate.canceled:
		require.Equal(t, key, k, "without the option the GET is canceled as before")
	case <-time.After(5 * time.Second):
		t.Fatal("GET was not canceled")
	}
	close(gate.release)
	time.Sleep(50 * time.Millisecond)
	_, err := store.bcache.load(key)
	require.Error(t, err)
}
