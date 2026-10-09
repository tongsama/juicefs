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
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

// stallStorage simulates a backend that is slow before the response headers
// (headerDelay) or while sending the body (bodyDelay).
type stallStorage struct {
	object.ObjectStorage
	headerDelay time.Duration
	bodyDelay   time.Duration
	data        []byte
}

// Get waits headerDelay before "returning headers", honoring ctx like an HTTP client.
func (s *stallStorage) Get(ctx context.Context, key string, off, limit int64, getters ...object.AttrGetter) (io.ReadCloser, error) {
	select {
	case <-time.After(s.headerDelay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &slowBody{ctx: ctx, delay: s.bodyDelay, r: bytes.NewReader(s.data)}, nil
}

// slowBody delays the first Read by delay and fails if its request context ends.
type slowBody struct {
	ctx     context.Context
	delay   time.Duration
	r       io.Reader
	started bool
}

// Read returns the body after the initial delay, or the context error.
func (b *slowBody) Read(p []byte) (int, error) {
	if !b.started {
		b.started = true
		select {
		case <-time.After(b.delay):
		case <-b.ctx.Done():
			return 0, b.ctx.Err()
		}
	}
	if err := b.ctx.Err(); err != nil {
		return 0, err
	}
	return b.r.Read(p)
}

// Close does nothing.
func (b *slowBody) Close() error { return nil }

// headerTimeoutStore returns an uncompressed store over a stallStorage holding one block.
func headerTimeoutStore(t *testing.T, headerTimeout, headerDelay, bodyDelay time.Duration) (*cachedStore, int) {
	conf := defaultConf
	conf.CacheDir = t.TempDir()
	conf.GetTimeout = 5 * time.Second
	conf.GetHeaderTimeout = headerTimeout
	data := bytes.Repeat([]byte{0x5a}, conf.BlockSize)
	s := &stallStorage{headerDelay: headerDelay, bodyDelay: bodyDelay, data: data}
	return NewCachedStore(s, conf, nil).(*cachedStore), len(data)
}

func TestGetHeaderTimeoutCutsStalledGet(t *testing.T) {
	store, size := headerTimeoutStore(t, 100*time.Millisecond, 3*time.Second, 0)
	page := NewOffPage(size)
	defer page.Release()
	start := time.Now()
	err := store.load(context.Background(), "chunks/0/0/1_0_1048576", page, false, false)
	require.Error(t, err)
	require.True(t, errors.Is(err, errGetHeaderTimeout), "unexpected error: %v", err)
	require.Less(t, time.Since(start), time.Second, "must give up at the header timeout, not --get-timeout")
	require.Equal(t, 1.0, testutil.ToFloat64(store.getHeaderTimeouts))
}

func TestGetHeaderTimeoutKeepsSlowBody(t *testing.T) {
	// Headers arrive in time; the body is slower than the header timeout and must not be cut.
	store, size := headerTimeoutStore(t, 100*time.Millisecond, 0, 300*time.Millisecond)
	page := NewOffPage(size)
	defer page.Release()
	require.NoError(t, store.load(context.Background(), "chunks/0/0/1_0_1048576", page, false, false))
	require.Equal(t, byte(0x5a), page.Data[size-1])
	require.Equal(t, 0.0, testutil.ToFloat64(store.getHeaderTimeouts))
}

func TestGetHeaderTimeoutDisabled(t *testing.T) {
	// With 0 the header wait is bounded only by --get-timeout (5s here).
	store, size := headerTimeoutStore(t, 0, 300*time.Millisecond, 0)
	page := NewOffPage(size)
	defer page.Release()
	require.NoError(t, store.load(context.Background(), "chunks/0/0/1_0_1048576", page, false, false))
	require.Equal(t, 0.0, testutil.ToFloat64(store.getHeaderTimeouts))
}
