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

package vfs

import (
	"context"
	"errors"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// flakyChunkReader fails every read while fail is set, like an object store
// that cannot serve a block for a while.
type flakyChunkReader struct {
	fail  atomic.Bool
	calls atomic.Int32
}

// ReadAt fails while fail is set, otherwise returns "data".
func (r *flakyChunkReader) ReadAt(ctx context.Context, p *chunk.Page, off int) (int, error) {
	r.calls.Add(1)
	if r.fail.Load() {
		return 0, errors.New("simulated object store failure")
	}
	return copy(p.Data, []byte("data")), nil
}

// flakyChunkStore serves every slice with one flakyChunkReader.
type flakyChunkStore struct {
	blockingChunkStore
	reader *flakyChunkReader
}

// NewReader returns the shared flaky reader.
func (s *flakyChunkStore) NewReader(id uint64, length int) chunk.Reader { return s.reader }

// readWithin reads 4 bytes at offset 0 and fails the test if it takes too long.
func readWithin(t *testing.T, fr FileReader) ([]byte, syscall.Errno) {
	t.Helper()
	type result struct {
		buf []byte
		err syscall.Errno
	}
	ch := make(chan result, 1)
	go func() {
		buf := make([]byte, 4)
		n, err := fr.Read(meta.Background(), 0, buf)
		ch <- result{buf[:n], err}
	}()
	select {
	case r := <-ch:
		return r.buf, r.err
	case <-time.After(10 * time.Second):
		t.Fatal("read did not return")
		return nil, 0
	}
}

// TestReadRecoversAfterEIO checks that once a read has failed with EIO after
// running out of retries, later reads of the same open file try again instead
// of failing forever.
func TestReadRecoversAfterEIO(t *testing.T) {
	reader := &flakyChunkReader{}
	reader.fail.Store(true)
	dr, inode := createCancellationTestReader(t, &flakyChunkStore{reader: reader})
	dr.maxRetries = 1
	fr := dr.Open(inode, 4)
	defer fr.Close(meta.Background())

	if _, err := readWithin(t, fr); err != syscall.EIO {
		t.Fatalf("read while the store fails: got %v, want EIO", err)
	}
	// Still failing: a new read tries again and fails again, without hanging.
	before := reader.calls.Load()
	if _, err := readWithin(t, fr); err != syscall.EIO {
		t.Fatalf("second read while the store fails: got %v, want EIO", err)
	}
	if reader.calls.Load() == before {
		t.Fatal("the second read must try the store again")
	}

	reader.fail.Store(false)
	buf, err := readWithin(t, fr)
	if err != 0 || string(buf) != "data" {
		t.Fatalf("read after the store recovered: got %q, %v", buf, err)
	}
}
