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
	"bytes"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// TestMetaWriteBatchNormalization keeps valid sizes and disables invalid ones.
func TestMetaWriteBatchNormalization(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	for _, tc := range []struct{ in, want int }{{0, 0}, {1, 1}, {64, 64}, {1024, 1024}, {-1, 0}, {1025, 0}} {
		conf := *v.Conf
		conf.MetaWriteBatch = tc.in
		w := NewDataWriter(&conf, v.Meta, v.writer.(*dataWriter).store, v.reader).(*dataWriter)
		require.Equal(t, tc.want, w.conf.MetaWriteBatch, tc.in)
	}
}

// batchRecordingMeta records metadata commits, optionally holds the first one,
// and can inject a result for the first batch.
type batchRecordingMeta struct {
	meta.Meta
	mu     sync.Mutex
	calls  [][]uint64 // slice IDs per metadata call, in call order
	mtimes []time.Time
	hold   chan struct{} // when set, the first call waits until it is closed
	held   chan struct{} // closed when the first call starts waiting
	inject func(slices []meta.SliceWrite) (int, syscall.Errno, bool)
	once   sync.Once
}

// record notes a call and waits on the hold gate for the first call.
func (m *batchRecordingMeta) record(ids []uint64, mtime time.Time) {
	m.mu.Lock()
	m.calls = append(m.calls, ids)
	m.mtimes = append(m.mtimes, mtime)
	first := len(m.calls) == 1
	m.mu.Unlock()
	if first && m.hold != nil {
		close(m.held)
		<-m.hold
	}
}

// Write records a single-slice commit.
func (m *batchRecordingMeta) Write(ctx meta.Context, inode Ino, indx, off uint32, s meta.Slice, mtime time.Time) syscall.Errno {
	m.record([]uint64{s.Id}, mtime)
	return m.Meta.Write(ctx, inode, indx, off, s, mtime)
}

// WriteSlices records a batch and applies an injected result once, if any.
func (m *batchRecordingMeta) WriteSlices(ctx meta.Context, inode Ino, indx uint32, slices []meta.SliceWrite, mtime time.Time) (int, syscall.Errno) {
	ids := make([]uint64, len(slices))
	for i, w := range slices {
		ids[i] = w.Slice.Id
	}
	m.record(ids, mtime)
	if m.inject != nil {
		var n int
		var st syscall.Errno
		var used bool
		m.once.Do(func() { n, st, used = m.inject(slices) })
		if used {
			return n, st
		}
	}
	return m.Meta.WriteSlices(ctx, inode, indx, slices, mtime)
}

// snapshot returns the recorded calls.
func (m *batchRecordingMeta) snapshot() [][]uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([][]uint64(nil), m.calls...)
}

// newBatchTestFile opens a file on a test VFS with batching of size n, slice
// timers that never fire, and a reuse window of 1 so new gaps make new slices.
func newBatchTestFile(t *testing.T, n int) (*rangeTestFile, *batchRecordingMeta) {
	t.Helper()
	f := newRangeTestFile(t, WriterFlushScopeFile)
	f.v.Conf.MetaWriteBatch = n
	f.v.Conf.WriterReuseWindow = 1
	rm := &batchRecordingMeta{Meta: f.v.Meta, hold: make(chan struct{}), held: make(chan struct{})}
	f.v.writer.(*dataWriter).m = rm
	return f, rm
}

// awaitAllDone waits until every pending slice of chunk indx finished its data upload.
func awaitAllDone(t *testing.T, fw *fileWriter, indx uint32) {
	t.Helper()
	deadline := time.Now().Add(rangeTestTimeout)
	for {
		fw.Lock()
		done := true
		for _, s := range fw.chunks[indx].slices {
			done = done && s.done
		}
		fw.Unlock()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("slices did not finish")
		}
		time.Sleep(time.Millisecond)
	}
}

// TestMetaWriteBatchCombinesSameChunk commits consecutive done slices of a chunk
// in one call, keeping creation order for overlapping writes.
func TestMetaWriteBatchCombinesSameChunk(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	f.write(t, 3<<20, []byte("s0"))                   // S0
	f.write(t, 4096, bytes.Repeat([]byte{'a'}, 4096)) // S1
	f.write(t, 1<<20, []byte("s2"))                   // S2: freezes S0 (window 1), whose commit is held
	<-rm.held
	// S3 overlaps S1 but starts before its writable range, so it cannot reuse S1.
	f.write(t, 0, bytes.Repeat([]byte{'b'}, 8192))
	fw := f.fileWriter(t)
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, fw, 0)
	close(rm.hold)
	select {
	case eno := <-fsynced:
		require.Zero(t, eno)
	case <-time.After(rangeTestTimeout):
		t.Fatal("fsync did not finish")
	}
	calls := rm.snapshot()
	require.Len(t, calls, 2, "S0 alone, then S1..S3 together: %v", calls)
	require.Len(t, calls[1], 3)
	require.Equal(t, bytes.Repeat([]byte{'b'}, 8192), f.read(t, 0, 8192), "creation order inside the batch")
}

// TestMetaWriteBatchDisabled never calls WriteSlices when batching is off.
func TestMetaWriteBatchDisabled(t *testing.T) {
	f, rm := newBatchTestFile(t, 0)
	rm.hold = nil
	for i := 0; i < 5; i++ {
		f.write(t, uint64(i)<<20, []byte("x"))
	}
	f.fsync(t)
	for _, c := range rm.snapshot() {
		require.Len(t, c, 1)
	}
}

// heldBatch writes S0 (held), then three more slices that end up in one batch.
func heldBatch(t *testing.T, rm *batchRecordingMeta, f *rangeTestFile) {
	t.Helper()
	f.write(t, 3<<20, []byte("s0"))
	f.write(t, 0, []byte("a1"))
	f.write(t, 1<<20, []byte("b2"))
	<-rm.held
	f.write(t, 2<<20, []byte("c3"))
}

// TestMetaWriteBatchPartialFailure keeps the committed prefix, reports the
// failing slice's errno and still commits the slices after it.
func TestMetaWriteBatchPartialFailure(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	rm.inject = func(slices []meta.SliceWrite) (int, syscall.Errno, bool) {
		require.Zero(t, rm.Meta.Write(meta.Background(), f.ino, 0, slices[0].Off, slices[0].Slice, time.Now()))
		return 1, syscall.EDQUOT, true
	}
	heldBatch(t, rm, f)
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, f.fileWriter(t), 0)
	close(rm.hold)
	require.Equal(t, syscall.EDQUOT, <-fsynced)
	batch := rm.snapshot()[1]
	ids := committedIDs(t, f)
	require.True(t, ids[batch[0]], "the committed prefix stays")
	require.False(t, ids[batch[1]], "the failed slice must not be registered")
	require.True(t, ids[batch[2]], "slices after an unapplied failure are still committed")
}

// committedIDs returns the slice IDs registered in chunk 0 of the test file.
// Reads through the VFS would return the file's sticky error instead.
func committedIDs(t *testing.T, f *rangeTestFile) map[uint64]bool {
	t.Helper()
	var ss []meta.Slice
	require.Zero(t, f.v.Meta.Read(meta.Background(), f.ino, 0, &ss))
	ids := map[uint64]bool{}
	for _, s := range ss {
		ids[s.Id] = true
	}
	return ids
}

// TestMetaWriteBatchUnknownFailure sends no slice of a batch whose outcome is
// unknown again, and marks the file failed with EIO.
func TestMetaWriteBatchUnknownFailure(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	rm.inject = func(slices []meta.SliceWrite) (int, syscall.Errno, bool) { return 0, syscall.EIO, true }
	heldBatch(t, rm, f)
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, f.fileWriter(t), 0)
	close(rm.hold)
	require.Equal(t, syscall.EIO, <-fsynced)
	calls := rm.snapshot()
	inBatch := map[uint64]bool{}
	for _, id := range calls[1] {
		inBatch[id] = true
	}
	for _, c := range calls[2:] {
		for _, id := range c {
			require.False(t, inBatch[id], "slice %d of an unknown-outcome batch was resent: %v", id, calls)
		}
	}
}

// TestMetaWriteBatchStopsAtFailedSlice cuts a batch before a slice whose data upload failed.
func TestMetaWriteBatchStopsAtFailedSlice(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	heldBatch(t, rm, f)
	fw := f.fileWriter(t)
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, fw, 0)
	fw.Lock()
	fw.chunks[0].slices[2].err = syscall.EIO // the "b2" slice: as if its upload failed
	fw.Unlock()
	close(rm.hold)
	require.Equal(t, syscall.EIO, <-fsynced)
	for _, c := range rm.snapshot() {
		require.LessOrEqual(t, len(c), 1, "no batch may include or skip past the failed slice: %v", rm.snapshot())
	}
}

// TestMetaWriteBatchMtime passes the newest write time of the batch.
func TestMetaWriteBatchMtime(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	f.write(t, 3<<20, []byte("s0"))
	f.write(t, 0, []byte("a1"))
	f.write(t, 1<<20, []byte("b2"))
	<-rm.held
	time.Sleep(20 * time.Millisecond)
	newest := time.Now()
	f.write(t, 2<<20, []byte("c3"))
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, f.fileWriter(t), 0)
	close(rm.hold)
	require.Zero(t, <-fsynced)
	rm.mu.Lock()
	defer rm.mu.Unlock()
	require.False(t, rm.mtimes[len(rm.mtimes)-1].Before(newest))
}

// TestMetaWriteBatchRecollectsAfterCommitOrder adds slices that finish while a
// chunk waits for another chunk's commit to the batch it then sends.
func TestMetaWriteBatchRecollectsAfterCommitOrder(t *testing.T) {
	f, rm := newBatchTestFile(t, 64)
	// Chunk 1: its first slice is frozen by the reuse window and its commit is held,
	// keeping the inode's commit order (commitMu) busy.
	f.write(t, meta.ChunkSize, []byte("x0"))
	f.write(t, meta.ChunkSize+3<<20, []byte("x1"))
	f.write(t, meta.ChunkSize+1<<20, []byte("x2"))
	<-rm.held
	// Chunk 0: the head becomes committable and its thread waits for the commit order.
	f.write(t, 3<<20, []byte("s0"))
	f.write(t, 0, []byte("s1"))
	f.write(t, 1<<20, []byte("s2")) // freezes s0
	fw := f.fileWriter(t)
	head := chunkSlices(fw, 0)[0]
	deadline := time.Now().Add(rangeTestTimeout)
	for {
		fw.Lock()
		done := head.done
		fw.Unlock()
		if done {
			break
		}
		require.True(t, time.Now().Before(deadline), "head slice did not finish")
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let chunk 0's thread reach the commit order
	f.write(t, 2<<20, []byte("s3"))   // freezes s1
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, fw, 0)
	ids := map[uint64]bool{}
	fw.Lock()
	headID := head.id
	for _, s := range fw.chunks[0].slices {
		ids[s.id] = true
	}
	fw.Unlock()
	close(rm.hold)
	select {
	case eno := <-fsynced:
		require.Zero(t, eno)
	case <-time.After(rangeTestTimeout):
		t.Fatal("fsync did not finish")
	}
	for _, c := range rm.snapshot() {
		for _, id := range c {
			if id == headID {
				require.Len(t, c, len(ids), "chunk 0's slices that finished while waiting should join the head's batch: %v", rm.snapshot())
				return
			}
		}
	}
	t.Fatal("chunk 0's head was never committed")
}
