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
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// rangeTestTimeout bounds operations that must not wait for commits outside their range.
const rangeTestTimeout = 5 * time.Second

// gatedWriteMeta holds metadata commits of selected chunks so tests can keep
// pending slices outside a range uncommitted while using the real backend.
type gatedWriteMeta struct {
	meta.Meta
	mu      sync.Mutex
	gates   map[uint32]chan struct{}
	started chan uint32
}

// newGatedWriteMeta wraps m without blocking any chunk yet.
func newGatedWriteMeta(m meta.Meta) *gatedWriteMeta {
	return &gatedWriteMeta{Meta: m, gates: make(map[uint32]chan struct{}), started: make(chan uint32, 1024)}
}

// block holds commits of chunk indx until the returned release is called (idempotent).
func (m *gatedWriteMeta) block(indx uint32) (release func()) {
	ch := make(chan struct{})
	m.mu.Lock()
	m.gates[indx] = ch
	m.mu.Unlock()
	var once sync.Once
	return func() {
		once.Do(func() {
			m.mu.Lock()
			delete(m.gates, indx)
			m.mu.Unlock()
			close(ch)
		})
	}
}

// Write reports that a commit reached the metadata layer and waits on its chunk's gate.
func (m *gatedWriteMeta) Write(ctx meta.Context, inode Ino, indx, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	m.mu.Lock()
	g := m.gates[indx]
	m.mu.Unlock()
	select {
	case m.started <- indx:
	default:
	}
	if g != nil {
		<-g
	}
	return m.Meta.Write(ctx, inode, indx, off, slice, mtime)
}

// WriteSlices passes the same gate as Write before committing the batch.
func (m *gatedWriteMeta) WriteSlices(ctx meta.Context, inode Ino, indx uint32, slices []meta.SliceWrite, mtime time.Time) (int, syscall.Errno) {
	m.mu.Lock()
	g := m.gates[indx]
	m.mu.Unlock()
	select {
	case m.started <- indx:
	default:
	}
	if g != nil {
		<-g
	}
	return m.Meta.WriteSlices(ctx, inode, indx, slices, mtime)
}

// awaitCommitStart waits until a commit of chunk indx reaches the gated metadata layer.
func (m *gatedWriteMeta) awaitCommitStart(t *testing.T, indx uint32) {
	t.Helper()
	timeout := time.After(rangeTestTimeout)
	for {
		select {
		case got := <-m.started:
			if got == indx {
				return
			}
		case <-timeout:
			t.Fatalf("commit of chunk %d did not start", indx)
		}
	}
}

// rangeTestFile is an open file on a test VFS whose slice timers never freeze
// pending writes, so only explicit barriers commit them.
type rangeTestFile struct {
	v    *VFS
	m    *gatedWriteMeta
	ctx  Context
	ino  Ino
	fh   uint64
	name string
}

// newRangeTestFile creates a file on a fresh VFS with the given flush scope.
func newRangeTestFile(t *testing.T, scope string) *rangeTestFile {
	t.Helper()
	v, _ := createTestVFS(nil, "")
	v.Conf.WriterFlushScope = scope
	v.Conf.SliceFlushWait = time.Hour
	v.Conf.SliceFlushIdle = time.Hour
	gm := newGatedWriteMeta(v.Meta)
	v.writer.(*dataWriter).m = gm
	ctx := NewLogContext(meta.Background())
	fe, fh, eno := v.Create(ctx, 1, "range", 0644, 0, syscall.O_RDWR)
	require.Zero(t, eno)
	return &rangeTestFile{v: v, m: gm, ctx: ctx, ino: fe.Inode, fh: fh, name: scope}
}

// write stores data at off through the VFS write path.
func (f *rangeTestFile) write(t *testing.T, off uint64, data []byte) {
	t.Helper()
	require.Zero(t, f.v.Write(f.ctx, f.ino, data, off, f.fh))
}

// fileWriter returns the live writer of the test file.
func (f *rangeTestFile) fileWriter(t *testing.T) *fileWriter {
	t.Helper()
	fw := f.v.writer.(*dataWriter).find(f.ino)
	require.NotNil(t, fw)
	return fw
}

// within runs op and fails the test if it does not return within rangeTestTimeout.
func within(t *testing.T, what string, op func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); op() }()
	select {
	case <-done:
	case <-time.After(rangeTestTimeout):
		t.Fatalf("%s waited for commits outside its range", what)
	}
}

// read reads size bytes at off and fails if the read waits for out-of-range commits.
func (f *rangeTestFile) read(t *testing.T, off uint64, size int) []byte {
	t.Helper()
	buf := make([]byte, size)
	var n int
	var eno syscall.Errno
	within(t, fmt.Sprintf("read(%d,%d)", off, size), func() { n, eno = f.v.Read(f.ctx, f.ino, buf, off, f.fh) })
	require.Zero(t, eno)
	return buf[:n]
}

// chunkSlices snapshots the pending slices of chunk indx.
func chunkSlices(fw *fileWriter, indx uint32) []*sliceWriter {
	fw.Lock()
	defer fw.Unlock()
	if c := fw.chunks[indx]; c != nil {
		return append([]*sliceWriter(nil), c.slices...)
	}
	return nil
}

// requireUnfrozen checks that no pending slice of chunk indx was frozen by a range barrier.
func requireUnfrozen(t *testing.T, fw *fileWriter, indx uint32) {
	t.Helper()
	ss := chunkSlices(fw, indx)
	require.NotEmpty(t, ss, "chunk %d should still have pending slices", indx)
	fw.Lock()
	defer fw.Unlock()
	for _, s := range ss {
		require.False(t, s.freezed, "slice outside the flushed range was frozen")
	}
}

// TestRangeFlushReadAfterWrite reads the latest data in a range while other chunks stay uncommitted.
func TestRangeFlushReadAfterWrite(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	release := f.m.block(2)
	defer release()
	f.write(t, 2*meta.ChunkSize, bytes.Repeat([]byte{'a'}, 4096))
	f.write(t, 0, bytes.Repeat([]byte{'b'}, 4096))
	// A write crossing the chunk 0/1 boundary.
	f.write(t, meta.ChunkSize-2048, bytes.Repeat([]byte{'c'}, 4096))

	require.Equal(t, bytes.Repeat([]byte{'b'}, 4096), f.read(t, 0, 4096))
	require.Equal(t, bytes.Repeat([]byte{'c'}, 4096), f.read(t, meta.ChunkSize-2048, 4096))
	// A newer overwrite in the same chunk is committed by the next read barrier.
	f.write(t, 1024, []byte("new"))
	require.Equal(t, "bbbbnew", string(f.read(t, 1020, 7)))
	requireUnfrozen(t, f.fileWriter(t), 2)

	release()
	var eno syscall.Errno
	within(t, "fsync", func() { eno = f.v.Fsync(f.ctx, f.ino, 0, f.fh) })
	require.Zero(t, eno)
	require.Equal(t, bytes.Repeat([]byte{'a'}, 4096), f.read(t, 2*meta.ChunkSize, 4096))
}

// TestRangeFlushDoesNotBlockOutsideWrites keeps writes to other chunks flowing during a range barrier.
func TestRangeFlushDoesNotBlockOutsideWrites(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	release := f.m.block(0)
	defer release()
	f.write(t, 0, bytes.Repeat([]byte{'x'}, 4096))
	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, 4096)
		n, eno := f.v.Read(f.ctx, f.ino, buf, 0, f.fh)
		if eno != 0 {
			n = 0
		}
		got <- buf[:n]
	}()
	// The read barrier is now waiting for chunk 0's commit.
	f.m.awaitCommitStart(t, 0)

	// Use another handle: the same handle's Read keeps its read lock (handle.Rlock)
	// during the barrier, which serializes that handle's writes independently of the writer.
	_, other, eno := f.v.Open(f.ctx, f.ino, syscall.O_RDWR)
	require.Zero(t, eno)
	// Cleanup runs after the deferred gate release, so a failed assertion cannot deadlock the close-time flush.
	t.Cleanup(func() { f.v.Release(f.ctx, f.ino, other) })
	within(t, "write to another chunk", func() {
		eno = f.v.Write(f.ctx, f.ino, []byte("outside"), 2*meta.ChunkSize, other)
	})
	require.Zero(t, eno)
	fw := f.fileWriter(t)
	fw.Lock()
	flushwaiting := fw.flushwaiting
	fw.Unlock()
	require.Zero(t, flushwaiting, "a range barrier must not stop writes of the whole file")
	requireUnfrozen(t, fw, 2)
	select {
	case <-got:
		t.Fatal("read returned before the in-range commit completed")
	default:
	}

	release()
	// commitThread wakes range barriers on each commit; without that wakeup the
	// barrier would only notice after its 3s poll interval.
	select {
	case data := <-got:
		require.Equal(t, bytes.Repeat([]byte{'x'}, 4096), data)
	case <-time.After(time.Second):
		t.Fatal("read was not woken promptly after the in-range commit completed")
	}
}

// TestRangeFlushFreezesDependencyClosure commits a growing slice of the previous chunk
// that an in-range slice depends on, without waiting for slice timers.
func TestRangeFlushFreezesDependencyClosure(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	release := f.m.block(3)
	defer release()
	// Sequential append across the chunk 0/1 boundary: chunk 1's first slice depends on chunk 0's.
	f.write(t, meta.ChunkSize-8192, bytes.Repeat([]byte{'p'}, 4096))
	f.write(t, meta.ChunkSize-4096, bytes.Repeat([]byte{'x'}, 4096))
	f.write(t, meta.ChunkSize, bytes.Repeat([]byte{'y'}, 4096))
	fw := f.fileWriter(t)
	dep := chunkSlices(fw, 1)
	require.Len(t, dep, 1)
	fw.Lock()
	require.NotNil(t, dep[0].dep, "chunk 1's first slice should depend on chunk 0's growing slice")
	fw.Unlock()
	f.write(t, 3*meta.ChunkSize, []byte("later"))

	require.Equal(t, bytes.Repeat([]byte{'y'}, 4096), f.read(t, meta.ChunkSize, 4096))
	require.Empty(t, chunkSlices(fw, 0), "the dependency closure in chunk 0 should be committed")
	requireUnfrozen(t, fw, 3)
	require.Equal(t, bytes.Repeat([]byte{'x'}, 4096), f.read(t, meta.ChunkSize-4096, 4096))
}

// TestRangeFlushEOFWithPendingAppend reads near the committed EOF while a later append is uncommitted.
func TestRangeFlushEOFWithPendingAppend(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	f.write(t, 0, bytes.Repeat([]byte{'i'}, 3*4096))
	require.Zero(t, f.v.Fsync(f.ctx, f.ino, 0, f.fh))
	release := f.m.block(2)
	defer release()
	f.write(t, 2*meta.ChunkSize, []byte("tail"))

	want := append(bytes.Repeat([]byte{'i'}, 4096), make([]byte, 4096)...)
	require.Equal(t, want, f.read(t, 2*4096, 2*4096))
	requireUnfrozen(t, f.fileWriter(t), 2)
}

// fallocateResult is the observable outcome of fallocateSequence.
type fallocateResult struct {
	writerLen uint64 // writer length right after fallocate, before out-of-range commits finish
	attrLen   uint64
	content   []byte
}

// fallocateSequence runs the same writes and fallocate under scope; when gate is
// set, an append outside the fallocate range stays uncommitted during fallocate.
func fallocateSequence(t *testing.T, scope string, mode uint8, off, size int64, gate bool) fallocateResult {
	t.Helper()
	f := newRangeTestFile(t, scope)
	f.write(t, 0, bytes.Repeat([]byte{'i'}, 3*4096))
	require.Zero(t, f.v.Fsync(f.ctx, f.ino, 0, f.fh))
	release := func() {}
	if gate {
		release = f.m.block(2)
	}
	defer release()
	// Pending in-range data overlapping the fallocate range, and an out-of-range append.
	f.write(t, 4096, bytes.Repeat([]byte{'p'}, 4096))
	f.write(t, 2*meta.ChunkSize, bytes.Repeat([]byte{'q'}, 4096))

	var eno syscall.Errno
	within(t, "fallocate", func() { eno = f.v.Fallocate(f.ctx, f.ino, mode, off, size, f.fh) })
	require.Zero(t, eno)
	var res fallocateResult
	res.writerLen = f.v.writer.GetLength(f.ino)
	if gate {
		requireUnfrozen(t, f.fileWriter(t), 2)
	}
	release()
	within(t, "fsync", func() { eno = f.v.Fsync(f.ctx, f.ino, 0, f.fh) })
	require.Zero(t, eno)
	var attr meta.Attr
	require.Zero(t, f.v.Meta.GetAttr(f.ctx, f.ino, &attr))
	res.attrLen = attr.Length
	res.content = f.read(t, 0, int(attr.Length))
	return res
}

// TestRangeFlushFallocateKeepsLength checks that fallocate with an uncommitted
// out-of-range append neither shrinks the file nor changes the final content,
// including ranges ending between the committed length and the writer length.
func TestRangeFlushFallocateKeepsLength(t *testing.T) {
	const fullLen = 2*meta.ChunkSize + 4096
	for _, tc := range []struct {
		name      string
		mode      uint8
		off, size int64
	}{
		{"zero_range", 0x10, 2048, 8192},
		{"punch_hole_keep_size", 0x03, 2048, 8192},
		{"zero_range_keep_size", 0x11, 2048, 8192},
		{"allocate", 0, 2048, 8192},
		// The range ends past the committed length (3*4096) but before the pending append.
		{"zero_range_past_committed", 0x10, 8192, 16384},
		{"zero_range_keep_size_past_committed", 0x11, 8192, 16384},
		{"punch_hole_past_committed", 0x03, 8192, 16384},
		{"allocate_past_committed", 0, 8192, 16384},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy := fallocateSequence(t, WriterFlushScopeFile, tc.mode, tc.off, tc.size, false)
			ranged := fallocateSequence(t, WriterFlushScopeRange, tc.mode, tc.off, tc.size, true)
			require.Equal(t, uint64(fullLen), legacy.writerLen)
			require.Equal(t, uint64(fullLen), ranged.writerLen, "fallocate must not shrink the writer length")
			require.Equal(t, uint64(fullLen), legacy.attrLen)
			require.Equal(t, legacy.attrLen, ranged.attrLen)
			require.True(t, bytes.Equal(legacy.content, ranged.content), "content differs between scopes")
			end := tc.off + tc.size
			if tc.mode&0x12 != 0 {
				// The zeroed range wins over the pending write it overlaps.
				require.Equal(t, make([]byte, tc.size), ranged.content[tc.off:end])
			} else {
				require.Equal(t, bytes.Repeat([]byte{'p'}, 4096), ranged.content[4096:8192])
			}
			require.Equal(t, bytes.Repeat([]byte{'q'}, 4096), ranged.content[2*meta.ChunkSize:])
		})
	}
}

// TestRangeFlushReportsEarlierError keeps returning a writer failure even when the range has no pending slices.
func TestRangeFlushReportsEarlierError(t *testing.T) {
	for _, eno := range []syscall.Errno{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
		t.Run(eno.Error(), func(t *testing.T) {
			f, _ := pendingFlushWriter(t, true)
			f.Lock()
			f.err = eno
			f.Unlock()
			require.Equal(t, eno, f.w.FlushRange(meta.Background(), f.inode, 10*meta.ChunkSize, 4096))
		})
	}
}

// TestRangeFlushReadError returns the commit failure of an in-range write instead of stale data.
func TestRangeFlushReadError(t *testing.T) {
	for _, eno := range []syscall.Errno{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT, syscall.ENOENT} {
		t.Run(eno.Error(), func(t *testing.T) {
			f := newRangeTestFile(t, WriterFlushScopeRange)
			f.write(t, 0, []byte("old"))
			require.Zero(t, f.v.Fsync(f.ctx, f.ino, 0, f.fh))
			f.v.writer.(*dataWriter).m = &failingWriteMeta{Meta: f.v.Meta, err: eno}
			f.write(t, 0, []byte("new"))
			buf := []byte("???")
			var n int
			var got syscall.Errno
			within(t, "read", func() { n, got = f.v.Read(f.ctx, f.ino, buf, 0, f.fh) })
			require.Equal(t, eno, got)
			require.Zero(t, n)
			require.Equal(t, "???", string(buf))
		})
	}
}

// TestWriterFlushScopeNormalization falls back to the whole-file barrier for unknown scopes.
func TestWriterFlushScopeNormalization(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	for _, tc := range []struct{ in, want string }{
		{"", WriterFlushScopeFile}, {"file", WriterFlushScopeFile}, {"range", WriterFlushScopeRange}, {"chunk", WriterFlushScopeFile},
	} {
		conf := *v.Conf
		conf.WriterFlushScope = tc.in
		w := NewDataWriter(&conf, v.Meta, v.writer.(*dataWriter).store, v.reader).(*dataWriter)
		require.Equal(t, tc.want, w.conf.WriterFlushScope, tc.in)
	}
}

// TestRangeFlushConcurrentReadahead mixes sequential reads (readahead) of chunks that
// another handle keeps rewriting with read-after-write checks in a different chunk,
// then verifies that no stale readahead buffer survives a range barrier's commit.
func TestRangeFlushConcurrentReadahead(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	const region = 1 << 20
	model := make([][]byte, 4) // chunks 1..3 as last written by the background writer
	for i := 1; i < 4; i++ {
		model[i] = make([]byte, region)
		f.write(t, uint64(i)*meta.ChunkSize, model[i])
	}
	_, wfh, eno := f.v.Open(f.ctx, f.ino, syscall.O_RDWR)
	require.Zero(t, eno)
	t.Cleanup(func() { f.v.Release(f.ctx, f.ino, wfh) })
	_, rfh, eno := f.v.Open(f.ctx, f.ino, syscall.O_RDONLY)
	require.Zero(t, eno)
	t.Cleanup(func() { f.v.Release(f.ctx, f.ino, rfh) })

	stop := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	wg.Add(2)
	go func() { // background writer of chunks 1..3
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			c := i%3 + 1
			off := (i * 4096 * 7) % region
			data := bytes.Repeat([]byte{byte(i%250 + 1)}, 4096)
			if eno := f.v.Write(f.ctx, f.ino, data, uint64(c)*meta.ChunkSize+uint64(off), wfh); eno != 0 {
				errs <- fmt.Errorf("write: %s", eno)
				return
			}
			copy(model[c][off:], data)
		}
	}()
	go func() { // sequential reader that triggers readahead over chunks 1..3
		defer wg.Done()
		buf := make([]byte, 128<<10)
		for {
			for c := 1; c < 4; c++ {
				for off := 0; off < region; off += len(buf) {
					select {
					case <-stop:
						return
					default:
					}
					if _, eno := f.v.Read(f.ctx, f.ino, buf, uint64(c)*meta.ChunkSize+uint64(off), rfh); eno != 0 {
						errs <- fmt.Errorf("read: %s", eno)
						return
					}
				}
			}
		}
	}()
	for k := 0; k < 200; k++ {
		off := uint64(k*4096) % region
		data := bytes.Repeat([]byte{byte(k%250 + 1)}, 4096)
		f.write(t, off, data)
		require.Equal(t, data, f.read(t, off, 4096), "read-after-write in chunk 0, round %d", k)
	}
	close(stop)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	// Every write is complete; a range barrier must expose all of them through the shared reader.
	buf := make([]byte, region)
	for c := 1; c < 4; c++ {
		var n int
		within(t, "final read", func() { n, eno = f.v.Read(f.ctx, f.ino, buf, uint64(c)*meta.ChunkSize, rfh) })
		require.Zero(t, eno)
		require.Equal(t, region, n)
		require.True(t, bytes.Equal(model[c], buf), "stale data in chunk %d", c)
	}
}
