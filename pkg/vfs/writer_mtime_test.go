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
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// mtime returns the committed modification time of the test file.
func (f *rangeTestFile) mtime(t *testing.T) time.Time {
	t.Helper()
	var attr meta.Attr
	require.Zero(t, f.v.Meta.GetAttr(f.ctx, f.ino, &attr))
	return time.Unix(attr.Mtime, int64(attr.Mtimensec))
}

// fsync waits for every pending write of the test file.
func (f *rangeTestFile) fsync(t *testing.T) {
	t.Helper()
	var eno syscall.Errno
	within(t, "fsync", func() { eno = f.v.Fsync(f.ctx, f.ino, 0, f.fh) })
	require.Zero(t, eno)
}

// TestMtimeNotRolledBackByOlderCommit keeps the file mtime at the newest
// modification when an older slice of another chunk commits last.
func TestMtimeNotRolledBackByOlderCommit(t *testing.T) {
	for _, scope := range []string{WriterFlushScopeFile, WriterFlushScopeRange} {
		t.Run(scope, func(t *testing.T) {
			f := newRangeTestFile(t, scope)
			release := f.m.block(5)
			defer release()
			f.write(t, 5*meta.ChunkSize, []byte("older"))
			time.Sleep(20 * time.Millisecond)
			newest := time.Now()
			f.write(t, 0, []byte("newer"))
			done := make(chan syscall.Errno, 1)
			go func() { done <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
			// Hold chunk 5 (older) in its metadata write and give chunk 0 (newer) a
			// chance to commit first.
			f.m.awaitCommitStart(t, 5)
			time.Sleep(50 * time.Millisecond)
			release()
			select {
			case eno := <-done:
				require.Zero(t, eno)
			case <-time.After(rangeTestTimeout):
				t.Fatal("fsync did not finish")
			}
			require.False(t, f.mtime(t).Before(newest), "mtime rolled back to an older write")
		})
	}
}

// TestMtimeNotRolledBackAfterFallocate keeps the fallocate time when an older
// out-of-range slice commits after a range-scoped fallocate.
func TestMtimeNotRolledBackAfterFallocate(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	f.write(t, 0, bytes.Repeat([]byte{'i'}, 8192))
	f.fsync(t)
	release := f.m.block(2)
	defer release()
	f.write(t, 2*meta.ChunkSize, []byte("older append"))
	time.Sleep(20 * time.Millisecond)
	before := time.Now()
	var eno syscall.Errno
	within(t, "fallocate", func() { eno = f.v.Fallocate(f.ctx, f.ino, 0x10, 0, 4096, f.fh) })
	require.Zero(t, eno)
	release()
	f.fsync(t)
	require.False(t, f.mtime(t).Before(before), "mtime rolled back below the fallocate time")
}

// TestMtimeExplicitPastTimeWins lets utimes set an older mtime even after newer commits.
func TestMtimeExplicitPastTimeWins(t *testing.T) {
	for _, scope := range []string{WriterFlushScopeFile, WriterFlushScopeRange} {
		t.Run(scope, func(t *testing.T) {
			f := newRangeTestFile(t, scope)
			f.write(t, 0, []byte("first"))
			f.fsync(t)
			f.write(t, 4096, []byte("pending"))
			past := time.Unix(1000000000, 0)
			_, eno := f.v.SetAttr(f.ctx, f.ino, meta.SetAttrMtime, f.fh, 0, 0, 0, 0, past.Unix(), 0, 0, 0)
			require.Zero(t, eno)
			f.fsync(t)
			require.True(t, f.mtime(t).Equal(past), "explicit mtime %s was overridden by %s", past, f.mtime(t))
			// Later writes move mtime forward again.
			f.write(t, 8192, []byte("after"))
			f.fsync(t)
			require.True(t, f.mtime(t).After(past))
		})
	}
}

// TestMtimeFloorIgnoresFailedCommit does not advance the floor for a commit that failed.
func TestMtimeFloorIgnoresFailedCommit(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeFile)
	f.v.writer.(*dataWriter).m = &failingWriteMeta{Meta: f.v.Meta, err: syscall.EIO}
	f.write(t, 0, []byte("lost"))
	var eno syscall.Errno
	within(t, "fsync", func() { eno = f.v.Fsync(f.ctx, f.ino, 0, f.fh) })
	require.Equal(t, syscall.EIO, eno)
	fw := f.fileWriter(t)
	fw.Lock()
	defer fw.Unlock()
	require.True(t, fw.mtimeFloor.IsZero(), "a failed commit advanced the mtime floor")
}

// TestRangePreflushReleasesHandleForRead lets writes through the same handle
// proceed while a read waits for in-range commits, and still returns writes
// that completed before the read took the handle lock.
func TestRangePreflushReleasesHandleForRead(t *testing.T) {
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
	f.m.awaitCommitStart(t, 0)
	var eno syscall.Errno
	within(t, "write through the same handle", func() {
		eno = f.v.Write(f.ctx, f.ino, []byte("outside"), 2*meta.ChunkSize, f.fh)
	})
	require.Zero(t, eno)
	// An overlapping write that completes before the read locks the handle must be visible.
	within(t, "overlapping write through the same handle", func() {
		eno = f.v.Write(f.ctx, f.ino, bytes.Repeat([]byte{'z'}, 4096), 0, f.fh)
	})
	require.Zero(t, eno)
	release()
	select {
	case data := <-got:
		require.Equal(t, bytes.Repeat([]byte{'z'}, 4096), data)
	case <-time.After(rangeTestTimeout):
		t.Fatal("read did not finish")
	}
}

// TestRangePreflushReleasesHandleForFallocate lets reads and writes through the
// same handle proceed while a fallocate waits for in-range commits.
func TestRangePreflushReleasesHandleForFallocate(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeRange)
	f.write(t, 2*meta.ChunkSize, []byte("other"))
	f.fsync(t)
	release := f.m.block(0)
	defer release()
	f.write(t, 0, bytes.Repeat([]byte{'x'}, 8192))
	done := make(chan syscall.Errno, 1)
	go func() { done <- f.v.Fallocate(f.ctx, f.ino, 0x10, 0, 4096, f.fh) }()
	f.m.awaitCommitStart(t, 0)
	require.Equal(t, []byte("other"), f.read(t, 2*meta.ChunkSize, 5))
	var eno syscall.Errno
	within(t, "write through the same handle", func() {
		eno = f.v.Write(f.ctx, f.ino, []byte("more"), 2*meta.ChunkSize+4096, f.fh)
	})
	require.Zero(t, eno)
	release()
	select {
	case eno := <-done:
		require.Zero(t, eno)
	case <-time.After(rangeTestTimeout):
		t.Fatal("fallocate did not finish")
	}
	want := append(make([]byte, 4096), bytes.Repeat([]byte{'x'}, 4096)...)
	require.Equal(t, want, f.read(t, 0, 8192))
}
