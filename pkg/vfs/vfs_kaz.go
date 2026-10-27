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
	"syscall"

	"github.com/juicedata/juicefs/pkg/meta"
)

// kazMetaFallocate runs Meta.Fallocate through the writer. Fallocate sets mtime
// to now; ordering it with slice commits keeps pending commits from moving it back.
func (v *VFS) kazMetaFallocate(ctx Context, ino Ino, mode uint8, off, size uint64, length *uint64) syscall.Errno {
	return v.writer.UpdateMeta(ino, func() syscall.Errno {
		return v.Meta.Fallocate(ctx, ino, mode, off, size, length)
	})
}

// kazSyncWriterLength updates the writer length after a successful fallocate and
// returns the length the caller should use for invalidation.
func (v *VFS) kazSyncWriterLength(ino Ino, length uint64) uint64 {
	if v.Conf.WriterFlushScope == WriterFlushScopeRange {
		// Uncommitted appends outside the range are not in the metadata length yet,
		// and fallocate never shrinks a file, so keep the larger writer length.
		v.writer.GrowTo(ino, length)
		return max(length, v.writer.GetLength(ino))
	}
	v.writer.Truncate(ino, length)
	return length
}

// flushForRange commits the pending writes a Read or Fallocate of [off, off+size)
// depends on: only those of the touched chunks with --kaz-writer-flush-scope=range,
// otherwise every pending write of the file. fsync, close, truncate and
// copy_file_range keep using whole-file flushes.
func (v *VFS) flushForRange(ctx meta.Context, ino Ino, off, size uint64) syscall.Errno {
	if v.Conf.WriterFlushScope == WriterFlushScopeRange {
		return v.writer.FlushRange(ctx, ino, off, size)
	}
	return v.writer.Flush(ctx, ino)
}

// prelockFlush runs a range barrier before the handle lock is taken, so the
// barrier taken under the lock only waits for writes that arrive meanwhile.
// It does nothing for whole-file barriers, whose flush already stops writes.
func (v *VFS) prelockFlush(ctx meta.Context, ino Ino, off, size uint64) syscall.Errno {
	if v.Conf.WriterFlushScope == WriterFlushScopeRange {
		return v.writer.FlushRange(ctx, ino, off, size)
	}
	return 0
}
