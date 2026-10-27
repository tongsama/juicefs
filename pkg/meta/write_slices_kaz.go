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

package meta

import (
	"syscall"
	"time"
)

// KazSliceWriter is implemented by metadata clients that can commit several
// slices of one chunk at once. It is kept out of the upstream Meta interface,
// so a Meta without it is written one slice at a time with Write.
type KazSliceWriter interface {
	// WriteSlices appends slices to chunk indx in order, using one metadata
	// transaction when the engine supports it. The first n slices are committed;
	// if n < len(slices), slices[n] failed with st. When uncertain is true, a
	// batch transaction failed with an unknown outcome and slices[n:] may or may
	// not have been written; otherwise the slices after slices[n] were not written.
	WriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time) (n int, st syscall.Errno, uncertain bool)
}

// SliceWrite is one slice to append to a chunk, starting at Off within the chunk.
type SliceWrite struct {
	Off   uint32
	Slice Slice
}

// compactionWanted reports whether a chunk whose slice count grew from prev to
// now passed a count at which a single Write would request compaction
// (every count ending in 99, or more than 350 slices).
func compactionWanted(prev, now int) bool {
	if now > 350 {
		return true
	}
	for n := max(prev+1, 0); n <= now; n++ {
		if n%100 == 99 {
			return true
		}
	}
	return false
}

// sliceBatchWriter is implemented by engines that can append several slices of
// one chunk in a single transaction, all or nothing.
type sliceBatchWriter interface {
	doWriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno
}

// writePhases records where a metadata write spent its time, for slow-write reports.
type writePhases struct {
	numSlices              int
	backend, stat, compact time.Duration
}

// writeLocked commits one slice and applies its statistics and compaction
// triggers; the caller holds the open-file lock (if the file is open) and
// invalidates the chunk cache.
func (m *baseMeta) writeLocked(ctx Context, inode Ino, indx uint32, off uint32, slice Slice, mtime time.Time, ph *writePhases) syscall.Errno {
	var delta dirStat
	var attr Attr
	phaseStart := time.Now()
	st := m.en.doWrite(ctx, inode, indx, off, slice, mtime, &ph.numSlices, &delta, &attr)
	ph.backend = time.Since(phaseStart)
	if st == 0 {
		logger.Debugf("metadata write inode=%d chunk=%d slice=%d phase=stat slices=%d doWrite=%s", inode, indx, slice.Id, ph.numSlices, ph.backend)
		m.afterWrite(ctx, inode, indx, &attr, delta, ph.numSlices-1, ph)
	}
	return st
}

// afterWrite applies the statistics of committed slices and requests
// compaction when the chunk's slice count passed a trigger.
func (m *baseMeta) afterWrite(ctx Context, inode Ino, indx uint32, attr *Attr, delta dirStat, prevSlices int, ph *writePhases) {
	phaseStart := time.Now()
	m.updateParentStat(ctx, inode, attr.Parent, delta.length, delta.space)
	m.updateUserGroupStat(ctx, attr.Uid, attr.Gid, delta.space, 0)
	ph.stat = time.Since(phaseStart)
	if want, count := m.kazCompactionWanted(inode, indx, prevSlices, ph.numSlices); want {
		if ph.numSlices < maxSlices {
			m.requestBackgroundCompaction(inode, indx, count, int(attr.Tier))
		} else {
			logger.Debugf("metadata write inode=%d chunk=%d phase=compact slices=%d", inode, indx, ph.numSlices)
			phaseStart = time.Now()
			m.compactChunk(inode, indx, true, false, int(attr.Tier))
			ph.compact = time.Since(phaseStart)
		}
	}
}

// errWriteSlicesFallback is returned by doWriteSlices, before changing anything,
// for a batch the engine will not commit at once (for example one that would
// take a chunk past maxSlices); WriteSlices then writes the slices one by one.
const errWriteSlicesFallback = syscall.E2BIG

// batchErrorUnapplied reports errors that a write transaction returns before
// changing anything, so its slices can safely be retried one by one.
func batchErrorUnapplied(st syscall.Errno) bool {
	return st == syscall.ENOENT || st == syscall.EPERM || st == syscall.ENOSPC || st == syscall.EDQUOT || st == errWriteSlicesFallback
}

// WriteSlices appends slices to chunk indx in creation order. Engines that
// implement sliceBatchWriter commit them in one transaction; otherwise, or for
// a single slice, they are written one by one with Write.
func (m *baseMeta) WriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time) (int, syscall.Errno, bool) {
	bw, ok := m.en.(sliceBatchWriter)
	if len(slices) <= 1 || !ok {
		for i, w := range slices {
			if st := m.Write(ctx, inode, indx, w.Off, w.Slice, mtime); st != 0 {
				return i, st, false
			}
		}
		return len(slices), 0, false
	}
	start := time.Now()
	defer m.timeit("WriteSlices", start)
	first := slices[0].Slice.Id
	var lockWait time.Duration
	var ph writePhases
	var st syscall.Errno
	defer func() {
		if total := time.Since(start); total >= time.Second {
			logger.Warnf("slow metadata write batch inode=%d chunk=%d first_slice=%d count=%d slices=%d total=%s lock_wait=%s doWrite=%s stat=%s compact=%s errno=%s",
				inode, indx, first, len(slices), ph.numSlices, total, lockWait, ph.backend, ph.stat, ph.compact, st)
		}
	}()
	logger.Debugf("metadata write batch inode=%d chunk=%d first_slice=%d count=%d phase=lock_wait", inode, indx, first, len(slices))
	f := m.of.find(inode)
	if f != nil {
		lockStart := time.Now()
		f.Lock()
		defer f.Unlock()
		lockWait = time.Since(lockStart)
	}
	defer func() { m.of.InvalidateChunk(inode, indx) }()
	var delta dirStat
	var attr Attr
	phaseStart := time.Now()
	st = bw.doWriteSlices(ctx, inode, indx, slices, mtime, &ph.numSlices, &delta, &attr)
	ph.backend = time.Since(phaseStart)
	logger.Debugf("metadata write batch inode=%d chunk=%d first_slice=%d count=%d phase=done lock_wait=%s doWrite=%s slices=%d errno=%s",
		inode, indx, first, len(slices), lockWait, ph.backend, ph.numSlices, st)
	if st == 0 {
		m.afterWrite(ctx, inode, indx, &attr, delta, ph.numSlices-len(slices), &ph)
		return len(slices), 0, false
	}
	if !batchErrorUnapplied(st) {
		// The transaction may have been applied; retrying could register slices twice.
		return 0, st, true
	}
	for i, w := range slices {
		var one writePhases
		if st = m.writeLocked(ctx, inode, indx, w.Off, w.Slice, mtime, &one); st != 0 {
			return i, st, false
		}
	}
	return len(slices), 0, false
}
