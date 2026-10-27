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
	"runtime"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
)

// kazFileWriterState holds the fileWriter state added by kaz. It is embedded
// in fileWriter and starts zero; it holds a lock, so it must not be copied.
type kazFileWriterState struct {
	rangewaiting uint16     // range barriers in progress; they never block writes
	mtimeFloor   time.Time  // newest mtime this writer has recorded; later commits never go below it
	mtimeGen     uint64     // bumped by explicit mtime updates so in-flight commits do not restore the floor
	commitMu     sync.Mutex // orders mtime choice with metadata updates; taken before the file lock
}

const (
	defaultSliceFlushWait = flushDuration
	defaultSliceFlushIdle = time.Second
	// AutoWriterFlushTimeout explicitly selects the legacy retry-derived flush deadline.
	AutoWriterFlushTimeout time.Duration = -1

	// WriterFlushScopeFile makes Read and Fallocate wait for every pending write of the file.
	WriterFlushScopeFile = "file"
	// WriterFlushScopeRange makes Read and Fallocate wait only for pending writes of the
	// chunks they touch (and the slices those depend on); fsync and close stay whole-file.
	WriterFlushScopeRange = "range"
)

// kazNormalizeWriterConf validates the writer scheduling settings and replaces
// invalid or unset values with defaults; NewDataWriter calls it before starting
// the background flusher.
func kazNormalizeWriterConf(conf *Config) {
	if conf.WriterFlushTimeout < 0 && conf.WriterFlushTimeout != AutoWriterFlushTimeout {
		logger.Warnf("invalid writer flush timeout %s: using no deadline; use AutoWriterFlushTimeout for the legacy deadline", conf.WriterFlushTimeout)
		conf.WriterFlushTimeout = 0
	}
	if conf.WriterReuseWindow < 0 || conf.WriterReuseWindow > 64 {
		logger.Warnf("invalid writer reuse window %d: using default 4 (valid range 1..64)", conf.WriterReuseWindow)
		conf.WriterReuseWindow = 4
	}
	if conf.WriterReuseWindow == 0 {
		conf.WriterReuseWindow = 4
	}
	if conf.WriterFlushScope == "" {
		conf.WriterFlushScope = WriterFlushScopeFile
	} else if conf.WriterFlushScope != WriterFlushScopeFile && conf.WriterFlushScope != WriterFlushScopeRange {
		logger.Warnf("invalid writer flush scope %q: using %q", conf.WriterFlushScope, WriterFlushScopeFile)
		conf.WriterFlushScope = WriterFlushScopeFile
	}
	if conf.MetaWriteBatch < 0 || conf.MetaWriteBatch > 1024 {
		logger.Warnf("invalid meta write batch %d: disabling batching (valid range 0..1024)", conf.MetaWriteBatch)
		conf.MetaWriteBatch = 0
	}
	if conf.SliceFlushWait <= 0 {
		conf.SliceFlushWait = defaultSliceFlushWait
	}
	if conf.SliceFlushIdle <= 0 {
		conf.SliceFlushIdle = defaultSliceFlushIdle
	}
}

// freeze records why a writable slice stops growing; the caller holds the file lock.
func (s *sliceWriter) freeze(reason string) {
	s.freezeWithTrace(reason, writerFlushTrace{})
}

// freezeWithTrace associates explicit barriers with the slices they actually freeze.
func (s *sliceWriter) freezeWithTrace(reason string, trace writerFlushTrace) {
	if s.freezed {
		return
	}
	s.freezed = true
	s.logFreeze(reason, trace)
	go s.flushData()
}

// commitBatch returns the head slice and, when batching is enabled, the
// following slices that are already done without error, up to the batch size.
// Only a chunk's first slice can have a dependency, so later ones need no wait.
// The caller holds the file lock.
func (c *chunkWriter) commitBatch() []*sliceWriter {
	head := c.slices[0]
	limit := c.file.w.conf.MetaWriteBatch
	if head.err != 0 || limit <= 1 {
		return c.slices[:1]
	}
	n := 1
	for n < len(c.slices) && n < limit && c.slices[n].done && c.slices[n].err == 0 {
		n++
	}
	return c.slices[:n:n]
}

// commitSlices writes the metadata of a batch without the file lock and
// invalidates the reader for the slices that were, or may have been, committed.
// It returns how many leading slices were committed, the errno of the next one,
// and whether the slices from there on have an unknown outcome.
func (c *chunkWriter) commitSlices(batch []*sliceWriter, mtime time.Time) (int, syscall.Errno, bool) {
	f := c.file
	start := time.Now()
	var n int
	var err syscall.Errno
	var uncertain bool
	if len(batch) == 1 {
		s := batch[0]
		logger.Debugf("slice commit inode=%d chunk=%d slice=%d phase=metadata", f.inode, c.indx, s.id)
		if err = f.w.m.Write(meta.Background(), f.inode, c.indx, s.off, meta.Slice{Id: s.id, Size: s.length, Off: s.soff, Len: s.slen}, mtime); err == 0 {
			n = 1
		}
	} else {
		ws := make([]meta.SliceWrite, len(batch))
		for i, s := range batch {
			ws[i] = meta.SliceWrite{Off: s.off, Slice: meta.Slice{Id: s.id, Size: s.length, Off: s.soff, Len: s.slen}}
			logger.Debugf("slice commit inode=%d chunk=%d slice=%d phase=metadata batch=%d", f.inode, c.indx, s.id, len(batch))
		}
		if bw, ok := f.w.m.(meta.KazSliceWriter); ok {
			n, err, uncertain = bw.WriteSlices(meta.Background(), f.inode, c.indx, ws, mtime)
		} else {
			// A Meta without KazSliceWriter commits the batch one slice at a time,
			// like baseMeta.WriteSlices does for engines without batch support:
			// slices after a failed one are not written, so the outcome is certain.
			n = len(ws)
			for i, w := range ws {
				if err = f.w.m.Write(meta.Background(), f.inode, c.indx, w.Off, w.Slice, mtime); err != 0 {
					n = i
					break
				}
			}
		}
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		logger.Warnf("slow slice commit inode=%d chunk=%d slice=%d metadata=%s errno=%s batch=%d", f.inode, c.indx, batch[0].id, elapsed, err, len(batch))
	}
	end := n
	if uncertain {
		end = len(batch)
	} else if err != 0 && !metaErrorUnapplied(err) {
		end = n + 1 // this slice's commit may have been applied
	}
	for _, s := range batch[:end] {
		f.w.reader.Invalidate(f.inode, uint64(c.indx)*meta.ChunkSize+uint64(s.off), uint64(s.slen))
		// A slice that is, or may be, in the chunk records can be packed (staging_pack_kaz.go).
		f.w.kazRememberStaged(f.inode, c.indx, s)
	}
	return n, err, uncertain
}

// metaErrorUnapplied reports metadata errors raised before a transaction wrote
// anything; other errors leave it unknown whether the batch was applied.
func metaErrorUnapplied(err syscall.Errno) bool {
	return err == syscall.ENOENT || err == syscall.EPERM || err == syscall.ENOSPC || err == syscall.EDQUOT
}

// markCommitted records that a slice's commit finished (successfully or not)
// and wakes the waiters that depend on it; the caller holds the file lock.
func (c *chunkWriter) markCommitted(s *sliceWriter) {
	f := c.file
	s.committed = true
	if s.growing {
		f.commitcond.Broadcast()
	}
	if f.rangewaiting > 0 {
		// Range barriers wait for specific slices rather than for every chunk to drain.
		f.flushcond.Broadcast()
	}
}

// raiseMtimeFloor records that the file's mtime has reached t; the caller holds the file lock.
func (f *fileWriter) raiseMtimeFloor(t time.Time) {
	if t.After(f.mtimeFloor) {
		f.mtimeFloor = t
	}
}

// flushWait describes what a writer barrier must commit before it returns.
// All callbacks run with the file lock held.
type flushWait struct {
	done    func() bool                  // reports that every slice of the barrier is committed
	freeze  func(trace writerFlushTrace) // freezes the slices the barrier needs; repeated each round
	pending func() (chunks, slices int)  // counts what is still pending, for progress warnings
	dump    func()                       // logs the pending slices when an explicit deadline expires
}

// waitFlushed blocks with the file lock held until the barrier completes, a real
// writer error appears, the caller is interrupted, or an explicitly configured
// deadline elapses. Elapsed time alone never turns a healthy wait into an error.
func (f *fileWriter) waitFlushed(ctx meta.Context, trace writerFlushTrace, start time.Time, fw flushWait) (err syscall.Errno) {
	wait := f.w.flushTimeout()
	var deadline time.Time
	if wait > 0 {
		deadline = start.Add(wait)
	}
	nextWarning := start.Add(5 * time.Minute)
	for !fw.done() && err == 0 {
		if f.err != 0 {
			err = f.err
			break
		}
		fw.freeze(trace)
		poll := 3 * time.Second
		if !deadline.IsZero() {
			poll = min(poll, max(time.Until(deadline), 0))
		}
		f.flushcond.WaitWithTimeout(poll)
		// A completed commit wins even if the deadline elapsed while acquiring the lock.
		if fw.done() {
			break
		}
		if f.err != 0 {
			err = f.err
			break
		}
		if ctx.Canceled() && time.Since(start) > f.w.conf.Chunk.PutTimeout*2 {
			logger.Warnf("flush %d interrupted after %d", f.inode, time.Since(start))
			err = syscall.EINTR
			break
		}
		if !deadline.IsZero() && !time.Now().Before(deadline) {
			logger.Errorf("flush %d timeout after waited %s", f.inode, wait)
			fw.dump()
			buf := make([]byte, 1<<20)
			n := runtime.Stack(buf, true)
			logger.Warnf("All goroutines (%d):\n%s", runtime.NumGoroutine(), buf[:n])
			err = syscall.EIO
			break
		}
		if deadline.IsZero() && !time.Now().Before(nextWarning) {
			chunks, slices := fw.pending()
			logger.Warnf("flush %d still waiting after %s: pending_chunks=%d pending_slices=%d; no flush deadline configured", f.inode, time.Since(start), chunks, slices)
			nextWarning = time.Now().Add(5 * time.Minute)
		}
	}
	return err
}

// rangeFlushSet returns the uncommitted slices a barrier for [off, off+size) must
// wait for: every slice pending in the chunks the range touches at this moment,
// plus their dependency closure. A slice whose dep (the growing slice of an
// earlier chunk) is uncommitted cannot commit before it, and that chunk commits
// in creation order, so its slices up to dep are included, recursively.
// Writes that arrive later are not included, so a busy file cannot starve the barrier.
// The caller holds the file lock.
func (f *fileWriter) rangeFlushSet(off, size uint64) []*sliceWriter {
	var set []*sliceWriter
	seen := make(map[*sliceWriter]bool)
	add := func(s *sliceWriter) {
		if !seen[s] {
			seen[s] = true
			set = append(set, s)
		}
	}
	if size == 0 {
		size = 1
	}
	first, last := off/meta.ChunkSize, (off+size-1)/meta.ChunkSize
	if uint64(len(f.chunks)) < last-first+1 {
		for indx, c := range f.chunks {
			if uint64(indx) >= first && uint64(indx) <= last {
				for _, s := range c.slices {
					add(s)
				}
			}
		}
	} else {
		for indx := first; indx <= last; indx++ {
			if c := f.chunks[uint32(indx)]; c != nil {
				for _, s := range c.slices {
					add(s)
				}
			}
		}
	}
	for i := 0; i < len(set); i++ {
		d := set[i].dep
		if d == nil || d.committed {
			continue
		}
		for _, s := range d.chunk.slices {
			add(s)
			if s == d {
				break
			}
		}
	}
	return set
}

// flushRange commits the pending writes that a Read or Fallocate of [off, off+size)
// depends on, without stopping writes elsewhere in the file. It shares flush's
// error, interruption and deadline handling; a failure of any earlier commit of
// the file is still returned, as with a whole-file flush.
func (f *fileWriter) flushRange(ctx meta.Context, off, size uint64) (err syscall.Errno) {
	trace := beginWriterFlush(ctx, f.inode)
	defer func() { trace.end(err) }()
	s := time.Now()
	f.Lock()
	defer f.Unlock()
	set := f.rangeFlushSet(off, size)
	f.rangewaiting++
	err = f.waitFlushed(ctx, trace, s, flushWait{
		done: func() bool {
			for _, s := range set {
				if !s.committed {
					return false
				}
			}
			return true
		},
		freeze: func(trace writerFlushTrace) {
			for _, s := range set {
				if !s.freezed {
					s.freezeWithTrace("explicit_flush", trace)
				}
			}
		},
		pending: func() (int, int) {
			chunks := make(map[*chunkWriter]struct{})
			pending := 0
			for _, s := range set {
				if !s.committed {
					chunks[s.chunk] = struct{}{}
					pending++
				}
			}
			return len(chunks), pending
		},
		dump: func() {
			for _, s := range set {
				if !s.committed {
					logger.Errorf("pending slice %d-%d: %+v", f.inode, s.chunk.indx, *s)
				}
			}
		},
	})
	f.rangewaiting--
	if err == 0 {
		err = f.err
	}
	return err
}

// GrowTo raises the writer length to at least length and never shrinks it.
// Fallocate uses it after a range barrier, where the metadata length may not yet
// include uncommitted appends outside the range.
func (f *fileWriter) GrowTo(length uint64) {
	f.Lock()
	defer f.Unlock()
	if length > f.length {
		f.length = length
	}
}

// flushTimeout keeps legacy retry coupling opt-in and lets zero mean completion-based waiting.
func (w *dataWriter) flushTimeout() time.Duration {
	if w.conf.WriterFlushTimeout == AutoWriterFlushTimeout {
		const maxTimeout time.Duration = 1<<63 - 1
		retries := uint64(w.maxRetries) + 2
		// Check before squaring or converting seconds to nanoseconds to avoid a shorter deadline.
		maxSeconds := uint64(maxTimeout / time.Second)
		if retries > (maxSeconds*2+1)/retries {
			return maxTimeout
		}
		return max(time.Second*time.Duration(retries*retries/2), 5*time.Minute)
	}
	return w.conf.WriterFlushTimeout
}

// FlushRange commits the pending writes that a Read or Fallocate of [off, off+size) depends on.
func (w *dataWriter) FlushRange(ctx meta.Context, inode Ino, off, size uint64) syscall.Errno {
	f := w.find(inode)
	if f != nil {
		return f.flushRange(ctx, off, size)
	}
	return 0
}

// GrowTo raises the writer length of inode without shrinking it.
func (w *dataWriter) GrowTo(inode Ino, length uint64) {
	f := w.find(inode)
	if f != nil {
		f.GrowTo(length)
	}
}

// UpdateMeta runs a metadata update that sets the file mtime to about now (such as
// fallocate) in order with the inode's slice commits, and keeps later commits
// from recording an older mtime.
func (w *dataWriter) UpdateMeta(inode Ino, update func() syscall.Errno) syscall.Errno {
	f := w.find(inode)
	if f == nil {
		return update()
	}
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	st := update()
	if st == 0 {
		f.Lock()
		f.raiseMtimeFloor(time.Now())
		f.Unlock()
	}
	return st
}
