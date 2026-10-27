/*
 * JuiceFS, Copyright 2020 Juicedata, Inc.
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
	"math/rand"
	"sync"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
)

const (
	flushDuration = time.Second * 5
)

type FileWriter interface {
	Write(ctx meta.Context, offset uint64, data []byte) syscall.Errno
	Flush(ctx meta.Context) syscall.Errno
	Close(ctx meta.Context) syscall.Errno
	GetLength() uint64
	Truncate(length uint64)
}

type DataWriter interface {
	Open(inode Ino, fleng uint64, tierID uint8) FileWriter
	Flush(ctx meta.Context, inode Ino) syscall.Errno
	FlushRange(ctx meta.Context, inode Ino, off, size uint64) syscall.Errno
	GetLength(inode Ino) uint64
	Truncate(inode Ino, length uint64)
	GrowTo(inode Ino, length uint64)
	UpdateMeta(inode Ino, update func() syscall.Errno) syscall.Errno
	UpdateMtime(inode Ino, mtime time.Time)
	FlushAll() error
}

type sliceWriter struct {
	id      uint64
	chunk   *chunkWriter
	off     uint32
	length  uint32
	soff    uint32
	slen    uint32
	writer  chunk.Writer
	freezed bool
	done    bool
	err     syscall.Errno
	notify  *utils.Cond
	started time.Time
	lastMod time.Time

	growing   bool
	committed bool
	dep       *sliceWriter
}

func (s *sliceWriter) prepareID(ctx meta.Context, retry bool) {
	f := s.chunk.file
	f.Lock()
	for s.id == 0 {
		var id uint64
		f.Unlock()
		st := f.w.m.NewSlice(ctx, &id)
		f.Lock()
		if st != 0 && st != syscall.EIO {
			s.err = st
			break
		}
		if !retry || st == 0 {
			if s.id == 0 {
				s.id = id
			}
			break
		}
		f.Unlock()
		logger.Debugf("meta is not available: %s", st)
		time.Sleep(time.Millisecond * 100)
		f.Lock()
	}
	if s.writer != nil && s.writer.ID() == 0 {
		s.writer.SetID(s.id)
	}
	f.Unlock()
}

func (s *sliceWriter) markDone() {
	f := s.chunk.file
	f.Lock()
	s.done = true
	s.notify.Signal()
	f.Unlock()
}

// freezed, no more data
func (s *sliceWriter) flushData() {
	defer s.markDone()
	if s.slen == 0 {
		return
	}
	s.prepareID(meta.Background(), true)
	// kaz: trace when a frozen slice starts finishing its blocks.
	s.logFinish()
	if s.err != 0 {
		logger.Infof("flush inode: %v chunk: %d err: %s", s.chunk.file.inode, s.id, s.err)
		s.writer.Abort()
		return
	}
	s.length = s.slen
	if err := s.writer.Finish(int(s.length)); err != nil {
		logger.Errorf("upload inode: %v chunk: %v (length: %v) fail: %s", s.chunk.file.inode, s.id, s.length, err)

		s.writer.Abort()
		s.err = syscall.EIO
	}
}

// protected by s.chunk.file
func (s *sliceWriter) write(ctx meta.Context, off uint32, data []uint8) syscall.Errno {
	f := s.chunk.file
	_, err := s.writer.WriteAt(data, int64(off))
	if err != nil {
		logger.Warnf("write inode: %v chunk: %d off: %d %s", s.chunk.file.inode, s.id, off, err)
		return syscall.EIO
	}
	if off+uint32(len(data)) > s.slen {
		s.slen = off + uint32(len(data))
	}
	s.lastMod = time.Now()
	if s.slen == meta.ChunkSize {
		// kaz: freeze a full slice so that no later write reuses it.
		s.freeze("full_slice")
	} else if int(s.slen) >= f.w.blockSize {
		if s.id > 0 {
			err := s.writer.FlushTo(int(s.slen))
			if err != nil {
				logger.Warnf("write inode: %v chunk: %d off: %d %s", s.chunk.file.inode, s.id, off, err)
				return syscall.EIO
			}
		}
	}
	return 0
}

type chunkWriter struct {
	indx   uint32
	file   *fileWriter
	slices []*sliceWriter
}

// protected by file
func (c *chunkWriter) findWritableSlice(pos uint32, size uint32) *sliceWriter {
	blockSize := uint32(c.file.w.blockSize)
	for i := range c.slices {
		s := c.slices[len(c.slices)-1-i]
		if !s.freezed {
			flushoff := s.slen / blockSize * blockSize
			if pos >= s.off+flushoff && pos <= s.off+s.slen {
				return s
			} else if i >= c.file.w.conf.WriterReuseWindow {
				// kaz: only the unflushed tail is reused; older candidates are frozen.
				s.freeze("writable_window")
			}
		}
		if pos < s.off+s.slen && s.off < pos+size {
			// overlaped
			// TODO: write into multiple slices
			return nil
		}
	}
	return nil
}

func (c *chunkWriter) commitThread() {
	f := c.file
	defer f.w.free(f)
	f.Lock()

	// the slices should be committed in the order that are created
	for len(c.slices) > 0 {
		s := c.slices[0]
		for !s.done {
			if s.notify.WaitWithTimeout(time.Millisecond*100) && !s.freezed && time.Since(s.started) > f.w.conf.SliceFlushWait*2 {
				s.freeze("commit_age")
			}
		}
		for s.dep != nil && !s.dep.committed {
			f.commitcond.WaitWithTimeout(time.Millisecond * 100)
		}
		// kaz: commit consecutive done slices together, in creation order.
		batch := c.commitBatch()
		err := s.err
		f.Unlock()

		committed := 0
		uncertain := false
		var mtime time.Time
		var gen uint64
		ordered := err == 0
		if ordered {
			// Chunks commit concurrently; choosing the mtime and applying it in the
			// same order keeps an older slice from moving the file mtime back.
			f.commitMu.Lock()
			f.Lock()
			// Slices that finished while this chunk waited for the commit order join the batch.
			batch = c.commitBatch()
			mtime, gen = s.lastMod, f.mtimeGen
			for _, b := range batch[1:] {
				if b.lastMod.After(mtime) {
					mtime = b.lastMod
				}
			}
			if mtime.Before(f.mtimeFloor) {
				mtime = f.mtimeFloor
			}
			f.Unlock()
			committed, err, uncertain = c.commitSlices(batch, mtime)
		}

		f.Lock()
		if committed > 0 && gen == f.mtimeGen {
			f.raiseMtimeFloor(mtime)
		}
		if ordered {
			f.commitMu.Unlock()
		}
		for _, b := range batch[:committed] {
			c.markCommitted(b)
		}
		if err != 0 {
			// Only the failed slice fails, unless a batch transaction failed with an
			// unknown outcome: then the rest may have been applied and must not be
			// sent again.
			end := committed + 1
			if uncertain {
				end = len(batch)
			}
			if err == syscall.ENOENT || err == syscall.ENOSPC || err == syscall.EDQUOT {
				for _, failed := range batch[committed:end] {
					go func(id uint64, length int) {
						_ = f.w.store.Remove(id, length)
					}(failed.id, int(failed.length))
				}
			} else {
				logger.Warnf("write inode:%d error: %s", f.inode, err)
				err = syscall.EIO
			}
			f.err = err
			logger.Errorf("write inode:%d indx:%d %s", f.inode, c.indx, err)
			for _, failed := range batch[committed:end] {
				c.markCommitted(failed)
			}
			committed = end
		}
		c.slices = c.slices[committed:]
	}
	f.freeChunk(c)
	f.Unlock()
}

type fileWriter struct {
	sync.Mutex
	w *dataWriter

	inode        Ino
	length       uint64
	tierID       uint8
	err          syscall.Errno
	flushwaiting uint16
	writewaiting uint16
	refs         uint16
	chunks       map[uint32]*chunkWriter
	// kaz: writer state of the kaz features (writer_kaz.go).
	kazFileWriterState

	flushcond  *utils.Cond // wait for chunks==nil (flush) or committed slices (flushRange)
	writecond  *utils.Cond // wait for flushwaiting==0 (write)
	commitcond *utils.Cond // wait for committed==true of dependency slice (commit)
}

// protected by file
func (f *fileWriter) findChunk(i uint32) *chunkWriter {
	c := f.chunks[i]
	if c == nil {
		c = &chunkWriter{indx: i, file: f}
		f.chunks[i] = c
	}
	return c
}

// protected by file
func (f *fileWriter) freeChunk(c *chunkWriter) {
	delete(f.chunks, c.indx)
	if len(f.chunks) == 0 && f.flushwaiting > 0 {
		f.flushcond.Broadcast()
	}
}

// protected by file
func (f *fileWriter) writeChunk(ctx meta.Context, indx uint32, off uint32, data []byte) syscall.Errno {
	c := f.findChunk(indx)
	s := c.findWritableSlice(off, uint32(len(data)))
	if s == nil {
		s = &sliceWriter{
			chunk:   c,
			off:     off,
			writer:  f.w.store.NewWriter(0, f.tierID),
			notify:  utils.NewCond(&f.Mutex),
			started: time.Now(),
		}
		go s.prepareID(meta.Background(), false)
		c.slices = append(c.slices, s)
		if len(c.slices) == 1 {
			f.w.Lock()
			f.refs++
			f.w.Unlock()
			go c.commitThread()
			if uint64(indx)*meta.ChunkSize >= f.length {
				// first slice of a new chunk, try to find the last slice of the last chunk as dependency
				var lastChunk *chunkWriter
				for i, c := range f.chunks {
					if i < indx && (lastChunk == nil || i > lastChunk.indx) {
						lastChunk = c
					}
				}
				if lastChunk != nil {
					var lastSlice *sliceWriter
					for _, s := range lastChunk.slices {
						if s.growing {
							lastSlice = s
						}
					}
					s.dep = lastSlice
				}
			}
		}
	}
	if !s.growing && uint64(indx)*meta.ChunkSize+uint64(off)+uint64(len(data)) > f.length {
		s.growing = true
	}
	return s.write(ctx, off-s.off, data)
}

func (f *fileWriter) totalSlices() int {
	var cnt int
	f.Lock()
	for _, c := range f.chunks {
		cnt += len(c.slices)
	}
	f.Unlock()
	return cnt
}

func (w *dataWriter) usedBufferSize() int64 {
	return utils.AllocMemory() - w.store.UsedMemory()
}

func (f *fileWriter) Write(ctx meta.Context, off uint64, data []byte) syscall.Errno {
	for f.totalSlices() >= 1000 {
		time.Sleep(time.Millisecond)
	}
	if f.w.usedBufferSize() > f.w.bufferSize {
		// slow down
		time.Sleep(time.Millisecond * 10)
		for f.w.usedBufferSize() > f.w.bufferSize*2 {
			time.Sleep(time.Millisecond * 100)
		}
	}

	s := time.Now()
	f.Lock()
	defer f.Unlock()
	size := uint64(len(data))
	f.writewaiting++
	for f.flushwaiting > 0 {
		if f.writecond.WaitWithTimeout(time.Second) && ctx.Canceled() {
			f.writewaiting--
			logger.Warnf("write %d interrupted after %d", f.inode, time.Since(s))
			return syscall.EINTR
		}
	}
	f.writewaiting--

	indx := uint32(off / meta.ChunkSize)
	pos := uint32(off % meta.ChunkSize)
	for len(data) > 0 {
		n := uint32(len(data))
		if pos+n > meta.ChunkSize {
			n = meta.ChunkSize - pos
		}
		if st := f.writeChunk(ctx, indx, pos, data[:n]); st != 0 {
			return st
		}
		data = data[n:]
		indx++
		pos = (pos + n) % meta.ChunkSize
	}
	if off+size > f.length {
		f.length = off + size
	}
	return f.err
}

func (f *fileWriter) updateMtime(t time.Time) {
	// kaz: an explicit mtime (utimes) drops the mtime floor, so an older explicit
	// time is not overridden by earlier commits.
	f.commitMu.Lock()
	defer f.commitMu.Unlock()
	f.Lock()
	defer f.Unlock()
	f.mtimeFloor = time.Time{}
	f.mtimeGen++
	for _, c := range f.chunks {
		for _, s := range c.slices {
			s.lastMod = t
		}
	}
}

// flush waits for pending commits, reporting actual failures and only explicitly configured deadlines.
func (f *fileWriter) flush(ctx meta.Context, writeback bool) (err syscall.Errno) {
	trace := beginWriterFlush(ctx, f.inode)
	defer func() { trace.end(err) }()
	s := time.Now()
	f.Lock()
	defer f.Unlock()
	f.flushwaiting++

	err = f.waitFlushed(ctx, trace, s, flushWait{
		done: func() bool { return len(f.chunks) == 0 },
		freeze: func(trace writerFlushTrace) {
			for _, c := range f.chunks {
				for _, s := range c.slices {
					if !s.freezed {
						s.freezeWithTrace("explicit_flush", trace)
					}
				}
			}
		},
		pending: func() (int, int) {
			pending := 0
			for _, c := range f.chunks {
				pending += len(c.slices)
			}
			return len(f.chunks), pending
		},
		dump: func() {
			for _, c := range f.chunks {
				for _, s := range c.slices {
					logger.Errorf("pending slice %d-%d: %+v", f.inode, c.indx, *s)
				}
			}
		},
	})
	f.flushwaiting--
	if f.flushwaiting == 0 && f.writewaiting > 0 {
		f.writecond.Broadcast()
	}
	if err == 0 {
		err = f.err
	}
	return err
}

func (f *fileWriter) Flush(ctx meta.Context) syscall.Errno {
	return f.flush(ctx, false)
}

func (f *fileWriter) Close(ctx meta.Context) syscall.Errno {
	// kaz: tag the flush origin for writer flush diagnostics.
	if writerFlushOrigin(ctx) == "unknown" {
		ctx = WithWriterFlushOrigin(ctx, "internal.close")
	}
	defer f.w.free(f)
	return f.Flush(ctx)
}

func (f *fileWriter) GetLength() uint64 {
	f.Lock()
	defer f.Unlock()
	return f.length
}

func (f *fileWriter) Truncate(length uint64) {
	f.Lock()
	defer f.Unlock()
	// TODO: truncate write buffer if length < f.length
	f.length = length
}

type dataWriter struct {
	sync.Mutex
	m          meta.Meta
	store      chunk.ChunkStore
	conf       *Config
	reader     DataReader
	blockSize  int
	bufferSize int64
	files      map[Ino]*fileWriter
	maxRetries uint32
}

func NewDataWriter(conf *Config, m meta.Meta, store chunk.ChunkStore, reader DataReader) DataWriter {
	kazNormalizeWriterConf(conf)
	w := &dataWriter{
		m:          m,
		store:      store,
		reader:     reader,
		conf:       conf,
		blockSize:  conf.Chunk.BlockSize,
		bufferSize: int64(conf.Chunk.BufferSize),
		files:      make(map[Ino]*fileWriter),
		maxRetries: uint32(conf.Meta.Retries),
	}
	go w.flushAll()
	return w
}

func (w *dataWriter) flushAll() {
	for {
		w.Lock()
		now := time.Now()
		for _, f := range w.files {
			f.refs++
			w.Unlock()
			tooMany := f.totalSlices() > 800
			f.Lock()

			lastBit := uint32(rand.Int() % 2) // choose half of chunks randomly
			for i, c := range f.chunks {
				hs := len(c.slices) / 2
				for j, s := range c.slices {
					if !s.freezed && (now.Sub(s.started) > w.conf.SliceFlushWait ||
						now.Sub(s.lastMod) > w.conf.SliceFlushIdle && now.Sub(s.started) > w.conf.SliceFlushIdle ||
						tooMany && i%2 == lastBit && j <= hs) {
						// kaz: record why a slice was closed (age, idle or pressure).
						reason := "slice_pressure"
						if now.Sub(s.started) > w.conf.SliceFlushWait {
							reason = "age"
						} else if now.Sub(s.lastMod) > w.conf.SliceFlushIdle && now.Sub(s.started) > w.conf.SliceFlushIdle {
							reason = "idle"
						}
						s.freeze(reason)
					}
				}
			}
			f.Unlock()
			w.free(f)
			w.Lock()
		}
		w.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

func (w *dataWriter) Open(inode Ino, len uint64, tierID uint8) FileWriter {
	w.Lock()
	defer w.Unlock()
	f, ok := w.files[inode]
	if !ok {
		f = &fileWriter{
			w:      w,
			inode:  inode,
			length: len,
			tierID: tierID,
			chunks: make(map[uint32]*chunkWriter),
		}
		f.flushcond = utils.NewCond(f)
		f.writecond = utils.NewCond(f)
		f.commitcond = utils.NewCond(f)
		w.files[inode] = f
	}
	f.refs++
	return f
}

func (w *dataWriter) find(inode Ino) *fileWriter {
	w.Lock()
	defer w.Unlock()
	return w.files[inode]
}

func (w *dataWriter) free(f *fileWriter) {
	w.Lock()
	defer w.Unlock()
	f.refs--
	if f.refs == 0 {
		delete(w.files, f.inode)
	}
}

func (w *dataWriter) Flush(ctx meta.Context, inode Ino) syscall.Errno {
	f := w.find(inode)
	if f != nil {
		return f.Flush(ctx)
	}
	return 0
}

func (w *dataWriter) GetLength(inode Ino) uint64 {
	f := w.find(inode)
	if f != nil {
		return f.GetLength()
	}
	return 0
}

func (w *dataWriter) Truncate(inode Ino, len uint64) {
	f := w.find(inode)
	if f != nil {
		f.Truncate(len)
	}
}

func (w *dataWriter) UpdateMtime(inode Ino, mtime time.Time) {
	f := w.find(inode)
	if f != nil {
		f.updateMtime(mtime)
	}
}

func (w *dataWriter) FlushAll() error {
	var err error
	w.Lock()
	for inode, ind := range w.files {
		ind.refs++
		w.Unlock()
		eno := ind.Flush(WithWriterFlushOrigin(meta.Background(), "internal.FlushAll"))
		w.free(ind)
		if eno != 0 {
			logger.Errorf("flush %s: %s", inode, eno)
			return eno
		}
		logger.Debugf("Flush %d", inode)
		w.Lock()
	}
	w.Unlock()
	return err
}
