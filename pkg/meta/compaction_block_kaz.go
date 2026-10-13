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
	"errors"
	"strings"
	"sync"
	"syscall"
)

// kazChunkReplacer replaces the leading records of a chunk with an arbitrary new
// list in one transaction (kaz block compaction). It mirrors doCompactChunk,
// which can only put a single new record, and lives outside the engine interface
// so upstream changes to that interface do not touch it.
type kazChunkReplacer interface {
	// doKazReplaceChunk replaces origin (the chunk's leading records) by replaced
	// when they still match, creates a reference for each created slice and either
	// records delayed (trash, keyed by delayedKey) or drops one reference of each
	// removed record. It returns EINVAL when the chunk no longer starts with origin.
	doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []Slice, delayed []byte, delayedKey uint64) syscall.Errno
}

const (
	// kazUpstreamMaxCompactSlices is the upstream value of maxCompactSlices;
	// engines lower it (etcd: 100) to bound their transaction size.
	kazUpstreamMaxCompactSlices = 1000
	// kazBlockMinCompactSlices is the smallest useful record limit after making
	// room for the new references of a lowered engine limit.
	kazBlockMinCompactSlices = 10
	// kazBlockWriteConcurrency bounds the blocks written at once by one compaction.
	kazBlockWriteConcurrency = 16
)

// kazBlockSize returns the volume block size in bytes, or 0 when it cannot split
// a chunk into whole blocks (block compaction then falls back to upstream).
func (m *baseMeta) kazBlockSize() uint32 {
	f := m.getFormat()
	if f == nil || f.BlockSize <= 0 {
		return 0
	}
	bs := uint64(f.BlockSize) << 10
	if bs > ChunkSize || ChunkSize%bs != 0 {
		return 0
	}
	return uint32(bs)
}

// kazBlockCompactLimit returns how many leading records one block compaction may
// take. Engines that lowered maxCompactSlices (etcd) also need room for one new
// reference per block; ok is false when too little room is left.
func kazBlockCompactLimit(blockSize uint32) (int, bool) {
	if blockSize == 0 {
		return 0, false
	}
	if maxCompactSlices >= kazUpstreamMaxCompactSlices {
		return maxCompactSlices, true
	}
	limit := maxCompactSlices - int(ChunkSize/uint64(blockSize))
	return limit, limit >= kazBlockMinCompactSlices
}

// kazBlockEvent adds n to one block-compaction counter.
func (m *baseMeta) kazBlockEvent(event string, n int) {
	if m.kazCompactMetrics != nil && n > 0 {
		m.kazCompactMetrics.block.WithLabelValues(event).Add(float64(n))
	}
}

// compactChunkBlocks rewrites only the blocks of a chunk that small records
// touch, one new slice per block, and replaces the leading records through r.
// It shares m.compacting with upstream compactChunk, so a chunk is never
// compacted twice at once by this client.
func (m *baseMeta) compactChunkBlocks(r kazChunkReplacer, inode Ino, indx uint32, tier int) {
	// The dispatcher chooses upstream or block and checks these first; this is
	// only a defensive guard (it must not call upstream: it would hold m.compacting).
	bs := m.kazBlockSize()
	limit, ok := kazBlockCompactLimit(bs)
	if bs == 0 || !ok {
		logger.Warnf("block compaction %d:%d skipped: block size %d unusable", inode, indx, bs)
		m.kazBlockEvent("error", 1)
		return
	}
	k := uint64(inode) + (uint64(indx) << 40)
	m.Lock()
	if (m.sessCtx != nil && m.sessCtx.Canceled()) || len(m.compacting) > 10 || m.compacting[k] {
		m.Unlock()
		return
	}
	m.compacting[k] = true
	m.Unlock()
	defer func() {
		m.Lock()
		delete(m.compacting, k)
		m.Unlock()
	}()

	ss, st := m.en.doRead(Background(), inode, indx)
	if st != 0 {
		return
	}
	if ss == nil {
		logger.Errorf("Corrupt value for inode %d chunk indx %d", inode, indx)
		return
	}
	if len(ss) > limit {
		ss = ss[:limit]
	}
	plan := planKazBlockCompaction(ss, bs, m.conf.kazBlockHiddenRatio(), m.conf.kazBlockMaxSlices(int(ChunkSize/uint64(bs))))
	if plan == nil {
		m.kazBlockEvent("noop", 1)
		return
	}
	if tier == -1 {
		var attr Attr
		if eno := m.GetAttr(Background(), inode, &attr); eno != 0 {
			return
		}
		tier = int(attr.Tier)
	}
	created, err := m.writeKazBlocks(inode, indx, plan.outs, tier)
	if err != nil {
		for _, c := range created {
			m.deleteSlice(c.Id, c.Size)
		}
		if !strings.Contains(err.Error(), "not exist") && !strings.Contains(err.Error(), "not found") {
			logger.Warnf("block compaction %d %d with %d slices: %s", inode, indx, len(plan.removed), err)
		}
		m.kazBlockEvent("error", 1)
		return
	}

	replaced := make([]byte, 0, (len(plan.kept)+len(plan.outs))*sliceBytes)
	for _, s := range plan.kept {
		replaced = append(replaced, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	masks, ci := 0, 0
	for _, o := range plan.outs {
		if o.mask {
			replaced = append(replaced, marshalSlice(o.pos, 0, 0, 0, o.length)...)
			masks++
			continue
		}
		c := created[ci]
		ci++
		replaced = append(replaced, marshalSlice(o.pos, c.Id, c.Size, 0, c.Size)...)
	}
	origin := make([]byte, 0, len(ss)*sliceBytes)
	for _, s := range ss {
		origin = append(origin, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	var delayed []byte
	var delayedKey uint64
	if m.toTrash(0) {
		delayed = make([]byte, 0, len(plan.removed)*12)
		for _, s := range plan.removed {
			if s.id > 0 {
				delayed = append(delayed, m.encodeDelayedSlice(s.id, s.size)...)
			}
		}
		if len(created) > 0 {
			delayedKey = created[0].Id
		} else if len(delayed) > 0 {
			if st = m.NewSlice(Background(), &delayedKey); st != 0 {
				m.kazBlockEvent("error", 1)
				return
			}
		}
	}

	st = r.doKazReplaceChunk(inode, indx, origin, replaced, plan.removed, created, delayed, delayedKey)
	mode := "partial"
	if plan.full {
		mode = "full"
	}
	logger.Debugf("compaction block inode=%d chunk=%d mode=%s removed=%d kept=%d blocks=%d mask=%d hidden=%.2f errno=%s",
		inode, indx, mode, len(plan.removed), len(plan.kept), len(created), masks, plan.hidden, st)
	switch st {
	case 0:
		m.of.InvalidateChunk(inode, indx)
		m.kazBlockEvent(mode, 1)
		m.kazBlockEvent("blocks_written", len(created))
		m.kazBlockEvent("masks", masks)
		m.kazBlockEvent("removed", len(plan.removed))
	case syscall.EINVAL:
		logger.Infof("block compaction for %d:%d is wasted, delete %d new slices", inode, indx, len(created))
		for _, c := range created {
			m.deleteSlice(c.Id, c.Size)
		}
		m.kazBlockEvent("wasted", 1)
	default:
		logger.Warnf("block compaction %d %d: %s; %d new slices may be orphaned, will be cleaned by gc", inode, indx, st, len(created))
		m.kazBlockEvent("error", 1)
	}
}

// writeKazBlocks allocates one slice per data output and writes them through the
// CompactChunk message, at most kazBlockWriteConcurrency at a time, so a chunk's
// blocks upload in parallel as upstream's single slice does. It returns the
// slices written successfully (in output order) and the joined errors.
func (m *baseMeta) writeKazBlocks(inode Ino, indx uint32, outs []kazBlockOut, tier int) ([]Slice, error) {
	var data []kazBlockOut
	for _, o := range outs {
		if !o.mask {
			data = append(data, o)
		}
	}
	ids := make([]uint64, len(data))
	for i := range ids {
		if st := m.NewSlice(Background(), &ids[i]); st != 0 {
			return nil, st
		}
	}
	errs := make([]error, len(data))
	sem := make(chan struct{}, kazBlockWriteConcurrency)
	var wg sync.WaitGroup
	for i, o := range data {
		logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=object slices=%d bytes=%d", inode, indx, ids[i], len(o.pieces), o.length)
		sem <- struct{}{}
		wg.Add(1)
		go func(i int, o kazBlockOut) {
			defer wg.Done()
			defer func() { <-sem }()
			errs[i] = m.newMsg(CompactChunk, o.pieces, ids[i], uint8(tier))
		}(i, o)
	}
	wg.Wait()
	var written []Slice
	var err error
	for i, o := range data {
		if errs[i] != nil {
			err = errors.Join(err, errs[i])
			continue
		}
		written = append(written, Slice{Id: ids[i], Size: o.length, Len: o.length})
	}
	return written, err
}
