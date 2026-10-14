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
	"time"
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
// a chunk into whole blocks (the dispatcher then falls back to upstream compaction).
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

// kazBlockAmount adds n to one block-compaction amount counter.
func (m *baseMeta) kazBlockAmount(kind string, n int) {
	if m.kazCompactMetrics != nil && n > 0 {
		m.kazCompactMetrics.amount.WithLabelValues(kind).Add(float64(n))
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
	start := time.Now()
	k := uint64(inode) + (uint64(indx) << 40)
	m.Lock()
	if (m.sessCtx != nil && m.sessCtx.Canceled()) || len(m.compacting) > 10 || m.compacting[k] {
		m.Unlock()
		return
	}
	m.compacting[k] = true
	m.Unlock()
	queueWait := time.Since(start)
	var objectTime, metadataTime time.Duration
	var sliceCount int
	var compactSize uint32
	// Same slow-compaction warning as upstream, marked with mode=block.
	defer func() {
		if total := time.Since(start); total >= time.Second {
			logger.Warnf("slow compaction inode=%d chunk=%d once=false force=false slices=%d bytes=%d total=%s queue_wait=%s object=%s metadata=%s mode=block",
				inode, indx, sliceCount, compactSize, total, queueWait, objectTime, metadataTime)
		}
	}()
	defer func() {
		m.Lock()
		delete(m.compacting, k)
		m.Unlock()
	}()

	logger.Debugf("compaction inode=%d chunk=%d phase=read queue_wait=%s", inode, indx, queueWait)
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
	sliceCount = len(ss)
	plan := planKazBlockCompaction(ss, bs, m.conf.kazBlockHiddenRatio(), m.conf.kazBlockMaxSlices(int(ChunkSize/uint64(bs))))
	if plan == nil {
		m.kazBlockEvent("noop", 1)
		logger.Debugf("compaction block inode=%d chunk=%d mode=noop removed=0 kept=%d blocks=0 mask=0 hidden=0.00 slices=%d total=%s errno=0",
			inode, indx, len(ss), len(ss), time.Since(start))
		return
	}
	if tier == -1 {
		var attr Attr
		if eno := m.GetAttr(Background(), inode, &attr); eno != 0 {
			return
		}
		tier = int(attr.Tier)
	}
	phaseStart := time.Now()
	created, err := m.writeKazBlocks(inode, indx, plan.outs, tier, sliceCount, &compactSize)
	objectTime = time.Since(phaseStart)
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

	var firstID uint64
	if len(created) > 0 {
		firstID = created[0].Id
	}
	phaseStart = time.Now()
	st = r.doKazReplaceChunk(inode, indx, origin, replaced, plan.removed, created, delayed, delayedKey)
	metadataTime = time.Since(phaseStart)
	logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=done errno=%s", inode, indx, firstID, st)
	mode := "partial"
	if plan.full {
		mode = "full"
	}
	logger.Debugf("compaction block inode=%d chunk=%d mode=%s removed=%d kept=%d blocks=%d mask=%d hidden=%.2f slices=%d total=%s errno=%s",
		inode, indx, mode, len(plan.removed), len(plan.kept), len(created), masks, plan.hidden, sliceCount, time.Since(start), st)
	switch st {
	case 0:
		m.of.InvalidateChunk(inode, indx)
		m.kazBlockEvent(mode, 1)
		m.kazBlockAmount("blocks_written", len(created))
		m.kazBlockAmount("masks", masks)
		m.kazBlockAmount("removed", len(plan.removed))
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
func (m *baseMeta) writeKazBlocks(inode Ino, indx uint32, outs []kazBlockOut, tier int, readSlices int, totalBytes *uint32) ([]Slice, error) {
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
	// One upstream-shaped line per compaction (first new slice id, bytes of all blocks);
	// per-block lines let PUTs be attributed to slice ids.
	for _, o := range data {
		*totalBytes += o.length
	}
	var firstID uint64
	if len(ids) > 0 {
		firstID = ids[0]
	}
	logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=object slices=%d bytes=%d", inode, indx, firstID, readSlices, *totalBytes)
	errs := make([]error, len(data))
	sem := make(chan struct{}, kazBlockWriteConcurrency)
	var wg sync.WaitGroup
	for i, o := range data {
		logger.Debugf("compaction block-object inode=%d chunk=%d slice=%d pos=%d bytes=%d", inode, indx, ids[i], o.pos, o.length)
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

// backgroundCompactChunk runs one background compaction of a chunk: the kaz
// block mode when selected and usable, otherwise upstream compactChunk. Forced
// and synchronous compaction call compactChunk directly and never come here.
func (m *baseMeta) backgroundCompactChunk(inode Ino, indx uint32, tier int) {
	if m.conf.kazBlockCompactionEnabled() {
		r, ok := m.en.(kazChunkReplacer)
		if bs := m.kazBlockSize(); ok && bs > 0 {
			if _, fits := kazBlockCompactLimit(bs); fits {
				m.compactChunkBlocks(r, inode, indx, tier)
				return
			}
		}
		m.kazBlockEvent("fallback_upstream", 1)
	}
	m.compactChunk(inode, indx, false, false, tier)
}

// logKazCompactionMode states once at startup which background compaction runs.
func (m *baseMeta) logKazCompactionMode() {
	if !m.conf.kazBlockCompactionEnabled() {
		logger.Infof("kaz compaction mode: upstream")
		return
	}
	bs := m.kazBlockSize()
	_, ok := m.en.(kazChunkReplacer)
	_, fits := kazBlockCompactLimit(max(bs, 1))
	if !ok || bs == 0 || !fits {
		logger.Warnf("kaz compaction mode: block is not usable here (engine support %t, block size %d, room for new references %t); using upstream compaction", ok, bs, fits)
		return
	}
	blocks := int(ChunkSize / uint64(bs))
	logger.Infof("kaz compaction mode: block (block size %d, hidden-ratio %.2f, max-slices %d)", bs, m.conf.kazBlockHiddenRatio(), m.conf.kazBlockMaxSlices(blocks))
	if v := m.conf.KazCompactionBlockMaxSlices; v != 0 && v < blocks+1 {
		logger.Warnf("kaz compaction mode: max-slices %d is below blocks per chunk + 1 (%d); a chunk is rewritten whole whenever its kept and rewritten records exceed it, which for a chunk already split into blocks means every compaction that has new writes", v, blocks+1)
	}
}
