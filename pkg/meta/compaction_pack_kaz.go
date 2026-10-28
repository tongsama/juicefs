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

import "syscall"

// kazPackOut is one visible range that the packed slice takes over: the
// chunk position, the source slice id, the offset inside that source slice's
// data, and the length.
type kazPackOut struct {
	pos    uint32
	srcID  uint64
	srcOff uint32
	length uint32
}

// kazPackPlan is the result of planKazPack. removed are the records dropped
// from the chunk, kept the remaining records in their original order, outs the
// visible ranges (sorted by position) to copy into the new slice, and ids the
// source slices that contribute at least one out.
type kazPackPlan struct {
	removed []*slice
	kept    []*slice
	outs    []kazPackOut
	ids     []uint64
}

// planKazPack plans packing small slices of one chunk into one new slice.
// cands are processed in order (the first is the dequeued block). All records
// of a selected id are removed. An id whose visible bytes would push the total
// past maxBytes is skipped (its records stay). A selected id that is fully
// hidden is removed without outs and does not count toward the threshold: only
// ids that contribute outs are counted, and fewer than 2 of them returns nil
// (packing one slice saves no PUT). Ids absent from ss are ignored. Adjacent
// outs are not merged, so no source data has to be read to fill a gap.
func planKazPack(ss []*slice, cands []uint64, maxBytes uint32) *kazPackPlan {
	present := make(map[uint64]bool, len(ss))
	for _, s := range ss {
		present[s.id] = true
	}
	// Visible pieces per id, in chunk order.
	perID := make(map[uint64][]kazPackOut)
	var pos uint32
	for _, p := range buildSlice(ss) {
		if p.Id != 0 && p.Len > 0 {
			perID[p.Id] = append(perID[p.Id], kazPackOut{pos: pos, srcID: p.Id, srcOff: p.Off, length: p.Len})
		}
		pos += p.Len
	}
	selected := make(map[uint64]bool)
	var total uint64
	var ids []uint64
	var outs []kazPackOut
	for _, id := range cands {
		if id == 0 || !present[id] || selected[id] { // id 0 is hole records, never a staged slice
			continue
		}
		var vis uint64
		for _, o := range perID[id] {
			vis += uint64(o.length)
		}
		if total+vis > uint64(maxBytes) {
			continue
		}
		selected[id] = true
		total += vis
		if vis > 0 {
			ids = append(ids, id)
			outs = append(outs, perID[id]...)
		}
	}
	if len(ids) < 2 {
		return nil
	}
	plan := &kazPackPlan{ids: ids}
	for _, s := range ss {
		if selected[s.id] {
			plan.removed = append(plan.removed, s)
		} else {
			plan.kept = append(plan.kept, s)
		}
	}
	// Selection order is by candidate; outs must be sorted by position.
	for i := 1; i < len(outs); i++ {
		for j := i; j > 0 && outs[j].pos < outs[j-1].pos; j-- {
			outs[j], outs[j-1] = outs[j-1], outs[j]
		}
	}
	plan.outs = outs
	return plan
}

// KazPackResult is the outcome of KazPackChunk. Packed are the source slice ids
// removed from the chunk (including fully hidden ones), Dead those of them whose
// last reference was dropped, Bytes the size of the new slice, and Reason why
// nothing was packed (empty on success): skip_busy, skip_trash, skip_engine,
// skip_not_in_chunk, skip_no_delete, fail_put or fail_replace.
type KazPackResult struct {
	Packed []uint64
	Dead   []uint64
	Bytes  uint32
	Reason string
}

// KazStagingPacker is implemented by metadata clients that can pack the staged
// small slices of one chunk into a single slice (spec 5.4).
type KazStagingPacker interface {
	KazPackChunk(inode Ino, indx uint32, cands []uint64, data map[uint64][]byte, write func(id uint64, data []byte) error) KazPackResult
}

// KazPackChunk packs the candidate slices of one chunk into one new slice.
// data holds each candidate's whole bytes from offset 0, and write PUTs the new
// slice's bytes to the object store. It skips (never waits) when the chunk is
// being compacted, when trash is on or when the engine cannot replace chunks
// safely (delta mode must be active). The chunk is replaced only if it is
// unchanged since the read; records written meanwhile stay on top.
func (m *baseMeta) KazPackChunk(inode Ino, indx uint32, cands []uint64, data map[uint64][]byte, write func(id uint64, data []byte) error) KazPackResult {
	r, isReplacer := m.en.(kazChunkReplacer)
	if !isReplacer || !m.kazDeltaActive() {
		return KazPackResult{Reason: "skip_engine"}
	}
	if m.toTrash(0) {
		return KazPackResult{Reason: "skip_trash"}
	}
	if m.conf.MaxDeletes == 0 {
		// deleteSlice is a no-op without delete workers, so Retire never runs and packed items would stay
		return KazPackResult{Reason: "skip_no_delete"}
	}
	bs := m.kazBlockSize()
	limit, fits := kazBlockCompactLimit(bs)
	if bs == 0 || !fits {
		return KazPackResult{Reason: "skip_engine"}
	}
	k := uint64(inode) + (uint64(indx) << 40)
	m.Lock()
	if m.sessCtx != nil && m.sessCtx.Canceled() {
		m.Unlock()
		return KazPackResult{Reason: "skip_busy"}
	}
	if m.compacting[k] {
		m.Unlock()
		return KazPackResult{Reason: "skip_busy"}
	}
	m.compacting[k] = true
	m.kazPacking++ // packs do not count against the background compaction limit
	m.Unlock()
	defer func() {
		m.Lock()
		delete(m.compacting, k)
		m.kazPacking--
		m.Unlock()
	}()

	ss, st := m.en.doRead(Background(), inode, indx)
	if st != 0 || ss == nil {
		return KazPackResult{Reason: "skip_not_in_chunk"}
	}
	if len(ss) > limit {
		ss = ss[:limit] // plan and origin cover the same prefix; later records stay on top
	}
	plan := planKazPack(ss, cands, uint32(bs))
	if plan == nil {
		return KazPackResult{Reason: "skip_not_in_chunk"}
	}
	var size uint32
	for _, o := range plan.outs {
		src := data[o.srcID]
		if uint64(o.srcOff)+uint64(o.length) > uint64(len(src)) {
			logger.Warnf("staging pack inode=%d chunk=%d: no data for slice %d [%d,+%d)", inode, indx, o.srcID, o.srcOff, o.length)
			return KazPackResult{Reason: "fail_put"}
		}
		size += o.length
	}
	buf := make([]byte, 0, size)
	for _, o := range plan.outs {
		buf = append(buf, data[o.srcID][o.srcOff:o.srcOff+o.length]...)
	}
	var id uint64
	if st := m.NewSlice(Background(), &id); st != 0 {
		return KazPackResult{Reason: "fail_put"}
	}
	if err := write(id, buf); err != nil {
		logger.Warnf("staging pack inode=%d chunk=%d slice=%d: put failed: %s", inode, indx, id, err)
		m.deleteSlice(id, size)
		return KazPackResult{Reason: "fail_put"}
	}
	replaced := make([]byte, 0, (len(plan.kept)+len(plan.outs))*sliceBytes)
	for _, s := range plan.kept {
		replaced = append(replaced, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	var off uint32
	for _, o := range plan.outs {
		replaced = append(replaced, marshalSlice(o.pos, id, size, off, o.length)...)
		off += o.length
	}
	origin := make([]byte, 0, len(ss)*sliceBytes)
	for _, s := range ss {
		origin = append(origin, marshalSlice(s.pos, s.id, s.size, s.off, s.len)...)
	}
	var dead []uint64
	created := []kazCreated{{Slice: Slice{Id: id, Size: size, Len: size}, Refs: len(plan.outs)}}
	if eno := r.doKazReplaceChunk(inode, indx, origin, replaced, plan.removed, created, nil, 0, func(did uint64, _ uint32) { dead = append(dead, did) }); eno != 0 {
		logger.Infof("staging pack inode=%d chunk=%d slice=%d: replace failed: %s", inode, indx, id, eno)
		if eno == syscall.EINVAL {
			m.deleteSlice(id, size) // wasted: the chunk changed; EINVAL already released the references
		} else {
			logger.Warnf("staging pack inode=%d chunk=%d: replace failed, the new slice %d may be orphaned and will be cleaned by gc: %s", inode, indx, id, eno)
		}
		return KazPackResult{Reason: "fail_replace"}
	}
	m.of.InvalidateChunk(inode, indx)
	var packed []uint64
	seen := make(map[uint64]bool)
	for _, s := range plan.removed {
		if s.id != 0 && !seen[s.id] {
			seen[s.id] = true
			packed = append(packed, s.id)
		}
	}
	logger.Debugf("staging pack inode=%d chunk=%d slice=%d slices=%d bytes=%d dead=%d", inode, indx, id, len(packed), size, len(dead))
	return KazPackResult{Packed: packed, Dead: dead, Bytes: size}
}
