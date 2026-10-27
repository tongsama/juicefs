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
	"sort"
	"sync"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// This file connects the kaz staging pack (--kaz-staging-pack): the writer
// remembers which chunk each small staged slice belongs to, and the packer
// called by the chunk store's uploader packs the waiting slices of that chunk.

// kazPackIndexMax bounds the slice -> chunk index; the oldest entries are
// dropped first (a dropped slice is simply uploaded by itself).
const kazPackIndexMax = 1 << 18

// kazPackLoc is where a committed small slice lives.
type kazPackLoc struct {
	inode  Ino
	indx   uint32
	length int
}

// kazChunkKey names one chunk of one file.
type kazChunkKey struct {
	inode Ino
	indx  uint32
}

// kazPackIndex maps committed single-block staged slices to their chunk and
// keeps, per chunk, the ids with their lengths so peers can be found.
type kazPackIndex struct {
	sync.Mutex
	byID    map[uint64]kazPackLoc
	byChunk map[kazChunkKey]map[uint64]int // chunk -> id -> length
	order   []uint64                       // insertion order; may hold removed ids
}

// newKazPackIndex returns an empty index.
func newKazPackIndex() *kazPackIndex {
	x := &kazPackIndex{}
	x.reset()
	return x
}

// reset drops every entry (as after a restart).
func (x *kazPackIndex) reset() {
	x.Lock()
	defer x.Unlock()
	x.byID = make(map[uint64]kazPackLoc)
	x.byChunk = make(map[kazChunkKey]map[uint64]int)
	x.order = nil
}

// add remembers slice id at loc and drops the oldest entries above the bound.
func (x *kazPackIndex) add(id uint64, loc kazPackLoc) {
	x.Lock()
	defer x.Unlock()
	if _, ok := x.byID[id]; ok {
		return
	}
	x.byID[id] = loc
	k := kazChunkKey{loc.inode, loc.indx}
	ids := x.byChunk[k]
	if ids == nil {
		ids = make(map[uint64]int)
		x.byChunk[k] = ids
	}
	ids[id] = loc.length
	x.order = append(x.order, id)
	for len(x.byID) > kazPackIndexMax && len(x.order) > 0 {
		old := x.order[0]
		x.order = x.order[1:]
		x.removeLocked(old)
	}
	if len(x.order) >= 2*kazPackIndexMax {
		// Removed ids stay in order until here; keep only the live ones.
		live := make([]uint64, 0, len(x.byID))
		for _, o := range x.order {
			if _, ok := x.byID[o]; ok {
				live = append(live, o)
			}
		}
		x.order = live
	}
}

// remove forgets the given ids.
func (x *kazPackIndex) remove(ids ...uint64) {
	if len(ids) == 0 {
		return
	}
	x.Lock()
	defer x.Unlock()
	for _, id := range ids {
		x.removeLocked(id)
	}
}

// removeLocked forgets id; the caller holds the lock.
func (x *kazPackIndex) removeLocked(id uint64) {
	loc, ok := x.byID[id]
	if !ok {
		return
	}
	delete(x.byID, id)
	k := kazChunkKey{loc.inode, loc.indx}
	if ids := x.byChunk[k]; ids != nil {
		delete(ids, id)
		if len(ids) == 0 {
			delete(x.byChunk, k)
		}
	}
}

// candidates returns the chunk of id and the slices to offer for packing: id
// first, then the other known slices of the chunk in ascending id order while
// the total length stays within limit. ok is false when id is unknown.
func (x *kazPackIndex) candidates(id uint64, limit int) (loc kazPackLoc, refs []chunk.KazStagedRef, ok bool) {
	x.Lock()
	defer x.Unlock()
	loc, ok = x.byID[id]
	if !ok {
		return loc, nil, false
	}
	refs = append(refs, chunk.KazStagedRef{ID: id, Length: loc.length})
	total := loc.length
	peers := x.byChunk[kazChunkKey{loc.inode, loc.indx}]
	others := make([]uint64, 0, len(peers))
	for pid := range peers {
		if pid != id {
			others = append(others, pid)
		}
	}
	sort.Slice(others, func(i, j int) bool { return others[i] < others[j] })
	for _, pid := range others {
		l := peers[pid]
		if total+l > limit {
			continue // too big for this pack; it waits for a later one or is uploaded alone
		}
		total += l
		refs = append(refs, chunk.KazStagedRef{ID: pid, Length: l})
	}
	return loc, refs, true
}

// kazStagingPackStore is the chunk-store side of the staging pack.
type kazStagingPackStore interface {
	KazSetStagingPacker(p func(id uint64, length int) bool)
	KazClaimStaged(refs []chunk.KazStagedRef) (claimed []chunk.KazStagedData, absent []uint64)
	KazReleaseStaged(ids []uint64, packed map[uint64]bool)
	KazStagingPackEvent(event string, n int)
	KazPutStaged(id uint64, data []byte, tier uint8) error
}

// kazRememberStaged records a committed (or possibly committed) slice in the
// pack index when packing is on, writeback stages it, and it is one block.
func (w *dataWriter) kazRememberStaged(inode Ino, indx uint32, s *sliceWriter) {
	idx := w.conf.kazPackIdx
	if idx == nil || !w.conf.Chunk.Writeback || s.length == 0 || int(s.length) >= w.conf.Chunk.BlockSize {
		return
	}
	idx.add(s.id, kazPackLoc{inode: inode, indx: indx, length: int(s.length)})
}

// kazInitStagingPack registers the staging packer when --kaz-staging-pack is on
// and both the chunk store and the metadata client support it.
func (v *VFS) kazInitStagingPack() {
	if !v.Conf.KazStagingPack {
		return
	}
	st, ok := v.Store.(kazStagingPackStore)
	if !ok {
		logger.Warnf("kaz staging pack: the chunk store does not support it; disabled")
		return
	}
	if _, ok := v.Meta.(meta.KazStagingPacker); !ok {
		logger.Warnf("kaz staging pack: the metadata client does not support it; disabled")
		return
	}
	v.Conf.kazPackIdx = newKazPackIndex()
	st.KazSetStagingPacker(v.kazPackStaged)
}

// kazPackStaged is called by the uploader for the dequeued small slice id. It
// claims the waiting slices of the same chunk, packs them into one slice and
// releases every claim: only slices whose last reference was dropped become
// packed; the others go back to the upload queue. It reports whether id itself
// was packed (its own object is then never put).
func (v *VFS) kazPackStaged(id uint64, length int) bool {
	idx := v.Conf.kazPackIdx
	st, sok := v.Store.(kazStagingPackStore)
	pk, mok := v.Meta.(meta.KazStagingPacker)
	if idx == nil || !sok || !mok {
		return false
	}
	loc, refs, ok := idx.candidates(id, v.Conf.Chunk.BlockSize)
	if ok && loc.length != length {
		idx.remove(id) // not the slice the index knows; leave it to the plain upload
		ok = false
	}
	if !ok {
		st.KazStagingPackEvent("skip_unknown", 1)
		return false
	}
	claimed, absent := st.KazClaimStaged(refs)
	idx.remove(absent...)

	ids := make([]uint64, 0, len(claimed))
	data := make(map[uint64][]byte, len(claimed))
	var tier uint8
	var haveTrigger bool
	for _, c := range claimed {
		ids = append(ids, c.ID)
		data[c.ID] = c.Data
		if c.ID == id {
			tier, haveTrigger = c.Tier, true
		}
	}
	var dead map[uint64]bool
	defer func() {
		// Every claim ends here, on every path; the trigger is uploaded now unless
		// packed, so it leaves the index either way.
		st.KazReleaseStaged(ids, dead)
		idx.remove(id)
	}()
	if !haveTrigger || len(ids) < 2 {
		st.KazStagingPackEvent("skip_no_peer", 1)
		return false
	}
	res := pk.KazPackChunk(loc.inode, loc.indx, ids, data, func(nid uint64, b []byte) error {
		return st.KazPutStaged(nid, b, tier)
	})
	idx.remove(res.Packed...)
	if res.Reason != "" {
		st.KazStagingPackEvent(res.Reason, 1)
		return false
	}
	st.KazStagingPackEvent("packed", 1)
	st.KazStagingPackEvent("packed_slices", len(res.Packed))
	st.KazStagingPackEvent("packed_bytes", int(res.Bytes))
	dead = make(map[uint64]bool, len(res.Dead))
	for _, d := range res.Dead {
		dead[d] = true
	}
	return dead[id]
}
