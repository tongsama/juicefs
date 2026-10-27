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

import "sort"

// kazBlockOut is one record that block compaction places above the kept records.
type kazBlockOut struct {
	pos    uint32  // chunk offset where the record starts
	length uint32  // bytes the record covers
	mask   bool    // zero record (id 0) hiding kept data; no object is written
	pieces []Slice // visible pieces copied into a new slice when mask is false
	// delta marks a range of the plan's one new delta tier: every delta out of a
	// plan shares one slice holding their pieces concatenated in plan order.
	delta bool
}

// kazBlockPlan is the outcome of planning one block compaction of a chunk.
// The chunk after compaction holds kept (in their original order), then outs.
type kazBlockPlan struct {
	full    bool          // every block of the data extent is rewritten
	kept    []*slice      // records left unchanged
	removed []*slice      // records dropped from the chunk
	outs    []kazBlockOut // new records in ascending chunk order
	hidden  float64       // share of kept data hidden in the partial result
	event   string        // delta plan kind (delta_tier, ...); empty for block plans
	// Written (non-mask) blocks of a delta-mode plan by reason, one reason per
	// block (whole > fallback > fold > nobase > ratio), adding up to the plan's
	// written blocks, for kaz_compaction_block_amount_total: wholeBlocks are the
	// blocks of a whole rewrite (plan.full), fallbackBlocks those of a partial
	// block-mode fallback (step 9), foldBlocks are chosen to stay under the
	// record limit, nobaseBlocks have no base record (id > 0) below, ratioBlocks
	// have changes reaching the block ratio.
	wholeBlocks, fallbackBlocks, foldBlocks, nobaseBlocks, ratioBlocks int
	// mergeBytes is the delta slice size of a delta_merge plan (every earlier
	// tier packed again), so merge cost is separable from new-tier bytes.
	mergeBytes uint64
}

// kazBlockGeom answers block questions for one chunk: block size and data extent.
type kazBlockGeom struct {
	size, start, end uint32
}

// first returns the block holding the first byte of s.
func (g kazBlockGeom) first(s *slice) uint32 { return s.pos / g.size }

// last returns the block holding the last byte of s (s.len > 0).
func (g kazBlockGeom) last(s *slice) uint32 { return (s.pos + s.len - 1) / g.size }

// span returns the part of block b inside the data extent.
func (g kazBlockGeom) span(b uint32) (uint32, uint32) {
	return max(b*g.size, g.start), min((b+1)*g.size, g.end)
}

// fills reports whether s covers exactly the in-extent part of its block, the
// shape block compaction writes.
func (g kazBlockGeom) fills(s *slice) bool {
	lo, hi := g.span(g.first(s))
	return s.pos == lo && s.pos+s.len == hi
}

// small reports whether s is a record block compaction merges away.
func (g kazBlockGeom) small(s *slice) bool {
	return s.len < g.size && !g.fills(s)
}

// within reports whether every block s touches is in t.
func (g kazBlockGeom) within(s *slice, t map[uint32]bool) bool {
	for b := g.first(s); b <= g.last(s); b++ {
		if !t[b] {
			return false
		}
	}
	return true
}

// spanned returns how many blocks the data extent touches, the most records a
// whole rewrite can leave.
func (g kazBlockGeom) spanned() int {
	return int((g.end+g.size-1)/g.size - g.start/g.size)
}

// outside counts the bytes of [a, b) in blocks not in t.
func (g kazBlockGeom) outside(a, b uint32, t map[uint32]bool) uint64 {
	var n uint64
	for a < b {
		blk := a / g.size
		e := min((blk+1)*g.size, b)
		if !t[blk] {
			n += uint64(e - a)
		}
		a = e
	}
	return n
}

// hiddenShare returns the share of the kept records' data that a reader would no
// longer see once the blocks in t are rewritten above them.
func (g kazBlockGeom) hiddenShare(kept []*slice, t map[uint32]bool) float64 {
	var total, visible uint64
	for _, s := range kept {
		if s.id > 0 {
			total += uint64(s.len)
		}
	}
	if total == 0 {
		return 0
	}
	var pos uint32
	for _, p := range buildSlice(kept) {
		if p.Id > 0 {
			visible += g.outside(pos, pos+p.Len, t)
		}
		pos += p.Len
	}
	return 1 - float64(visible)/float64(total)
}

// kazFullyHidden marks records that later records cover completely (and empty
// ones); dropping them does not change what a reader sees.
func kazFullyHidden(ss []*slice) []bool {
	hidden := make([]bool, len(ss))
	var cov [][2]uint32 // sorted, merged ranges covered by later records
	for i := len(ss) - 1; i >= 0; i-- {
		lo, hi := ss[i].pos, ss[i].pos+ss[i].len
		k := sort.Search(len(cov), func(k int) bool { return cov[k][0] > lo }) - 1
		hidden[i] = lo == hi || (k >= 0 && cov[k][1] >= hi)
		if lo < hi {
			cov = kazAddRange(cov, lo, hi)
		}
	}
	return hidden
}

// kazAddRange merges [lo, hi) into the sorted, merged ranges cov.
func kazAddRange(cov [][2]uint32, lo, hi uint32) [][2]uint32 {
	out := make([][2]uint32, 0, len(cov)+1)
	i := 0
	for ; i < len(cov) && cov[i][1] < lo; i++ {
		out = append(out, cov[i])
	}
	for ; i < len(cov) && cov[i][0] <= hi; i++ {
		lo, hi = min(lo, cov[i][0]), max(hi, cov[i][1])
	}
	out = append(out, [2]uint32{lo, hi})
	return append(out, cov[i:]...)
}

// kazClipPieces returns the pieces of a chunk view (from buildSlice, starting at
// offset 0) inside [lo, hi); offsets into slices are adjusted, holes stay id 0.
func kazClipPieces(view []Slice, lo, hi uint32) []Slice {
	var out []Slice
	var pos uint32
	for _, p := range view {
		a, b := max(pos, lo), min(pos+p.Len, hi)
		if a < b {
			if p.Id == 0 {
				out = append(out, Slice{Size: b - a, Len: b - a})
			} else {
				out = append(out, Slice{Id: p.Id, Size: p.Size, Off: p.Off + (a - pos), Len: b - a})
			}
		}
		pos += p.Len
		if pos >= hi {
			break
		}
	}
	if pos < hi {
		a := max(pos, lo)
		out = append(out, Slice{Size: hi - a, Len: hi - a})
	}
	return out
}

// kazAllHoles reports whether every piece is a hole.
func kazAllHoles(pieces []Slice) bool {
	for _, p := range pieces {
		if p.Id != 0 {
			return false
		}
	}
	return true
}

// kazOverlapsAny reports whether any record overlaps [lo, hi).
func kazOverlapsAny(ss []*slice, lo, hi uint32) bool {
	for _, s := range ss {
		if s.pos < hi && s.pos+s.len > lo {
			return true
		}
	}
	return false
}

// planKazBlockCompaction decides which records of a chunk (ss, in chunk order) to
// keep, which to drop and which blocks to rewrite so that the chunk reads the
// same afterwards. It returns nil when nothing can be dropped, unless the chunk
// is over maxSlices and a whole rewrite (block by block) would lower its count.
// Records of at least one block that are not block-shaped are kept, so a large
// unaligned staged slice may still upload, unlike upstream which rewrites them.
// The rules are in
// docs/superpowers/specs/2026-10-14-kaz-block-compaction-design.md §5.
func planKazBlockCompaction(ss []*slice, blockSize uint32, hiddenRatio float64, maxSlices int) *kazBlockPlan {
	return planKazBlocks(ss, blockSize, hiddenRatio, maxSlices, false)
}

// planKazBlockCompactionForced plans a forced compaction (juicefs compact, gc
// --compact) in block mode: afterwards the chunk holds block-shaped records with
// no hidden data. A chunk already in that shape gets nil; one with only small
// writes above block-shaped records is rewritten partially; any hidden data,
// or more records than the extent has blocks, makes it a whole rewrite.
func planKazBlockCompactionForced(ss []*slice, blockSize uint32, maxSlices int) *kazBlockPlan {
	if p := planKazBlocks(ss, blockSize, 0, maxSlices, false); p != nil || len(ss) == 0 || blockSize == 0 {
		return p
	}
	g := kazNewBlockGeom(ss, blockSize)
	if g.hiddenShare(ss, nil) > 0 || len(ss) > g.spanned() {
		return planKazBlocks(ss, blockSize, 0, maxSlices, true)
	}
	return nil
}

// kazNewBlockGeom returns the block geometry of a chunk's records (len(ss) > 0).
func kazNewBlockGeom(ss []*slice, blockSize uint32) kazBlockGeom {
	g := kazBlockGeom{size: blockSize, start: ss[0].pos, end: ss[0].pos + ss[0].len}
	for _, s := range ss[1:] {
		g.start, g.end = min(g.start, s.pos), max(g.end, s.pos+s.len)
	}
	return g
}

// planKazBlocks implements both plans; whole makes it rewrite every block of
// the extent regardless of the other rules.
func planKazBlocks(ss []*slice, blockSize uint32, hiddenRatio float64, maxSlices int, whole bool) *kazBlockPlan {
	if len(ss) == 0 || blockSize == 0 {
		return nil
	}
	g := kazNewBlockGeom(ss, blockSize)
	hidden := kazFullyHidden(ss)
	touched := make(map[uint32]bool)
	drop := make([]bool, len(ss))
	for i, s := range ss {
		switch {
		case hidden[i]:
			drop[i] = true
		case g.small(s):
			drop[i] = true
			for b := g.first(s); b <= g.last(s); b++ {
				touched[b] = true
			}
		}
	}
	p := &kazBlockPlan{}
	for i, s := range ss {
		if drop[i] || g.within(s, touched) {
			p.removed = append(p.removed, s)
		} else {
			p.kept = append(p.kept, s)
		}
	}
	// Nothing removable: a chunk over the record limit is still rewritten (fully),
	// but only when that lowers its record count. A whole rewrite leaves at most one
	// record per block of the extent, so a chunk of block-shaped records under a
	// limit below the block count would otherwise be rewritten on every request.
	overLimit := len(p.removed) == 0 && len(ss) > maxSlices && len(ss) > g.spanned()
	if len(p.removed) == 0 && !overLimit && !whole {
		return nil
	}
	p.hidden = g.hiddenShare(p.kept, touched)
	if whole || overLimit || p.hidden > hiddenRatio || len(p.kept)+len(touched) > maxSlices {
		p.full, p.kept, p.removed = true, nil, append([]*slice(nil), ss...)
		touched = make(map[uint32]bool)
		for b := g.start / g.size; b*g.size < g.end; b++ {
			touched[b] = true
		}
	}
	blocks := make([]uint32, 0, len(touched))
	for b := range touched {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	view := buildSlice(ss)
	for _, b := range blocks {
		lo, hi := g.span(b)
		if hi <= lo {
			continue
		}
		pieces := kazClipPieces(view, lo, hi)
		if kazAllHoles(pieces) {
			if kazOverlapsAny(p.kept, lo, hi) {
				p.outs = append(p.outs, kazBlockOut{pos: lo, length: hi - lo, mask: true})
			}
			continue
		}
		p.outs = append(p.outs, kazBlockOut{pos: lo, length: hi - lo, pieces: pieces})
	}
	return p
}
