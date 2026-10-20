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

// kazRecKind is the role delta compaction gives a record of a chunk. Metadata
// carries no delta mark, so the kind is derived from the records every time.
type kazRecKind uint8

const (
	kazRecBase  kazRecKind = iota // block-shaped or larger record: the data below changes
	kazRecZero                    // id 0 record (truncate, hole punch, block mask); kept like a base
	kazRecDelta                   // record of an earlier delta tier (shared id, or off/len not the whole slice)
	kazRecNew                     // small record written since the last compaction
)

// Delta plan events (kazBlockPlan.event), also used for metrics and logs.
const (
	kazDeltaEventTier     = "delta_tier"           // new records packed into one new tier
	kazDeltaEventMerge    = "delta_merge"          // every earlier tier packed again with the new records
	kazDeltaEventFold     = "delta_fold"           // blocks rewritten to stay under the record limit
	kazDeltaEventPrune    = "delta_prune"          // only hidden records dropped, nothing uploaded
	kazDeltaEventFallback = "delta_block_fallback" // block plan used instead (PUT bound or hidden base)
)

// kazClassify returns the kind of every record of a chunk (spec §5.2 of the kaz
// delta compaction design). A misclassification only changes cost, never what
// a reader sees, because a plan copies removed ranges from the current view.
func kazClassify(ss []*slice, g kazBlockGeom) []kazRecKind {
	refs := make(map[uint64]int)
	for _, s := range ss {
		if s.id > 0 {
			refs[s.id]++
		}
	}
	kinds := make([]kazRecKind, len(ss))
	for i, s := range ss {
		switch {
		case s.id == 0:
			kinds[i] = kazRecZero
		case refs[s.id] > 1 || s.off != 0 || s.len != s.size:
			kinds[i] = kazRecDelta
		case g.small(s):
			kinds[i] = kazRecNew
		default:
			kinds[i] = kazRecBase
		}
	}
	return kinds
}

// kazGapJoin sorts ranges and merges those that overlap or are at most gap
// bytes apart; the joined gaps are copied from the view like changed bytes.
func kazGapJoin(rs [][2]uint32, gap uint32) [][2]uint32 {
	if len(rs) == 0 {
		return nil
	}
	sorted := append([][2]uint32(nil), rs...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i][0] < sorted[j][0] })
	out := [][2]uint32{sorted[0]}
	for _, r := range sorted[1:] {
		last := &out[len(out)-1]
		if r[0] <= last[1]+gap {
			last[1] = max(last[1], r[1])
		} else {
			out = append(out, r)
		}
	}
	return out
}

// kazRangeBytesIn returns how many bytes of the sorted, disjoint ranges lie in [lo, hi).
func kazRangeBytesIn(rs [][2]uint32, lo, hi uint32) uint32 {
	var n uint32
	for _, r := range rs {
		if r[0] >= hi {
			break
		}
		if a, b := max(r[0], lo), min(r[1], hi); a < b {
			n += b - a
		}
	}
	return n
}

// kazDeltaPlanner holds what planKazDelta derives once from a chunk's records
// so that the block selection can be redone cheaply while folding.
type kazDeltaPlanner struct {
	ss     []*slice
	g      kazBlockGeom
	kinds  []kazRecKind
	hidden []bool
	drop0  []bool          // records removed before the block selection (step 3)
	joined [][2]uint32     // gapjoin of the removed records' ranges: the delta candidates
	view   []Slice         // the chunk as a reader sees it now
	fset   map[uint32]bool // blocks rewritten whole
}

// build returns which records the plan removes and the delta ranges, for the
// current set of rewritten blocks (steps 6 and 7).
func (d *kazDeltaPlanner) build() ([]bool, [][2]uint32) {
	drop := append([]bool(nil), d.drop0...)
	for i, s := range d.ss {
		if !drop[i] && s.len > 0 && d.g.within(s, d.fset) {
			drop[i] = true
		}
	}
	var t [][2]uint32
	for _, r := range d.joined {
		for a := r[0]; a < r[1]; {
			b := a / d.g.size
			e := min((b+1)*d.g.size, r[1])
			if !d.fset[b] {
				t = append(t, [2]uint32{a, e})
			}
			a = e
		}
	}
	return drop, t
}

// outs builds the plan for the current rewritten blocks: kept records in their
// order, removed records, and outs (rewritten blocks and delta ranges) by position.
func (d *kazDeltaPlanner) outs(drop []bool, t [][2]uint32) *kazBlockPlan {
	p := &kazBlockPlan{}
	for i, s := range d.ss {
		if drop[i] {
			p.removed = append(p.removed, s)
		} else {
			p.kept = append(p.kept, s)
		}
	}
	blocks := make([]uint32, 0, len(d.fset))
	for b := range d.fset {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool { return blocks[i] < blocks[j] })
	for _, b := range blocks {
		lo, hi := d.g.span(b)
		if hi <= lo {
			continue
		}
		pieces := kazClipPieces(d.view, lo, hi)
		if kazAllHoles(pieces) {
			if kazOverlapsAny(p.kept, lo, hi) {
				p.outs = append(p.outs, kazBlockOut{pos: lo, length: hi - lo, mask: true})
			}
			continue
		}
		p.outs = append(p.outs, kazBlockOut{pos: lo, length: hi - lo, pieces: pieces})
	}
	for _, r := range t {
		p.outs = append(p.outs, kazBlockOut{pos: r[0], length: r[1] - r[0], delta: true, pieces: kazClipPieces(d.view, r[0], r[1])})
	}
	sort.Slice(p.outs, func(i, j int) bool { return p.outs[i].pos < p.outs[j].pos })
	return p
}

// foldCandidate returns the block outside the rewritten set holding the most
// kept delta records (wholly inside it) and delta ranges, or false if none has any.
func (d *kazDeltaPlanner) foldCandidate(drop []bool, t [][2]uint32) (uint32, bool) {
	count := make(map[uint32]int)
	for i, s := range d.ss {
		if !drop[i] && d.kinds[i] == kazRecDelta && s.len > 0 && d.g.first(s) == d.g.last(s) && !d.fset[d.g.first(s)] {
			count[d.g.first(s)]++
		}
	}
	for _, r := range t {
		count[r[0]/d.g.size]++
	}
	best, bestN := uint32(0), 0
	for b, n := range count {
		if n > bestN || (n == bestN && b < best) {
			best, bestN = b, n
		}
	}
	return best, bestN > 0
}

// baseHiddenShare returns the share of the kept base records' data a reader no
// longer sees once outs are placed above the kept records (spec §5.4-7).
func (d *kazDeltaPlanner) baseHiddenShare(p *kazBlockPlan, drop []bool) float64 {
	ids := make(map[uint64]bool)
	var total uint64
	for i, s := range d.ss {
		if d.kinds[i] == kazRecBase && s.id > 0 && !drop[i] {
			ids[s.id] = true
			total += uint64(s.len)
		}
	}
	if total == 0 {
		return 0
	}
	after := append([]*slice(nil), p.kept...)
	for _, o := range p.outs {
		after = append(after, &slice{pos: o.pos, len: o.length}) // id 0: hides what is below
	}
	var visible uint64
	for _, piece := range buildSlice(after) {
		if ids[piece.Id] {
			visible += uint64(piece.Len)
		}
	}
	return 1 - float64(visible)/float64(total)
}

// planKazDelta plans one delta compaction of a chunk (records ss in chunk order):
// new small records (and, past the tier limit, every earlier tier) go into one
// new delta slice shared by several records, and blocks changed by half or more,
// or with no base data below, are rewritten whole as in block mode. The reader's
// view never changes: outs copy the current view and are placed above the kept
// records. plan is nil when nothing is to be done; base is the record count the
// caller remembers as not new (len(ss) minus the new records for nil, otherwise
// the records left once the plan commits). The rules are in
// docs/superpowers/specs/2026-10-20-kaz-delta-compaction-design.md §5.4.
func planKazDelta(ss []*slice, blockSize uint32, c kazDeltaConf) (*kazBlockPlan, int) {
	if len(ss) == 0 || blockSize == 0 {
		return nil, len(ss)
	}
	d := &kazDeltaPlanner{ss: ss, g: kazNewBlockGeom(ss, blockSize)}
	d.kinds = kazClassify(ss, d.g)
	d.hidden = kazFullyHidden(ss)
	d.view = buildSlice(ss)

	// Step 1: counts.
	var news, prunable int
	tierIDs := make(map[uint64]bool)
	for i, s := range ss {
		switch {
		case d.hidden[i]:
			if s.id > 0 {
				prunable++
			}
		case d.kinds[i] == kazRecNew:
			news++
		case d.kinds[i] == kazRecDelta:
			tierIDs[s.id] = true
		}
	}
	noopBase := len(ss) - news

	// Step 2: noop.
	if news < c.trigger && prunable == 0 && len(ss) <= c.maxRecords && len(tierIDs) <= c.tiers {
		return nil, noopBase
	}

	// Step 3: records removed before the block selection.
	newTier := 0
	if news > 0 {
		newTier = 1
	}
	merge := len(tierIDs)+newTier > c.tiers
	d.drop0 = make([]bool, len(ss))
	var u, keptDelta [][2]uint32
	for i, s := range ss {
		switch {
		case d.hidden[i]:
			d.drop0[i] = true
		case d.kinds[i] == kazRecNew || (merge && d.kinds[i] == kazRecDelta):
			d.drop0[i] = true
		case d.kinds[i] == kazRecDelta:
			keptDelta = append(keptDelta, [2]uint32{s.pos, s.pos + s.len})
			continue
		default:
			continue
		}
		// Step 4: U, the ranges of visible removed data records.
		if !d.hidden[i] && s.id > 0 {
			u = append(u, [2]uint32{s.pos, s.pos + s.len})
		}
	}
	d.joined = kazGapJoin(u, c.gap)

	// Step 5: blocks rewritten whole: changed by half or more, or no base below.
	changed := kazGapJoin(append(append([][2]uint32(nil), u...), keptDelta...), c.gap)
	d.fset = make(map[uint32]bool)
	for _, r := range d.joined {
		for b := r[0] / d.g.size; b*d.g.size < r[1]; b++ {
			if d.fset[b] {
				continue
			}
			lo, hi := d.g.span(b)
			if kazRangeBytesIn(changed, lo, hi) >= d.g.size/2 || !d.hasBase(lo, hi) {
				d.fset[b] = true
			}
		}
	}

	// Steps 6-8: build, folding blocks while over the record limit.
	drop, t := d.build()
	p := d.outs(drop, t)
	folded := false
	if len(p.kept)+len(p.outs) > c.maxRecords {
		folded = true
		target := c.maxRecords - kazDeltaFoldMargin
		for len(p.kept)+len(p.outs) > target {
			b, ok := d.foldCandidate(drop, t)
			if !ok {
				return kazDeltaWhole(ss, blockSize, c, kazDeltaEventFold)
			}
			d.fset[b] = true
			drop, t = d.build()
			p = d.outs(drop, t)
		}
	}

	// Step 9: never upload more than block mode's partial rewrite would.
	if kazPlanPutBytes(p) > d.smallTouchedBytes() {
		bp := planKazBlocks(ss, blockSize, c.hiddenRatio, c.maxRecords, false)
		if bp == nil {
			return nil, noopBase
		}
		bp.event = kazDeltaEventFallback
		return bp, len(bp.kept) + len(bp.outs)
	}

	// Step 10: too much of the base hidden: rewrite the chunk whole.
	p.hidden = d.baseHiddenShare(p, drop)
	if p.hidden > c.hiddenRatio {
		return kazDeltaWhole(ss, blockSize, c, kazDeltaEventFallback)
	}
	if len(p.removed) == 0 && len(p.outs) == 0 {
		return nil, noopBase
	}

	// Steps 11-12: event and base count.
	switch {
	case folded:
		p.event = kazDeltaEventFold
	case merge:
		p.event = kazDeltaEventMerge
	case len(p.outs) == 0:
		p.event = kazDeltaEventPrune
	default:
		p.event = kazDeltaEventTier
	}
	return p, len(p.kept) + len(p.outs)
}

// kazDeltaWhole returns block mode's whole rewrite of the chunk, tagged with event.
func kazDeltaWhole(ss []*slice, blockSize uint32, c kazDeltaConf, event string) (*kazBlockPlan, int) {
	p := planKazBlocks(ss, blockSize, 0, c.maxRecords, true)
	p.event = event
	return p, len(p.kept) + len(p.outs)
}

// hasBase reports whether a visible base record with data overlaps [lo, hi).
func (d *kazDeltaPlanner) hasBase(lo, hi uint32) bool {
	for i, s := range d.ss {
		if d.kinds[i] == kazRecBase && s.id > 0 && !d.hidden[i] && s.pos < hi && s.pos+s.len > lo {
			return true
		}
	}
	return false
}

// smallTouchedBytes returns the bytes of the blocks touched by visible delta and
// new records: what block mode's partial rewrite of the same records uploads
// (spec §5.4-6), the bound a delta plan must not exceed.
func (d *kazDeltaPlanner) smallTouchedBytes() uint64 {
	blocks := make(map[uint32]bool)
	for i, s := range d.ss {
		if !d.hidden[i] && (d.kinds[i] == kazRecDelta || d.kinds[i] == kazRecNew) {
			for b := d.g.first(s); b <= d.g.last(s); b++ {
				blocks[b] = true
			}
		}
	}
	var n uint64
	for b := range blocks {
		lo, hi := d.g.span(b)
		n += uint64(hi - lo)
	}
	return n
}

// kazPlanPutBytes returns the bytes a plan uploads: delta ranges plus rewritten
// blocks (masks write no object).
func kazPlanPutBytes(p *kazBlockPlan) uint64 {
	var n uint64
	for _, o := range p.outs {
		if !o.mask {
			n += uint64(o.length)
		}
	}
	return n
}
