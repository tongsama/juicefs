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
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

// kazDC returns a delta setting for the 4 KiB fixed cases: hidden ratio 0.5,
// 8 tiers and a 500-record limit unless the test changes them.
func kazDC(trigger int, gap uint32) kazDeltaConf {
	return kazDeltaConf{tiers: 8, maxRecords: 500, trigger: trigger, blockRatio: 0.5, gap: gap, hiddenRatio: 0.5}
}

// kazDeltaReasons returns the per-reason written block counts of a plan in
// the order ratio, nobase, fold, whole, fallback.
func kazDeltaReasons(p *kazBlockPlan) [5]int {
	return [5]int{p.ratioBlocks, p.nobaseBlocks, p.foldBlocks, p.wholeBlocks, p.fallbackBlocks}
}

// kazBase16 returns 16 block-shaped base records (ids 1..16) of kazEB bytes.
func kazBase16() []*slice {
	var ss []*slice
	for b := uint32(0); b < 16; b++ {
		ss = append(ss, kazRec(b*kazEB, uint64(b+1), kazEB))
	}
	return ss
}

// kazApplyDelta applies plan p to ss with a fake store, checks (I1) and (I4)
// and returns the records afterwards and the tier slice id (0 if none).
func kazApplyDelta(t *testing.T, f *kazFakeStore, ss []*slice, p *kazBlockPlan, next *uint64, n uint32) ([]*slice, uint64) {
	t.Helper()
	before := f.view(ss, n)
	first := *next + 1
	after := applyKazPlan(t, f, p, next)
	require.Equal(t, before, f.view(after, n), "view changed (I1)")
	var deltas, refs int
	for _, o := range p.outs {
		if o.delta {
			deltas++
		}
	}
	var tier uint64
	if deltas > 0 {
		tier = first // applyKazPlan gives the tier slice the first fresh id
	}
	for _, s := range after {
		if tier != 0 && s.id == tier {
			refs++
		}
	}
	require.Equal(t, deltas, refs, "tier references (I4)")
	return after, tier
}

// kazSmallTouched returns the bytes of the blocks touched by visible delta and
// new records: the partial rewrite of block mode, the bound of spec §5.4-6.
func kazSmallTouched(ss []*slice, bs uint32) uint64 {
	if len(ss) == 0 {
		return 0
	}
	g := kazNewBlockGeom(ss, bs)
	kinds := kazClassify(ss, g)
	hidden := kazFullyHidden(ss)
	blocks := make(map[uint32]bool)
	for i, s := range ss {
		if !hidden[i] && (kinds[i] == kazRecDelta || kinds[i] == kazRecNew) {
			for b := g.first(s); b <= g.last(s); b++ {
				blocks[b] = true
			}
		}
	}
	var n uint64
	for b := range blocks {
		lo, hi := g.span(b)
		n += uint64(hi - lo)
	}
	return n
}

// kazNewWrites returns n new 8-byte records spread over blocks, 100 bytes apart
// inside each block, with ids from id upwards.
func kazNewWrites(blocks []uint32, n int, shift uint32, id uint64) []*slice {
	var ss []*slice
	for i := 0; i < n; i++ {
		b := blocks[i%len(blocks)]
		k := uint32(i / len(blocks))
		ss = append(ss, kazRec(b*kazEB+k*100+shift, id+uint64(i), 8))
	}
	return ss
}

// TestKazClassify checks the record kinds of spec §5.2.
func TestKazClassify(t *testing.T) {
	ss := []*slice{
		kazRec(0, 1, 16*kazEB),                             // larger than a block: base
		{pos: kazEB, len: 100},                             // id 0: zero
		{pos: 2 * kazEB, id: 7, size: 50, off: 0, len: 20}, // shared id: delta
		{pos: 3 * kazEB, id: 7, size: 50, off: 20, len: 30},
		{pos: 4 * kazEB, id: 8, size: 30, off: 10, len: 20}, // off 10: delta
		kazRec(5*kazEB, 9, kazEB),                           // fills its block: base
		kazRec(6*kazEB+5, 10, 100),                          // small: new
	}
	g := kazNewBlockGeom(ss, kazEB)
	require.Equal(t, []kazRecKind{kazRecBase, kazRecZero, kazRecDelta, kazRecDelta, kazRecDelta, kazRecBase, kazRecNew}, kazClassify(ss, g))
}

// TestPlanKazDeltaNoop does nothing below the trigger and plans at it.
func TestPlanKazDeltaNoop(t *testing.T) {
	ss := append(kazBase16(), kazNewWrites([]uint32{2, 5, 11}, 98, 0, 100)...)
	p, base := planKazDelta(ss, kazEB, kazDC(kazDeltaTrigger, 16))
	require.Nil(t, p)
	require.Equal(t, 16, base)
	ss = append(kazBase16(), kazNewWrites([]uint32{2, 5, 11}, 99, 0, 100)...)
	p, _ = planKazDelta(ss, kazEB, kazDC(kazDeltaTrigger, 16))
	require.NotNil(t, p)
}

// TestPlanKazDeltaTier packs 99 small writes into one tier without rewriting blocks.
func TestPlanKazDeltaTier(t *testing.T) {
	base := kazBase16()
	ss := append(append([]*slice(nil), base...), kazNewWrites([]uint32{2, 5, 11}, 99, 0, 100)...)
	p, n := planKazDelta(ss, kazEB, kazDC(kazDeltaTrigger, 16))
	require.NotNil(t, p)
	require.Equal(t, "delta_tier", p.event)
	require.False(t, p.full)
	require.Equal(t, base, p.kept)
	require.Len(t, p.removed, 99)
	require.Len(t, p.outs, 99)
	for _, o := range p.outs {
		require.True(t, o.delta)
		require.Equal(t, uint32(8), o.length)
	}
	require.Equal(t, 16+99, n)
	f := newKazFakeStore()
	next := uint64(1000)
	after, tier := kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
	require.NotZero(t, tier)
	require.Len(t, after, 115)
	// The tier records are now delta records; nothing new is left to plan.
	p, n = planKazDelta(after, kazEB, kazDC(kazDeltaTrigger, 16))
	require.Nil(t, p)
	require.Equal(t, 115, n)
}

// TestPlanKazDeltaGap joins writes with a gap up to delta-gap, and not with gap 0.
func TestPlanKazDeltaGap(t *testing.T) {
	ss := append(kazBase16(), kazRec(3*kazEB+100, 100, 8), kazRec(3*kazEB+124, 101, 8)) // gap 16
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Len(t, p.outs, 1)
	require.Equal(t, kazBlockOut{pos: 3*kazEB + 100, length: 32, delta: true, pieces: p.outs[0].pieces}, p.outs[0])
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)

	p, _ = planKazDelta(ss, kazEB, kazDC(2, 0))
	require.NotNil(t, p)
	require.Len(t, p.outs, 2)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestPlanKazDeltaHalfBlock rewrites a block whose changes reach half of it.
func TestPlanKazDeltaHalfBlock(t *testing.T) {
	ss := append(kazBase16(),
		kazRec(3*kazEB+10, 100, 700), kazRec(3*kazEB+1000, 101, 700), kazRec(3*kazEB+2000, 102, 700), // 2100 bytes
		kazRec(7*kazEB+10, 103, 8))
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Len(t, p.outs, 2)
	require.Equal(t, uint32(3*kazEB), p.outs[0].pos)
	require.Equal(t, kazEB, p.outs[0].length)
	require.False(t, p.outs[0].delta)
	require.True(t, p.outs[1].delta)
	require.Equal(t, uint32(7*kazEB+10), p.outs[1].pos)
	require.Equal(t, []uint64{4, 100, 101, 102, 103}, kazIDs(p.removed)) // base block 3 is inside the rewrite
	require.Equal(t, [5]int{1, 0, 0, 0, 0}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)

	// Just under half stays a delta.
	ss = append(kazBase16(), kazRec(3*kazEB+10, 100, 1000), kazRec(3*kazEB+2000, 101, 1047))
	p, _ = planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Len(t, p.outs, 2)
	require.True(t, p.outs[0].delta && p.outs[1].delta)
}

// TestPlanKazDeltaNoBase rewrites a block with no base data below the new writes.
func TestPlanKazDeltaNoBase(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 4*kazEB), kazRec(kazEB+10, 100, 8), kazRec(6*kazEB+10, 101, 8)}
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Len(t, p.outs, 2)
	require.True(t, p.outs[0].delta)
	require.Equal(t, kazBlockOut{pos: 6 * kazEB, length: 18, pieces: p.outs[1].pieces}, p.outs[1])
	require.Equal(t, [5]int{0, 1, 0, 0, 0}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 8*kazEB)
}

// TestPlanKazDeltaMerge drops every earlier tier once a new one would pass the tier limit.
func TestPlanKazDeltaMerge(t *testing.T) {
	c := kazDC(kazDeltaTrigger, 16)
	c.tiers = 2
	f := newKazFakeStore()
	next := uint64(1 << 20)
	ss := kazBase16()
	all := []uint32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	var tiers []uint64
	for round := 0; round < 3; round++ {
		ss = append(ss, kazNewWrites(all, 99, uint32(round)*30, uint64(1000*(round+1)))...)
		p, _ := planKazDelta(ss, kazEB, c)
		require.NotNil(t, p)
		if round < 2 {
			require.Equal(t, "delta_tier", p.event)
		} else {
			require.Equal(t, "delta_merge", p.event)
			removedTiers := make(map[uint64]int)
			for _, s := range p.removed {
				removedTiers[s.id]++
			}
			require.Equal(t, 99, removedTiers[tiers[0]])
			require.Equal(t, 99, removedTiers[tiers[1]])
			require.Equal(t, kazBase16(), p.kept)
			var deltaBytes int
			for _, o := range p.outs {
				if o.delta {
					deltaBytes += int(o.length)
				}
			}
			require.NotZero(t, deltaBytes)
			require.Equal(t, uint64(deltaBytes), p.mergeBytes)
		}
		if round < 2 {
			require.Zero(t, p.mergeBytes)
		}
		var tier uint64
		ss, tier = kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
		tiers = append(tiers, tier)
	}
	ids := make(map[uint64]bool)
	for _, s := range ss[16:] {
		ids[s.id] = true
	}
	require.Equal(t, map[uint64]bool{tiers[2]: true}, ids)
	require.Len(t, ss, 16+3*99)
}

// TestPlanKazDeltaFold rewrites the blocks with the most delta records until the
// chunk is the fold margin below the record limit.
func TestPlanKazDeltaFold(t *testing.T) {
	c := kazDC(kazDeltaTrigger, 16)
	c.maxRecords = 60
	ss := []*slice{kazRec(0, 1, 16*kazEB)}
	id := uint64(100)
	for _, bc := range [][2]uint32{{1, 40}, {2, 35}, {3, 20}, {4, 4}} {
		for k := uint32(0); k < bc[1]; k++ {
			ss = append(ss, kazRec(bc[0]*kazEB+k*100, id, 8))
			id++
		}
	}
	p, n := planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, "delta_fold", p.event)
	require.False(t, p.full)
	require.LessOrEqual(t, n, c.maxRecords-kazDeltaFoldMargin)
	var blocks []uint32
	var deltas int
	for _, o := range p.outs {
		if o.delta {
			deltas++
			require.Equal(t, uint32(4), o.pos/kazEB)
		} else {
			blocks = append(blocks, o.pos/kazEB)
		}
	}
	require.Equal(t, []uint32{1, 2, 3}, blocks)
	require.Equal(t, 4, deltas)
	require.Equal(t, [5]int{0, 0, 3, 0, 0}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1 << 20)
	after, _ := kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
	require.Len(t, after, n)
}

// TestPlanKazDeltaFoldZero folds a block whose record count comes from many
// small id 0 records (guest discard) instead of rewriting the chunk whole
// (spec §5.2: such records are cleaned by the §5.4-5 fold).
func TestPlanKazDeltaFoldZero(t *testing.T) {
	c := kazDC(kazDeltaTrigger, 16)
	c.maxRecords = 60
	ss := []*slice{kazRec(0, 1, 16*kazEB), kazRec(kazEB+100, 100, 8), kazRec(kazEB+200, 101, 8)}
	for k := uint32(0); k < 64; k++ { // block 6 discarded in 64-byte pieces
		ss = append(ss, &slice{pos: 6*kazEB + k*64, len: 64})
	}
	p, n := planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, "delta_fold", p.event)
	require.False(t, p.full)
	require.Zero(t, p.foldBlocks) // the folded block became a mask: no block written
	require.LessOrEqual(t, n, c.maxRecords-kazDeltaFoldMargin)
	var masks, deltas int
	for _, o := range p.outs {
		switch {
		case o.mask:
			masks++
			require.Equal(t, uint32(6), o.pos/kazEB)
		case o.delta:
			deltas++
		}
	}
	require.Equal(t, 1, masks)
	require.Equal(t, 2, deltas)
	require.Len(t, p.removed, 66)
	f := newKazFakeStore()
	next := uint64(1 << 20)
	after, _ := kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
	require.Len(t, after, n)
}

// TestPlanKazDeltaPrune drops a fully hidden base record without uploading anything.
func TestPlanKazDeltaPrune(t *testing.T) {
	ss := append(kazBase16(), kazRec(3*kazEB, 100, kazEB))
	p, n := planKazDelta(ss, kazEB, kazDC(kazDeltaTrigger, 16))
	require.NotNil(t, p)
	require.Equal(t, "delta_prune", p.event)
	require.Empty(t, p.outs)
	require.Equal(t, []uint64{4}, kazIDs(p.removed))
	require.Equal(t, 16, n)
}

// TestPlanKazDeltaZeroKept keeps zero records out of the tier unless a rewritten
// block holds them.
func TestPlanKazDeltaZeroKept(t *testing.T) {
	zero := &slice{pos: 4*kazEB + 100, len: 100}
	ss := append(kazBase16(), zero, kazRec(6*kazEB+10, 100, 8), kazRec(9*kazEB+10, 101, 8))
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Contains(t, p.kept, zero)
	require.NotContains(t, p.removed, zero)
	require.Len(t, p.outs, 2)
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)

	// The same zero record inside a block rewritten by the half-block rule goes.
	ss = append(kazBase16(), zero, kazRec(4*kazEB+1000, 100, 2100), kazRec(9*kazEB+10, 101, 8))
	p, _ = planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Contains(t, p.removed, zero)
	require.NotContains(t, p.kept, zero)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestPlanKazDeltaSpansFoldedBlock covers a write across a block boundary when
// only the first block is rewritten (Review Focus 1).
func TestPlanKazDeltaSpansFoldedBlock(t *testing.T) {
	cross := kazRec(3*kazEB-8, 101, 16)
	ss := append(kazBase16(), kazRec(2*kazEB+100, 100, 2100), cross)
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	var n int
	for _, s := range p.removed {
		if s == cross {
			n++
		}
	}
	require.Equal(t, 1, n)
	require.Len(t, p.outs, 2)
	require.Equal(t, kazBlockOut{pos: 2 * kazEB, length: kazEB, pieces: p.outs[0].pieces}, p.outs[0])
	require.Equal(t, kazBlockOut{pos: 3 * kazEB, length: 8, delta: true, pieces: p.outs[1].pieces}, p.outs[1])
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestPlanKazDeltaProperty applies random plans to random write sequences
// (small, block-sized, crossing, zero, append, truncate) and checks the
// invariants, the record limit and the PUT bound of spec §5.4.
func TestPlanKazDeltaProperty(t *testing.T) {
	bs := kazEB
	const maxBlocks = 24
	limit := maxBlocks * bs
	rng := rand.New(rand.NewSource(20261020))
	events := make(map[string]int)
	for run := 0; run < 200; run++ {
		c := kazDeltaConf{tiers: 1 + rng.Intn(16), maxRecords: 50 + rng.Intn(151), trigger: kazDeltaTrigger,
			blockRatio: 1 - rng.Float64(), gap: uint32(rng.Intn(65)), hiddenRatio: 0.5}
		switch rng.Intn(4) {
		case 0, 1: // plan more often to reach tiers, merges and folds
			c.trigger = 5 + rng.Intn(95)
		case 2: // the --kaz-compaction-delta-trigger range
			c.trigger = 10 + rng.Intn(391)
		}
		if rng.Intn(8) == 0 {
			c.blockRatio = 1
		}
		// Half of the runs are mostly small writes (the VM shape), so that tiers
		// pile up to the record limit instead of base rewrites resetting them.
		smallHeavy := rng.Intn(2) == 0
		f := newKazFakeStore()
		next := uint64(1 << 40)
		id := uint64(1)
		length := 16 * bs
		var ss []*slice
		if rng.Intn(2) == 0 {
			ss = append(ss, kazRec(0, id, length))
			id++
		} else {
			for b := uint32(0); b < 16; b++ {
				ss = append(ss, kazRec(b*bs, id, bs))
				id++
			}
		}
		for w := 1; w <= 300; w++ {
			var pos, l uint32
			zero, skip := false, false
			k := rng.Intn(20)
			if smallHeavy && k >= 10 && rng.Intn(5) > 0 {
				k = 0
			}
			switch {
			case k < 10: // small overwrite
				pos = uint32(rng.Intn(int(length)))
				l = 4 + uint32(rng.Intn(61))
			case k < 12: // one whole block
				pos = uint32(rng.Intn(int(min(length/bs+1, maxBlocks)))) * bs
				l = bs
			case k < 14: // across a block boundary
				l = 8 + uint32(rng.Intn(int(2*bs)))
				pos = (1+uint32(rng.Intn(int(max(length/bs, 1)))))*bs - (1 + uint32(rng.Intn(int(min(l-1, bs)))))
			case k < 16: // zero range inside the file
				pos = uint32(rng.Intn(int(length)))
				l = min(4+uint32(rng.Intn(197)), length-pos)
				zero = true
			case k < 18: // append
				pos = length
				l = 4 + uint32(rng.Intn(61))
			default: // truncate: hide the tail under a zero record
				if length <= 2*bs {
					skip = true
					break
				}
				pos = length - 1 - uint32(rng.Intn(int(2*bs)))
				l = length - pos
				zero = true
				length = pos
			}
			if skip || pos >= limit {
				// nothing written this step
			} else if l = min(l, limit-pos); zero {
				ss = append(ss, &slice{pos: pos, len: l})
			} else {
				ss = append(ss, kazRec(pos, id, l))
				id++
				length = max(length, pos+l)
			}
			if w%50 != 0 {
				continue
			}
			bound := kazSmallTouched(ss, bs)
			p, base := planKazDelta(ss, bs, c)
			if p == nil {
				continue
			}
			events[p.event]++
			if p.full {
				events[p.event+"/full"]++
			}
			seen := make(map[*slice]bool)
			for _, s := range p.removed {
				require.False(t, seen[s], "run=%d w=%d removed twice", run, w)
				seen[s] = true
			}
			var after []*slice
			after, _ = kazApplyDelta(t, f, ss, p, &next, limit)
			require.Equal(t, len(after), base, "run=%d w=%d", run, w)
			if !p.full {
				require.LessOrEqual(t, len(after), c.maxRecords, "run=%d w=%d %s", run, w, p.event)
				require.LessOrEqual(t, kazPlanPutBytes(p), bound, "run=%d w=%d %s", run, w, p.event)
			}
			// Every written block has exactly one reason (whole > fallback > fold >
			// nobase > ratio): the reasons add up to the written blocks.
			r := kazDeltaReasons(p)
			require.Equal(t, kazDeltaDataBlocks(p), r[0]+r[1]+r[2]+r[3]+r[4], "run=%d w=%d %s %v", run, w, p.event, r)
			switch {
			case p.full:
				require.Equal(t, [5]int{0, 0, 0, kazDeltaDataBlocks(p), 0}, r, "run=%d w=%d %s", run, w, p.event)
			case p.event == kazDeltaEventFallback:
				require.Equal(t, [5]int{0, 0, 0, 0, kazDeltaDataBlocks(p)}, r, "run=%d w=%d", run, w)
				events["fallback/partial"]++
			default:
				require.Zero(t, r[3]+r[4])
			}
			if p.event != kazDeltaEventMerge {
				require.Zero(t, p.mergeBytes)
			}
			ss = after
		}
	}
	t.Logf("events: %v", events)
	for _, e := range []string{"delta_tier", "delta_merge", "delta_fold", "delta_prune"} {
		require.NotZero(t, events[e], "event %s never planned", e)
	}
}

// kazApplyNoData returns the records after plan p without materializing bytes,
// for the real-scale pattern comparison (tier records share one fresh id).
func kazApplyNoData(p *kazBlockPlan, next *uint64) []*slice {
	out := append([]*slice(nil), p.kept...)
	var tierSize, off uint32
	for _, o := range p.outs {
		if o.delta {
			tierSize += o.length
		}
	}
	var tier uint64
	if tierSize > 0 {
		*next++
		tier = *next
	}
	for _, o := range p.outs {
		switch {
		case o.mask:
			out = append(out, &slice{pos: o.pos, len: o.length})
		case o.delta:
			out = append(out, &slice{pos: o.pos, id: tier, size: tierSize, off: off, len: o.length})
			off += o.length
		default:
			*next++
			out = append(out, &slice{pos: o.pos, id: *next, size: o.length, len: o.length})
		}
	}
	return out
}

// TestPlanKazDeltaPatternsVsBlock compares the PUT bytes of delta and block
// planning at real scale (4 MiB blocks) over the I/O patterns of spec §6-2,
// planning every 99 writes.
func TestPlanKazDeltaPatternsVsBlock(t *testing.T) {
	const bs = uint32(4 << 20)
	base16 := func() [][2]uint32 {
		var w [][2]uint32
		for b := uint32(0); b < 16; b++ {
			w = append(w, [2]uint32{b * bs, bs})
		}
		return w
	}
	rng := rand.New(rand.NewSource(7))
	sparse := base16()
	for i := 0; i < 2000; i++ {
		l := uint32(4<<10 + rng.Intn(4<<10+1))
		sparse = append(sparse, [2]uint32{uint32(rng.Intn(int(16*bs - l))), l})
	}
	dense := base16()
	for pass := 0; pass < 8; pass++ {
		for off := uint32(0); off < bs; off += 64 << 10 {
			dense = append(dense, [2]uint32{5*bs + off, 64 << 10})
		}
	}
	appendW := [][2]uint32{{0, bs}, {bs, bs}, {2 * bs, bs}, {3 * bs, bs}}
	for i := uint32(0); i < 2000; i++ {
		appendW = append(appendW, [2]uint32{4*bs + i*(16<<10), 16 << 10})
	}
	large := base16()
	for i := 0; i < 200; i++ {
		large = append(large, [2]uint32{uint32(rng.Intn(16)) * bs, bs})
	}
	conf := (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: KazCompactionModeDelta}}).kazDeltaConf()
	// run replays writes (after the first initial ones) and plans every 99 writes.
	run := func(writes [][2]uint32, initial int, delta bool) uint64 {
		var ss []*slice
		next := uint64(1 << 40)
		var put uint64
		for i, w := range writes {
			ss = append(ss, kazRec(w[0], uint64(i+1), w[1]))
			if i < initial || (i-initial+1)%99 != 0 {
				continue
			}
			var p *kazBlockPlan
			if delta {
				p, _ = planKazDelta(ss, bs, conf)
			} else {
				p = planKazBlockCompaction(ss, bs, 0.5, 17)
			}
			if p != nil {
				put += kazPlanPutBytes(p)
				ss = kazApplyNoData(p, &next)
			}
		}
		return put
	}
	for _, c := range []struct {
		name    string
		writes  [][2]uint32
		initial int
		factor  uint64 // delta*factor <= block
	}{
		{"sparse", sparse, 16, 3},
		{"dense", dense, 16, 1},
		{"append", appendW, 4, 1},
		{"large", large, 16, 1},
	} {
		d, b := run(c.writes, c.initial, true), run(c.writes, c.initial, false)
		t.Logf("%s: delta %d MiB, block %d MiB", c.name, d>>20, b>>20)
		require.LessOrEqual(t, d*c.factor, b, c.name)
	}
}

// TestPlanKazDeltaHiddenFull rewrites the chunk whole, as event delta_hidden_full,
// when more than hidden-ratio of the kept base data is hidden (step 10); the
// event delta_block_fallback stays for the PUT-bound fallback.
func TestPlanKazDeltaHiddenFull(t *testing.T) {
	ss := []*slice{
		kazRec(0, 1, 16*kazEB), // three stacked large base records: 2/3 hidden
		kazRec(1, 2, 16*kazEB-1),
		kazRec(2, 3, 16*kazEB-2),
		kazRec(5*kazEB+10, 4, 8),
		kazRec(9*kazEB+10, 5, 8),
	}
	p, n := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Equal(t, "delta_hidden_full", p.event)
	require.True(t, p.full)
	require.Equal(t, [5]int{0, 0, 0, 16, 0}, kazDeltaReasons(p))
	require.Equal(t, len(p.kept)+len(p.outs), n)
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestKazDeltaGivenUpBase checks the base remembered by a noop after step 2: new
// records at or above the trigger count as base (Ruling 6), fewer do not. (The
// planner's give-up paths with news >= trigger could not be reached with random
// chunks, so the rule is tested directly.)
func TestKazDeltaGivenUpBase(t *testing.T) {
	require.Equal(t, 120, kazDeltaGivenUpBase(120, kazDeltaTrigger, kazDeltaTrigger))
	require.Equal(t, 120, kazDeltaGivenUpBase(120, 150, kazDeltaTrigger))
	require.Equal(t, 22, kazDeltaGivenUpBase(120, kazDeltaTrigger-1, kazDeltaTrigger))
	require.Equal(t, 120, kazDeltaGivenUpBase(120, 0, kazDeltaTrigger))
}

// TestPlanKazDeltaBlockRatio keeps a block with 60% of it changed in the delta
// at block ratio 0.75 and rewrites it at 0.5.
func TestPlanKazDeltaBlockRatio(t *testing.T) {
	ss := append(kazBase16(), // 3 x 820 = 2460 bytes of block 3 (60%)
		kazRec(3*kazEB+10, 100, 820), kazRec(3*kazEB+1000, 101, 820), kazRec(3*kazEB+2000, 102, 820),
		kazRec(7*kazEB+10, 103, 8))
	c := kazDC(2, 16)
	p, _ := planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, 1, kazDeltaDataBlocks(p))
	require.Equal(t, [5]int{1, 0, 0, 0, 0}, kazDeltaReasons(p))

	c.blockRatio = 0.75
	p, _ = planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, "delta_tier", p.event)
	require.Zero(t, kazDeltaDataBlocks(p))
	require.Len(t, p.outs, 4)
	require.Equal(t, [5]int{}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestPlanKazDeltaBlockRatioOne rewrites a block at ratio 1 only when the delta
// covers a whole block size: 4095 of 4096 bytes stay a delta, 4096 are rewritten.
func TestPlanKazDeltaBlockRatioOne(t *testing.T) {
	c := kazDC(2, 0)
	c.blockRatio = 1
	ss := append(kazBase16(), kazRec(3*kazEB, 100, 2048), kazRec(3*kazEB+2048, 101, 2047))
	p, _ := planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Zero(t, kazDeltaDataBlocks(p))
	require.Len(t, p.outs, 1) // the two writes touch: one delta range
	require.Equal(t, uint32(4095), p.outs[0].length)

	// One large base stays visible elsewhere, so block 3 keeps a base below it
	// (with kazBase16 its block-shaped base would be hidden: no base instead).
	ss = []*slice{kazRec(0, 1, 16*kazEB), kazRec(3*kazEB, 100, 2048), kazRec(3*kazEB+2048, 101, 2048)}
	p, _ = planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, 1, kazDeltaDataBlocks(p))
	require.Equal(t, [5]int{1, 0, 0, 0, 0}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)

	// The ratio is a share of the full block size, so at 1 the tail block of the
	// extent (1000 in-extent bytes, all changed) is never reached and stays a delta.
	ss = []*slice{kazRec(0, 1, 2*kazEB+1000), kazRec(2*kazEB, 100, 600), kazRec(2*kazEB+600, 101, 400), kazRec(10, 102, 8)}
	p, _ = planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, "delta_tier", p.event)
	require.Zero(t, kazDeltaDataBlocks(p))
	require.Equal(t, [5]int{}, kazDeltaReasons(p))
	kazApplyDelta(t, f, ss, p, &next, 3*kazEB)

	// At 0.5 the tail block is compared with size/2 = 2048 as before: 1000 stays a delta.
	c.blockRatio = 0.5
	p, _ = planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Zero(t, kazDeltaDataBlocks(p))
}

// TestPlanKazDeltaReasons counts one ratio block and one no-base block in one
// plan, and the blocks of a fold that ends in a whole rewrite as whole only.
func TestPlanKazDeltaReasons(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 4*kazEB),
		kazRec(kazEB+10, 100, 700), kazRec(kazEB+1000, 101, 700), kazRec(kazEB+2000, 102, 700), // 2100 bytes: ratio
		kazRec(2*kazEB+10, 103, 8), // delta
		kazRec(6*kazEB+10, 104, 8)} // beyond the base: no base
	p, _ := planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Equal(t, "delta_tier", p.event)
	require.Equal(t, 2, kazDeltaDataBlocks(p))
	require.Equal(t, [5]int{1, 1, 0, 0, 0}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1000)
	kazApplyDelta(t, f, ss, p, &next, 8*kazEB)

	// A block that is both above the ratio and without base counts as no base.
	ss = []*slice{kazRec(0, 1, 4*kazEB), kazRec(2*kazEB+10, 103, 8),
		kazRec(6*kazEB, 100, 3000), kazRec(6*kazEB+3010, 101, 8)}
	p, _ = planKazDelta(ss, kazEB, kazDC(2, 16))
	require.NotNil(t, p)
	require.Equal(t, [5]int{0, 1, 0, 0, 0}, kazDeltaReasons(p))

	// 60 block-shaped bases leave no fold candidate once blocks 3 and 7 are
	// folded: the fold ends in a whole rewrite, counted as whole, not fold.
	c := kazDC(2, 16)
	c.maxRecords = 60
	ss = nil
	for b := uint32(0); b < 60; b++ {
		ss = append(ss, kazRec(b*kazEB, uint64(b+1), kazEB))
	}
	ss = append(ss, kazRec(3*kazEB+10, 100, 8), kazRec(7*kazEB+10, 101, 8))
	p, _ = planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, "delta_fold", p.event)
	require.True(t, p.full)
	require.Equal(t, [5]int{0, 0, 0, 60, 0}, kazDeltaReasons(p))
}

// TestPlanKazDeltaFallbackReason counts the written blocks of a partial step-9
// fallback as fallback: folding block 6 (many id 0 records over visible base
// data) would upload a block no small record touches, more than block mode.
func TestPlanKazDeltaFallbackReason(t *testing.T) {
	c := kazDC(2, 16)
	c.maxRecords = 60
	ss := []*slice{kazRec(0, 1, 16*kazEB), kazRec(kazEB+100, 100, 8), kazRec(kazEB+200, 101, 8)}
	for k := uint32(0); k < 60; k++ { // block 6 punched in 32-byte holes, 64 bytes apart
		ss = append(ss, &slice{pos: 6*kazEB + k*64, len: 32})
	}
	p, _ := planKazDelta(ss, kazEB, c)
	require.NotNil(t, p)
	require.Equal(t, kazDeltaEventFallback, p.event)
	require.False(t, p.full)
	require.NotZero(t, kazDeltaDataBlocks(p))
	require.Equal(t, [5]int{0, 0, 0, 0, kazDeltaDataBlocks(p)}, kazDeltaReasons(p))
	f := newKazFakeStore()
	next := uint64(1 << 20)
	kazApplyDelta(t, f, ss, p, &next, 16*kazEB)
}

// TestKazDeltaRatioParts checks the fixed-point block ratio and the exact
// integer comparison with the full block size.
func TestKazDeltaRatioParts(t *testing.T) {
	require.Equal(t, uint64(500000), kazDeltaRatioParts(0.5))
	require.Equal(t, uint64(600000), kazDeltaRatioParts(0.6))
	require.Equal(t, uint64(1000000), kazDeltaRatioParts(1))
	require.Equal(t, uint64(1), kazDeltaRatioParts(1e-9))
	require.True(t, kazDeltaRatioReached(2048, 4096, kazDeltaRatioParts(0.5)))
	require.False(t, kazDeltaRatioReached(2047, 4096, kazDeltaRatioParts(0.5)))
	require.True(t, kazDeltaRatioReached(3072, 4096, kazDeltaRatioParts(0.75)))
	require.False(t, kazDeltaRatioReached(3071, 4096, kazDeltaRatioParts(0.75)))
	require.True(t, kazDeltaRatioReached(4096, 4096, kazDeltaRatioParts(1)))
	require.False(t, kazDeltaRatioReached(4095, 4096, kazDeltaRatioParts(1)))
	require.True(t, kazDeltaRatioReached(2516583, 4<<20, kazDeltaRatioParts(0.6))) // ceil(0.6 * 4 MiB)
	require.False(t, kazDeltaRatioReached(2516582, 4<<20, kazDeltaRatioParts(0.6)))
}
