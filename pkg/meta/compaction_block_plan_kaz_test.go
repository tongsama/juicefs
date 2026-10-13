package meta

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

const kazTB = uint32(4096) // block size used by the fixed cases

// kazRec builds a record that references a whole fresh slice.
func kazRec(pos uint32, id uint64, l uint32) *slice {
	return &slice{pos: pos, id: id, size: l, len: l}
}

// TestPlanKazBlockCompactionKeepsView applies random plans repeatedly and checks
// that what a reader sees never changes and that records stay bounded.
func TestPlanKazBlockCompactionKeepsView(t *testing.T) {
	for _, bs := range []uint32{1 << 10, 4 << 10, 8 << 10} {
		rng := rand.New(rand.NewSource(int64(bs)))
		chunkLen := bs * 16
		for round := 0; round < 300; round++ {
			f := newKazFakeStore()
			next := uint64(1 << 40)
			id := uint64(1)
			var ss []*slice
			if rng.Intn(2) == 0 { // a large base, like an upstream compaction output
				ss = append(ss, kazRec(0, id, chunkLen))
				id++
			}
			for gen := 0; gen < 4; gen++ {
				for i := rng.Intn(30); i > 0; i-- {
					pos := uint32(rng.Intn(int(chunkLen)))
					l := min(uint32(1+rng.Intn(int(bs)*2)), chunkLen-pos)
					switch rng.Intn(8) {
					case 0: // zero range
						ss = append(ss, &slice{pos: pos, len: l})
					case 1: // reference into a larger slice
						extra := uint32(rng.Intn(1000))
						ss = append(ss, &slice{pos: pos, id: id, size: l + extra, off: extra / 2, len: l})
						id++
					default:
						ss = append(ss, kazRec(pos, id, l))
						id++
					}
				}
				before := f.view(ss, chunkLen)
				p := planKazBlockCompaction(ss, bs, 0.5, 17)
				if p == nil {
					continue
				}
				ss = applyKazPlan(t, f, p, &next)
				require.Equal(t, before, f.view(ss, chunkLen), "bs=%d round=%d gen=%d", bs, round, gen)
				require.LessOrEqual(t, len(ss), 17, "bs=%d round=%d gen=%d", bs, round, gen)
			}
		}
	}
}

// TestPlanKazBlockCompactionNoSmall does nothing on a chunk of block-shaped records.
func TestPlanKazBlockCompactionNoSmall(t *testing.T) {
	ss := []*slice{kazRec(0, 1, kazTB), kazRec(kazTB, 2, kazTB), kazRec(2*kazTB, 3, 100)}
	require.Nil(t, planKazBlockCompaction(ss, kazTB, 0.5, 17))
}

// TestPlanKazBlockCompactionPartial rewrites only the touched blocks above a large base,
// and a later round replaces the earlier block record it fully covers.
func TestPlanKazBlockCompactionPartial(t *testing.T) {
	base := kazRec(0, 1, 16*kazTB)
	ss := []*slice{base, kazRec(2*kazTB+100, 2, 300), kazRec(3*kazTB+10, 3, 20), kazRec(9*kazTB, 4, 50), kazRec(2*kazTB+200, 5, 10)}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.False(t, p.full)
	require.Equal(t, []*slice{base}, p.kept)
	require.Equal(t, []uint64{2, 3, 4, 5}, kazIDs(p.removed))
	require.Len(t, p.outs, 3)
	for i, b := range []uint32{2, 3, 9} {
		require.Equal(t, b*kazTB, p.outs[i].pos)
		require.Equal(t, kazTB, p.outs[i].length)
		require.False(t, p.outs[i].mask)
	}
	f := newKazFakeStore()
	next := uint64(100)
	before := f.view(ss, 16*kazTB)
	ss = applyKazPlan(t, f, p, &next)
	require.Equal(t, before, f.view(ss, 16*kazTB))

	ss = append(ss, kazRec(2*kazTB+5, 6, 5), kazRec(5*kazTB, 7, 5))
	p = planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.False(t, p.full)
	require.Equal(t, []uint64{1, 102, 103}, kazIDs(p.kept))  // base, block 3, block 9
	require.Equal(t, []uint64{101, 6, 7}, kazIDs(p.removed)) // old block 2, then the new writes
	require.Len(t, p.outs, 2)
}

// TestPlanKazBlockCompactionFullByHidden rewrites every block once most of the base is hidden.
func TestPlanKazBlockCompactionFullByHidden(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 16*kazTB)}
	for i := uint32(0); i < 9; i++ {
		ss = append(ss, kazRec(i*kazTB+1, uint64(10+i), 10))
	}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.True(t, p.full)
	require.Empty(t, p.kept)
	require.Len(t, p.removed, 10)
	require.Len(t, p.outs, 16)
}

// TestPlanKazBlockCompactionFullByCount rewrites every block when too many records would remain.
func TestPlanKazBlockCompactionFullByCount(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 16*kazTB), kazRec(kazTB, 2, 10), kazRec(5*kazTB, 3, 10), kazRec(7*kazTB, 4, 10)}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 3)
	require.NotNil(t, p)
	require.True(t, p.full)
	require.Len(t, p.outs, 16)
}

// TestPlanKazBlockCompactionMask hides kept data under a block of zero records without an object.
func TestPlanKazBlockCompactionMask(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 16*kazTB), {pos: 4 * kazTB, len: kazTB / 2}, {pos: 4*kazTB + kazTB/2, len: kazTB / 2}}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.Equal(t, []kazBlockOut{{pos: 4 * kazTB, length: kazTB, mask: true}}, p.outs)
	f := newKazFakeStore()
	next := uint64(100)
	before := f.view(ss, 16*kazTB)
	after := applyKazPlan(t, f, p, &next)
	require.Equal(t, before, f.view(after, 16*kazTB))
	require.Zero(t, f.callCount())
}

// TestPlanKazBlockCompactionSmallFile keeps outputs within the data extent of a tiny file.
func TestPlanKazBlockCompactionSmallFile(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 1000), kazRec(500, 2, 2500)}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.Len(t, p.outs, 1)
	require.Equal(t, uint32(0), p.outs[0].pos)
	require.Equal(t, uint32(3000), p.outs[0].length)
	f := newKazFakeStore()
	next := uint64(100)
	after := applyKazPlan(t, f, p, &next)
	require.Nil(t, planKazBlockCompaction(after, kazTB, 0.5, 17))
}

// TestPlanKazBlockCompactionFullyHidden drops a record covered by later ones without rewriting.
func TestPlanKazBlockCompactionFullyHidden(t *testing.T) {
	ss := []*slice{kazRec(0, 1, 2*kazTB), kazRec(0, 2, 2*kazTB)}
	p := planKazBlockCompaction(ss, kazTB, 0.5, 17)
	require.NotNil(t, p)
	require.Equal(t, []uint64{1}, kazIDs(p.removed))
	require.Equal(t, []uint64{2}, kazIDs(p.kept))
	require.Empty(t, p.outs)
}
