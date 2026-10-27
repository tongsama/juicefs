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

// TestKazReplaceOnDeadReports: removing records whose slices have no other
// reference reports each of those slices exactly once through onDead.
func TestKazReplaceOnDeadReports(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			r := e.en.(kazChunkReplacer)
			inode := e.file(t)
			e.write(t, inode, 0, 8192)
			b1 := e.write(t, inode, 100, 50)
			b2 := e.write(t, inode, 200, 50)
			ss := e.records(t, inode)
			dead := map[uint64]int{}
			sizes := map[uint64]uint32{}
			rest := kazMarshal(ss[:1])
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(ss), rest, ss[1:], nil, nil, 0, func(id uint64, size uint32) {
				dead[id]++
				sizes[id] = size
			}))
			require.Equal(t, map[uint64]int{b1: 1, b2: 1}, dead)
			require.Equal(t, map[uint64]uint32{b1: 50, b2: 50}, sizes)
		})
	}
}

// TestKazReplaceOnDeadSkipsShared: a slice still pointed to by another record
// (as after clone) is not reported when one of its records is removed.
func TestKazReplaceOnDeadSkipsShared(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			// c is pointed to by three records; drop one, then the other two at once.
			e, r, inode, c, cur := kazSharedSetup(t, backend, 3)
			var got []uint64
			onDead := func(id uint64, size uint32) { got = append(got, id) }
			rest := append(append([]*slice{}, cur[:1]...), cur[2:]...)
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(cur), kazMarshal(rest), cur[1:2], nil, nil, 0, onDead))
			require.Empty(t, got)

			rest2 := rest[:1]
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(rest), kazMarshal(rest2), rest[1:], nil, nil, 0, onDead))
			require.Equal(t, []uint64{c}, got)
			_ = e
		})
	}
}

// TestKazReplaceOnDeadNotWithTrash: with trash (delayed != nil) nothing is reported.
func TestKazReplaceOnDeadNotWithTrash(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			_, r, inode, _, cur := kazSharedSetup(t, backend, 1)
			called := false
			rest := cur[:1]
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(cur), kazMarshal(rest), cur[1:], nil, []byte{1}, 1, func(uint64, uint32) { called = true }))
			require.False(t, called)
		})
	}
}

// kazPackBytes materializes the visible bytes of a record list, where slice id
// i holds the byte pattern byte(i*7+j) at slice offset j; holes are 0xff.
func kazPackBytes(ss []*slice, n uint32) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = 0xff
	}
	var pos uint32
	for _, p := range buildSlice(ss) {
		if p.Id != 0 {
			for j := uint32(0); j < p.Len && pos+j < n; j++ {
				out[pos+j] = byte(p.Id*7 + uint64(p.Off+j))
			}
		}
		pos += p.Len
	}
	return out
}

// kazPackApply turns a plan into the records after replacement, with the new
// slice id newID whose data is the concatenation of the outs.
func kazPackApply(p *kazPackPlan, newID uint64) []*slice {
	res := append([]*slice{}, p.kept...)
	var off uint32
	for _, o := range p.outs {
		res = append(res, &slice{pos: o.pos, id: newID, size: 1 << 20, off: off, len: o.length})
		off += o.length
	}
	return res
}

// kazPackBytesAfter is like kazPackBytes for the replaced records, where the
// new slice's byte at offset k is the source byte of the k-th out byte.
func kazPackBytesAfter(p *kazPackPlan, n uint32) []byte {
	var data []byte
	for _, o := range p.outs {
		for j := uint32(0); j < o.length; j++ {
			data = append(data, byte(o.srcID*7+uint64(o.srcOff+j)))
		}
	}
	const newID = 1 << 40
	out := make([]byte, n)
	for i := range out {
		out[i] = 0xff
	}
	var pos uint32
	for _, q := range buildSlice(kazPackApply(p, newID)) {
		if q.Id != 0 {
			for j := uint32(0); j < q.Len && pos+j < n; j++ {
				if q.Id == newID {
					out[pos+j] = data[q.Off+j]
				} else {
					out[pos+j] = byte(q.Id*7 + uint64(q.Off+j))
				}
			}
		}
		pos += q.Len
	}
	return out
}

// TestKazPackPlanView: I1 (the visible bytes do not change) and I2 (every
// visible range of a removed record is covered by outs) on random chunks.
func TestKazPackPlanView(t *testing.T) {
	rnd := rand.New(rand.NewSource(42))
	const n = 20000
	planned := 0
	for iter := 0; iter < 100; iter++ {
		var ss []*slice
		for i, k := 0, 3+rnd.Intn(8); i < k; i++ {
			l := uint32(1 + rnd.Intn(3000))
			pos := uint32(rnd.Intn(n - 3000))
			off := uint32(rnd.Intn(100))
			ss = append(ss, &slice{id: uint64(i/2 + 1), size: off + l + uint32(rnd.Intn(50)), off: off, len: l, pos: pos})
		}
		var cands []uint64
		for _, id := range rnd.Perm(6) {
			cands = append(cands, uint64(id+1))
		}
		p := planKazPack(ss, cands, uint32(2000+rnd.Intn(6000)))
		if p == nil {
			continue
		}
		planned++
		require.Equal(t, kazPackBytes(ss, n), kazPackBytesAfter(p, n), "iter %d", iter)
		require.Equal(t, len(ss), len(p.removed)+len(p.kept))
		// I2: visible ranges of removed ids are inside outs.
		rm := map[uint64]bool{}
		for _, s := range p.removed {
			rm[s.id] = true
		}
		covered := map[uint32]bool{}
		for _, o := range p.outs {
			require.True(t, rm[o.srcID])
			for j := uint32(0); j < o.length; j++ {
				covered[o.pos+j] = true
			}
		}
		var pos uint32
		for _, q := range buildSlice(ss) {
			if rm[q.Id] {
				for j := uint32(0); j < q.Len; j++ {
					require.True(t, covered[pos+j], "iter %d pos %d", iter, pos+j)
				}
			}
			pos += q.Len
		}
		for i := 1; i < len(p.outs); i++ {
			require.Less(t, p.outs[i-1].pos, p.outs[i].pos)
		}
	}
	require.Greater(t, planned, 20)
}

// TestKazPackPlanCap: ids that do not fit in maxBytes are left out.
func TestKazPackPlanCap(t *testing.T) {
	ss := []*slice{
		{id: 1, size: 4096, len: 4096, pos: 0},
		{id: 2, size: 4096, len: 4096, pos: 4096},
		{id: 3, size: 4096, len: 4096, pos: 8192},
	}
	p := planKazPack(ss, []uint64{1, 2, 3}, 8192)
	require.NotNil(t, p)
	require.Equal(t, []uint64{1, 2}, p.ids)
	require.Len(t, p.kept, 1)
	require.Equal(t, uint64(3), p.kept[0].id)
}

// TestKazPackPlanSingle: one contributing id is not worth a pack.
func TestKazPackPlanSingle(t *testing.T) {
	ss := []*slice{{id: 1, size: 100, len: 100, pos: 0}, {id: 2, size: 100, len: 100, pos: 200}}
	require.Nil(t, planKazPack(ss, []uint64{1}, 1<<20))
	require.Nil(t, planKazPack(ss, []uint64{1, 9}, 1<<20))
}

// TestKazPackPlanHidden: a fully hidden candidate is removed without outs and
// does not count toward the two-id threshold.
func TestKazPackPlanHidden(t *testing.T) {
	ss := []*slice{
		{id: 1, size: 100, len: 100, pos: 0},
		{id: 2, size: 100, len: 100, pos: 400},
		{id: 3, size: 100, len: 100, pos: 500},
		{id: 4, size: 300, len: 300, pos: 0}, // hides id 1 (not a candidate)
	}
	p := planKazPack(ss, []uint64{1, 2, 3}, 1<<20)
	require.NotNil(t, p)
	require.Equal(t, []uint64{2, 3}, p.ids)
	require.Len(t, p.removed, 3)
	require.Len(t, p.outs, 2)
	// With only one visible candidate, the hidden one does not make a pack.
	require.Nil(t, planKazPack(ss, []uint64{1, 2}, 1<<20))
}

// TestKazPackPlanLaterWrite: a later non-candidate record hides part of a
// candidate; the hidden part is not in outs.
func TestKazPackPlanLaterWrite(t *testing.T) {
	ss := []*slice{
		{id: 1, size: 100, len: 100, pos: 0},
		{id: 2, size: 100, len: 100, pos: 200},
		{id: 9, size: 20, len: 20, pos: 40},
	}
	p := planKazPack(ss, []uint64{1, 2}, 1<<20)
	require.NotNil(t, p)
	require.Equal(t, []kazPackOut{
		{pos: 0, srcID: 1, srcOff: 0, length: 40},
		{pos: 60, srcID: 1, srcOff: 60, length: 40},
		{pos: 200, srcID: 2, srcOff: 0, length: 100},
	}, p.outs)
	require.Len(t, p.kept, 1)
}
