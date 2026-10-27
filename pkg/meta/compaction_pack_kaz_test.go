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

// TestKazPackPlanZeroCandidate: a candidate id 0 never selects hole records.
func TestKazPackPlanZeroCandidate(t *testing.T) {
	ss := []*slice{
		{id: 1, size: 100, len: 100, pos: 0},
		{id: 0, len: 50, pos: 100}, // hole record
		{id: 2, size: 100, len: 100, pos: 200},
	}
	p := planKazPack(ss, []uint64{0, 1, 2}, 1<<20)
	require.NotNil(t, p)
	require.Equal(t, []uint64{1, 2}, p.ids)
	require.Len(t, p.removed, 2)
	require.Len(t, p.kept, 1)
	require.Nil(t, planKazPack(ss, []uint64{0, 1}, 1<<20))
}

// kazPackData returns the bytes of a test-written slice (what the fake store
// reads for an id it has no data for).
func kazPackData(id uint64, size uint32) []byte {
	b := make([]byte, size)
	for i := range b {
		b[i] = kazFakeByte(id, uint32(i))
	}
	return b
}

// kazPackEnv is a delta-mode env holding five small writes in chunk 0 of one
// file; the fifth hides parts of the first two. data maps each id to its bytes.
type kazPackEnv struct {
	*kazBlockEnv
	inode Ino
	ids   []uint64
	data  map[uint64][]byte
	puts  int
	newID uint64
}

// newKazPackEnv builds the env (trash off) and writes the five slices.
func newKazPackEnv(t *testing.T, backend string) *kazPackEnv {
	t.Helper()
	e := newKazDeltaEnv(t, backend)
	e.kazTestUpdateFormat(func(f *Format) { f.TrashDays = 0 })
	pe := &kazPackEnv{kazBlockEnv: e, inode: e.file(t), data: map[uint64][]byte{}}
	for _, pos := range []uint32{0, 1000, 2000, 3000, 500} {
		id := e.write(t, pe.inode, pos, 1000)
		pe.ids = append(pe.ids, id)
		pe.data[id] = kazPackData(id, 1000)
	}
	return pe
}

// put is the chunk-store PUT stand-in: it stores the bytes in the fake store.
func (pe *kazPackEnv) put(hook func()) func(id uint64, data []byte) error {
	return func(id uint64, data []byte) error {
		pe.puts++
		pe.newID = id
		if hook != nil {
			hook()
		}
		pe.store.mu.Lock()
		pe.store.data[id] = append([]byte(nil), data...)
		pe.store.mu.Unlock()
		return nil
	}
}

// pack runs KazPackChunk over all five ids.
func (pe *kazPackEnv) pack(hook func()) KazPackResult {
	return pe.KazPackChunk(pe.inode, 0, pe.ids, pe.data, pe.put(hook))
}

// TestKazPackChunkView packs five slices: the view stays, the new slice has one
// record per out, and exactly the Dead ids are cleaned up.
func TestKazPackChunkView(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			pe := newKazPackEnv(t, backend)
			before := pe.store.view(pe.records(t, pe.inode), 4096)
			res := pe.pack(nil)
			if backend == "memkv" {
				require.Equal(t, "skip_engine", res.Reason)
				require.Zero(t, pe.puts)
				return
			}
			require.Empty(t, res.Reason)
			require.Equal(t, 1, pe.puts)
			require.ElementsMatch(t, pe.ids, res.Packed)
			require.Equal(t, uint32(4000), res.Bytes)
			after := pe.records(t, pe.inode)
			require.Equal(t, before, pe.store.view(after, 4096))
			n := 0
			for _, s := range after {
				if s.id == pe.newID {
					n++
					require.Equal(t, res.Bytes, s.size)
				} else {
					t.Fatalf("unexpected record %d", s.id)
				}
			}
			require.Equal(t, 5, n) // 0-500 A, 500-1500 E, 1500-2000 B, C, D
			require.NoError(t, pe.en.doCleanupSlices(Background(), nil))
			deleted := pe.deletedIDs()
			require.Zero(t, deleted[pe.newID])
			want := map[uint64]int{}
			for _, id := range res.Dead {
				want[id]++
			}
			require.Equal(t, want, deleted)
			require.Len(t, res.Dead, 5)
		})
	}
}

// TestKazPackChunkConcurrentWrite keeps a write added during the PUT on top.
func TestKazPackChunkConcurrentWrite(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			pe := newKazPackEnv(t, backend)
			var extra uint64
			res := pe.pack(func() { extra = pe.write(t, pe.inode, 100, 10) })
			require.Empty(t, res.Reason)
			after := pe.records(t, pe.inode)
			require.Equal(t, extra, after[len(after)-1].id)
			want := pe.store.view(append(kazPackOrig(pe), &slice{pos: 100, id: extra, size: 10, len: 10}), 4096)
			require.Equal(t, want, pe.store.view(after, 4096))
		})
	}
}

// kazPackOrig returns the records the five writes made (before any pack).
func kazPackOrig(pe *kazPackEnv) []*slice {
	var ss []*slice
	for i, pos := range []uint32{0, 1000, 2000, 3000, 500} {
		ss = append(ss, &slice{pos: pos, id: pe.ids[i], size: 1000, len: 1000})
	}
	return ss
}

// TestKazPackChunkChanged fails the replace when a candidate's record is
// removed during the PUT, and deletes the new slice.
func TestKazPackChunkChanged(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			pe := newKazPackEnv(t, backend)
			res := pe.pack(func() {
				ss := pe.records(t, pe.inode)
				r := pe.en.(kazChunkReplacer)
				require.Zero(t, r.doKazReplaceChunk(pe.inode, 0, kazMarshal(ss), kazMarshal(ss[:4]), ss[4:], nil, nil, 0, nil))
			})
			require.Equal(t, "fail_replace", res.Reason)
			require.Empty(t, res.Packed)
			require.Equal(t, 1, pe.deletedIDs()[pe.newID])
		})
	}
}

// TestKazPackChunkPutFailure deletes the new slice and reports fail_put.
func TestKazPackChunkPutFailure(t *testing.T) {
	pe := newKazPackEnv(t, "sqlite")
	before := kazIDs(pe.records(t, pe.inode))
	res := pe.KazPackChunk(pe.inode, 0, pe.ids, pe.data, func(id uint64, data []byte) error {
		pe.newID = id
		return errors.New("injected")
	})
	require.Equal(t, "fail_put", res.Reason)
	require.Equal(t, before, kazIDs(pe.records(t, pe.inode)))
	require.Equal(t, 1, pe.deletedIDs()[pe.newID])
}

// TestKazPackChunkTrash skips when trash is on, without calling write.
func TestKazPackChunkTrash(t *testing.T) {
	pe := newKazPackEnv(t, "sqlite")
	pe.kazTestUpdateFormat(func(f *Format) { f.TrashDays = 1 })
	res := pe.pack(nil)
	require.Equal(t, "skip_trash", res.Reason)
	require.Zero(t, pe.puts)
}

// TestKazPackChunkBusy skips (without waiting) when the chunk is compacting.
func TestKazPackChunkBusy(t *testing.T) {
	pe := newKazPackEnv(t, "sqlite")
	k := uint64(pe.inode)
	pe.Lock()
	pe.compacting[k] = true
	pe.Unlock()
	res := pe.pack(nil)
	require.Equal(t, "skip_busy", res.Reason)
	require.Zero(t, pe.puts)
	pe.Lock()
	require.True(t, pe.compacting[k]) // not released by the skipped call
	delete(pe.compacting, k)
	pe.Unlock()
	require.Empty(t, pe.pack(nil).Reason)
}

// TestKazPackChunkNotInChunk reports skip_not_in_chunk when no id is present.
func TestKazPackChunkNotInChunk(t *testing.T) {
	pe := newKazPackEnv(t, "sqlite")
	res := pe.KazPackChunk(pe.inode, 0, []uint64{pe.ids[0], 999999}, pe.data, pe.put(nil))
	require.Equal(t, "skip_not_in_chunk", res.Reason)
	require.Zero(t, pe.puts)
}

// TestKazPackChunkUpstreamRead reads the packed chunk the upstream way (Read
// gives Slices) and gets the bytes that were written.
func TestKazPackChunkUpstreamRead(t *testing.T) {
	for _, backend := range []string{"sqlite", "redis"} {
		t.Run(backend, func(t *testing.T) {
			pe := newKazPackEnv(t, backend)
			before := pe.store.view(pe.records(t, pe.inode), 4096)
			require.Empty(t, pe.pack(nil).Reason)
			var sl []Slice
			require.Zero(t, pe.Read(Background(), pe.inode, 0, &sl))
			var got []byte
			pe.store.mu.Lock()
			for _, s := range sl {
				for i := uint32(0); i < s.Len; i++ {
					got = append(got, pe.store.byteAtLocked(s.Id, s.Off+i))
				}
			}
			pe.store.mu.Unlock()
			require.Equal(t, before[:len(got)], got)
			require.Equal(t, 4000, len(got)) // the file ends at 4000
		})
	}
}
