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
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestKazReplaceChunkParity checks the replacement, reference counts and tail
// records on every engine through observable deletions.
func TestKazReplaceChunkParity(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			r, ok := e.en.(kazChunkReplacer)
			require.True(t, ok)
			inode := e.file(t)
			a := e.write(t, inode, 0, 8192)
			b1 := e.write(t, inode, 100, 50)
			b2 := e.write(t, inode, 200, 50)
			tail := e.write(t, inode, 300, 50) // committed after the origin was read
			ss := e.records(t, inode)
			var c uint64
			require.Zero(t, e.NewSlice(Background(), &c))
			replaced := append(kazMarshal(ss[:1]), marshalSlice(0, c, 4096, 0, 4096)...)
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(ss[:3]), replaced, ss[1:3], []kazCreated{{Slice{Id: c, Size: 4096, Len: 4096}, 1}}, nil, 0, nil))
			got := e.records(t, inode)
			require.Equal(t, []uint64{a, c, tail}, kazIDs(got))

			// Dropped records lost their only reference; kept, tail and created did not.
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, map[uint64]int{b1: 1, b2: 1}, e.deletedIDs())

			// The created slice holds exactly one reference: dropping its record frees it.
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(got), append(kazMarshal(got[:1]), kazMarshal(got[2:])...), got[1:2], nil, nil, 0, nil))
			require.Equal(t, []uint64{a, tail}, kazIDs(e.records(t, inode)))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, map[uint64]int{b1: 1, b2: 1, c: 1}, e.deletedIDs())
		})
	}
}

// TestKazReplaceChunkChanged rejects a stale origin, keeps the chunk and leaves
// the new slice to be deleted.
func TestKazReplaceChunkChanged(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			r := e.en.(kazChunkReplacer)
			inode := e.file(t)
			e.write(t, inode, 0, 100)
			e.write(t, inode, 50, 100)
			ss := e.records(t, inode)
			stale := kazMarshal(ss)
			stale[4] ^= 0xff // a different slice id in the first record
			var c uint64
			require.Zero(t, e.NewSlice(Background(), &c))
			st := r.doKazReplaceChunk(inode, 0, stale, marshalSlice(0, c, 150, 0, 150), ss, []kazCreated{{Slice{Id: c, Size: 150, Len: 150}, 1}}, nil, 0, nil)
			require.Equal(t, syscall.EINVAL, st)
			require.Equal(t, kazIDs(ss), kazIDs(e.records(t, inode)))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, map[uint64]int{c: 1}, e.deletedIDs())
		})
	}
}

// TestKazReplaceChunkDelayed records dropped slices for trash instead of releasing them.
func TestKazReplaceChunkDelayed(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			r := e.en.(kazChunkReplacer)
			inode := e.file(t)
			b1 := e.write(t, inode, 0, 100)
			b2 := e.write(t, inode, 50, 100)
			ss := e.records(t, inode)
			var c uint64
			require.Zero(t, e.NewSlice(Background(), &c))
			var delayed []byte
			for _, s := range ss {
				delayed = append(delayed, e.encodeDelayedSlice(s.id, s.size)...)
			}
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(ss), marshalSlice(0, c, 150, 0, 150), ss, []kazCreated{{Slice{Id: c, Size: 150, Len: 150}, 1}}, delayed, c, nil))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Empty(t, e.deletedIDs())
			_, err := e.en.doCleanupDelayedSlices(Background(), time.Now().Add(time.Hour).Unix())
			require.NoError(t, err)
			require.Equal(t, map[uint64]int{b1: 1, b2: 1}, e.deletedIDs())
		})
	}
}

// kazSharedSetup writes a base and three small records, then replaces the small
// ones by three records of one new slice c (150 bytes) holding refs references.
// It returns the env, the replacer, the inode, c and the records after the replacement.
func kazSharedSetup(t *testing.T, backend string, refs int) (*kazBlockEnv, kazChunkReplacer, Ino, uint64, []*slice) {
	e := newKazBlockEnv(t, backend, 4)
	r := e.en.(kazChunkReplacer)
	inode := e.file(t)
	a := e.write(t, inode, 0, 8192)
	_ = a
	e.write(t, inode, 100, 50)
	e.write(t, inode, 3000, 50)
	e.write(t, inode, 6000, 50)
	ss := e.records(t, inode)
	var c uint64
	require.Zero(t, e.NewSlice(Background(), &c))
	replaced := append(kazMarshal(ss[:1]), marshalSlice(100, c, 150, 0, 50)...)
	replaced = append(replaced, marshalSlice(3000, c, 150, 50, 50)...)
	replaced = append(replaced, marshalSlice(6000, c, 150, 100, 50)...)
	st := r.doKazReplaceChunk(inode, 0, kazMarshal(ss), replaced, ss[1:], []kazCreated{{Slice{Id: c, Size: 150, Len: 150}, refs}}, nil, 0, nil)
	require.Zero(t, st)
	return e, r, inode, c, e.records(t, inode)
}

// dropRecord removes record i of the chunk through the replacer, as a later compaction would.
func kazDropRecord(t *testing.T, r kazChunkReplacer, inode Ino, cur []*slice, i int, delayed []byte, delayedKey uint64) []*slice {
	rest := append(append([]*slice{}, cur[:i]...), cur[i+1:]...)
	require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(cur), kazMarshal(rest), cur[i:i+1], nil, delayed, delayedKey, nil))
	return rest
}

// TestKazReplaceChunkSharedRefs: a slice pointed to by 3 records is freed only
// when the last record goes, once.
func TestKazReplaceChunkSharedRefs(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e, r, inode, c, cur := kazSharedSetup(t, backend, 3)
			require.Equal(t, 4, len(cur))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[c])
			cur = kazDropRecord(t, r, inode, cur, 1, nil, 0)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[c])
			cur = kazDropRecord(t, r, inode, cur, 1, nil, 0)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[c])
			kazDropRecord(t, r, inode, cur, 1, nil, 0)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[c])
		})
	}
}

// TestKazReplaceChunkSharedRefsChanged: a stale origin gives up the whole slice
// (refs 3) with one deletion.
func TestKazReplaceChunkSharedRefsChanged(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			r := e.en.(kazChunkReplacer)
			inode := e.file(t)
			e.write(t, inode, 0, 100)
			e.write(t, inode, 50, 100)
			ss := e.records(t, inode)
			stale := kazMarshal(ss)
			stale[4] ^= 0xff
			var c uint64
			require.Zero(t, e.NewSlice(Background(), &c))
			replaced := append(marshalSlice(0, c, 150, 0, 50), marshalSlice(60, c, 150, 50, 50)...)
			replaced = append(replaced, marshalSlice(120, c, 150, 100, 50)...)
			st := r.doKazReplaceChunk(inode, 0, stale, replaced, ss, []kazCreated{{Slice{Id: c, Size: 150, Len: 150}, 3}}, nil, 0, nil)
			require.Equal(t, syscall.EINVAL, st)
			require.Equal(t, kazIDs(ss), kazIDs(e.records(t, inode)))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, map[uint64]int{c: 1}, e.deletedIDs())
		})
	}
}

// TestKazReplaceChunkSharedRefsDelayed: with trash, dropping the 3 records keeps
// the slice until the delay expires, then frees it once.
func TestKazReplaceChunkSharedRefsDelayed(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e, r, inode, c, cur := kazSharedSetup(t, backend, 3)
			for i := 0; i < 3; i++ {
				delayed := e.encodeDelayedSlice(c, 150)
				cur = kazDropRecord(t, r, inode, cur, 1, delayed, c+uint64(i)+1000)
			}
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Zero(t, e.deletedIDs()[c])
			_, err := e.en.doCleanupDelayedSlices(Background(), time.Now().Add(time.Hour).Unix())
			require.NoError(t, err)
			require.Equal(t, 1, e.deletedIDs()[c])
		})
	}
}

// TestKazDeltaCapable: redis and SQL can serve delta mode, TKV cannot.
func TestKazDeltaCapable(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			dc, ok := e.en.(kazDeltaCapable)
			if backend == "memkv" {
				require.False(t, ok)
				return
			}
			require.True(t, ok)
			require.True(t, dc.kazDeltaCapable())
		})
	}
}

// TestKazDedupDeletes keeps the first dead record of each slice id.
func TestKazDedupDeletes(t *testing.T) {
	rs := []*slice{{id: 1, size: 5}, {id: 2, size: 5}, {id: 1, size: 5}, {id: 0}, {id: 3, size: 5}, {id: 2, size: 5}}
	got := kazDedupDeletes(rs, func(s *slice) bool { return s.id != 3 })
	require.Equal(t, []*slice{rs[0], rs[1]}, got)
}

// TestKazBlockEnvZero checks the zero helper appends the same id-0 records on every engine, repeatedly.
func TestKazBlockEnvZero(t *testing.T) {
	for _, backend := range kazBlockBackends {
		t.Run(backend, func(t *testing.T) {
			e := newKazBlockEnv(t, backend, 4)
			inode := e.file(t)
			a := e.write(t, inode, 0, 8192)
			e.zero(t, inode, 100, 50)
			e.zero(t, inode, 300, 70)
			got := e.records(t, inode)
			require.Equal(t, []uint64{a, 0, 0}, kazIDs(got))
			require.Equal(t, [2]uint32{100, 50}, [2]uint32{got[1].pos, got[1].len})
			require.Equal(t, [2]uint32{300, 70}, [2]uint32{got[2].pos, got[2].len})
		})
	}
}
