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
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(ss[:3]), replaced, ss[1:3], []Slice{{Id: c, Size: 4096, Len: 4096}}, nil, 0))
			got := e.records(t, inode)
			require.Equal(t, []uint64{a, c, tail}, kazIDs(got))

			// Dropped records lost their only reference; kept, tail and created did not.
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, map[uint64]int{b1: 1, b2: 1}, e.deletedIDs())

			// The created slice holds exactly one reference: dropping its record frees it.
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(got), append(kazMarshal(got[:1]), kazMarshal(got[2:])...), got[1:2], nil, nil, 0))
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
			st := r.doKazReplaceChunk(inode, 0, stale, marshalSlice(0, c, 150, 0, 150), ss, []Slice{{Id: c, Size: 150, Len: 150}}, nil, 0)
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
			require.Zero(t, r.doKazReplaceChunk(inode, 0, kazMarshal(ss), marshalSlice(0, c, 150, 0, 150), ss, []Slice{{Id: c, Size: 150, Len: 150}}, delayed, c))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Empty(t, e.deletedIDs())
			_, err := e.en.doCleanupDelayedSlices(Background(), time.Now().Add(time.Hour).Unix())
			require.NoError(t, err)
			require.Equal(t, map[uint64]int{b1: 1, b2: 1}, e.deletedIDs())
		})
	}
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
