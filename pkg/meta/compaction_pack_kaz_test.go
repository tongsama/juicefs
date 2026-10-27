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
