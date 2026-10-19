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
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// kazCompactionWriter is implemented by chunk writers that can place a
// compaction output in the kaz-2q cache like its sources (chunk.wSlice).
type kazCompactionWriter interface {
	KazSetCompactionSources(f func(indx int) []chunk.KazBlockRef)
}

// kazCompactionSources returns, for the compaction of slices (pieces laid
// out back to back, each starting at the cumulative Len of the previous ones;
// Id 0 is a hole), a function that lists the source blocks of output block
// indx: for every piece overlapping the output range [indx*bs, (indx+1)*bs),
// the blocks of the source slice that hold the overlapping bytes (slice range
// starting at Off plus the offset of the overlap inside the piece). Blocks
// are listed in piece order without duplicates. The kaz-2q cache uses it to
// place each output block like its sources.
func kazCompactionSources(slices []meta.Slice, blockSize int) func(indx int) []chunk.KazBlockRef {
	pieces := append([]meta.Slice(nil), slices...)
	return func(indx int) []chunk.KazBlockRef {
		lo, hi := indx*blockSize, (indx+1)*blockSize
		var refs []chunk.KazBlockRef
		seen := make(map[chunk.KazBlockRef]bool)
		var pos int
		for _, s := range pieces {
			start, end := pos, pos+int(s.Len)
			pos = end
			if s.Id == 0 || s.Len == 0 || end <= lo || start >= hi {
				continue
			}
			from := int(s.Off) + max(start, lo) - start
			to := from + min(end, hi) - max(start, lo)
			for b := from / blockSize; b <= (to-1)/blockSize; b++ {
				ref := chunk.KazBlockRef{ID: s.Id, SliceSize: s.Size, Indx: uint32(b)}
				if !seen[ref] {
					seen[ref] = true
					refs = append(refs, ref)
				}
			}
		}
		return refs
	}
}
