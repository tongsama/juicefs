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
	"sort"

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
// are listed in piece order without duplicates. Whole is set on a block when
// the union of its byte ranges copied into this output block, across all
// pieces, covers the block entirely ([0, min(bs, SliceSize-Indx*bs))). Bytes
// is the size of that union (bytes of the block copied twice by overlapping
// pieces count once), and OutBytes is the number of non-hole bytes of output
// block indx, set in every ref of it. OutBytes is the sum of the overlaps of
// the non-hole pieces with the output range: pieces are laid out back to back
// and never overlap in output coordinates, so each output byte is counted
// once. It is not the sum of Bytes, which is smaller when overlapping pieces
// copy the same source bytes twice. The kaz-2q cache uses it to place each
// output block like its sources.
func kazCompactionSources(slices []meta.Slice, blockSize int) func(indx int) []chunk.KazBlockRef {
	pieces := append([]meta.Slice(nil), slices...)
	return func(indx int) []chunk.KazBlockRef {
		lo, hi := indx*blockSize, (indx+1)*blockSize
		var refs []chunk.KazBlockRef
		pos := make(map[chunk.KazBlockRef]int)         // ref (Whole false) -> index in refs
		copied := make(map[chunk.KazBlockRef][][2]int) // ref -> copied ranges inside the block
		var off, outBytes int
		for _, s := range pieces {
			start, end := off, off+int(s.Len)
			off = end
			if s.Id == 0 || s.Len == 0 || end <= lo || start >= hi {
				continue
			}
			from := int(s.Off) + max(start, lo) - start
			to := from + min(end, hi) - max(start, lo)
			outBytes += to - from
			for b := from / blockSize; b <= (to-1)/blockSize; b++ {
				ref := chunk.KazBlockRef{ID: s.Id, SliceSize: s.Size, Indx: uint32(b)}
				if _, ok := pos[ref]; !ok {
					pos[ref] = len(refs)
					refs = append(refs, ref)
				}
				base := b * blockSize
				copied[ref] = append(copied[ref], [2]int{max(from, base) - base, min(to, base+blockSize) - base})
			}
		}
		for ref, i := range pos {
			blockLen := min(blockSize, int(ref.SliceSize)-int(ref.Indx)*blockSize)
			refs[i].Whole = kazRangesCover(copied[ref], blockLen)
			refs[i].Bytes = uint32(kazRangesLen(copied[ref]))
		}
		for i := range refs {
			refs[i].OutBytes = uint32(outBytes)
		}
		return refs
	}
}

// kazRangesCover reports whether the union of the half-open ranges covers
// [0, n). Ranges may overlap and come in any order; ranges is reordered.
func kazRangesCover(ranges [][2]int, n int) bool {
	if n <= 0 {
		return false
	}
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	covered := 0 // [0, covered) is covered so far
	for _, r := range ranges {
		if r[0] > covered {
			return false
		}
		covered = max(covered, r[1])
		if covered >= n {
			return true
		}
	}
	return false
}

// kazRangesLen returns the length of the union of the half-open ranges.
// Ranges may overlap and come in any order; ranges is reordered.
func kazRangesLen(ranges [][2]int) int {
	sort.Slice(ranges, func(i, j int) bool { return ranges[i][0] < ranges[j][0] })
	total, end := 0, 0 // end: the union so far ends at end
	for _, r := range ranges {
		if r[1] <= end {
			continue
		}
		total += r[1] - max(r[0], end)
		end = r[1]
	}
	return total
}
