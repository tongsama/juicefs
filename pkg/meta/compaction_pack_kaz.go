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

// kazPackOut is one visible range that the packed slice takes over: the
// chunk position, the source slice id, the offset inside that source slice's
// data, and the length.
type kazPackOut struct {
	pos    uint32
	srcID  uint64
	srcOff uint32
	length uint32
}

// kazPackPlan is the result of planKazPack. removed are the records dropped
// from the chunk, kept the remaining records in their original order, outs the
// visible ranges (sorted by position) to copy into the new slice, and ids the
// source slices that contribute at least one out.
type kazPackPlan struct {
	removed []*slice
	kept    []*slice
	outs    []kazPackOut
	ids     []uint64
}

// planKazPack plans packing small slices of one chunk into one new slice.
// cands are processed in order (the first is the dequeued block). All records
// of a selected id are removed. An id whose visible bytes would push the total
// past maxBytes is skipped (its records stay). A selected id that is fully
// hidden is removed without outs and does not count toward the threshold: only
// ids that contribute outs are counted, and fewer than 2 of them returns nil
// (packing one slice saves no PUT). Ids absent from ss are ignored. Adjacent
// outs are not merged, so no source data has to be read to fill a gap.
func planKazPack(ss []*slice, cands []uint64, maxBytes uint32) *kazPackPlan {
	present := make(map[uint64]bool, len(ss))
	for _, s := range ss {
		present[s.id] = true
	}
	// Visible pieces per id, in chunk order.
	perID := make(map[uint64][]kazPackOut)
	var pos uint32
	for _, p := range buildSlice(ss) {
		if p.Id != 0 && p.Len > 0 {
			perID[p.Id] = append(perID[p.Id], kazPackOut{pos: pos, srcID: p.Id, srcOff: p.Off, length: p.Len})
		}
		pos += p.Len
	}
	selected := make(map[uint64]bool)
	var total uint64
	var ids []uint64
	var outs []kazPackOut
	for _, id := range cands {
		if !present[id] || selected[id] {
			continue
		}
		var vis uint64
		for _, o := range perID[id] {
			vis += uint64(o.length)
		}
		if total+vis > uint64(maxBytes) {
			continue
		}
		selected[id] = true
		total += vis
		if vis > 0 {
			ids = append(ids, id)
			outs = append(outs, perID[id]...)
		}
	}
	if len(ids) < 2 {
		return nil
	}
	plan := &kazPackPlan{ids: ids}
	for _, s := range ss {
		if selected[s.id] {
			plan.removed = append(plan.removed, s)
		} else {
			plan.kept = append(plan.kept, s)
		}
	}
	// Selection order is by candidate; outs must be sorted by position.
	for i := 1; i < len(outs); i++ {
		for j := i; j > 0 && outs[j].pos < outs[j-1].pos; j-- {
			outs[j], outs[j-1] = outs[j-1], outs[j]
		}
	}
	plan.outs = outs
	return plan
}
