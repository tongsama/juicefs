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
	"container/list"
	"strings"
	"sync"
)

// kazDeltaBaseTable remembers, per chunk, how many records were not new when
// this client last planned a delta compaction of it (the base count of spec
// §5.3). It holds at most kazDeltaBaseCap chunks and drops the least recently
// set one when full; a dropped or stale entry only sends the chunk back to the
// usual trigger, so it never affects correctness. The zero value is ready to use.
type kazDeltaBaseTable struct {
	mu    sync.Mutex
	items map[uint64]*list.Element // chunk key -> element holding a kazDeltaBaseEntry
	order list.List                // oldest set first
}

// kazDeltaBaseEntry is one remembered base count.
type kazDeltaBaseEntry struct {
	key uint64
	n   int
}

// kazDeltaChunkKey packs a chunk the way m.compacting keys it.
func kazDeltaChunkKey(inode Ino, indx uint32) uint64 {
	return uint64(inode) + (uint64(indx) << 40)
}

// get returns the remembered base count of a chunk.
func (t *kazDeltaBaseTable) get(inode Ino, indx uint32) (int, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.items[kazDeltaChunkKey(inode, indx)]; ok {
		return e.Value.(*kazDeltaBaseEntry).n, true
	}
	return 0, false
}

// set remembers n as the base count of a chunk, making it the newest entry and
// dropping the oldest one past kazDeltaBaseCap.
func (t *kazDeltaBaseTable) set(inode Ino, indx uint32, n int) {
	k := kazDeltaChunkKey(inode, indx)
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.items == nil {
		t.items = make(map[uint64]*list.Element)
	}
	if e, ok := t.items[k]; ok {
		e.Value.(*kazDeltaBaseEntry).n = n
		t.order.MoveToBack(e)
		return
	}
	t.items[k] = t.order.PushBack(&kazDeltaBaseEntry{key: k, n: n})
	for len(t.items) > kazDeltaBaseCap {
		old := t.order.Front()
		t.order.Remove(old)
		delete(t.items, old.Value.(*kazDeltaBaseEntry).key)
	}
}

// forget drops the base count of a chunk.
func (t *kazDeltaBaseTable) forget(inode Ino, indx uint32) {
	k := kazDeltaChunkKey(inode, indx)
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.items[k]; ok {
		t.order.Remove(e)
		delete(t.items, k)
	}
}

// kazDeltaUsable reports why delta compaction can or cannot run on this mount:
// the engine must replace chunks and count shared references safely (spec §5.9),
// block mode must be usable, and one compaction must read at least
// max-records + trigger + fold margin records.
func (m *baseMeta) kazDeltaUsable() (capable bool, limit, need int, ok bool) {
	_, replacer := m.en.(kazChunkReplacer)
	dc, isDC := m.en.(kazDeltaCapable)
	capable = replacer && isDC && dc.kazDeltaCapable()
	need = m.conf.kazDeltaConf().maxRecords + kazDeltaTrigger + kazDeltaFoldMargin
	bs := m.kazBlockSize()
	limit, fits := kazBlockCompactLimit(bs)
	return capable, limit, need, capable && bs > 0 && fits && limit >= need
}

// kazDeltaActive reports whether background compaction of this mount uses the
// delta mode (selected and usable); otherwise block mode (or upstream) runs.
func (m *baseMeta) kazDeltaActive() bool {
	if !m.conf.kazDeltaEnabled() {
		return false
	}
	_, _, _, ok := m.kazDeltaUsable()
	return ok
}

// kazDeltaPlanStats summarizes a committed delta plan for logs and metrics:
// distinct tier slices left in the chunk, records and bytes of the new tier.
type kazDeltaPlanStats struct {
	tiers, records int
	bytes          uint64
}

// kazDeltaStats returns the tier statistics of plan p made from records ss
// with block size bs.
func kazDeltaStats(ss []*slice, bs uint32, p *kazBlockPlan) kazDeltaPlanStats {
	var st kazDeltaPlanStats
	for _, o := range p.outs {
		if o.delta {
			st.records++
			st.bytes += uint64(o.length)
		}
	}
	if len(ss) > 0 {
		kinds := kazClassify(ss, kazNewBlockGeom(ss, bs))
		kept := make(map[*slice]bool, len(p.kept))
		for _, s := range p.kept {
			kept[s] = true
		}
		ids := make(map[uint64]bool)
		for i, s := range ss {
			if kept[s] && kinds[i] == kazRecDelta {
				ids[s.id] = true
			}
		}
		st.tiers = len(ids)
	}
	if st.records > 0 {
		st.tiers++
	}
	return st
}

// kazDeltaLogMode turns a delta plan event (delta_tier) into the log mode
// (delta-tier) of the "compaction block" DEBUG line.
func kazDeltaLogMode(event string) string {
	return strings.ReplaceAll(event, "_", "-")
}
