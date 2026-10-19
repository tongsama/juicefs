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

package chunk

import (
	"container/heap"
	"container/list"
	"sync"
	"time"
)

// kazSegment is the 2Q segment a cached block belongs to.
type kazSegment uint8

const (
	kazProbation kazSegment = 1 // newly cached blocks (2Q A1in); staging blocks also live here
	kazProtected kazSegment = 2 // blocks read again after the correlation period (2Q Am)
)

// String returns the metrics label of the segment.
func (s kazSegment) String() string {
	if s == kazProtected {
		return "protected"
	}
	return "probation"
}

// kazOrigin records how a block entered the cache (for metrics and tuning only).
type kazOrigin uint8

const (
	kazOriginLegacy     kazOrigin = iota // found by a scan, not recently written or filled
	kazOriginWrite                       // written through staging / write cache
	kazOriginFill                        // filled after a read miss (GET)
	kazOriginCompaction                  // output of a compaction
	kazOriginCount                       // number of origins (array size, not an origin)
)

// String returns the metrics label of the origin.
func (o kazOrigin) String() string {
	switch o {
	case kazOriginWrite:
		return "write"
	case kazOriginFill:
		return "fill"
	case kazOriginCompaction:
		return "compaction"
	default:
		return "legacy"
	}
}

const (
	kazFillWindow     = 60   // seconds: a new key with atime this recent counts as a fill
	kazExpectTTL      = 600  // seconds: expectations older than this are dropped (on reset and while registering)
	kazExpectPruneMin = 1024 // expectation count below which kazExpect does not scan for expired ones
	kazHitsSaturated  = ^uint8(0)
)

// kazItem is the per-key state of the kaz 2Q index.
type kazItem struct {
	cacheItem            // size (<0: staging) and atime (last user read)
	key       cacheKey   // key of this item, needed when popping from a heap
	seg       kazSegment // current segment
	inserted  uint32     // when it entered probation (start of the correlation period)
	order     uint32     // eviction order inside probation
	origin    kazOrigin  // how it entered the cache
	hits      uint8      // user hits, saturating
	demoted   bool       // demoted from protected and not promoted since (set again on each demotion)
	pos       int        // index in its segment heap, notInLru when not in a heap
}

// kazSaved is the state of a key kept across reset() (and loaded from the
// state file) so that a re-added key gets its segment and times back.
type kazSaved struct {
	seg      kazSegment
	atime    uint32
	inserted uint32
	order    uint32
	origin   kazOrigin
	hits     uint8
	demoted  bool
}

// kazExpectation is the placement registered for a coming compaction output.
type kazExpectation struct {
	seg   kazSegment
	atime uint32
	at    uint32 // when it was registered
}

// kazGhostPrior is the segment an evicted key was in before its eviction; it
// splits ghost hits by where the block came from.
type kazGhostPrior uint8

const (
	kazPriorUnknown   kazGhostPrior = 0 // not recorded (loaded from a version 1 state file)
	kazPriorProbation kazGhostPrior = 1 // evicted from probation without the demoted mark (not a compaction output)
	kazPriorDemoted   kazGhostPrior = 2 // evicted from probation after a demotion from protected
	kazPriorProtected kazGhostPrior = 3 // evicted straight from protected
	// kazPriorCompaction: a compaction output evicted from probation without
	// the demoted mark (never protected); split from 1 to tell output
	// placement apart from the promotion condition.
	kazPriorCompaction kazGhostPrior = 4
	kazPriorMax                      = kazPriorCompaction // largest valid prior
)

// event returns the name of the ghost-hit event counted for this prior;
// an unknown prior counts as probation.
func (g kazGhostPrior) event() string {
	switch g {
	case kazPriorDemoted:
		return "ghost_hit_demoted"
	case kazPriorProtected:
		return "ghost_hit_protected"
	case kazPriorCompaction:
		return "ghost_hit_compaction"
	default:
		return "ghost_hit_probation"
	}
}

// kazGhost is an entry of the evicted-key list (2Q A1out).
type kazGhost struct {
	key   cacheKey
	size  int64
	prior kazGhostPrior // segment before the eviction
}

// kazHeap is a min-heap of kazItem ordered by order (probation) or atime
// (protected); ties prefer larger blocks, then lower keys.
type kazHeap struct {
	items   []*kazItem
	byOrder bool
}

// rank returns the ordering time of an item in this heap.
func (h *kazHeap) rank(it *kazItem) uint32 {
	if h.byOrder {
		return it.order
	}
	return it.atime
}

// Len implements heap.Interface.
func (h *kazHeap) Len() int { return len(h.items) }

// Less implements heap.Interface (min-heap by rank).
func (h *kazHeap) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	if ra, rb := h.rank(a), h.rank(b); ra != rb {
		return ra < rb
	}
	if a.size != b.size {
		return a.size > b.size
	}
	if a.key.id != b.key.id {
		return a.key.id < b.key.id
	}
	return a.key.indx < b.key.indx
}

// Swap implements heap.Interface and keeps pos up to date.
func (h *kazHeap) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].pos = i
	h.items[j].pos = j
}

// Push implements heap.Interface.
func (h *kazHeap) Push(x any) {
	it := x.(*kazItem)
	it.pos = len(h.items)
	h.items = append(h.items, it)
}

// Pop implements heap.Interface.
func (h *kazHeap) Pop() any {
	n := len(h.items)
	it := h.items[n-1]
	h.items[n-1] = nil
	h.items = h.items[:n-1]
	it.pos = notInLru
	return it
}

// kaz2QIndex is the kaz-2q KeyIndex: a 2Q cache with a probation segment
// (ordered by order), a protected segment (ordered by atime), an evicted-key
// list and state kept across rescans. All methods run under the cacheStore
// mutex, so it needs no locking of its own.
type kaz2QIndex struct {
	keys   map[cacheKey]*kazItem
	prob   kazHeap // probation, evictable (size>0) items only
	prot   kazHeap // protected, evictable items only
	used   [3]int64
	counts [3]int64
	// usedByOrigin splits used by the origin of the items (staging excluded).
	usedByOrigin [3][kazOriginCount]int64

	capacity       int64   // bytes set by kazSetCapacity; 0 = no protected limit, no ghost list
	probationRatio float64 // share of probation
	promoteDelay   uint32  // correlation period in seconds
	ghostRatio     float64 // evicted-key list capacity as a multiple of capacity

	ghost      *list.List // FIFO of kazGhost, oldest at the front
	ghostKeys  map[cacheKey]*list.Element
	ghostBytes int64

	restore         map[cacheKey]kazSaved
	restoreFromFile bool // restore was loaded from the state file
	expect          map[cacheKey]kazExpectation
	expectPruneAt   int // size of expect above which kazExpect drops expired entries

	stateInterval time.Duration // save interval of the state file
	placement     string        // compaction output placement (KazPlacement*), fixed at creation
	// saveMu serializes writers of the state file (periodic saver and the
	// flush at umount); it is not the store mutex and guards no index field.
	saveMu sync.Mutex

	now     func() uint32
	metrics *kaz2QMetrics
	// pubUsed and pubCounts are the per-segment values this index has added
	// to the gauges, which are shared by all stores of a cache manager.
	pubUsed   [3]int64
	pubCounts [3]int64
	// pubUsedByOrigin is the per-segment, per-origin share added to the
	// shared bytes_by_origin gauges.
	pubUsedByOrigin [3][kazOriginCount]int64
	// pubGhostBlocks and pubGhostBytes are the evicted-key list size this
	// index has added to the shared ghost gauges.
	pubGhostBlocks int64
	pubGhostBytes  int64
}

// newKaz2QIndex creates an empty kaz 2Q index from the (self-checked) config.
func newKaz2QIndex(conf *Config) *kaz2QIndex {
	ratio := conf.KazCacheProbationRatio
	if ratio <= 0 || ratio >= 1 {
		ratio = 0.4
	}
	ghost := conf.KazCacheGhostRatio
	if ghost < 0 {
		ghost = 0
	}
	delay := conf.KazCachePromoteDelay
	if delay < 0 {
		delay = 0
	}
	interval := conf.KazCacheStateInterval
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	placement := conf.KazCacheCompactionPlacement
	if placement != KazPlacementProbation && placement != KazPlacementReplace {
		placement = KazPlacementInherit
	}
	return &kaz2QIndex{
		keys:           make(map[cacheKey]*kazItem),
		prob:           kazHeap{byOrder: true},
		probationRatio: ratio,
		promoteDelay:   uint32(delay / time.Second),
		ghostRatio:     ghost,
		ghost:          list.New(),
		ghostKeys:      make(map[cacheKey]*list.Element),
		restore:        make(map[cacheKey]kazSaved),
		expect:         make(map[cacheKey]kazExpectation),
		stateInterval:  interval,
		placement:      placement,
		now:            func() uint32 { return uint32(time.Now().Unix()) },
	}
}

// kazSetCapacity sets the cache capacity in bytes used for the protected
// limit and the evicted-key list cap.
func (p *kaz2QIndex) kazSetCapacity(capacity int64) {
	p.capacity = capacity
	p.trimGhost()
	p.syncGauges()
}

// kazSetMetrics sets the collectors to update; nil disables counting. The
// share this index published to previous collectors is withdrawn first, so
// calling it again does not double count; the share is then published anew.
func (p *kaz2QIndex) kazSetMetrics(m *kaz2QMetrics) {
	if old := p.metrics; old != nil {
		for _, s := range []kazSegment{kazProbation, kazProtected} {
			old.blocks.WithLabelValues(s.String()).Sub(float64(p.pubCounts[s]))
			old.bytes.WithLabelValues(s.String()).Sub(float64(p.pubUsed[s]))
			for o := kazOrigin(0); o < kazOriginCount; o++ {
				old.bytesByOrigin.WithLabelValues(s.String(), o.String()).Sub(float64(p.pubUsedByOrigin[s][o]))
			}
		}
		old.ghostBlocks.Sub(float64(p.pubGhostBlocks))
		old.ghostBytes.Sub(float64(p.pubGhostBytes))
	}
	p.metrics = m
	p.pubUsed, p.pubCounts = [3]int64{}, [3]int64{}
	p.pubUsedByOrigin = [3][kazOriginCount]int64{}
	p.pubGhostBlocks, p.pubGhostBytes = 0, 0
	if m != nil { // export both segments from the start, even when empty
		for _, s := range []kazSegment{kazProbation, kazProtected} {
			m.blocks.WithLabelValues(s.String()).Add(0)
			m.bytes.WithLabelValues(s.String()).Add(0)
			for o := kazOrigin(0); o < kazOriginCount; o++ {
				m.bytesByOrigin.WithLabelValues(s.String(), o.String()).Add(0)
			}
		}
		// export ghost_hit and its per-prior split at 0 from the mount on, so
		// that tools taking differences do not miss the first hits; Add(0)
		// on a counter shared by several stores changes nothing.
		m.events.WithLabelValues("ghost_hit").Add(0)
		for g := kazPriorUnknown; g <= kazPriorMax; g++ {
			m.events.WithLabelValues(g.event()).Add(0)
		}
	}
	p.syncGauges()
}

// event counts one 2Q event when metrics are enabled.
func (p *kaz2QIndex) event(name string) {
	if p.metrics != nil {
		p.metrics.events.WithLabelValues(name).Inc()
	}
}

// syncGauges publishes the per-segment block and byte counts (also split by
// origin) and the size of the evicted-key list. The gauges are shared by the stores of a cache
// manager, so only the change since the last sync of this index is added; the
// gauges then hold the sum over all stores.
func (p *kaz2QIndex) syncGauges() {
	if p.metrics == nil {
		return
	}
	for _, s := range []kazSegment{kazProbation, kazProtected} {
		if d := p.counts[s] - p.pubCounts[s]; d != 0 {
			p.metrics.blocks.WithLabelValues(s.String()).Add(float64(d))
			p.pubCounts[s] = p.counts[s]
		}
		if d := p.used[s] - p.pubUsed[s]; d != 0 {
			p.metrics.bytes.WithLabelValues(s.String()).Add(float64(d))
			p.pubUsed[s] = p.used[s]
		}
		for o := kazOrigin(0); o < kazOriginCount; o++ {
			if d := p.usedByOrigin[s][o] - p.pubUsedByOrigin[s][o]; d != 0 {
				p.metrics.bytesByOrigin.WithLabelValues(s.String(), o.String()).Add(float64(d))
				p.pubUsedByOrigin[s][o] = p.usedByOrigin[s][o]
			}
		}
	}
	if d := int64(p.ghost.Len()) - p.pubGhostBlocks; d != 0 {
		p.metrics.ghostBlocks.Add(float64(d))
		p.pubGhostBlocks += d
	}
	if d := p.ghostBytes - p.pubGhostBytes; d != 0 {
		p.metrics.ghostBytes.Add(float64(d))
		p.pubGhostBytes += d
	}
}

// heapOf returns the heap of a segment.
func (p *kaz2QIndex) heapOf(seg kazSegment) *kazHeap {
	if seg == kazProtected {
		return &p.prot
	}
	return &p.prob
}

// link puts an evictable item into its segment heap and accounts its size
// (per segment and per segment and origin). Staging items (size<0) stay only
// in the map. A change of segment, size or origin goes unlink -> change -> link.
func (p *kaz2QIndex) link(it *kazItem) {
	if it.size <= 0 {
		return
	}
	heap.Push(p.heapOf(it.seg), it)
	p.used[it.seg] += int64(it.size)
	p.usedByOrigin[it.seg][it.origin] += int64(it.size)
	p.counts[it.seg]++
}

// unlink removes an item from its segment heap (if it is in one) and its
// size from the segment accounting.
func (p *kaz2QIndex) unlink(it *kazItem) {
	if it.pos == notInLru {
		return
	}
	heap.Remove(p.heapOf(it.seg), it.pos)
	p.used[it.seg] -= int64(it.size)
	p.usedByOrigin[it.seg][it.origin] -= int64(it.size)
	p.counts[it.seg]--
}

// name implements KeyIndex.
func (p *kaz2QIndex) name() string {
	return EvictionKaz2Q
}

// add implements KeyIndex. A new key is placed by (in order) staging,
// a compaction expectation, the restore state, the evicted-key list, or as a
// new probation block. A compaction output keeps the inherited atime; in
// protected it is also ordered by it, in probation it enters the newest end
// (inserted and order are now). An existing key only gets its size updated. A ghost
// hit counts ghost_hit and one of ghost_hit_{probation,demoted,protected,compaction} by
// the prior segment of the evicted key; it enters protected without the
// demoted mark.
func (p *kaz2QIndex) add(key cacheKey, item cacheItem) {
	if it, ok := p.keys[key]; ok {
		p.updateSize(it, item.size)
		p.syncGauges()
		return
	}
	now := p.now()
	it := &kazItem{cacheItem: item, key: key, seg: kazProbation, pos: notInLru}
	if item.size < 0 {
		it.origin = kazOriginWrite
		it.inserted, it.order = now, now
	} else if e, ok := p.expect[key]; ok {
		delete(p.expect, key)
		it.seg, it.atime, it.origin = e.seg, e.atime, kazOriginCompaction
		if e.seg == kazProtected {
			it.inserted, it.order = e.atime, e.atime
		} else {
			// probation: enter the newest end (and start the correlation
			// period) now, keeping the inherited atime; with the old atime as
			// order it would be the next eviction victim and never get the
			// chance to be read again and promoted.
			it.inserted, it.order = now, now
		}
	} else if s, ok := p.restore[key]; ok {
		delete(p.restore, key)
		it.seg, it.atime, it.inserted, it.order = s.seg, s.atime, s.inserted, s.order
		it.origin, it.hits, it.demoted = s.origin, s.hits, s.demoted
		if item.atime > it.atime { // the block was read after the state was saved
			it.atime = item.atime
		}
	} else if el, ok := p.ghostKeys[key]; ok {
		g := p.dropGhost(el)
		it.seg = kazProtected
		it.inserted, it.order = item.atime, item.atime
		it.origin = kazOriginFill
		p.event("ghost_hit")
		p.event(g.prior.event())
	} else {
		it.inserted, it.order = item.atime, item.atime
		if item.atime+kazFillWindow >= now {
			it.origin = kazOriginFill
		} else {
			it.origin = kazOriginLegacy
		}
	}
	if it.seg != kazProtected {
		it.seg = kazProbation
	}
	p.keys[key] = it
	p.link(it)
	p.syncGauges()
}

// updateSize changes the size of an existing item without changing its
// segment; an upload completion (negative to positive) enters probation now.
func (p *kaz2QIndex) updateSize(it *kazItem, size int32) {
	if it.size < 0 && size > 0 {
		now := p.now()
		it.size, it.seg = size, kazProbation
		it.inserted, it.order = now, now
		p.link(it)
		return
	}
	p.unlink(it)
	it.size = size
	p.link(it)
}

// remove implements KeyIndex. Staging items are kept unless staging is true.
// Removed keys (retire, expiry) do not enter the evicted-key list.
func (p *kaz2QIndex) remove(key cacheKey, staging bool) *cacheItem {
	it, ok := p.keys[key]
	if !ok {
		return nil
	}
	if it.size < 0 && !staging {
		return nil
	}
	delete(p.keys, key)
	p.unlink(it)
	p.syncGauges()
	ci := it.cacheItem
	return &ci
}

// get implements KeyIndex as a user hit: it updates atime and hits, counts
// the hit by origin, and promotes a probation block read after the
// correlation period (clearing its demoted mark). It returns a copy of the
// updated item.
func (p *kaz2QIndex) get(key cacheKey) *cacheItem {
	it, ok := p.keys[key]
	if !ok {
		return nil
	}
	now := p.now()
	if p.metrics != nil {
		p.metrics.hits.WithLabelValues(it.origin.String()).Inc()
	}
	if it.hits < kazHitsSaturated {
		it.hits++
	}
	it.atime = now
	switch {
	case it.size < 0: // staging: never promoted
		it.order = now
	case it.seg == kazProbation && now >= it.inserted && now-it.inserted >= p.promoteDelay:
		p.unlink(it)
		it.seg = kazProtected
		it.demoted = false
		p.link(it)
		p.event("promote")
		p.syncGauges()
	case it.seg == kazProbation:
		it.order = now
		if it.pos != notInLru {
			heap.Fix(&p.prob, it.pos)
		}
	default: // protected
		if it.pos != notInLru {
			heap.Fix(&p.prot, it.pos)
		}
	}
	ci := it.cacheItem
	return &ci
}

// kazPeek returns a copy of the item without any side effect.
func (p *kaz2QIndex) kazPeek(key cacheKey) *cacheItem {
	if it, ok := p.keys[key]; ok {
		ci := it.cacheItem
		return &ci
	}
	return nil
}

// kazSegmentOf returns the segment of a key (staging counts as probation).
func (p *kaz2QIndex) kazSegmentOf(key cacheKey) (kazSegment, bool) {
	if it, ok := p.keys[key]; ok {
		return it.seg, true
	}
	return 0, false
}

// kazPlacement decides where a compaction output goes from its source keys:
// protected if any source is protected, probation if they are only in
// probation (or staging), with the newest atime among them; ok is false when
// no source is cached. A key not in the index but kept in restore (moved there
// by reset() during a rescan, or loaded from the state file) counts with its
// saved segment and atime, so that a rescan window does not lose placements.
func (p *kaz2QIndex) kazPlacement(keys []cacheKey) (seg kazSegment, atime uint32, ok bool) {
	for _, k := range keys {
		var kseg kazSegment
		var katime uint32
		if it, found := p.keys[k]; found {
			kseg, katime = it.seg, it.atime
		} else if s, found := p.restore[k]; found {
			kseg, katime = s.seg, s.atime
		} else {
			continue
		}
		if !ok {
			seg = kazProbation
		}
		if kseg == kazProtected {
			seg = kazProtected
		}
		ok = true
		if katime > atime {
			atime = katime
		}
	}
	return
}

// kazExpect registers the segment and atime the next add of key should use
// (a compaction output). Expectations whose output never arrived are dropped
// here as well as on reset(): when the map outgrows expectPruneAt, entries
// older than kazExpectTTL are removed and the threshold is set to twice the
// remaining size (at least kazExpectPruneMin), so the scan cost is amortized
// and the map stays bounded without rescans.
func (p *kaz2QIndex) kazExpect(key cacheKey, seg kazSegment, atime uint32) {
	now := p.now()
	p.expect[key] = kazExpectation{seg: seg, atime: atime, at: now}
	if len(p.expect) > p.expectPruneAt {
		p.pruneExpect(now)
		p.expectPruneAt = max(2*len(p.expect), kazExpectPruneMin)
	}
}

// pruneExpect drops the expectations registered more than kazExpectTTL ago.
func (p *kaz2QIndex) pruneExpect(now uint32) {
	for k, e := range p.expect {
		if now-e.at > kazExpectTTL {
			delete(p.expect, k)
		}
	}
}

// peekAtime implements KeyIndex.
func (p *kaz2QIndex) peekAtime(key cacheKey) uint32 {
	if it, ok := p.keys[key]; ok {
		return it.atime
	}
	return 0
}

// len implements KeyIndex; it counts all items including staging.
func (p *kaz2QIndex) len() int {
	return len(p.keys)
}

// reset implements KeyIndex. It empties the receiver, moves the per-key state
// into restore (merged with a restore loaded from the state file, current
// items winning), keeps the evicted-key list and settings, drops stale
// expectations, and returns a snapshot that answers peekAtime.
func (p *kaz2QIndex) reset() KeyIndex {
	snap := &kaz2QIndex{keys: p.keys, prob: p.prob, prot: p.prot, now: p.now}
	restore := make(map[cacheKey]kazSaved, len(p.keys))
	if p.restoreFromFile {
		for k, s := range p.restore {
			restore[k] = s
		}
	}
	for k, it := range p.keys {
		restore[k] = kazSaved{seg: it.seg, atime: it.atime, inserted: it.inserted,
			order: it.order, origin: it.origin, hits: it.hits, demoted: it.demoted}
	}
	p.restore, p.restoreFromFile = restore, false
	p.pruneExpect(p.now())
	p.keys = make(map[cacheKey]*kazItem, len(snap.keys))
	p.prob = kazHeap{byOrder: true}
	p.prot = kazHeap{}
	p.used, p.counts = [3]int64{}, [3]int64{}
	p.usedByOrigin = [3][kazOriginCount]int64{}
	p.syncGauges()
	return snap
}

// randomIter implements KeyIndex (map order).
func (p *kaz2QIndex) randomIter() func(yield func(key cacheKey, item cacheItem) bool) {
	return func(yield func(key cacheKey, item cacheItem) bool) {
		for k, it := range p.keys {
			if !yield(k, it.cacheItem) {
				return
			}
		}
	}
}

// evictionIter implements KeyIndex. Before each eviction it demotes the
// oldest protected blocks to the newest end of probation while protected
// exceeds capacity*(1-probationRatio); then it evicts the oldest probation
// block, or the oldest protected block when probation is empty. Each victim
// is removed from the index and remembered in the evicted-key list, with its
// prior segment (probation, demoted, protected, or compaction for a never
// demoted compaction output in probation), before it is yielded.
// Staging items are never yielded.
func (p *kaz2QIndex) evictionIter() func(yield func(key cacheKey, item cacheItem) bool) {
	return func(yield func(key cacheKey, item cacheItem) bool) {
		for {
			p.demoteOverflow()
			var it *kazItem
			var ev string
			var prior kazGhostPrior
			if p.prob.Len() > 0 {
				it, ev, prior = p.prob.items[0], "evict_probation", kazPriorProbation
				if it.demoted {
					prior = kazPriorDemoted
				} else if it.origin == kazOriginCompaction {
					prior = kazPriorCompaction
				}
			} else if p.prot.Len() > 0 {
				it, ev, prior = p.prot.items[0], "evict_protected", kazPriorProtected
			} else {
				return
			}
			p.unlink(it)
			delete(p.keys, it.key)
			p.addGhost(it.key, int64(it.size), prior)
			p.event(ev)
			p.syncGauges()
			if !yield(it.key, it.cacheItem) {
				return
			}
		}
	}
}

// demoteOverflow moves the least recently read protected blocks to the newest
// end of probation while protected is over its limit (no limit without a
// capacity), marking them as demoted.
func (p *kaz2QIndex) demoteOverflow() {
	if p.capacity <= 0 {
		return
	}
	limit := int64(float64(p.capacity) * (1 - p.probationRatio))
	now := p.now()
	for p.used[kazProtected] > limit && p.prot.Len() > 0 {
		it := p.prot.items[0]
		p.unlink(it)
		it.seg = kazProbation
		it.inserted, it.order = now, now
		it.demoted = true
		p.link(it)
		p.event("demote")
	}
}

// ghostCap returns the byte cap of the evicted-key list (0 = disabled).
func (p *kaz2QIndex) ghostCap() int64 {
	if p.capacity <= 0 || p.ghostRatio <= 0 {
		return 0
	}
	return int64(float64(p.capacity) * p.ghostRatio)
}

// addGhost remembers an evicted key with the segment it was evicted from,
// forgetting the oldest beyond the cap.
func (p *kaz2QIndex) addGhost(key cacheKey, size int64, prior kazGhostPrior) {
	if p.ghostCap() == 0 {
		return
	}
	if el, ok := p.ghostKeys[key]; ok {
		p.dropGhost(el)
	}
	p.ghostKeys[key] = p.ghost.PushBack(kazGhost{key: key, size: size, prior: prior})
	p.ghostBytes += size
	p.trimGhost()
}

// dropGhost removes one entry from the evicted-key list and returns it.
func (p *kaz2QIndex) dropGhost(el *list.Element) kazGhost {
	g := p.ghost.Remove(el).(kazGhost)
	delete(p.ghostKeys, g.key)
	p.ghostBytes -= g.size
	return g
}

// trimGhost forgets the oldest evicted keys while the list is over its cap.
func (p *kaz2QIndex) trimGhost() {
	limit := p.ghostCap()
	for p.ghost.Len() > 0 && p.ghostBytes > limit {
		p.dropGhost(p.ghost.Front())
	}
}
