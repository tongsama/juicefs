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
	"sync"
)

// compactionKey identifies one chunk independently of its current hint generation.
type compactionKey struct {
	inode Ino
	chunk uint32
}

// compactionHint coalesces notifications and retains changes made during execution.
type compactionHint struct {
	key         compactionKey
	count, tier int
	generation  uint64
	active      bool
	element     *list.Element
	bucket      int
}

// compactionInode holds one inode's pending chunks and enforces its execution quota.
type compactionInode struct {
	pending [3]list.List
	active  bool
	ready   *list.Element
	bucket  int
}

// compactionScheduler bounds active plus pending hints, with inode round-robin
// within priority buckets. Hints are volatile; callers retain correctness fallbacks.
type compactionScheduler struct {
	mu       sync.Mutex
	wake     *sync.Cond
	workers  sync.WaitGroup
	run      func(Ino, uint32, int)
	capacity int
	closed   bool
	jobs     map[compactionKey]*compactionHint
	inodes   map[Ino]*compactionInode
	ready    [3]list.List
	turn     int
}

// newCompactionScheduler starts a fixed worker pool; invalid limits reject hints.
func newCompactionScheduler(run func(Ino, uint32, int), workers, capacity int) *compactionScheduler {
	s := &compactionScheduler{run: run, capacity: capacity, jobs: make(map[compactionKey]*compactionHint), inodes: make(map[Ino]*compactionInode)}
	s.wake = sync.NewCond(&s.mu)
	if workers <= 0 || capacity <= 0 || run == nil {
		s.closed = true
		return s
	}
	s.workers.Add(workers)
	for i := 0; i < workers; i++ {
		go s.worker()
	}
	return s
}

// compactionPriority classifies fragmentation without consulting metadata storage.
func compactionPriority(count int) int {
	if count >= 1000 {
		return 2
	}
	if count >= 100 {
		return 1
	}
	return 0
}

// schedule merges a hint without waiting for storage or exceeding map capacity.
// Tier is the storage destination and does not determine scheduling priority.
func (s *compactionScheduler) schedule(inode Ino, chunk uint32, count, tier int) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return false
	}
	key := compactionKey{inode, chunk}
	hint := s.jobs[key]
	if hint == nil {
		if len(s.jobs) >= s.capacity {
			return false
		}
		hint = &compactionHint{key: key, count: count, tier: tier}
		s.jobs[key] = hint
	} else {
		if count > hint.count {
			hint.count = count
		}
		hint.tier = tier
	}
	hint.generation++
	if hint.active {
		return true
	}
	state := s.inodes[inode]
	if state == nil {
		state = &compactionInode{}
		s.inodes[inode] = state
	}
	bucket := compactionPriority(hint.count)
	if hint.element == nil {
		hint.bucket = bucket
		hint.element = state.pending[bucket].PushBack(hint)
	} else if hint.bucket != bucket {
		state.pending[hint.bucket].Remove(hint.element)
		hint.bucket = bucket
		hint.element = state.pending[bucket].PushBack(hint)
	}
	s.makeReady(inode, state)
	s.wake.Signal()
	return true
}

// makeReady queues an idle inode once, promoting it if a more urgent chunk arrives.
// The caller holds mu; ready entries and inode states remain bounded by jobs.
func (s *compactionScheduler) makeReady(inode Ino, state *compactionInode) {
	if state.active {
		return
	}
	bucket := -1
	for i := 2; i >= 0; i-- {
		if state.pending[i].Len() > 0 {
			bucket = i
			break
		}
	}
	if bucket < 0 {
		return
	}
	if state.ready != nil {
		if state.bucket == bucket {
			return
		}
		s.ready[state.bucket].Remove(state.ready)
	}
	state.bucket = bucket
	state.ready = s.ready[bucket].PushBack(inode)
}

// take selects weighted priority buckets so ready lower-priority inodes receive
// turns, then rotates inodes by placing completions at the tail.
// The caller holds mu and has checked that the scheduler is open.
func (s *compactionScheduler) take() (*compactionHint, *compactionInode) {
	order := [7]int{2, 2, 2, 2, 1, 1, 0}
	for offset := 0; offset < len(order); offset++ {
		turn := (s.turn + offset) % len(order)
		bucket := order[turn]
		first := s.ready[bucket].Front()
		if first == nil {
			continue
		}
		s.turn = (turn + 1) % len(order)
		inode := first.Value.(Ino)
		state := s.inodes[inode]
		s.ready[bucket].Remove(first)
		state.ready = nil
		state.active = true
		element := state.pending[bucket].Front()
		hint := element.Value.(*compactionHint)
		state.pending[bucket].Remove(element)
		hint.element = nil
		hint.active = true
		return hint, state
	}
	return nil, nil
}

// worker runs storage callbacks outside mu and requeues dirty active generations.
func (s *compactionScheduler) worker() {
	defer s.workers.Done()
	for {
		s.mu.Lock()
		var hint *compactionHint
		var state *compactionInode
		for !s.closed {
			hint, state = s.take()
			if hint != nil {
				break
			}
			s.wake.Wait()
		}
		if s.closed {
			s.mu.Unlock()
			return
		}
		generation, tier := hint.generation, hint.tier
		hint.count = 0
		s.mu.Unlock()
		s.run(hint.key.inode, hint.key.chunk, tier)
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return
		}
		hint.active = false
		state.active = false
		if generation != hint.generation {
			hint.bucket = compactionPriority(hint.count)
			hint.element = state.pending[hint.bucket].PushBack(hint)
		} else {
			delete(s.jobs, hint.key)
		}
		s.makeReady(hint.key.inode, state)
		if state.ready == nil {
			delete(s.inodes, hint.key.inode)
		}
		s.wake.Broadcast()
		s.mu.Unlock()
	}
}

// close rejects notifications and discards queued advisory work without waiting
// for active storage I/O. An already dispatched callback may finish after close.
func (s *compactionScheduler) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	clear(s.jobs)
	clear(s.inodes)
	for i := range s.ready {
		s.ready[i].Init()
	}
	s.wake.Broadcast()
}

// wait joins callbacks after close before metadata storage is shut down.
func (s *compactionScheduler) wait() { s.workers.Wait() }
