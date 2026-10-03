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
	"sync"
	"testing"
	"time"
)

// schedulerCall records the observable compaction callback arguments.
type schedulerCall struct {
	inode Ino
	chunk uint32
	tier  int
}

// receiveSchedulerCall bounds waits for a worker callback.
func receiveSchedulerCall(t *testing.T, calls <-chan schedulerCall) schedulerCall {
	t.Helper()
	select {
	case call := <-calls:
		return call
	case <-time.After(3 * time.Second):
		t.Fatal("compaction callback did not run")
		return schedulerCall{}
	}
}

// TestCompactionSchedulerBoundedDedup verifies active jobs consume capacity and pending updates coalesce.
func TestCompactionSchedulerBoundedDedup(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) {
		calls <- schedulerCall{i, c, tier}
		if i == 1 {
			<-gate
		}
	}, 1, 2)
	defer s.close()
	defer once.Do(func() { close(gate) })
	if !s.schedule(1, 0, 10, 0) {
		t.Fatal("initial hint rejected")
	}
	receiveSchedulerCall(t, calls)
	if !s.schedule(2, 0, 10, 0) || !s.schedule(2, 0, 1200, 7) {
		t.Fatal("duplicate should fit bounded map")
	}
	if s.schedule(3, 0, 10, 0) {
		t.Fatal("capacity excludes active jobs")
	}
	once.Do(func() { close(gate) })
	if got := receiveSchedulerCall(t, calls); got != (schedulerCall{2, 0, 7}) {
		t.Fatalf("coalesced call: %+v", got)
	}
}

// TestCompactionSchedulerUrgentFairDispatch verifies urgency and inode round-robin ordering.
func TestCompactionSchedulerUrgentFairDispatch(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) {
		calls <- schedulerCall{i, c, tier}
		if i == 99 {
			<-gate
		}
	}, 1, 20)
	defer s.close()
	defer once.Do(func() { close(gate) })
	s.schedule(99, 0, 1, 0)
	receiveSchedulerCall(t, calls)
	s.schedule(1, 0, 10, 0)
	s.schedule(2, 0, 1200, 0)
	s.schedule(2, 1, 1200, 0)
	s.schedule(2, 2, 1200, 0)
	s.schedule(3, 0, 1200, 0)
	once.Do(func() { close(gate) })
	expected := []Ino{2, 3, 2, 2, 1}
	for _, inode := range expected {
		if got := receiveSchedulerCall(t, calls); got.inode != inode {
			t.Fatalf("dispatch inode %d, want %d", got.inode, inode)
		}
	}
}

// TestCompactionSchedulerActiveNotification verifies updates during storage I/O survive completion.
func TestCompactionSchedulerActiveNotification(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) {
		calls <- schedulerCall{i, c, tier}
		if tier == 0 {
			<-gate
		}
	}, 1, 1)
	defer s.close()
	defer once.Do(func() { close(gate) })
	s.schedule(1, 0, 10, 0)
	receiveSchedulerCall(t, calls)
	if !s.schedule(1, 0, 20, 1) || !s.schedule(1, 0, 30, 2) {
		t.Fatal("active updates rejected")
	}
	once.Do(func() { close(gate) })
	if got := receiveSchedulerCall(t, calls); got.tier != 2 {
		t.Fatalf("lost latest active generation: %+v", got)
	}
}

// TestCompactionSchedulerPerInodeQuota verifies other inodes can use idle workers while one inode is active.
func TestCompactionSchedulerPerInodeQuota(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) {
		calls <- schedulerCall{i, c, tier}
		if i == 1 {
			<-gate
		}
	}, 3, 10)
	defer s.close()
	defer once.Do(func() { close(gate) })
	s.schedule(1, 0, 1200, 0)
	receiveSchedulerCall(t, calls)
	s.schedule(1, 1, 1200, 0)
	s.schedule(1, 2, 1200, 0)
	s.schedule(2, 0, 10, 0)
	if got := receiveSchedulerCall(t, calls); got.inode != 2 {
		t.Fatalf("hot inode monopolized workers: %+v", got)
	}
}

// TestCompactionSchedulerClose verifies cancellation does not wait for I/O or dispatch queued work.
func TestCompactionSchedulerClose(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) { calls <- schedulerCall{i, c, tier}; <-gate }, 1, 10)
	defer once.Do(func() { close(gate) })
	s.schedule(1, 0, 10, 0)
	receiveSchedulerCall(t, calls)
	s.schedule(2, 0, 10, 0)
	done := make(chan struct{})
	go func() { s.close(); s.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("close waited on storage callback")
	}
	if s.schedule(3, 0, 10, 0) || s.schedule(1, 0, 10, 0) {
		t.Fatal("closed scheduler accepted hint")
	}
	once.Do(func() { close(gate) })
	s.workers.Wait()
	select {
	case got := <-calls:
		t.Fatalf("queued callback after close: %+v", got)
	default:
	}
}

// TestCompactionSchedulerLowerPriorityProgress bounds delays under a sustained urgent backlog.
func TestCompactionSchedulerLowerPriorityProgress(t *testing.T) {
	calls := make(chan schedulerCall, 64)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) {
		calls <- schedulerCall{i, c, tier}
		if i == 99 {
			<-gate
		}
	}, 1, 64)
	defer s.close()
	defer once.Do(func() { close(gate) })
	s.schedule(99, 0, 1, 0)
	receiveSchedulerCall(t, calls)
	for i := Ino(1); i <= 20; i++ {
		s.schedule(i, 0, 1200, 0)
	}
	s.schedule(98, 0, 100, 0)
	s.schedule(97, 0, 1, 0)
	once.Do(func() { close(gate) })
	seenMedium, seenLow := false, false
	for i := 0; i < 7; i++ {
		got := receiveSchedulerCall(t, calls)
		seenMedium = seenMedium || got.inode == 98
		seenLow = seenLow || got.inode == 97
	}
	if !seenMedium || !seenLow {
		t.Fatalf("lower buckets starved: medium %v, low %v", seenMedium, seenLow)
	}
}

// TestCompactionSchedulerCallbackOutsideLock permits a callback to notify another inode.
func TestCompactionSchedulerCallbackOutsideLock(t *testing.T) {
	calls := make(chan schedulerCall, 10)
	start := make(chan struct{})
	var s *compactionScheduler
	s = newCompactionScheduler(func(i Ino, c uint32, tier int) {
		<-start
		if i == 1 {
			s.schedule(2, 0, 10, 3)
		}
		calls <- schedulerCall{i, c, tier}
	}, 1, 10)
	defer s.close()
	s.schedule(1, 0, 10, 0)
	close(start)
	receiveSchedulerCall(t, calls)
	if got := receiveSchedulerCall(t, calls); got != (schedulerCall{2, 0, 3}) {
		t.Fatalf("callback notification failed: %+v", got)
	}
}

// TestCompactionSchedulerInvalidLimits verifies unsafe configurations create no workers.
func TestCompactionSchedulerInvalidLimits(t *testing.T) {
	for _, limit := range [][2]int{{0, 1}, {-1, 1}, {1, 0}, {1, -1}} {
		s := newCompactionScheduler(func(Ino, uint32, int) { t.Error("invalid scheduler ran") }, limit[0], limit[1])
		if s.schedule(1, 0, 10, 0) {
			t.Fatalf("invalid limits accepted: %v", limit)
		}
		s.close()
		s.workers.Wait()
	}
	s := newCompactionScheduler(nil, 1, 1)
	if s.schedule(1, 0, 10, 0) {
		t.Fatal("nil callback accepted")
	}
	s.close()
}

// TestCompactionSchedulerConcurrentClose exercises racing notifications and cancellation.
func TestCompactionSchedulerConcurrentClose(t *testing.T) {
	s := newCompactionScheduler(func(Ino, uint32, int) {}, 4, 32)
	var writers sync.WaitGroup
	writers.Add(8)
	for i := 0; i < 8; i++ {
		go func(inode Ino) {
			defer writers.Done()
			for j := 0; j < 500; j++ {
				s.schedule(inode, uint32(j%8), j, j%3)
			}
		}(Ino(i + 1))
	}
	s.close()
	writers.Wait()
	s.workers.Wait()
	if s.schedule(1, 0, 10, 0) {
		t.Fatal("close failed to reject concurrent hints")
	}
}

// TestCompactionSchedulerWaitJoinsActiveCallbacks protects metadata shutdown ordering.
func TestCompactionSchedulerWaitJoinsActiveCallbacks(t *testing.T) {
	calls := make(chan schedulerCall, 1)
	gate := make(chan struct{})
	var once sync.Once
	s := newCompactionScheduler(func(i Ino, c uint32, tier int) { calls <- schedulerCall{i, c, tier}; <-gate }, 1, 1)
	defer s.close()
	defer once.Do(func() { close(gate) })
	s.schedule(1, 0, 10, 0)
	receiveSchedulerCall(t, calls)
	s.close()
	joined := make(chan struct{})
	go func() { s.wait(); close(joined) }()
	select {
	case <-joined:
		t.Fatal("wait returned before the storage callback finished")
	case <-time.After(20 * time.Millisecond):
	}
	once.Do(func() { close(gate) })
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("wait did not join completed callback")
	}
}
