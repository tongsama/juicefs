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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestKazCompactionPolicyConfig checks the ranges, the zero-value defaults and the SelfCheck fallback.
func TestKazCompactionPolicyConfig(t *testing.T) {
	for _, bound := range []int{kazCompactionMaxReadMin, kazCompactionMaxForce} {
		if bound != maxSlices-1 {
			t.Fatalf("policy bound %d must stay just below maxSlices %d", bound, maxSlices)
		}
	}
	var zero Config
	if err := zero.ValidateKazCompactionPolicy(); err != nil {
		t.Fatalf("zero config rejected: %v", err)
	}
	if r, cd, f := zero.kazCompactionSettings(); r != 5 || cd != 0 || f != 1000 {
		t.Fatalf("zero settings = %d/%s/%d, want 5/0s/1000", r, cd, f)
	}
	if zero.kazCompactionPolicyEnabled() {
		t.Fatal("zero config must keep the upstream behavior")
	}
	for _, c := range []Config{
		{KazCompactionReadMin: 5}, {KazCompactionReadMin: 2499},
		{KazCompactionCooldown: time.Second}, {KazCompactionCooldown: time.Hour},
		{KazCompactionForceSlices: 100}, {KazCompactionForceSlices: 2499},
	} {
		if err := c.ValidateKazCompactionPolicy(); err != nil {
			t.Errorf("%+v rejected: %v", c, err)
		}
	}
	for _, c := range []Config{
		{KazCompactionReadMin: -1}, {KazCompactionReadMin: 4}, {KazCompactionReadMin: 2500},
		{KazCompactionCooldown: -time.Second}, {KazCompactionCooldown: 999 * time.Millisecond}, {KazCompactionCooldown: time.Hour + time.Second},
		{KazCompactionForceSlices: -1}, {KazCompactionForceSlices: 99}, {KazCompactionForceSlices: 2500},
	} {
		if err := c.ValidateKazCompactionPolicy(); err == nil {
			t.Errorf("%+v accepted", c)
		}
	}
	for _, tc := range []struct {
		c    Config
		want bool
	}{
		{Config{KazCompactionReadMin: 5}, false},
		{Config{KazCompactionReadMin: 100}, true},
		{Config{KazCompactionCooldown: time.Minute}, true},
		{Config{KazCompactionForceSlices: 700}, false},
	} {
		if got := tc.c.kazCompactionPolicyEnabled(); got != tc.want {
			t.Errorf("%+v enabled=%v, want %v", tc.c, got, tc.want)
		}
	}
	bad := Config{KazCompactionReadMin: 3, KazCompactionCooldown: time.Minute, KazCompactionForceSlices: 700, MaxDeletes: 1}
	bad.SelfCheck()
	if bad.KazCompactionReadMin != 0 || bad.KazCompactionCooldown != 0 || bad.KazCompactionForceSlices != 0 {
		t.Fatalf("SelfCheck kept invalid policy: %+v", bad)
	}
}

// policyRecorder collects forwarded requests in order.
type policyRecorder struct {
	mu  sync.Mutex
	got []string
}

// forward records one request as "inode/chunk/count/tier".
func (r *policyRecorder) forward(inode Ino, chunk uint32, count, tier int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.got = append(r.got, fmt.Sprintf("%d/%d/%d/%d", inode, chunk, count, tier))
}

// take returns and clears the recorded requests.
func (r *policyRecorder) take() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	got := r.got
	r.got = nil
	return got
}

// fakeClock is a manually advanced clock for the policy.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

// now returns the current fake time.
func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

// add advances the fake time.
func (c *fakeClock) add(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// newTestPolicy builds a policy with a fake clock, a recorder and fresh metrics; it does not start the patrol.
func newTestPolicy(readMin int, cooldown time.Duration, force int) (*compactionPolicy, *policyRecorder, *fakeClock, *kazCompactionMetrics) {
	rec := &policyRecorder{}
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	m := newKazCompactionMetrics()
	p := newCompactionPolicy(readMin, cooldown, force, rec.forward, m)
	p.now = clk.now
	return p, rec, clk, m
}

// policyEvents reads one event counter.
func policyEvents(m *kazCompactionMetrics, name string) float64 {
	return testutil.ToFloat64(m.events.WithLabelValues(name))
}

// expectForwarded fails unless exactly want was forwarded since the last call.
func expectForwarded(t *testing.T, rec *policyRecorder, want ...string) {
	t.Helper()
	got := rec.take()
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("forwarded %v, want %v", got, want)
	}
}

// TestCompactionPolicyReadMin drops small read requests but never commit requests.
func TestCompactionPolicyReadMin(t *testing.T) {
	p, rec, _, m := newTestPolicy(100, 0, 1000)
	p.request(1, 0, 99, 2, true)
	expectForwarded(t, rec)
	p.request(1, 0, 100, 2, true)
	p.request(1, 1, 6, 2, false)
	expectForwarded(t, rec, "1/0/100/2", "1/1/6/2")
	if policyEvents(m, "read_below_min") != 1 || policyEvents(m, "pass") != 2 {
		t.Fatalf("events below=%v pass=%v", policyEvents(m, "read_below_min"), policyEvents(m, "pass"))
	}
}

// TestCompactionPolicyForceBelowReadMin pins the order: the read minimum applies before force.
func TestCompactionPolicyForceBelowReadMin(t *testing.T) {
	p, rec, _, _ := newTestPolicy(500, time.Minute, 100)
	p.onCommit(1, 0, 300)
	p.request(1, 0, 300, 0, true)
	expectForwarded(t, rec)
	p.request(1, 0, 300, 0, false)
	expectForwarded(t, rec, "1/0/300/0")
}

// TestCompactionPolicyCooldownHoldsBusyChunk holds a recently written chunk until it stays idle for the cooldown.
func TestCompactionPolicyCooldownHoldsBusyChunk(t *testing.T) {
	p, rec, clk, m := newTestPolicy(5, time.Minute, 700)
	p.request(9, 0, 99, 1, false) // never written here (or by another client): passes
	expectForwarded(t, rec, "9/0/99/1")
	p.onCommit(1, 0, 99)
	p.request(1, 0, 99, 3, false)
	expectForwarded(t, rec)
	if policyEvents(m, "deferred") != 1 || testutil.ToFloat64(m.deferred) != 1 {
		t.Fatalf("deferred=%v gauge=%v", policyEvents(m, "deferred"), testutil.ToFloat64(m.deferred))
	}
	clk.add(30 * time.Second)
	p.onCommit(1, 0, 120)
	p.patrol()
	clk.add(59 * time.Second)
	p.patrol()
	expectForwarded(t, rec)
	clk.add(time.Second)
	p.patrol()
	expectForwarded(t, rec, "1/0/120/3")
	p.patrol()
	expectForwarded(t, rec)
	if policyEvents(m, "released_idle") != 1 || testutil.ToFloat64(m.deferred) != 0 {
		t.Fatalf("released=%v gauge=%v", policyEvents(m, "released_idle"), testutil.ToFloat64(m.deferred))
	}
}

// TestCompactionPolicyHeldRequestUpdates keeps one entry per chunk with the largest count and the latest tier.
func TestCompactionPolicyHeldRequestUpdates(t *testing.T) {
	p, rec, clk, m := newTestPolicy(5, time.Minute, 700)
	p.onCommit(1, 0, 10)
	p.request(1, 0, 400, 1, false)
	p.request(1, 0, 360, 2, true)
	expectForwarded(t, rec)
	if policyEvents(m, "deferred") != 1 {
		t.Fatalf("deferred=%v, want 1", policyEvents(m, "deferred"))
	}
	clk.add(time.Minute)
	p.patrol()
	expectForwarded(t, rec, "1/0/400/2")
}

// TestCompactionPolicyForce passes at the force threshold, on a request or on a commit.
func TestCompactionPolicyForce(t *testing.T) {
	p, rec, _, m := newTestPolicy(5, time.Minute, 700)
	p.onCommit(1, 0, 350)
	p.request(1, 0, 350, 4, false)
	p.onCommit(1, 0, 699)
	expectForwarded(t, rec)
	p.onCommit(1, 0, 700)
	expectForwarded(t, rec, "1/0/700/4")
	if policyEvents(m, "released_forced") != 1 {
		t.Fatalf("released_forced=%v", policyEvents(m, "released_forced"))
	}
	p.onCommit(2, 0, 10)
	p.request(2, 0, 700, 0, false)
	expectForwarded(t, rec, "2/0/700/0")
}

// TestCompactionPolicyCapacity falls back to the upstream behavior when its maps are full.
func TestCompactionPolicyCapacity(t *testing.T) {
	p, rec, _, m := newTestPolicy(5, time.Minute, 700)
	for i := 0; i < kazCompactionMaxDeferred; i++ {
		p.onCommit(Ino(i+1), 0, 99)
		p.request(Ino(i+1), 0, 99, 0, false)
	}
	expectForwarded(t, rec)
	p.onCommit(100000, 0, 99)
	p.request(100000, 0, 99, 0, false)
	expectForwarded(t, rec, "100000/0/99/0")
	if policyEvents(m, "overflow") != 1 {
		t.Fatalf("overflow=%v", policyEvents(m, "overflow"))
	}
	for i := 0; i < kazCompactionMaxTracked; i++ {
		p.onCommit(Ino(200000+i), 0, 1)
	}
	p.onCommit(999999, 0, 99) // not tracked: the map is full
	p.request(999999, 0, 99, 0, false)
	expectForwarded(t, rec, "999999/0/99/0")
}

// TestCompactionPolicyCloseDiscards drops held work on close and ignores later calls.
func TestCompactionPolicyCloseDiscards(t *testing.T) {
	p, rec, clk, m := newTestPolicy(100, time.Minute, 700)
	p.onCommit(1, 0, 99)
	p.request(1, 0, 99, 0, false)
	p.onCommit(2, 0, 99)
	p.request(2, 0, 99, 0, false)
	p.close()
	p.close()
	if policyEvents(m, "discarded_on_stop") != 2 || testutil.ToFloat64(m.deferred) != 0 {
		t.Fatalf("discarded=%v gauge=%v", policyEvents(m, "discarded_on_stop"), testutil.ToFloat64(m.deferred))
	}
	clk.add(time.Hour)
	p.request(3, 0, 200, 0, true)
	p.request(3, 0, 200, 0, false)
	p.onCommit(1, 0, 900)
	p.patrol()
	expectForwarded(t, rec)
}

// TestCompactionPolicyPatrolGoroutine releases an idle chunk from the real patrol and stops it on close.
func TestCompactionPolicyPatrolGoroutine(t *testing.T) {
	rec := &policyRecorder{}
	p := newCompactionPolicy(5, time.Second, 700, rec.forward, nil)
	p.start()
	p.onCommit(1, 0, 99)
	p.request(1, 0, 99, 0, false)
	deadline := time.Now().Add(5 * time.Second)
	for {
		rec.mu.Lock()
		n := len(rec.got)
		rec.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("patrol did not release the idle chunk")
		}
		time.Sleep(50 * time.Millisecond)
	}
	done := make(chan struct{})
	go func() { p.close(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("close did not stop the patrol")
	}
}

// TestCompactionPolicyConcurrent exercises requests, commits and patrols together for the race detector.
func TestCompactionPolicyConcurrent(t *testing.T) {
	p, _, clk, _ := newTestPolicy(50, time.Minute, 300)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				ino, chunk := Ino(g%3+1), uint32(i%7)
				switch i % 4 {
				case 0:
					p.onCommit(ino, chunk, i%400)
				case 1:
					p.request(ino, chunk, i%400, g, false)
				case 2:
					p.request(ino, chunk, i%400, g, true)
				default:
					clk.add(time.Second)
					p.patrol()
				}
			}
		}(g)
	}
	wg.Wait()
	p.close()
}
