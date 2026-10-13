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

	"github.com/prometheus/client_golang/prometheus/testutil"
)

// TestKazCompactionPolicyConfig checks the read-minimum range, the zero-value default and the SelfCheck fallback.
func TestKazCompactionPolicyConfig(t *testing.T) {
	if kazCompactionMaxReadMin != maxSlices-1 {
		t.Fatalf("read-min bound %d must stay just below maxSlices %d", kazCompactionMaxReadMin, maxSlices)
	}
	var zero Config
	if err := zero.ValidateKazCompactionPolicy(); err != nil {
		t.Fatalf("zero config rejected: %v", err)
	}
	if got := zero.kazCompactionReadMin(); got != 5 {
		t.Fatalf("zero read-min = %d, want 5", got)
	}
	if zero.kazCompactionPolicyEnabled() {
		t.Fatal("zero config must keep the upstream behavior")
	}
	for _, v := range []int{5, 100, 2499} {
		c := Config{KazCompactionReadMin: v}
		if err := c.ValidateKazCompactionPolicy(); err != nil {
			t.Errorf("read-min %d rejected: %v", v, err)
		}
		if got, want := c.kazCompactionPolicyEnabled(), v != 5; got != want {
			t.Errorf("read-min %d enabled=%v, want %v", v, got, want)
		}
	}
	for _, v := range []int{-1, 4, 2500} {
		c := Config{KazCompactionReadMin: v}
		if err := c.ValidateKazCompactionPolicy(); err == nil {
			t.Errorf("read-min %d accepted", v)
		}
	}
	bad := Config{KazCompactionReadMin: 3, MaxDeletes: 1}
	bad.SelfCheck()
	if bad.KazCompactionReadMin != 0 {
		t.Fatalf("SelfCheck kept invalid read-min: %+v", bad)
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

// policyEvents reads one event counter.
func policyEvents(m *kazCompactionMetrics, name string) float64 {
	return testutil.ToFloat64(m.events.WithLabelValues(name))
}

// TestCompactionPolicyReadMin drops read requests below the minimum and forwards the rest unchanged.
func TestCompactionPolicyReadMin(t *testing.T) {
	rec := &policyRecorder{}
	m := newKazCompactionMetrics()
	p := newCompactionPolicy(100, rec.forward, m)
	p.requestFromRead(1, 0, 99, 2)
	if got := rec.take(); len(got) != 0 {
		t.Fatalf("forwarded %v below the minimum", got)
	}
	p.requestFromRead(1, 0, 100, 2)
	p.requestFromRead(1, 3, 2400, 1)
	if got := fmt.Sprint(rec.take()); got != "[1/0/100/2 1/3/2400/1]" {
		t.Fatalf("forwarded %s", got)
	}
	if policyEvents(m, "read_below_min") != 1 || policyEvents(m, "pass") != 2 {
		t.Fatalf("events below=%v pass=%v", policyEvents(m, "read_below_min"), policyEvents(m, "pass"))
	}
}

// TestCompactionPolicyConcurrent checks that the filter is safe without locks.
func TestCompactionPolicyConcurrent(t *testing.T) {
	rec := &policyRecorder{}
	p := newCompactionPolicy(50, rec.forward, newKazCompactionMetrics())
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				p.requestFromRead(Ino(g+1), uint32(i%7), i%100, g)
			}
		}(g)
	}
	wg.Wait()
	if got := len(rec.take()); got != 8*500 {
		t.Fatalf("forwarded %d, want %d", got, 8*500)
	}
}
