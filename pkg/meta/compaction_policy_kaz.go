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
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Defaults and bounds of the kaz compaction policy. The defaults keep the
// upstream behavior: a read requests compaction from 5 slices and nothing is held.
// The upper bounds stay below maxSlices (2500, a variable in base.go), where
// writes compact synchronously; TestKazCompactionPolicyConfig pins the relation.
const (
	kazCompactionDefaultReadMin = 5
	kazCompactionMaxReadMin     = 2499
	kazCompactionDefaultForce   = 1000
	kazCompactionMinForce       = 100
	kazCompactionMaxForce       = 2499
	kazCompactionMinCooldown    = time.Second
	kazCompactionMaxCooldown    = time.Hour
	kazCompactionMaxDeferred    = 1024
	kazCompactionMaxTracked     = 4096
)

// ValidateKazCompactionPolicy rejects policy knobs outside their documented ranges.
// Zero values mean "use the default", so Go callers that never set them keep upstream behavior.
func (c *Config) ValidateKazCompactionPolicy() error {
	if v := c.KazCompactionReadMin; v != 0 && (v < kazCompactionDefaultReadMin || v > kazCompactionMaxReadMin) {
		return fmt.Errorf("invalid kaz-compaction-read-min %d (want %d..%d)", v, kazCompactionDefaultReadMin, kazCompactionMaxReadMin)
	}
	if v := c.KazCompactionCooldown; v != 0 && (v < kazCompactionMinCooldown || v > kazCompactionMaxCooldown) {
		return fmt.Errorf("invalid kaz-compaction-cooldown %s (want 0 or %s..%s)", v, kazCompactionMinCooldown, kazCompactionMaxCooldown)
	}
	if v := c.KazCompactionForceSlices; v != 0 && (v < kazCompactionMinForce || v > kazCompactionMaxForce) {
		return fmt.Errorf("invalid kaz-compaction-force-slices %d (want %d..%d)", v, kazCompactionMinForce, kazCompactionMaxForce)
	}
	return nil
}

// kazCompactionSettings returns the effective read minimum, cooldown and force threshold.
func (c *Config) kazCompactionSettings() (readMin int, cooldown time.Duration, force int) {
	readMin, cooldown, force = c.KazCompactionReadMin, c.KazCompactionCooldown, c.KazCompactionForceSlices
	if readMin == 0 {
		readMin = kazCompactionDefaultReadMin
	}
	if force == 0 {
		force = kazCompactionDefaultForce
	}
	return
}

// kazCompactionPolicyEnabled reports whether any knob changes the upstream behavior.
func (c *Config) kazCompactionPolicyEnabled() bool {
	readMin, cooldown, _ := c.kazCompactionSettings()
	return readMin != kazCompactionDefaultReadMin || cooldown > 0
}

// kazCompactionMetrics groups the observations exported for the kaz compaction policy.
type kazCompactionMetrics struct {
	events    *prometheus.CounterVec
	deferred  prometheus.Gauge
	queueWait prometheus.Histogram
	slices    prometheus.Histogram
}

// newKazCompactionMetrics creates unregistered collectors; register attaches them.
func newKazCompactionMetrics() *kazCompactionMetrics {
	return &kazCompactionMetrics{
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_compaction_policy_events_total",
			Help: "Decisions of the kaz compaction policy: read_below_min, pass, deferred, released_idle, released_forced, overflow, discarded_on_stop.",
		}, []string{"event"}),
		deferred: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kaz_compaction_policy_deferred",
			Help: "Chunks whose background compaction is currently held by the kaz compaction policy.",
		}),
		queueWait: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kaz_compaction_queue_wait_seconds",
			Help:    "Time from queuing a chunk in the priority compaction scheduler to the start of its compaction.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 18),
		}),
		slices: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "kaz_compaction_scheduled_slices",
			Help:    "Slice count recorded in the priority scheduler hint when its compaction starts.",
			Buckets: []float64{5, 10, 20, 50, 100, 200, 350, 500, 700, 1000, 1500, 2000, 2500},
		}),
	}
}

// register attaches the collectors to reg.
func (k *kazCompactionMetrics) register(reg prometheus.Registerer) {
	reg.MustRegister(k.events, k.deferred, k.queueWait, k.slices)
}

// observeStart records one compaction start taken from the priority scheduler.
func (k *kazCompactionMetrics) observeStart(wait time.Duration, count int) {
	k.queueWait.Observe(wait.Seconds())
	k.slices.Observe(float64(count))
}

// heldCompaction is one background compaction request held by the policy.
type heldCompaction struct {
	count, tier int
	since       time.Time
}

// compactionPolicy decides when advisory background compaction requests reach
// the scheduler. It never touches metadata: dropping or holding a request only
// leaves more slices in a chunk until a later request, commit or patrol.
type compactionPolicy struct {
	readMin  int
	cooldown time.Duration
	force    int
	forward  func(inode Ino, chunk uint32, count, tier int)
	now      func() time.Time
	metrics  *kazCompactionMetrics

	mu         sync.Mutex
	closed     bool
	started    bool
	held       map[compactionKey]*heldCompaction
	lastCommit map[compactionKey]time.Time
	stopCh     chan struct{}
	done       chan struct{}
}

// newCompactionPolicy builds a policy; start launches its patrol when a cooldown is set.
func newCompactionPolicy(readMin int, cooldown time.Duration, force int, forward func(inode Ino, chunk uint32, count, tier int), metrics *kazCompactionMetrics) *compactionPolicy {
	return &compactionPolicy{
		readMin: readMin, cooldown: cooldown, force: force, forward: forward, now: time.Now, metrics: metrics,
		held:       make(map[compactionKey]*heldCompaction),
		lastCommit: make(map[compactionKey]time.Time),
		stopCh:     make(chan struct{}),
		done:       make(chan struct{}),
	}
}

// event counts n decisions when metrics are attached.
func (p *compactionPolicy) event(name string, n int) {
	if p.metrics != nil && n > 0 {
		p.metrics.events.WithLabelValues(name).Add(float64(n))
	}
}

// heldChanged publishes the number of held chunks; the caller holds mu.
func (p *compactionPolicy) heldChanged() {
	if p.metrics != nil {
		p.metrics.deferred.Set(float64(len(p.held)))
	}
}

// pass forwards one request; callers must not hold mu. forward is read under mu
// so tests may swap it while the patrol goroutine runs.
func (p *compactionPolicy) pass(event string, inode Ino, chunk uint32, count, tier int) {
	p.mu.Lock()
	forward := p.forward
	p.mu.Unlock()
	p.event(event, 1)
	forward(inode, chunk, count, tier)
}

// request handles one compaction request; fromRead applies the read minimum first.
func (p *compactionPolicy) request(inode Ino, chunk uint32, count, tier int, fromRead bool) {
	if fromRead && count < p.readMin {
		p.event("read_below_min", 1)
		return
	}
	key := compactionKey{inode, chunk}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if p.cooldown <= 0 {
		p.mu.Unlock()
		p.pass("pass", inode, chunk, count, tier)
		return
	}
	held := p.held[key]
	if held != nil && held.count > count {
		count = held.count
	}
	last, tracked := p.lastCommit[key]
	if count >= p.force || !tracked || p.now().Sub(last) >= p.cooldown {
		if held != nil {
			delete(p.held, key)
			p.heldChanged()
		}
		p.mu.Unlock()
		p.pass("pass", inode, chunk, count, tier)
		return
	}
	if held != nil {
		held.count, held.tier = count, tier
		p.mu.Unlock()
		return
	}
	if len(p.held) >= kazCompactionMaxDeferred {
		p.mu.Unlock()
		p.pass("overflow", inode, chunk, count, tier)
		return
	}
	p.held[key] = &heldCompaction{count: count, tier: tier, since: p.now()}
	p.heldChanged()
	p.mu.Unlock()
	p.event("deferred", 1)
	logger.Debugf("kaz compaction hold inode=%d chunk=%d slices=%d", inode, chunk, count)
}

// onCommit records a commit to the chunk and releases its held request at the force threshold.
func (p *compactionPolicy) onCommit(inode Ino, chunk uint32, count int) {
	if p.cooldown <= 0 {
		return
	}
	key := compactionKey{inode, chunk}
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	if _, ok := p.lastCommit[key]; ok || len(p.lastCommit) < kazCompactionMaxTracked {
		p.lastCommit[key] = p.now()
	}
	held := p.held[key]
	if held == nil {
		p.mu.Unlock()
		return
	}
	if count > held.count {
		held.count = count
	}
	if held.count < p.force {
		p.mu.Unlock()
		return
	}
	h := *held
	delete(p.held, key)
	p.heldChanged()
	waited := p.now().Sub(h.since)
	p.mu.Unlock()
	logger.Debugf("kaz compaction force inode=%d chunk=%d slices=%d held=%s", inode, chunk, h.count, waited)
	p.pass("released_forced", inode, chunk, h.count, h.tier)
}

// patrol forgets chunks idle for the cooldown and releases their held requests.
func (p *compactionPolicy) patrol() {
	type release struct {
		key  compactionKey
		held heldCompaction
	}
	var out []release
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	now := p.now()
	for key, last := range p.lastCommit {
		if now.Sub(last) >= p.cooldown {
			delete(p.lastCommit, key)
		}
	}
	for key, held := range p.held {
		if _, busy := p.lastCommit[key]; !busy {
			out = append(out, release{key, *held})
			delete(p.held, key)
		}
	}
	if len(out) > 0 {
		p.heldChanged()
	}
	p.mu.Unlock()
	for _, r := range out {
		logger.Debugf("kaz compaction release inode=%d chunk=%d slices=%d held=%s", r.key.inode, r.key.chunk, r.held.count, now.Sub(r.held.since))
		p.pass("released_idle", r.key.inode, r.key.chunk, r.held.count, r.held.tier)
	}
}

// patrolInterval returns a quarter of the cooldown, kept between 1s and 5s.
func (p *compactionPolicy) patrolInterval() time.Duration {
	return min(max(p.cooldown/4, time.Second), 5*time.Second)
}

// start launches the patrol goroutine when a cooldown is configured.
func (p *compactionPolicy) start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cooldown <= 0 || p.started || p.closed {
		return
	}
	p.started = true
	go func() {
		defer close(p.done)
		ticker := time.NewTicker(p.patrolInterval())
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.patrol()
			}
		}
	}()
}

// close discards held requests, stops the patrol and makes later calls no-ops.
func (p *compactionPolicy) close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	discarded := len(p.held)
	p.held = make(map[compactionKey]*heldCompaction)
	p.lastCommit = make(map[compactionKey]time.Time)
	p.heldChanged()
	started := p.started
	p.mu.Unlock()
	p.event("discarded_on_stop", discarded)
	close(p.stopCh)
	if started {
		<-p.done
	}
}
