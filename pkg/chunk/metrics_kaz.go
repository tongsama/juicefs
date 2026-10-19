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
	"github.com/prometheus/client_golang/prometheus"
)

// kaz2QMetrics holds the collectors of the kaz-2q disk cache policy. The names
// carry no "juicefs_" prefix; the caller registers them with a prefixed
// registerer.
type kaz2QMetrics struct {
	blocks *prometheus.GaugeVec   // evictable blocks per segment (probation|protected)
	bytes  *prometheus.GaugeVec   // evictable bytes per segment
	events *prometheus.CounterVec // promote, demote, evict_*, ghost_hit, compaction_*, state_*
	hits   *prometheus.CounterVec // user cache hits by origin (legacy, write, fill, compaction)
}

// newKaz2QMetrics creates (but does not register) the kaz-2q collectors.
func newKaz2QMetrics() *kaz2QMetrics {
	return &kaz2QMetrics{
		blocks: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kaz_cache_2q_blocks",
			Help: "[kaz] Number of cached blocks per 2Q segment (staging excluded).",
		}, []string{"segment"}),
		bytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kaz_cache_2q_bytes",
			Help: "[kaz] Bytes of cached blocks per 2Q segment (staging excluded).",
		}, []string{"segment"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_cache_2q_events_total",
			Help: "[kaz] 2Q cache events (promote, demote, evictions, ghost hits, compaction output, state file).",
		}, []string{"event"}),
		hits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_cache_2q_hits_total",
			Help: "[kaz] User cache hits by the origin of the block (legacy, write, fill, compaction).",
		}, []string{"origin"}),
	}
}

// kazInitMetrics creates the kaz-2q collectors of a cache manager. They are
// shared by all cache stores of that manager.
func (c *cacheManagerMetrics) kazInitMetrics() {
	c.kaz2Q = newKaz2QMetrics()
}

// kazRegisterMetrics registers the kaz-2q collectors; reg must not be nil
// (the caller checks it, as for the other cache metrics).
func (c *cacheManagerMetrics) kazRegisterMetrics(reg prometheus.Registerer) {
	reg.MustRegister(c.kaz2Q.blocks)
	reg.MustRegister(c.kaz2Q.bytes)
	reg.MustRegister(c.kaz2Q.events)
	reg.MustRegister(c.kaz2Q.hits)
}
