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
	events *prometheus.CounterVec // promote, demote, evict_*, ghost_hit(_*), compaction_*, state_*
	hits   *prometheus.CounterVec // user cache hits by origin (legacy, write, fill, compaction)
	// bytesByOrigin splits bytes by the origin of the blocks (segment, origin).
	bytesByOrigin *prometheus.GaugeVec

	ghostBlocks prometheus.Gauge // keys in the evicted-key lists (2Q A1out)
	ghostBytes  prometheus.Gauge // bytes the evicted keys had when cached
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
		bytesByOrigin: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kaz_cache_2q_bytes_by_origin",
			Help: "[kaz] Bytes of cached blocks per 2Q segment and origin (legacy, write, fill, compaction; staging excluded).",
		}, []string{"segment", "origin"}),
		events: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_cache_2q_events_total",
			Help: "[kaz] 2Q cache events (promote, demote, evictions, ghost hits in total and by prior segment, compaction output, state file).",
		}, []string{"event"}),
		hits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "kaz_cache_2q_hits_total",
			Help: "[kaz] User cache hits by the origin of the block (legacy, write, fill, compaction).",
		}, []string{"origin"}),
		ghostBlocks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kaz_cache_2q_ghost_blocks",
			Help: "[kaz] Number of keys in the 2Q evicted-key list.",
		}),
		ghostBytes: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "kaz_cache_2q_ghost_bytes",
			Help: "[kaz] Bytes the keys in the 2Q evicted-key list had when cached.",
		}),
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
	reg.MustRegister(c.kaz2Q.bytesByOrigin)
	reg.MustRegister(c.kaz2Q.events)
	reg.MustRegister(c.kaz2Q.hits)
	reg.MustRegister(c.kaz2Q.ghostBlocks)
	reg.MustRegister(c.kaz2Q.ghostBytes)
}
