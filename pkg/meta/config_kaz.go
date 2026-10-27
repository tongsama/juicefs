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
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

// KazMetaConfig holds the metadata settings added by kaz. It is embedded at the
// end of Config without a JSON tag, so its fields keep their names and stay
// top-level keys of the meta section of .config.
type KazMetaConfig struct {
	CompactionGCMode    string // empty or legacy keeps synchronous notification; deferred requires recovery
	CompactionScheduler string // empty or legacy keeps opportunistic admission; priority uses bounded hints

	// [kaz] compaction policy; zero keeps the upstream trigger.
	// KazCompactionReadMin: when Read reloads a chunk's slice list from the engine
	// (open-file chunk cache miss), background compaction of that chunk is queued
	// only if it has at least this many slice records. Commit-triggered compaction
	// is unchanged. 0 means 5, the upstream threshold.
	KazCompactionReadMin int
	// [kaz] block compaction; "" or "upstream" keeps upstream compaction.
	// KazCompactionMode "block" rewrites only the blocks changed since the last
	// background compaction, one new slice per block. The two thresholds switch a
	// chunk back to a whole rewrite (still one slice per block); 0 means the default.
	KazCompactionMode             string
	KazCompactionBlockHiddenRatio float64
	KazCompactionBlockMaxSlices   int
	// [kaz] delta compaction knobs, used with KazCompactionMode "delta"; 0 means the default.
	// Tiers: max delta tiers (default 8); MaxRecords: chunk record limit before
	// folding (default 500); Gap: bytes, gaps up to this are joined (0 = default 16 KiB, -1 = no joining, other negatives invalid);
	// MinFileSize: bytes, shorter files use block mode (default 0, no limit);
	// Trigger: new records that start a compaction (default 99, 10..400);
	// BlockRatio: share of the block size a delta may reach before the block is
	// rewritten whole (default 0.5, 0 < ratio <= 1).
	KazCompactionDeltaTiers       int
	KazCompactionDeltaMaxRecords  int
	KazCompactionDeltaGap         int
	KazCompactionDeltaMinFileSize uint64
	KazCompactionDeltaTrigger     int
	KazCompactionDeltaBlockRatio  float64
}

// kazMetaState holds the baseMeta state added by kaz. It is embedded at the
// end of baseMeta; newBaseMeta sets the metrics in its literal and the
// pointers start nil. It holds atomics, so it must not be copied.
type kazMetaState struct {
	compactionGC       atomic.Pointer[compactionGC]
	priorityCompactor  atomic.Pointer[compactionScheduler]
	kazCompact         atomic.Pointer[compactionPolicy]
	kazDeltaBases      kazDeltaBaseTable // kaz: per-chunk base counts of delta compaction
	kazPacking         int               // staging packs in flight; they hold m.compacting entries too (guarded by baseMeta's mutex)
	compactionGCEvents *prometheus.CounterVec
	kazCompactMetrics  *kazCompactionMetrics
}

// newCompactionGCEvents creates the counter of compaction GC phase outcomes
// (registered by InitSharedMetrics).
func newCompactionGCEvents() *prometheus.CounterVec {
	return prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "compaction_gc_events_total",
		Help: "Compaction GC phase outcomes; local success counts callbacks, not removed files; deferred remote intent remains durable.",
	}, []string{"event"})
}

// kazSelfCheck validates the kaz settings, falling back to the upstream
// behavior and warning about modes that may disrupt the workload.
func (c *Config) kazSelfCheck() {
	if err := c.ValidateCompactionScheduler(); err != nil {
		logger.Warnf("%s; falling back to legacy compaction scheduler", err)
		c.CompactionScheduler = "legacy"
	}
	if err := c.ValidateCompactionGC(); err != nil {
		logger.Warnf("%s; falling back to legacy compaction GC", err)
		c.CompactionGCMode = "legacy"
	}
	if err := c.ValidateKazCompactionPolicy(); err != nil {
		logger.Warnf("%s; falling back to the upstream compaction triggers", err)
		c.KazCompactionReadMin = 0
	}
	if err := c.ValidateKazBlockCompaction(); err != nil {
		logger.Warnf("%s; falling back to upstream compaction", err)
		c.KazCompactionMode, c.KazCompactionBlockHiddenRatio, c.KazCompactionBlockMaxSlices = "", 0, 0
		c.KazCompactionDeltaTiers, c.KazCompactionDeltaMaxRecords, c.KazCompactionDeltaGap, c.KazCompactionDeltaMinFileSize = 0, 0, 0, 0
		c.KazCompactionDeltaTrigger, c.KazCompactionDeltaBlockRatio = 0, 0
	}
}
