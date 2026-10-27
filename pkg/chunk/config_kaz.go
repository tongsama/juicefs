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
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// KazChunkConfig holds the chunk settings added by kaz. It is embedded in
// Config without a JSON tag, so its fields keep their names and stay
// top-level keys of the chunk section of .config.
type KazChunkConfig struct {
	StagingNoSync bool // skip fdatasync/directory fsync of staged writeback blocks (unsafe on OS crash)
	// FinishCanceledGet keeps a full-block GET running after its reader is canceled,
	// and stores the block in the disk cache instead of discarding the download.
	FinishCanceledGet bool
	// GetHeaderTimeout bounds only the wait for the response headers of a full-block GET
	// (0 disables it). GetTimeout still bounds the whole GET, headers and body together.
	GetHeaderTimeout time.Duration

	// kaz-2q cache eviction options (used only when CacheEviction is kaz-2q).
	// KazCacheProbationRatio is the share of the cache capacity for newly cached blocks (0 < x < 1).
	KazCacheProbationRatio float64
	// KazCachePromoteDelay is the correlated-reference period before a block can be promoted.
	KazCachePromoteDelay time.Duration
	// KazCacheGhostRatio is the evicted-key list capacity as a multiple of the cache capacity (0 disables).
	KazCacheGhostRatio float64
	// KazCacheStateInterval is the save interval of the kaz-cache-state.v1 file.
	KazCacheStateInterval time.Duration
	// KazCacheCompactionPlacement selects the segment of a compaction output (inherit, probation, replace, share).
	KazCacheCompactionPlacement string
	// KazCacheCompactionPlacementShareRatio is the protected share threshold of the share placement (0 < x <= 1).
	KazCacheCompactionPlacementShareRatio float64
	// KazCacheDemoteUnreadFirst demotes protected blocks without the read mark before those with it.
	KazCacheDemoteUnreadFirst bool
	// KazCacheCompactionHotAge keeps protected compaction outputs younger than this out of protected placement (0 disables).
	KazCacheCompactionHotAge time.Duration
	// KazWholeReadMax makes a cache-enabled read of a block of at most this many bytes fetch the whole object instead of a range (0 disables).
	KazWholeReadMax int
	// KazSkipUnuploadedDelete skips the object DELETE of a staged block that this mount never tried to PUT.
	KazSkipUnuploadedDelete bool
}

// kazStoreState holds the cachedStore state added by kaz. It is embedded in
// cachedStore and initialized by kazInitState; it holds locks, so it must not
// be copied.
type kazStoreState struct {
	activeStaging     map[string]int // Protected by pendingMutex; bounded by currentUpload slots.
	activeStageWrites map[string]int // Actual stage callbacks, including callbacks surviving their timeout.

	// detachedFetches bounds the GETs kept running for canceled readers (FinishCanceledGet).
	detachedFetches     chan struct{}
	canceledGetFinished prometheus.Counter
	canceledGetDropped  prometheus.Counter
	getHeaderTimeouts   prometheus.Counter

	// headerTimedOut remembers keys recently cut by GetHeaderTimeout (key -> time).
	headerTimedOutMu sync.Mutex
	headerTimedOut   map[string]time.Time

	// kazUnuploaded holds keys never PUT whose slice was retired; protected by pendingMutex.
	kazUnuploaded    map[string]struct{}
	kazDeleteSkipped prometheus.Counter
}

// kazInitState initializes the kaz state of a new store right after the
// upstream fields, as the cachedStore literal of NewCachedStore did.
func (store *cachedStore) kazInitState(config Config) {
	store.detachedFetches = make(chan struct{}, max(1, config.MaxDownload/2))
	store.activeStaging = make(map[string]int)
	store.activeStageWrites = make(map[string]int)
}
