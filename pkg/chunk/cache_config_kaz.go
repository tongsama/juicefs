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

import "time"

// Values of Config.KazCacheCompactionPlacement (--kaz-cache-compaction-placement),
// which selects the kaz-2q segment of a cached compaction output block.
const (
	// KazPlacementInherit puts the output in protected when any cached source
	// block is protected, else in probation (the original rule).
	KazPlacementInherit = "inherit"
	// KazPlacementProbation always puts the output in probation.
	KazPlacementProbation = "probation"
	// KazPlacementReplace puts the output in protected only when a protected
	// source block was copied whole into it, else in probation.
	KazPlacementReplace = "replace"
	// KazPlacementShare puts the output in protected when the bytes copied
	// from protected source blocks are at least
	// Config.KazCacheCompactionPlacementShareRatio of its non-hole bytes,
	// else in probation.
	KazPlacementShare = "share"
)

// kazDefaultShareRatio is the default (and fallback) of
// Config.KazCacheCompactionPlacementShareRatio.
const kazDefaultShareRatio = 0.5

// kazCacheSelfCheck normalizes the kaz-2q options, warning and falling back to
// defaults for out-of-range values (an empty compaction placement silently
// becomes inherit, an unknown one warns and becomes inherit; the share ratio
// is checked only for the share placement, which is the only one using it).
// It never changes other eviction settings.
func (c *Config) kazCacheSelfCheck() {
	if c.KazCacheProbationRatio <= 0 || c.KazCacheProbationRatio >= 1 {
		logger.Warnf("kaz-cache-probation-ratio should be in (0, 1), setting it to 0.4")
		c.KazCacheProbationRatio = 0.4
	}
	if c.KazCachePromoteDelay < 0 {
		logger.Warnf("kaz-cache-promote-delay should not be negative, setting it to 10m")
		c.KazCachePromoteDelay = 10 * time.Minute
	}
	if c.KazCacheGhostRatio < 0 {
		logger.Warnf("kaz-cache-ghost-ratio should not be negative, setting it to 4")
		c.KazCacheGhostRatio = 4
	}
	if c.KazCacheStateInterval <= 0 {
		logger.Warnf("kaz-cache-state-interval should be positive, setting it to 5m")
		c.KazCacheStateInterval = 5 * time.Minute
	}
	switch c.KazCacheCompactionPlacement {
	case KazPlacementInherit, KazPlacementProbation, KazPlacementReplace, KazPlacementShare:
	case "":
		c.KazCacheCompactionPlacement = KazPlacementInherit
	default:
		logger.Warnf("kaz-cache-compaction-placement should be one of [%s, %s, %s, %s], setting it to %s",
			KazPlacementInherit, KazPlacementProbation, KazPlacementReplace, KazPlacementShare, KazPlacementInherit)
		c.KazCacheCompactionPlacement = KazPlacementInherit
	}
	if c.KazCacheCompactionPlacement == KazPlacementShare && !kazShareRatioValid(c.KazCacheCompactionPlacementShareRatio) {
		logger.Warnf("kaz-cache-compaction-placement-share-ratio should be in (0, 1], setting it to %v", kazDefaultShareRatio)
		c.KazCacheCompactionPlacementShareRatio = kazDefaultShareRatio
	}
}

// kazShareRatioValid reports whether r is a valid share ratio, in (0, 1].
func kazShareRatioValid(r float64) bool {
	return r > 0 && r <= 1
}
