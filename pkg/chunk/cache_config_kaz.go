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

// kazCacheSelfCheck normalizes the kaz-2q options, warning and falling back to
// defaults for out-of-range values. It never changes other eviction settings.
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
}
