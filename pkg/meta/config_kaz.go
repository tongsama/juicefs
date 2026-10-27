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
