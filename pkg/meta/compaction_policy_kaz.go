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
	"time"
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
