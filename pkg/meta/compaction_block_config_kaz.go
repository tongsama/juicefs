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

import "fmt"

// Modes of --kaz-compaction-mode. Upstream is the default and keeps the
// upstream compaction of the whole merged range into one slice.
const (
	KazCompactionModeUpstream = "upstream"
	KazCompactionModeBlock    = "block"

	kazBlockDefaultHiddenRatio = 0.5
)

// ValidateKazBlockCompaction rejects an unknown mode or out-of-range thresholds.
// Zero thresholds mean "use the default", so Go callers that never set them keep defaults.
func (c *Config) ValidateKazBlockCompaction() error {
	switch c.KazCompactionMode {
	case "", KazCompactionModeUpstream, KazCompactionModeBlock, KazCompactionModeDelta:
	default:
		return fmt.Errorf("invalid kaz-compaction-mode %q (want upstream, block or delta)", c.KazCompactionMode)
	}
	if r := c.KazCompactionBlockHiddenRatio; r != 0 && !(r > 0 && r <= 1) {
		return fmt.Errorf("invalid kaz-compaction-block-hidden-ratio %v (want 0 < ratio <= 1)", r)
	}
	if c.KazCompactionBlockMaxSlices < 0 {
		return fmt.Errorf("invalid kaz-compaction-block-max-slices %d (want >= 0)", c.KazCompactionBlockMaxSlices)
	}
	return c.validateKazDelta()
}

// kazBlockCompactionEnabled reports whether background compaction should go
// through the kaz block replacer: block mode, or delta mode (which plans delta
// or block compaction inside runKazBlockCompaction).
func (c *Config) kazBlockCompactionEnabled() bool {
	return c.KazCompactionMode == KazCompactionModeBlock || c.KazCompactionMode == KazCompactionModeDelta
}

// kazBlockHiddenRatio returns the hidden-data share above which a chunk is rewritten whole.
func (c *Config) kazBlockHiddenRatio() float64 {
	if c.KazCompactionBlockHiddenRatio == 0 {
		return kazBlockDefaultHiddenRatio
	}
	return c.KazCompactionBlockHiddenRatio
}

// kazBlockMaxSlices returns the record limit after a partial rewrite; 0 means blocks+1.
func (c *Config) kazBlockMaxSlices(blocks int) int {
	if c.KazCompactionBlockMaxSlices == 0 {
		return blocks + 1
	}
	return c.KazCompactionBlockMaxSlices
}
