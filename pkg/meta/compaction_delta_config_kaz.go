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

// KazCompactionModeDelta packs scattered small changes over existing blocks
// into one small slice per compaction (see the kaz delta compaction design).
const KazCompactionModeDelta = "delta"

const (
	// Defaults used when the Config knob is 0.
	kazDeltaDefaultTiers      = 8
	kazDeltaDefaultMaxRecords = 500
	kazDeltaDefaultGap        = 16 << 10

	// Ranges accepted by validateKazDelta.
	kazDeltaMaxTiers      = 16
	kazDeltaMinMaxRecords = 50
	kazDeltaMaxMaxRecords = 800
	kazDeltaMaxGap        = 1 << 20

	// kazDeltaTrigger is the record count of a chunk that triggers delta compaction.
	kazDeltaTrigger = 99
	// kazDeltaFoldMargin is the headroom kept below the record limit before folding.
	kazDeltaFoldMargin = 50
	// kazDeltaBaseCap is the largest base slice size a delta tier may reference.
	kazDeltaBaseCap = 65536
)

// kazDeltaConf is the delta compaction setting with every default resolved.
type kazDeltaConf struct {
	tiers       int     // maximum number of delta tiers
	maxRecords  int     // record limit of a chunk before folding by block
	trigger     int     // record count that triggers compaction
	gap         uint32  // gaps up to this many bytes are joined into one delta
	hiddenRatio float64 // hidden share above which a base is rewritten whole
	minFileSize uint64  // files shorter than this use block mode
}

// kazDeltaEnabled reports whether background compaction should use the delta mode.
func (c *Config) kazDeltaEnabled() bool {
	return c.KazCompactionMode == KazCompactionModeDelta
}

// kazDeltaConf returns the delta settings with 0 replaced by the defaults;
// a gap of -1 means no joining (effective gap 0).
func (c *Config) kazDeltaConf() kazDeltaConf {
	d := kazDeltaConf{
		tiers:       c.KazCompactionDeltaTiers,
		maxRecords:  c.KazCompactionDeltaMaxRecords,
		trigger:     kazDeltaTrigger,
		gap:         uint32(c.KazCompactionDeltaGap),
		hiddenRatio: c.kazBlockHiddenRatio(),
		minFileSize: c.KazCompactionDeltaMinFileSize,
	}
	if d.tiers == 0 {
		d.tiers = kazDeltaDefaultTiers
	}
	if d.maxRecords == 0 {
		d.maxRecords = kazDeltaDefaultMaxRecords
	}
	switch c.KazCompactionDeltaGap {
	case 0:
		d.gap = kazDeltaDefaultGap
	case -1: // explicit "no joining"
		d.gap = 0
	}
	return d
}

// validateKazDelta rejects out-of-range delta knobs. Zero means "use the default".
// It checks nothing unless the mode is delta.
func (c *Config) validateKazDelta() error {
	if !c.kazDeltaEnabled() {
		return nil
	}
	if t := c.KazCompactionDeltaTiers; t < 0 || t > kazDeltaMaxTiers {
		return fmt.Errorf("invalid kaz-compaction-delta-tiers %d (want 1..%d)", t, kazDeltaMaxTiers)
	}
	if r := c.KazCompactionDeltaMaxRecords; r != 0 && (r < kazDeltaMinMaxRecords || r > kazDeltaMaxMaxRecords) {
		return fmt.Errorf("invalid kaz-compaction-delta-max-records %d (want %d..%d)", r, kazDeltaMinMaxRecords, kazDeltaMaxMaxRecords)
	}
	if g := c.KazCompactionDeltaGap; g < -1 || g > kazDeltaMaxGap {
		return fmt.Errorf("invalid kaz-compaction-delta-gap %d (want -1 (no joining), 0 (default) or 1..%d)", g, kazDeltaMaxGap)
	}
	return nil
}
