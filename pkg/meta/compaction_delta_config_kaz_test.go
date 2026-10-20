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
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKazDeltaConfigDefaults checks that zero knobs resolve to the documented defaults.
func TestKazDeltaConfigDefaults(t *testing.T) {
	c := Config{KazCompactionMode: "delta"}
	require.True(t, c.kazDeltaEnabled())
	require.Equal(t, kazDeltaConf{tiers: 8, maxRecords: 500, trigger: 99, gap: 16384, hiddenRatio: 0.5, minFileSize: 0}, c.kazDeltaConf())
	require.False(t, (&Config{KazCompactionMode: "block"}).kazDeltaEnabled())
}

// TestKazDeltaConfigValidate checks the range of every delta knob and the mode name.
func TestKazDeltaConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    Config
		ok   bool
	}{
		{"tiers 0", Config{KazCompactionMode: "delta"}, true},
		{"tiers 1", Config{KazCompactionMode: "delta", KazCompactionDeltaTiers: 1}, true},
		{"tiers 16", Config{KazCompactionMode: "delta", KazCompactionDeltaTiers: 16}, true},
		{"tiers 17", Config{KazCompactionMode: "delta", KazCompactionDeltaTiers: 17}, false},
		{"tiers -1", Config{KazCompactionMode: "delta", KazCompactionDeltaTiers: -1}, false},
		{"records 50", Config{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 50}, true},
		{"records 800", Config{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800}, true},
		{"records 49", Config{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 49}, false},
		{"records 801", Config{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 801}, false},
		{"gap 0", Config{KazCompactionMode: "delta", KazCompactionDeltaGap: 0}, true},
		{"gap 1M", Config{KazCompactionMode: "delta", KazCompactionDeltaGap: 1 << 20}, true},
		{"gap 1M+1", Config{KazCompactionMode: "delta", KazCompactionDeltaGap: 1<<20 + 1}, false},
		{"gap -1 (no joining)", Config{KazCompactionMode: "delta", KazCompactionDeltaGap: -1}, true},
		{"gap -2", Config{KazCompactionMode: "delta", KazCompactionDeltaGap: -2}, false},
		{"mode Delta", Config{KazCompactionMode: "Delta"}, false},
		{"block ignores delta knobs", Config{KazCompactionMode: "block", KazCompactionDeltaTiers: 99}, true},
	} {
		err := tc.c.ValidateKazBlockCompaction()
		if tc.ok {
			require.NoError(t, err, tc.name)
		} else {
			require.Error(t, err, tc.name)
		}
	}
}

// TestKazDeltaConfigSelfCheckResets checks that an invalid delta knob falls back to upstream and clears the knobs.
func TestKazDeltaConfigSelfCheckResets(t *testing.T) {
	c := Config{KazCompactionMode: "delta", KazCompactionDeltaTiers: 99, KazCompactionDeltaMaxRecords: 300, KazCompactionDeltaGap: 5, KazCompactionDeltaMinFileSize: 7}
	c.SelfCheck()
	require.Equal(t, "", c.KazCompactionMode)
	require.Zero(t, c.KazCompactionDeltaTiers)
	require.Zero(t, c.KazCompactionDeltaMaxRecords)
	require.Zero(t, c.KazCompactionDeltaGap)
	require.Zero(t, c.KazCompactionDeltaMinFileSize)
}

// TestKazDeltaConfigGapNoJoin checks that Config gap -1 resolves to an effective gap of 0 and 0 to the default.
func TestKazDeltaConfigGapNoJoin(t *testing.T) {
	require.Equal(t, uint32(0), (&Config{KazCompactionMode: "delta", KazCompactionDeltaGap: -1}).kazDeltaConf().gap)
	require.Equal(t, uint32(16384), (&Config{KazCompactionMode: "delta"}).kazDeltaConf().gap)
	require.Equal(t, uint32(4096), (&Config{KazCompactionMode: "delta", KazCompactionDeltaGap: 4096}).kazDeltaConf().gap)
}
