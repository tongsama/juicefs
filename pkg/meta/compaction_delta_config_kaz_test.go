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
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestKazDeltaConfigDefaults checks that zero knobs resolve to the documented defaults.
func TestKazDeltaConfigDefaults(t *testing.T) {
	c := Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta"}}
	require.True(t, c.kazDeltaEnabled())
	require.Equal(t, kazDeltaConf{tiers: 8, maxRecords: 500, trigger: 99, blockRatio: 0.5, gap: 16384, hiddenRatio: 0.5, minFileSize: 0}, c.kazDeltaConf())
	require.False(t, (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "block"}}).kazDeltaEnabled())
}

// TestKazDeltaConfigValidate checks the range of every delta knob and the mode name.
func TestKazDeltaConfigValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		c    Config
		ok   bool
	}{
		{"tiers 0", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta"}}, true},
		{"tiers 1", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTiers: 1}}, true},
		{"tiers 16", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTiers: 16}}, true},
		{"tiers 17", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTiers: 17}}, false},
		{"tiers -1", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTiers: -1}}, false},
		{"records 50", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 50}}, true},
		{"records 800", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800}}, true},
		{"records 49", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 49}}, false},
		{"records 801", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 801}}, false},
		{"gap 0", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: 0}}, true},
		{"gap 1M", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: 1 << 20}}, true},
		{"gap 1M+1", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: 1<<20 + 1}}, false},
		{"gap -1 (no joining)", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: -1}}, true},
		{"gap -2", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: -2}}, false},
		{"trigger 10", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 10}}, true},
		{"trigger 400", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 400}}, true},
		{"trigger 9", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 9}}, false},
		{"trigger 401", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 401}}, false},
		{"trigger -1", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: -1}}, false},
		{"ratio 1", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: 1}}, true},
		{"ratio 0.01", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: 0.01}}, true},
		{"ratio 1.01", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: 1.01}}, false},
		{"ratio -0.5", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: -0.5}}, false},
		{"ratio NaN", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: math.NaN()}}, false},
		// max-records + trigger + fold margin (50) must fit in one compaction read (1000).
		{"records 800 + trigger 150", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800, KazCompactionDeltaTrigger: 150}}, true},
		{"records 800 + trigger 151", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800, KazCompactionDeltaTrigger: 151}}, false},
		{"records 800 + trigger 200", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800, KazCompactionDeltaTrigger: 200}}, false},
		{"records 0 (500) + trigger 400", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 400}}, true},
		{"records 800 + trigger 0 (99)", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800}}, true},
		{"mode Delta", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "Delta"}}, false},
		{"block ignores delta knobs", Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "block", KazCompactionDeltaTiers: 99}}, true},
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
	c := Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTiers: 99, KazCompactionDeltaMaxRecords: 300, KazCompactionDeltaGap: 5, KazCompactionDeltaMinFileSize: 7,
		KazCompactionDeltaTrigger: 30, KazCompactionDeltaBlockRatio: 0.75}}
	c.SelfCheck()
	require.Equal(t, "", c.KazCompactionMode)
	require.Zero(t, c.KazCompactionDeltaTiers)
	require.Zero(t, c.KazCompactionDeltaTrigger)
	require.Zero(t, c.KazCompactionDeltaBlockRatio)
	require.Zero(t, c.KazCompactionDeltaMaxRecords)
	require.Zero(t, c.KazCompactionDeltaGap)
	require.Zero(t, c.KazCompactionDeltaMinFileSize)
}

// TestKazDeltaConfigGapNoJoin checks that Config gap -1 resolves to an effective gap of 0 and 0 to the default.
func TestKazDeltaConfigGapNoJoin(t *testing.T) {
	require.Equal(t, uint32(0), (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: -1}}).kazDeltaConf().gap)
	require.Equal(t, uint32(16384), (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta"}}).kazDeltaConf().gap)
	require.Equal(t, uint32(4096), (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaGap: 4096}}).kazDeltaConf().gap)
}

// TestKazDeltaConfigSelfCheckCombination resets the delta knobs when max-records
// plus trigger plus the fold margin no longer fit in one compaction read, and
// for an out-of-range trigger or block ratio alone.
func TestKazDeltaConfigSelfCheckCombination(t *testing.T) {
	for _, c := range []Config{
		{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaMaxRecords: 800, KazCompactionDeltaTrigger: 200}},
		{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 5}},
		{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaBlockRatio: 2}},
	} {
		c.SelfCheck()
		require.Equal(t, "", c.KazCompactionMode)
		require.Zero(t, c.KazCompactionDeltaMaxRecords)
		require.Zero(t, c.KazCompactionDeltaTrigger)
		require.Zero(t, c.KazCompactionDeltaBlockRatio)
	}
	ok := Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 30, KazCompactionDeltaBlockRatio: 0.75}}
	ok.SelfCheck()
	require.Equal(t, KazCompactionModeDelta, ok.KazCompactionMode)
	require.Equal(t, 30, ok.KazCompactionDeltaTrigger)
	require.Equal(t, 0.75, ok.KazCompactionDeltaBlockRatio)
}

// TestKazDeltaConfigTriggerRatio checks that set trigger and block ratio knobs reach kazDeltaConf.
func TestKazDeltaConfigTriggerRatio(t *testing.T) {
	d := (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: "delta", KazCompactionDeltaTrigger: 30, KazCompactionDeltaBlockRatio: 0.75}}).kazDeltaConf()
	require.Equal(t, 30, d.trigger)
	require.Equal(t, 0.75, d.blockRatio)
}
