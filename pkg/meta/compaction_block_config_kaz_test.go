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

// TestKazBlockCompactionConfig checks validation and effective values of the block mode options.
func TestKazBlockCompactionConfig(t *testing.T) {
	var zero Config
	require.NoError(t, zero.ValidateKazBlockCompaction())
	require.False(t, zero.kazBlockCompactionEnabled())
	require.Equal(t, 0.5, zero.kazBlockHiddenRatio())
	require.Equal(t, 17, zero.kazBlockMaxSlices(16))

	c := Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: KazCompactionModeBlock, KazCompactionBlockHiddenRatio: 0.25, KazCompactionBlockMaxSlices: 9}}
	require.NoError(t, c.ValidateKazBlockCompaction())
	require.True(t, c.kazBlockCompactionEnabled())
	require.Equal(t, 0.25, c.kazBlockHiddenRatio())
	require.Equal(t, 9, c.kazBlockMaxSlices(16))
	require.NoError(t, (&Config{KazMetaConfig: KazMetaConfig{KazCompactionMode: KazCompactionModeUpstream}}).ValidateKazBlockCompaction())

	for _, bad := range []Config{
		{KazMetaConfig: KazMetaConfig{KazCompactionMode: "blocks"}},
		{KazMetaConfig: KazMetaConfig{KazCompactionBlockHiddenRatio: -0.1}},
		{KazMetaConfig: KazMetaConfig{KazCompactionBlockHiddenRatio: 1.5}},
		{KazMetaConfig: KazMetaConfig{KazCompactionBlockHiddenRatio: math.NaN()}},
		{KazMetaConfig: KazMetaConfig{KazCompactionBlockMaxSlices: -1}},
	} {
		require.Error(t, bad.ValidateKazBlockCompaction(), "%+v", bad)
	}
}

// TestKazBlockCompactionSelfCheck falls back to upstream compaction on invalid settings.
func TestKazBlockCompactionSelfCheck(t *testing.T) {
	c := DefaultConf()
	c.KazCompactionMode = "bogus"
	c.KazCompactionBlockHiddenRatio = 2
	c.SelfCheck()
	require.Equal(t, "", c.KazCompactionMode)
	require.Zero(t, c.KazCompactionBlockHiddenRatio)
	require.Zero(t, c.KazCompactionBlockMaxSlices)
}
