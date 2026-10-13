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

package cmd

import (
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// metaConfFromArgs runs the mount flag set and returns the metadata config it builds.
func metaConfFromArgs(t *testing.T, args ...string) *meta.Config {
	var conf *meta.Config
	app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
		conf = getMetaConf(c, "/kaz-policy-test", false)
		return nil
	}}
	require.NoError(t, app.Run(append([]string{"juicefs"}, args...)))
	return conf
}

// TestKazCompactionPolicyFlags keeps the upstream behavior by default and parses the opt-in values.
func TestKazCompactionPolicyFlags(t *testing.T) {
	c := metaConfFromArgs(t)
	require.Equal(t, 5, c.KazCompactionReadMin)
	require.Zero(t, c.KazCompactionCooldown)
	require.Equal(t, 1000, c.KazCompactionForceSlices)

	c = metaConfFromArgs(t, "--kaz-compaction-read-min", "100", "--kaz-compaction-cooldown", "1m", "--kaz-compaction-force-slices", "700")
	require.Equal(t, 100, c.KazCompactionReadMin)
	require.Equal(t, time.Minute, c.KazCompactionCooldown)
	require.Equal(t, 700, c.KazCompactionForceSlices)

	c = metaConfFromArgs(t, "--kaz-compaction-cooldown", "90")
	require.Equal(t, 90*time.Second, c.KazCompactionCooldown)
}

// TestParseKazCompactionCooldown rejects malformed durations instead of reading them as 0.
func TestParseKazCompactionCooldown(t *testing.T) {
	for in, want := range map[string]time.Duration{"0": 0, "1m": time.Minute, "90": 90 * time.Second, "1.5": 1500 * time.Millisecond} {
		got, err := parseKazCompactionCooldown(in)
		require.NoError(t, err, in)
		require.Equal(t, want, got, in)
	}
	for _, in := range []string{"", "abc", "1x", "1d", "NaN", "Inf", "-Inf", "+Inf"} {
		_, err := parseKazCompactionCooldown(in)
		require.Error(t, err, in)
	}
}

// TestKazCompactionPolicyFromFlags rejects 0 on the command line, where it would silently mean the default.
func TestKazCompactionPolicyFromFlags(t *testing.T) {
	readMin, cooldown, force, err := kazCompactionPolicyFromFlags(100, "1m", 700)
	require.NoError(t, err)
	require.Equal(t, 100, readMin)
	require.Equal(t, time.Minute, cooldown)
	require.Equal(t, 700, force)
	for _, tc := range []struct {
		readMin  int
		cooldown string
		force    int
	}{
		{0, "0", 1000},
		{5, "0", 0},
		{4, "0", 1000},
		{5, "0", 2500},
		{5, "500ms", 1000},
		{5, "NaN", 1000},
	} {
		_, _, _, err := kazCompactionPolicyFromFlags(tc.readMin, tc.cooldown, tc.force)
		require.Error(t, err, "%+v", tc)
	}
}
