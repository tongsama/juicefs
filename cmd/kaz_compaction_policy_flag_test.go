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

// TestKazCompactionPolicyFlags keeps the upstream behavior by default and parses the opt-in value.
func TestKazCompactionPolicyFlags(t *testing.T) {
	require.Equal(t, 5, metaConfFromArgs(t).KazCompactionReadMin)
	require.Equal(t, 100, metaConfFromArgs(t, "--kaz-compaction-read-min", "100").KazCompactionReadMin)
}

// TestKazCompactionReadMinFromFlag rejects 0 on the command line, where it would silently mean the default.
func TestKazCompactionReadMinFromFlag(t *testing.T) {
	got, err := kazCompactionReadMinFromFlag(100)
	require.NoError(t, err)
	require.Equal(t, 100, got)
	for _, v := range []int{0, 4, 2500, -1} {
		_, err := kazCompactionReadMinFromFlag(v)
		require.Error(t, err, "%d", v)
	}
}

// TestKazCompactionCooldownFlagsRemoved keeps the measured-harmful cooldown out of the mount flags.
func TestKazCompactionCooldownFlagsRemoved(t *testing.T) {
	for _, f := range cmdMount().Flags {
		for _, name := range f.Names() {
			require.NotContains(t, []string{"kaz-compaction-cooldown", "kaz-compaction-force-slices"}, name)
		}
	}
}
