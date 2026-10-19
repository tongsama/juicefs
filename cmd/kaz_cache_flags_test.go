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
	"flag"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
)

// TestKazCacheDurationFlag checks that a valid duration (including a literal
// zero) is kept as utils.Duration parses it, and that an unparsable value
// falls back to the flag default instead of silently becoming 0.
func TestKazCacheDurationFlag(t *testing.T) {
	const def = 10 * time.Minute
	for in, want := range map[string]time.Duration{
		"0":     0,
		"0s":    0,
		"30s":   30 * time.Second,
		"1.5":   1500 * time.Millisecond,
		"2h":    2 * time.Hour,
		"1d":    24 * time.Hour,
		"1d12h": 36 * time.Hour,
		"-5m":   -5 * time.Minute, // negative stays for the chunk self-check
	} {
		require.Equal(t, want, kazCacheDurationFlag("kaz-cache-promote-delay", in, def), in)
	}
	for _, in := range []string{"", "abc", "10x", "5mm", "d", "1dx"} {
		require.Equal(t, def, kazCacheDurationFlag("kaz-cache-promote-delay", in, def), in)
		require.Equal(t, 5*time.Minute, kazCacheDurationFlag("kaz-cache-state-interval", in, 5*time.Minute), in)
	}
}

// TestKazCacheCompactionPlacementFlag checks that --kaz-cache-compaction-placement
// defaults to inherit and is copied as given into the chunk config (the value
// is validated later by the chunk self-check).
func TestKazCacheCompactionPlacementFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, chunk.KazPlacementInherit},
		{[]string{"--kaz-cache-compaction-placement", "probation"}, chunk.KazPlacementProbation},
		{[]string{"--kaz-cache-compaction-placement", "replace"}, chunk.KazPlacementReplace},
		{[]string{"--kaz-cache-compaction-placement", "bogus"}, "bogus"},
	} {
		set := flag.NewFlagSet("kaz-placement-test", flag.ContinueOnError)
		for _, f := range dataCacheFlags() {
			require.NoError(t, f.Apply(set))
		}
		require.NoError(t, set.Parse(tc.args))
		conf := &chunk.Config{}
		applyKazCacheFlags(cli.NewContext(nil, set, nil), conf)
		require.Equal(t, tc.want, conf.KazCacheCompactionPlacement, "%v", tc.args)
	}
}
