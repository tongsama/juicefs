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
		{[]string{"--kaz-cache-compaction-placement", "share"}, chunk.KazPlacementShare},
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

// TestKazCacheCompactionPlacementShareRatioFlag checks that
// --kaz-cache-compaction-placement-share-ratio defaults to 0.5 and is copied
// as given into the chunk config (the range is checked by the chunk self-check).
func TestKazCacheCompactionPlacementShareRatioFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want float64
	}{
		{nil, 0.5},
		{[]string{"--kaz-cache-compaction-placement-share-ratio", "0.8"}, 0.8},
		{[]string{"--kaz-cache-compaction-placement-share-ratio", "1.5"}, 1.5},
	} {
		set := flag.NewFlagSet("kaz-share-ratio-test", flag.ContinueOnError)
		for _, f := range dataCacheFlags() {
			require.NoError(t, f.Apply(set))
		}
		require.NoError(t, set.Parse(tc.args))
		conf := &chunk.Config{}
		applyKazCacheFlags(cli.NewContext(nil, set, nil), conf)
		require.Equal(t, tc.want, conf.KazCacheCompactionPlacementShareRatio, "%v", tc.args)
	}
}

// TestKazCacheDemoteUnreadFirstFlag checks that --kaz-cache-demote-unread-first
// defaults to off and is copied into the chunk config (it only takes effect
// under --cache-eviction kaz-2q, where the kaz-2q index reads it).
func TestKazCacheDemoteUnreadFirstFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"--kaz-cache-demote-unread-first"}, true},
		{[]string{"--kaz-cache-demote-unread-first=false"}, false},
	} {
		set := flag.NewFlagSet("kaz-demote-unread-first-test", flag.ContinueOnError)
		for _, f := range dataCacheFlags() {
			require.NoError(t, f.Apply(set))
		}
		require.NoError(t, set.Parse(tc.args))
		conf := &chunk.Config{}
		applyKazCacheFlags(cli.NewContext(nil, set, nil), conf)
		require.Equal(t, tc.want, conf.KazCacheDemoteUnreadFirst, "%v", tc.args)
	}
}

// TestKazCacheCompactionHotAgeFlag checks that --kaz-cache-compaction-hot-age
// defaults to 0 (disabled), keeps a valid duration, and turns an unparsable
// value into 0 with a warning (not the upstream silent 0 of some other
// meaning: 0 is this flag's default); a negative value is passed on for the
// chunk self-check, which makes it 0.
func TestKazCacheCompactionHotAgeFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 0},
		{[]string{"--kaz-cache-compaction-hot-age", "30m"}, 30 * time.Minute},
		{[]string{"--kaz-cache-compaction-hot-age", "90"}, 90 * time.Second},
		{[]string{"--kaz-cache-compaction-hot-age", "abc"}, 0},
		{[]string{"--kaz-cache-compaction-hot-age", "-5m"}, -5 * time.Minute},
	} {
		set := flag.NewFlagSet("kaz-hot-age-test", flag.ContinueOnError)
		for _, f := range dataCacheFlags() {
			require.NoError(t, f.Apply(set))
		}
		require.NoError(t, set.Parse(tc.args))
		conf := &chunk.Config{}
		applyKazCacheFlags(cli.NewContext(nil, set, nil), conf)
		require.Equal(t, tc.want, conf.KazCacheCompactionHotAge, "%v", tc.args)
	}
	conf := chunk.Config{CacheDir: "/tmp/x", CacheSize: 1 << 30, CacheEviction: chunk.EvictionKaz2Q,
		KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute, KazCacheCompactionHotAge: -5 * time.Minute}
	conf.SelfCheck("uuid")
	require.Equal(t, time.Duration(0), conf.KazCacheCompactionHotAge, "negative must become 0 under kaz-2q")
}
