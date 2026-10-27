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

package chunk

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestKaz2QConfigSelfCheck verifies that SelfCheck keeps kaz-2q and normalizes
// the kaz-2q options (ratio, delay, ghost ratio, state interval).
func TestKaz2QConfigSelfCheck(t *testing.T) {
	cases := []struct {
		name      string
		dir       string
		ratio     float64
		delay     time.Duration
		ghost     float64
		interval  time.Duration
		wEvict    string
		wRatio    float64
		wDelay    time.Duration
		wGhost    float64
		wInterval time.Duration
	}{
		{"valid", "/tmp/x", 0.3, time.Minute, 2, time.Minute, EvictionKaz2Q, 0.3, time.Minute, 2, time.Minute},
		{"memory", "memory", 0.3, time.Minute, 2, time.Minute, Eviction2Random, 0.3, time.Minute, 2, time.Minute},
		{"ratio0", "/tmp/x", 0, 0, 0, time.Minute, EvictionKaz2Q, 0.4, 0, 0, time.Minute},
		{"ratio1", "/tmp/x", 1, time.Minute, 1, time.Minute, EvictionKaz2Q, 0.4, time.Minute, 1, time.Minute},
		{"rationeg", "/tmp/x", -0.1, time.Minute, 1, time.Minute, EvictionKaz2Q, 0.4, time.Minute, 1, time.Minute},
		{"ghostneg", "/tmp/x", 0.4, time.Minute, -1, time.Minute, EvictionKaz2Q, 0.4, time.Minute, 4, time.Minute},
		{"interval0", "/tmp/x", 0.4, time.Minute, 1, 0, EvictionKaz2Q, 0.4, time.Minute, 1, 5 * time.Minute},
		{"delayneg", "/tmp/x", 0.4, -time.Second, 1, time.Minute, EvictionKaz2Q, 0.4, 10 * time.Minute, 1, time.Minute},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conf := Config{
				CacheDir: c.dir, CacheSize: 1 << 30, CacheEviction: EvictionKaz2Q,
				KazChunkConfig: KazChunkConfig{
					KazCacheProbationRatio: c.ratio, KazCachePromoteDelay: c.delay,
					KazCacheGhostRatio: c.ghost, KazCacheStateInterval: c.interval,
				},
			}
			conf.SelfCheck("uuid")
			if conf.CacheEviction != c.wEvict || conf.KazCacheProbationRatio != c.wRatio ||
				conf.KazCachePromoteDelay != c.wDelay || conf.KazCacheGhostRatio != c.wGhost ||
				conf.KazCacheStateInterval != c.wInterval {
				t.Fatalf("got %s %v %v %v %v", conf.CacheEviction, conf.KazCacheProbationRatio,
					conf.KazCachePromoteDelay, conf.KazCacheGhostRatio, conf.KazCacheStateInterval)
			}
		})
	}

	// Compaction output placement: "" and unknown values fall back to inherit
	// under kaz-2q; other eviction policies leave the value untouched.
	for _, c := range []struct{ evict, dir, in, want string }{
		{EvictionKaz2Q, "/tmp/x", "", KazPlacementInherit},
		{EvictionKaz2Q, "/tmp/x", "bogus", KazPlacementInherit},
		{EvictionKaz2Q, "/tmp/x", KazPlacementInherit, KazPlacementInherit},
		{EvictionKaz2Q, "/tmp/x", KazPlacementProbation, KazPlacementProbation},
		{EvictionKaz2Q, "/tmp/x", KazPlacementReplace, KazPlacementReplace},
		{EvictionKaz2Q, "/tmp/x", KazPlacementShare, KazPlacementShare},
		{EvictionLRU, "/tmp/x", "bogus", "bogus"},
		{Eviction2Random, "/tmp/x", "", ""},
		{EvictionKaz2Q, "memory", "bogus", "bogus"},
	} {
		conf := Config{CacheDir: c.dir, CacheSize: 1 << 30, CacheEviction: c.evict,
			KazChunkConfig: KazChunkConfig{KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute,
				KazCacheCompactionPlacement: c.in}}
		conf.SelfCheck("uuid")
		if conf.KazCacheCompactionPlacement != c.want {
			t.Fatalf("%s/%s placement %q: got %q, want %q", c.evict, c.dir, c.in,
				conf.KazCacheCompactionPlacement, c.want)
		}
	}

	// Share ratio: under kaz-2q with share, values outside (0, 1] warn and
	// become 0.5; other placements and other eviction policies leave it alone.
	for _, c := range []struct {
		evict, placement string
		in, want         float64
	}{
		{EvictionKaz2Q, KazPlacementShare, 0, 0.5},
		{EvictionKaz2Q, KazPlacementShare, -0.1, 0.5},
		{EvictionKaz2Q, KazPlacementShare, 1.5, 0.5},
		{EvictionKaz2Q, KazPlacementShare, 1, 1},
		{EvictionKaz2Q, KazPlacementShare, 0.3, 0.3},
		{EvictionKaz2Q, KazPlacementInherit, 0, 0},
		{EvictionLRU, KazPlacementShare, 0, 0},
		{Eviction2Random, KazPlacementShare, 1.5, 1.5},
	} {
		conf := Config{CacheDir: "/tmp/x", CacheSize: 1 << 30, CacheEviction: c.evict,
			KazChunkConfig: KazChunkConfig{KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute,
				KazCacheCompactionPlacement: c.placement, KazCacheCompactionPlacementShareRatio: c.in}}
		conf.SelfCheck("uuid")
		if conf.KazCacheCompactionPlacementShareRatio != c.want {
			t.Fatalf("%s/%s share ratio %v: got %v, want %v", c.evict, c.placement, c.in,
				conf.KazCacheCompactionPlacementShareRatio, c.want)
		}
	}
}

// TestKazConfigSelfCheckOnlyForKaz2Q verifies that non-kaz modes (and the
// memory fallback) leave zero-valued kaz options untouched, and that kaz-2q
// accepts a zero promote delay and ghost ratio.
func TestKazConfigSelfCheckOnlyForKaz2Q(t *testing.T) {
	for _, tc := range []struct{ evict, dir string }{
		{EvictionLRU, "/tmp/x"}, {Eviction2Random, "/tmp/x"}, {EvictionNone, "/tmp/x"},
		{EvictionKaz2Q, "memory"},
	} {
		conf := Config{CacheDir: tc.dir, CacheSize: 1 << 30, CacheEviction: tc.evict}
		conf.SelfCheck("uuid")
		if conf.KazCacheProbationRatio != 0 || conf.KazCachePromoteDelay != 0 ||
			conf.KazCacheGhostRatio != 0 || conf.KazCacheStateInterval != 0 {
			t.Fatalf("%s/%s: kaz fields changed: %+v", tc.evict, tc.dir, conf)
		}
	}
	conf := Config{CacheDir: "/tmp/x", CacheSize: 1 << 30, CacheEviction: EvictionKaz2Q,
		KazChunkConfig: KazChunkConfig{KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute}}
	conf.SelfCheck("uuid")
	if conf.KazCachePromoteDelay != 0 || conf.KazCacheGhostRatio != 0 {
		t.Fatalf("zero delay/ghost changed: %+v", conf)
	}
}

// TestKazDemoteUnreadFirstConfig checks that --kaz-cache-demote-unread-first
// reaches the protected heap of a kaz-2q index (off by default), survives the
// self-check and reset, and is ignored by other eviction policies, which do
// not use a kaz-2q index.
func TestKazDemoteUnreadFirstConfig(t *testing.T) {
	conf := Config{CacheDir: "/tmp/x", CacheSize: 1 << 30, CacheEviction: EvictionKaz2Q, KazChunkConfig: KazChunkConfig{KazCacheDemoteUnreadFirst: true}}
	conf.SelfCheck("uuid")
	require.True(t, conf.KazCacheDemoteUnreadFirst)
	p := newKaz2QIndex(&conf)
	require.True(t, p.prot.unreadFirst)
	require.False(t, p.prob.unreadFirst)
	p.reset()
	require.True(t, p.prot.unreadFirst, "reset keeps the option")
	require.False(t, newKaz2QIndex(&Config{CacheEviction: EvictionKaz2Q}).prot.unreadFirst)

	lru := defaultConf
	lru.CacheEviction = EvictionLRU
	lru.KazCacheDemoteUnreadFirst = true
	s := newTestCacheStore(t.TempDir(), &lru, nil)
	_, isKaz := s.keys.(*kaz2QIndex)
	require.False(t, isKaz)
}

// TestKazCompactionHotAgeConfig checks --kaz-cache-compaction-hot-age in the
// chunk self-check and the kaz-2q index: under kaz-2q a negative value warns
// and becomes 0 (disabled) while 0 and positive values are kept; other
// eviction policies leave the value untouched; the index stores it in seconds
// and treats a negative value as 0 even without the self-check.
func TestKazCompactionHotAgeConfig(t *testing.T) {
	for _, c := range []struct {
		evict    string
		in, want time.Duration
	}{
		{EvictionKaz2Q, 0, 0},
		{EvictionKaz2Q, 30 * time.Minute, 30 * time.Minute},
		{EvictionKaz2Q, -time.Second, 0},
		{EvictionLRU, -time.Second, -time.Second},
		{Eviction2Random, -time.Second, -time.Second},
	} {
		conf := Config{CacheDir: "/tmp/x", CacheSize: 1 << 30, CacheEviction: c.evict,
			KazChunkConfig: KazChunkConfig{KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute,
				KazCacheCompactionHotAge: c.in}}
		conf.SelfCheck("uuid")
		require.Equal(t, c.want, conf.KazCacheCompactionHotAge, "%s %v", c.evict, c.in)
	}
	require.Equal(t, uint32(1800), newKaz2QIndex(&Config{KazChunkConfig: KazChunkConfig{KazCacheCompactionHotAge: 30 * time.Minute}}).hotAge)
	require.Equal(t, uint32(0), newKaz2QIndex(&Config{KazChunkConfig: KazChunkConfig{KazCacheCompactionHotAge: -time.Minute}}).hotAge)
	require.Equal(t, uint32(0), newKaz2QIndex(&Config{}).hotAge)
}
