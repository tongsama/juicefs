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
				KazCacheProbationRatio: c.ratio, KazCachePromoteDelay: c.delay,
				KazCacheGhostRatio: c.ghost, KazCacheStateInterval: c.interval,
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
		KazCacheProbationRatio: 0.4, KazCacheStateInterval: time.Minute}
	conf.SelfCheck("uuid")
	if conf.KazCachePromoteDelay != 0 || conf.KazCacheGhostRatio != 0 {
		t.Fatalf("zero delay/ghost changed: %+v", conf)
	}
}
