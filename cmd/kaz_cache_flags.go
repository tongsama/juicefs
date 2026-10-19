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
	"strconv"
	"strings"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/utils"
)

// applyKazCacheFlags copies the kaz-2q cache eviction flags into the chunk config
// (values are range-checked later by the chunk self-check).
func applyKazCacheFlags(c *cli.Context, conf *chunk.Config) {
	conf.KazCacheProbationRatio = c.Float64("kaz-cache-probation-ratio")
	conf.KazCachePromoteDelay = kazCacheDurationFlag("kaz-cache-promote-delay", c.String("kaz-cache-promote-delay"), 10*time.Minute)
	conf.KazCacheGhostRatio = c.Float64("kaz-cache-ghost-ratio")
	conf.KazCacheStateInterval = kazCacheDurationFlag("kaz-cache-state-interval", c.String("kaz-cache-state-interval"), 5*time.Minute)
	conf.KazCacheCompactionPlacement = c.String("kaz-cache-compaction-placement")
}

// kazCacheDurationFlag parses the value s of the kaz-2q duration flag name.
// utils.Duration turns an unparsable value into 0, which is a valid and very
// different setting for these flags (0 promote delay means immediate
// promotion), so a value it cannot parse falls back to def with a warning
// instead. A parsable value, including a literal zero, is returned exactly as
// utils.Duration parses it; range checks stay in the chunk self-check.
func kazCacheDurationFlag(name, s string, def time.Duration) time.Duration {
	if !kazDurationParsable(s) {
		logger.Warnf("Invalid duration value for --%s: %q, setting it to %s", name, s, def)
		return def
	}
	return utils.Duration(s)
}

// kazDurationParsable reports whether utils.Duration can parse s without
// falling back to 0: a float number of seconds, or an optional "<float>d"
// day prefix followed by an optional Go duration (an empty string is not).
func kazDurationParsable(s string) bool {
	if s == "" {
		return false
	}
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		return true
	}
	rest := s
	if p := strings.Index(s, "d"); p >= 0 {
		if _, err := strconv.ParseFloat(s[:p], 64); err != nil {
			return false
		}
		rest = s[p+1:]
	}
	if rest == "" {
		return true
	}
	_, err := time.ParseDuration(rest)
	return err == nil
}
