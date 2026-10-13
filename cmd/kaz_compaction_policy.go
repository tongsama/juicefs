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
	"fmt"
	"math"
	"strconv"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

// parseKazCompactionCooldown reads --kaz-compaction-cooldown as seconds or a Go
// duration, and reports malformed text instead of reading it as 0.
func parseKazCompactionCooldown(s string) (time.Duration, error) {
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return 0, fmt.Errorf("invalid kaz-compaction-cooldown %q", s)
		}
		return time.Duration(v * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid kaz-compaction-cooldown %q: %w", s, err)
	}
	return d, nil
}

// kazCompactionPolicyFromFlags checks the raw flag values. Unlike the Go config,
// where 0 means "use the default", 0 on the command line is out of range.
func kazCompactionPolicyFromFlags(readMin int, cooldownText string, force int) (int, time.Duration, int, error) {
	if readMin == 0 {
		return 0, 0, 0, fmt.Errorf("invalid kaz-compaction-read-min 0 (want 5..2499)")
	}
	if force == 0 {
		return 0, 0, 0, fmt.Errorf("invalid kaz-compaction-force-slices 0 (want 100..2499)")
	}
	cooldown, err := parseKazCompactionCooldown(cooldownText)
	if err != nil {
		return 0, 0, 0, err
	}
	conf := meta.Config{KazCompactionReadMin: readMin, KazCompactionCooldown: cooldown, KazCompactionForceSlices: force}
	if err := conf.ValidateKazCompactionPolicy(); err != nil {
		return 0, 0, 0, err
	}
	return readMin, cooldown, force, nil
}

// applyKazCompactionPolicyFlags copies the kaz compaction policy flags into conf
// and exits on invalid values, like the other compaction modes.
func applyKazCompactionPolicyFlags(c *cli.Context, conf *meta.Config) {
	readMin, cooldown, force, err := kazCompactionPolicyFromFlags(c.Int("kaz-compaction-read-min"), c.String("kaz-compaction-cooldown"), c.Int("kaz-compaction-force-slices"))
	if err != nil {
		logger.Fatalf("%s", err)
	}
	conf.KazCompactionReadMin = readMin
	conf.KazCompactionCooldown = cooldown
	conf.KazCompactionForceSlices = force
}
