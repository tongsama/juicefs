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

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

// kazCompactionReadMinFromFlag checks --kaz-compaction-read-min. Unlike the Go
// config, where 0 means "use the default", 0 on the command line is out of range.
func kazCompactionReadMinFromFlag(readMin int) (int, error) {
	if readMin == 0 {
		return 0, fmt.Errorf("invalid kaz-compaction-read-min 0 (want 5..2499)")
	}
	conf := meta.Config{KazCompactionReadMin: readMin}
	if err := conf.ValidateKazCompactionPolicy(); err != nil {
		return 0, err
	}
	return readMin, nil
}

// kazBlockCompactionFromFlags checks the block compaction flags. Unlike the Go
// config, a hidden ratio of 0 on the command line is out of range.
func kazBlockCompactionFromFlags(mode string, ratio float64, maxSlices int) (meta.Config, error) {
	if ratio == 0 {
		return meta.Config{}, fmt.Errorf("invalid kaz-compaction-block-hidden-ratio 0 (want 0 < ratio <= 1)")
	}
	conf := meta.Config{KazCompactionMode: mode, KazCompactionBlockHiddenRatio: ratio, KazCompactionBlockMaxSlices: maxSlices}
	if err := conf.ValidateKazBlockCompaction(); err != nil {
		return meta.Config{}, err
	}
	return conf, nil
}

// applyKazCompactionPolicyFlags copies the kaz compaction policy and block
// compaction flags into conf and exits on an invalid value, like the other compaction modes.
func applyKazCompactionPolicyFlags(c *cli.Context, conf *meta.Config) {
	readMin, err := kazCompactionReadMinFromFlag(c.Int("kaz-compaction-read-min"))
	if err != nil {
		logger.Fatalf("%s", err)
	}
	conf.KazCompactionReadMin = readMin
	block, err := kazBlockCompactionFromFlags(c.String("kaz-compaction-mode"), c.Float64("kaz-compaction-block-hidden-ratio"), c.Int("kaz-compaction-block-max-slices"))
	if err != nil {
		logger.Fatalf("%s", err)
	}
	conf.KazCompactionMode = block.KazCompactionMode
	conf.KazCompactionBlockHiddenRatio = block.KazCompactionBlockHiddenRatio
	conf.KazCompactionBlockMaxSlices = block.KazCompactionBlockMaxSlices
}
