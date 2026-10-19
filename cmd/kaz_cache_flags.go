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
	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/utils"
)

// applyKazCacheFlags copies the kaz-2q cache eviction flags into the chunk config.
func applyKazCacheFlags(c *cli.Context, conf *chunk.Config) {
	conf.KazCacheProbationRatio = c.Float64("kaz-cache-probation-ratio")
	conf.KazCachePromoteDelay = utils.Duration(c.String("kaz-cache-promote-delay"))
	conf.KazCacheGhostRatio = c.Float64("kaz-cache-ghost-ratio")
	conf.KazCacheStateInterval = utils.Duration(c.String("kaz-cache-state-interval"))
}
