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

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// chunkConfFromArgs parses mount data cache flags and returns the resulting chunk config.
func chunkConfFromArgs(t *testing.T, args ...string) *chunk.Config {
	var conf *chunk.Config
	app := &cli.App{
		Flags: append(dataCacheFlags(), storageFlags()...),
		Action: func(c *cli.Context) error {
			conf = getChunkConf(c, &meta.Format{BlockSize: 4096})
			return nil
		},
	}
	require.NoError(t, app.Run(append([]string{"juicefs"}, args...)))
	return conf
}

func TestWritebackFsyncFlag(t *testing.T) {
	require.False(t, chunkConfFromArgs(t).StagingNoSync, "staged blocks must be synced by default")
	require.False(t, chunkConfFromArgs(t, "--writeback-fsync").StagingNoSync)
	require.True(t, chunkConfFromArgs(t, "--writeback-fsync=false").StagingNoSync)
}
