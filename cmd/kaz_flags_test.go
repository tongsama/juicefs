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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// TestKazRewordedUsage pins the kaz wording of the upstream --max-deletes and
// --cache-eviction usage and the panics of kazSetFlagUsage on bad input.
func TestKazRewordedUsage(t *testing.T) {
	usage := func(flags []cli.Flag, name string) string {
		for _, f := range flags {
			if f.Names()[0] == name {
				switch ff := f.(type) {
				case *cli.IntFlag:
					return ff.Usage
				case *cli.StringFlag:
					return ff.Usage
				}
			}
		}
		t.Fatalf("flag %s not found", name)
		return ""
	}
	wantDel := "number of background threads to delete objects; 0 disables deletion, negative values delete synchronously in the caller (not unlimited concurrency)"
	if got := usage(storageFlags(), "max-deletes"); got != wantDel {
		t.Fatalf("max-deletes usage = %q", got)
	}
	wantEv := fmt.Sprintf("cache eviction policy [%s, %s, %s, %s]", chunk.EvictionNone, chunk.Eviction2Random, chunk.EvictionLRU, chunk.EvictionKaz2Q)
	if got := usage(dataCacheFlags(), "cache-eviction"); got != wantEv {
		t.Fatalf("cache-eviction usage = %q", got)
	}
	mustPanic := func(name string, flags []cli.Flag) {
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		kazSetFlagUsage(flags, name, "x")
	}
	mustPanic("missing", []cli.Flag{&cli.IntFlag{Name: "other"}})
	mustPanic("b", []cli.Flag{&cli.BoolFlag{Name: "b"}})
}

// TestKazStagingPackFlag checks that --kaz-staging-pack reaches the VFS config
// only together with --kaz-compaction-mode delta, and is off by default.
func TestKazStagingPackFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"--kaz-staging-pack", "--kaz-compaction-mode", "delta"}, true},
		{[]string{"--kaz-staging-pack", "--kaz-compaction-mode", "upstream"}, false},
		{[]string{"--kaz-staging-pack"}, false},
		{[]string{"--kaz-compaction-mode", "delta"}, false},
	} {
		var got bool
		app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
			got = getVfsConf(c, meta.DefaultConf(), &meta.Format{}, &chunk.Config{}).KazStagingPack
			return nil
		}}
		require.NoError(t, app.Run(append([]string{"test"}, tc.args...)), "%v", tc.args)
		require.Equal(t, tc.want, got, "%v", tc.args)
	}
}

// TestKazSkipUnuploadedDeleteFlag checks that --kaz-skip-unuploaded-delete
// reaches the chunk config and is off by default.
func TestKazSkipUnuploadedDeleteFlag(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{nil, false},
		{[]string{"--kaz-skip-unuploaded-delete"}, true},
	} {
		var got bool
		app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
			got = getChunkConf(c, &meta.Format{BlockSize: 4096}).KazSkipUnuploadedDelete
			return nil
		}}
		require.NoError(t, app.Run(append([]string{"test"}, tc.args...)), "%v", tc.args)
		require.Equal(t, tc.want, got, "%v", tc.args)
	}
}
