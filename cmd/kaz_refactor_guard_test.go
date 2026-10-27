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
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// updateKazGolden rewrites the golden files of the refactoring guards.
var updateKazGolden = flag.Bool("update-kaz-golden", false, "rewrite cmd/testdata/kaz_*.golden")

// kazGuardNonDefaultArgs sets every value-taking --kaz-* flag (and the
// --cache-eviction value they depend on) to a value that is valid but not the
// default, so a field that stops being filled shows up as a changed golden line.
// Not set: --kaz-upgrade-busy, which is read by the mount command only and never
// reaches a config.
var kazGuardNonDefaultArgs = []string{
	"--kaz-get-header-timeout", "3s",
	"--kaz-compaction-gc-mode", "deferred",
	"--kaz-compaction-scheduler", "priority",
	"--kaz-compaction-read-min", "7",
	"--kaz-compaction-mode", "delta",
	"--kaz-compaction-block-hidden-ratio", "0.6",
	"--kaz-compaction-block-max-slices", "12",
	"--kaz-compaction-delta-tiers", "5",
	"--kaz-compaction-delta-max-records", "300",
	"--kaz-compaction-delta-gap", "8K",
	"--kaz-compaction-delta-min-file-size", "1G",
	"--kaz-compaction-delta-trigger", "60",
	"--kaz-compaction-delta-block-ratio", "0.7",
	"--kaz-finish-canceled-get",
	"--kaz-writeback-fsync=false",
	"--kaz-slice-flush-wait", "7s",
	"--kaz-slice-flush-idle", "2s",
	"--kaz-writer-reuse-window", "16",
	"--kaz-writer-flush-timeout", "1h",
	"--kaz-writer-flush-scope", "range",
	"--kaz-meta-write-batch", "32",
	"--cache-eviction", "kaz-2q",
	"--kaz-cache-probation-ratio", "0.3",
	"--kaz-cache-promote-delay", "7m",
	"--kaz-cache-ghost-ratio", "2",
	"--kaz-cache-state-interval", "3m",
	"--kaz-cache-compaction-placement", "share",
	"--kaz-cache-compaction-placement-share-ratio", "0.6",
	"--kaz-cache-demote-unread-first",
	"--kaz-cache-compaction-hot-age", "2m",
}

// kazGuardVolatileKeys are the JSON keys whose values change on every run
// (Pid and PPid are the process IDs; the others are listen addresses that depend
// on the run); only their presence is compared. StartTime-like fields do not
// exist in vfs.Config today, so the list is also a safeguard.
var kazGuardVolatileKeys = map[string]bool{
	"Pid": true, "PPid": true, "StartTime": true, "PrometheusAgent": true,
	"DebugAgent": true, "PyroscopeAddr": true, "ConsulAddr": true,
}

// flattenKazJSON appends "path=value" lines for every leaf of v, with nested
// object keys joined by dots; volatile leaves keep the key but drop the value.
func flattenKazJSON(prefix string, v interface{}, out *[]string) {
	switch x := v.(type) {
	case map[string]interface{}:
		for k, e := range x {
			p := k
			if prefix != "" {
				p = prefix + "." + k
			}
			if kazGuardVolatileKeys[k] {
				*out = append(*out, p+"=<volatile>")
				continue
			}
			flattenKazJSON(p, e, out)
		}
	case []interface{}:
		for i, e := range x {
			flattenKazJSON(fmt.Sprintf("%s[%d]", prefix, i), e, out)
		}
	default:
		*out = append(*out, fmt.Sprintf("%s=%v", prefix, x))
	}
}

// checkKazGolden compares lines (sorted) with the golden file, or rewrites it
// when -update-kaz-golden is given.
func checkKazGolden(t *testing.T, file string, lines []string) {
	t.Helper()
	sort.Strings(lines)
	got := strings.Join(lines, "\n") + "\n"
	if *updateKazGolden {
		require.NoError(t, os.WriteFile(file, []byte(got), 0644))
		return
	}
	want, err := os.ReadFile(file)
	require.NoError(t, err)
	require.Equal(t, string(want), got)
}

// TestKazConfigJSONKeys pins the keys and values of the vfs.Config JSON (the
// .config file) when every kaz flag has a non-default value.
func TestKazConfigJSONKeys(t *testing.T) {
	var conf interface{}
	app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
		metaConf := getMetaConf(c, "/kaz-guard", false)
		chunkConf := getChunkConf(c, &meta.Format{BlockSize: 4096})
		vfsConf := getVfsConf(c, metaConf, &meta.Format{}, chunkConf)
		data, err := json.Marshal(vfsConf)
		if err != nil {
			return err
		}
		return json.Unmarshal(data, &conf)
	}}
	require.NoError(t, app.Run(append([]string{"kaz-guard"}, kazGuardNonDefaultArgs...)))
	var lines []string
	flattenKazJSON("", conf, &lines)
	checkKazGolden(t, "testdata/kaz_config_keys.golden", lines)
}

// TestKazFlagSetUnchanged pins every mount flag: all its names (aliases
// included) and its default value.
func TestKazFlagSetUnchanged(t *testing.T) {
	var lines []string
	for _, f := range cmdMount().Flags {
		def := ""
		if d, ok := f.(cli.DocGenerationFlag); ok {
			def = d.GetValue()
		}
		lines = append(lines, strings.Join(f.Names(), ",")+" default="+def)
	}
	checkKazGolden(t, "testdata/kaz_mount_flags.golden", lines)
}
