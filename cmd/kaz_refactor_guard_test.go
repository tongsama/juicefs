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
	"--kaz-staging-pack",
	"--kaz-skip-unuploaded-delete",
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

// kazGuardVolatileKeys are the JSON paths (from the root, exact match) whose
// values change between runs: Pid and PPid are process IDs, Version depends on
// the build and the upstream version. Only their presence is compared.
var kazGuardVolatileKeys = map[string]bool{"Pid": true, "PPid": true, "Version": true}

// kazGuardNotInConfig lists the value-taking --kaz-* flags that are not in
// kazGuardNonDefaultArgs, with the reason.
var kazGuardNotInConfig = map[string]string{
	"kaz-upgrade-busy": "read by the mount command only; it never reaches a config",
}

// kazGuardPrepare fixes the environment the flag defaults depend on. The default
// cache dir comes from HOME, except for root on linux, where it is another path;
// the golden files are made for the non-root case, so the tests skip as root.
func kazGuardPrepare(t *testing.T) {
	t.Helper()
	if os.Getuid() == 0 {
		t.Skip("the default cache-dir differs for root; golden files are for non-root")
	}
	t.Setenv("HOME", "/kaz-guard-home")
	// Main (run by other tests in the package) sets the global debugAgent, which
	// getVfsConf copies into Port.DebugAgent; clear it for a stable result.
	saved := debugAgent
	debugAgent = ""
	t.Cleanup(func() { debugAgent = saved })
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
			flattenKazJSON(p, e, out)
		}
	case []interface{}:
		for i, e := range x {
			flattenKazJSON(fmt.Sprintf("%s[%d]", prefix, i), e, out)
		}
	default:
		if kazGuardVolatileKeys[prefix] {
			x = "<volatile>"
		}
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
	kazGuardPrepare(t)
	// Every value-taking --kaz-* flag must be given a value above (or be listed
	// with a reason), so a newly added flag is not forgotten.
	given := map[string]bool{}
	for _, a := range kazGuardNonDefaultArgs {
		given[strings.TrimPrefix(strings.SplitN(a, "=", 2)[0], "--")] = true
	}
	for _, f := range cmdMount().Flags {
		name := f.Names()[0]
		if _, isBool := f.(*cli.BoolFlag); isBool || !strings.HasPrefix(name, "kaz-") {
			continue
		}
		_, skipped := kazGuardNotInConfig[name]
		require.True(t, given[name] || skipped, "flag --%s has no value in kazGuardNonDefaultArgs", name)
	}
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
	kazGuardPrepare(t)
	var lines []string
	for _, f := range cmdMount().Flags {
		d, ok := f.(cli.DocGenerationFlag)
		require.True(t, ok, "flag %v has no default text", f.Names())
		lines = append(lines, strings.Join(f.Names(), ",")+" default="+d.GetValue())
	}
	checkKazGolden(t, "testdata/kaz_mount_flags.golden", lines)
}
