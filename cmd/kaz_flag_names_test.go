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
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// kazRenamedFlagConfs is what the renamed kaz flags set in the three configs.
type kazRenamedFlagConfs struct {
	meta  *meta.Config
	vfs   *vfs.Config
	chunk *chunk.Config
}

// kazConfsFromArgs runs the mount flags over args and returns the configs they produce.
func kazConfsFromArgs(t *testing.T, args ...string) kazRenamedFlagConfs {
	t.Helper()
	var out kazRenamedFlagConfs
	app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
		out.meta = getMetaConf(c, "/kaz-flag-names", false)
		out.chunk = getChunkConf(c, &meta.Format{BlockSize: 4096})
		out.vfs = getVfsConf(c, out.meta, &meta.Format{}, out.chunk)
		return nil
	}}
	require.NoError(t, app.Run(append([]string{"kaz-flag-names"}, args...)))
	return out
}

// kazRenamedFlagArgs sets every renamed flag to a non-default value, with the
// flag names produced by name (the new or the old spelling).
func kazRenamedFlagArgs(name func(old string) string) []string {
	return []string{
		"--" + name("compaction-gc-mode"), "deferred",
		"--" + name("compaction-scheduler"), "priority",
		"--" + name("writeback-fsync") + "=false",
		"--" + name("slice-flush-wait"), "7s",
		"--" + name("slice-flush-idle"), "2s",
		"--" + name("writer-reuse-window"), "16",
		"--" + name("writer-flush-timeout"), "1h",
		"--" + name("writer-flush-scope"), vfs.WriterFlushScopeRange,
		"--" + name("meta-write-batch"), "32",
	}
}

// requireKazRenamedFlagValues checks the values set by kazRenamedFlagArgs.
func requireKazRenamedFlagValues(t *testing.T, got kazRenamedFlagConfs) {
	t.Helper()
	require.Equal(t, "deferred", got.meta.CompactionGCMode)
	require.Equal(t, "priority", got.meta.CompactionScheduler)
	require.True(t, got.chunk.StagingNoSync)
	require.Equal(t, 7*time.Second, got.vfs.SliceFlushWait)
	require.Equal(t, 2*time.Second, got.vfs.SliceFlushIdle)
	require.Equal(t, 16, got.vfs.WriterReuseWindow)
	require.Equal(t, time.Hour, got.vfs.WriterFlushTimeout)
	require.Equal(t, vfs.WriterFlushScopeRange, got.vfs.WriterFlushScope)
	require.Equal(t, 32, got.vfs.MetaWriteBatch)
}

// TestKazRenamedFlagsNewNames sets the renamed flags by their kaz- names.
func TestKazRenamedFlagsNewNames(t *testing.T) {
	requireKazRenamedFlagValues(t, kazConfsFromArgs(t, kazRenamedFlagArgs(func(old string) string { return "kaz-" + old })...))
}

// TestKazRenamedFlagsOldNames keeps the old names working, since existing
// mount scripts use them.
func TestKazRenamedFlagsOldNames(t *testing.T) {
	requireKazRenamedFlagValues(t, kazConfsFromArgs(t, kazRenamedFlagArgs(func(old string) string { return old })...))
}

// TestKazRenamedFlagsDefinedOnce makes sure each renamed flag has its kaz- name
// with the old name as its only alias, so help and the warning agree.
func TestKazRenamedFlagsDefinedOnce(t *testing.T) {
	names := map[string][]string{}
	for _, f := range cmdMount().Flags {
		n := f.Names()
		names[n[0]] = n[1:]
	}
	for old, renamed := range kazRenamedFlags {
		require.Equal(t, "kaz-"+old, renamed)
		require.Equal(t, []string{old}, names[renamed], "flag %s", renamed)
		_, oldIsPrimary := names[old]
		require.False(t, oldIsPrimary, "old name %s must only be an alias", old)
	}
}

// TestKazOldFlagNamesUsed finds the old names in the three spellings the flag
// parser accepts and ignores the new names and plain values.
func TestKazOldFlagNamesUsed(t *testing.T) {
	args := []string{"juicefs", "mount", "--writer-flush-timeout", "1h", "--meta-write-batch=32", "-slice-flush-wait", "5s",
		"--kaz-writer-flush-scope", "range", "--cache-size", "100", "redis://localhost/1", "/mnt/jfs"}
	require.Equal(t, []string{"meta-write-batch", "slice-flush-wait", "writer-flush-timeout"}, kazOldFlagNamesUsed(args))
	require.Empty(t, kazOldFlagNamesUsed([]string{"juicefs", "mount", "--kaz-meta-write-batch", "8"}))
}
