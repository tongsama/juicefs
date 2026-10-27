//go:build !windows
// +build !windows

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
	"errors"
	"flag"
	"testing"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v2"
)

// kazUpgradeTestContext returns a cli context with --kaz-upgrade-busy set to
// value (left unset when value is empty).
func kazUpgradeTestContext(t *testing.T, value string) *cli.Context {
	t.Helper()
	set := flag.NewFlagSet("mount", flag.ContinueOnError)
	for _, f := range kazUpgradeFlags() {
		require.NoError(t, f.Apply(set))
	}
	if value != "" {
		require.NoError(t, set.Set("kaz-upgrade-busy", value))
	}
	return cli.NewContext(nil, set, nil)
}

// stubKazUpgradeState replaces the hand-over and inode probes for one test.
func stubKazUpgradeState(t *testing.T, handedOver bool, ino uint64, inoErr error) {
	t.Helper()
	oldHanded, oldIno := kazFuseHandedOver, kazMountInode
	kazFuseHandedOver = func() bool { return handedOver }
	kazMountInode = func(string) (uint64, error) { return ino, inoErr }
	t.Cleanup(func() { kazFuseHandedOver, kazMountInode = oldHanded, oldIno })
}

// TestKazUpgradeBusyDefaultIsFail checks that the default refuses stacking.
func TestKazUpgradeBusyDefaultIsFail(t *testing.T) {
	c := kazUpgradeTestContext(t, "")
	require.Equal(t, "fail", c.String("kaz-upgrade-busy"))
}

// TestKazCheckStackedMount covers when a new mount may go on and when it must
// stop instead of mounting on top of a JuiceFS mount that is still served.
func TestKazCheckStackedMount(t *testing.T) {
	root := uint64(meta.RootInode)
	cases := []struct {
		name       string
		mode       string
		handedOver bool
		ino        uint64
		inoErr     error
		wantErr    bool
	}{
		{"took over the old session", "fail", true, root, nil, false},
		{"mount point is not a JuiceFS root", "fail", false, 2, nil, false},
		{"old mount still served", "fail", false, root, nil, true},
		{"cannot stat the mount point", "fail", false, 0, errors.New("timeout"), true},
		{"stack keeps upstream behavior", "stack", false, root, nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stubKazUpgradeState(t, tc.handedOver, tc.ino, tc.inoErr)
			err := kazCheckStackedMount(kazUpgradeTestContext(t, tc.mode), "/mnt/jfs")
			if tc.wantErr {
				require.Error(t, err)
				require.Contains(t, err.Error(), "--kaz-upgrade-busy")
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestKazCheckStackedMountRejectsUnknownMode rejects a typo instead of
// silently picking one of the behaviors.
func TestKazCheckStackedMountRejectsUnknownMode(t *testing.T) {
	stubKazUpgradeState(t, true, uint64(meta.RootInode), nil)
	err := kazCheckStackedMount(kazUpgradeTestContext(t, "stak"), "/mnt/jfs")
	require.Error(t, err)
}
