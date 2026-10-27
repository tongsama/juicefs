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
	"context"
	"fmt"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/urfave/cli/v2"
)

const (
	// kazUpgradeBusyFail stops a mount that would go on top of a JuiceFS mount still in service.
	kazUpgradeBusyFail = "fail"
	// kazUpgradeBusyStack keeps upstream behavior: mount on top of the old mount.
	kazUpgradeBusyStack = "stack"
)

// kazFuseHandedOver reports whether this process took over the FUSE session of
// the old mount (shutdownGraceful leaves fuseFd set only when it did).
var kazFuseHandedOver = func() bool { return fuseFd != 0 }

// kazMountInode returns the inode of the mount point, bounded like the upstream checks.
var kazMountInode = func(mp string) (ino uint64, err error) {
	err = utils.WithTimeout(context.TODO(), func(context.Context) error {
		var e error
		ino, e = utils.GetFileInode(mp)
		return e
	}, 3*time.Second)
	return ino, err
}

// kazUpgradeFlags returns the kaz flags of mount that only apply on Unix.
func kazUpgradeFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:  "kaz-upgrade-busy",
			Value: kazUpgradeBusyFail,
			Usage: "[kaz] what to do when the mount point is already mounted by JuiceFS and its session cannot be handed over (the old mount is busy for more than 10 seconds, or its volume or options differ): fail (exit with an error and leave the old mount serving) or stack (mount on top of it, as upstream does, which leaves two mounts on the same path)",
			Action: func(c *cli.Context, v string) error {
				return kazValidateUpgradeBusy(v)
			},
		},
	}
}

// kazValidateUpgradeBusy rejects values other than fail and stack.
func kazValidateUpgradeBusy(v string) error {
	if v != kazUpgradeBusyFail && v != kazUpgradeBusyStack {
		return fmt.Errorf("invalid --kaz-upgrade-busy %q: use fail or stack", v)
	}
	return nil
}

// kazCheckStackedMount runs after the graceful upgrade attempt of launchMount.
// With --kaz-upgrade-busy=fail it returns an error when the new mount would go
// on top of a JuiceFS mount that is still being served, so that the old mount
// keeps serving alone instead of being hidden under a second mount.
func kazCheckStackedMount(c *cli.Context, mp string) error {
	mode := c.String("kaz-upgrade-busy")
	if err := kazValidateUpgradeBusy(mode); err != nil {
		return err
	}
	if mode == kazUpgradeBusyStack || csiCommPath != "" || kazFuseHandedOver() {
		return nil
	}
	ino, err := kazMountInode(mp)
	if err != nil {
		return fmt.Errorf("mount point %q: cannot tell whether it is still mounted (%s); not mounting on top of it (--kaz-upgrade-busy=fail)", mp, err)
	}
	if ino != uint64(meta.RootInode) {
		return nil
	}
	return fmt.Errorf("mount point %q is still served by another JuiceFS mount that could not be handed over; "+
		"not mounting on top of it (--kaz-upgrade-busy=fail). The old mount keeps serving. "+
		"Retry when it is idle, umount it first, or use --kaz-upgrade-busy=stack", mp)
}
