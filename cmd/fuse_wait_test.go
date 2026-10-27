//go:build linux

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
	"flag"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

// TestMountWaitPolicyReachesFuse prevents mount setup from overriding VFS completion with a watchdog.
func TestMountWaitPolicyReachesFuse(t *testing.T) {
	for _, tc := range []struct {
		name     string
		args     []string
		deadline time.Duration
	}{
		{"default", nil, 0}, {"explicit_hour", []string{"--writer-flush-timeout", "1h"}, time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			set := flag.NewFlagSet("fuse-wait", flag.ContinueOnError)
			for _, f := range cmdMount().Flags {
				if err := f.Apply(set); err != nil {
					t.Fatal(err)
				}
			}
			if err := set.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			ctx := cli.NewContext(nil, set, nil)
			format := &meta.Format{Name: "wait-test"}
			cfg := getVfsConf(ctx, meta.DefaultConf(), format, &chunk.Config{})
			setFuseOption(ctx, format, cfg)
			if cfg.WriterFlushTimeout != tc.deadline || cfg.FuseOpts.Timeout != 0 {
				t.Fatalf("mount writer timeout=%s FUSE watchdog=%s; want writer=%s and no watchdog", cfg.WriterFlushTimeout, cfg.FuseOpts.Timeout, tc.deadline)
			}
		})
	}
}
