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
	"testing"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/urfave/cli/v2"
)

// TestCombinedVMOptions carries real flag defaults and opt-ins to metadata and writer configuration.
func TestCombinedVMOptions(t *testing.T) {
	for _, tc := range []struct {
		name          string
		args          []string
		gc, scheduler string
		window        int
	}{
		{"defaults", nil, "legacy", "legacy", 4},
		{"opt_in", []string{"--compaction-gc-mode", "deferred", "--compaction-scheduler", "priority", "--writer-reuse-window", "16"}, "deferred", "priority", 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			called := false
			app := &cli.App{Flags: cmdMount().Flags, Action: func(c *cli.Context) error {
				called = true
				mc := getMetaConf(c, "/combined-test", false)
				cfg := getVfsConf(c, mc, &meta.Format{}, &chunk.Config{})
				data, err := json.Marshal(mc)
				if err != nil {
					return err
				}
				var fields map[string]interface{}
				if err = json.Unmarshal(data, &fields); err != nil {
					return err
				}
				if fields["CompactionGCMode"] != tc.gc || fields["CompactionScheduler"] != tc.scheduler || cfg.WriterReuseWindow != tc.window {
					t.Errorf("config GC=%v scheduler=%v window=%d, want %s/%s/%d", fields["CompactionGCMode"], fields["CompactionScheduler"], cfg.WriterReuseWindow, tc.gc, tc.scheduler, tc.window)
				}
				return nil
			}}
			if err := app.Run(append([]string{"options-test"}, tc.args...)); err != nil {
				t.Fatal(err)
			}
			if !called {
				t.Fatal("valid option did not reach action")
			}
		})
	}
}

// TestCombinedVMOptionsRejectInvalid prevents unsafe modes from reaching mount actions.
func TestCombinedVMOptionsRejectInvalid(t *testing.T) {
	for _, args := range [][]string{
		{"--compaction-gc-mode", "bad"}, {"--compaction-scheduler", "bad"},
		{"--writer-reuse-window", "0"}, {"--writer-reuse-window", "-1"}, {"--writer-reuse-window", "65"},
		{"--compaction-gc-mode", "deferred", "--max-deletes", "0"},
		{"--compaction-gc-mode", "deferred", "--max-deletes", "-1"},
		{"--compaction-gc-mode", "deferred", "--no-bgjob"},
		{"--compaction-gc-mode", "deferred", "--read-only"},
	} {
		t.Run(args[0]+"/"+args[1], func(t *testing.T) {
			called := false
			app := &cli.App{Flags: cmdMount().Flags, Action: func(*cli.Context) error { called = true; return nil }}
			if err := app.Run(append([]string{"options-test"}, args...)); err == nil {
				t.Fatal("invalid option accepted")
			}
			if called {
				t.Fatal("invalid option reached action")
			}
		})
	}
}
