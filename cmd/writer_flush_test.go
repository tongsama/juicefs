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
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

// TestWriterFlushOption validates the actual flag and resulting VFS configuration without mounting.
func TestWriterFlushOption(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want time.Duration
	}{
		{nil, 0}, {[]string{"--writer-flush-timeout", "0s"}, 0},
		{[]string{"--writer-flush-timeout", "auto"}, vfs.AutoWriterFlushTimeout},
		{[]string{"--writer-flush-timeout", "1h"}, time.Hour},
	} {
		t.Run(time.Duration(tc.want).String(), func(t *testing.T) {
			set := flag.NewFlagSet("flush-test", flag.ContinueOnError)
			for _, f := range clientFlags(1) {
				if err := f.Apply(set); err != nil {
					t.Fatal(err)
				}
			}
			if err := set.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			cfg := getVfsConf(cli.NewContext(nil, set, nil), meta.DefaultConf(), &meta.Format{}, &chunk.Config{})
			if cfg.WriterFlushTimeout != tc.want {
				t.Fatalf("timeout=%s, want %s", cfg.WriterFlushTimeout, tc.want)
			}
		})
	}
}

// TestWriterFlushOptionRejectsInvalid rejects invalid durations before a command action can perform I/O.
func TestWriterFlushOptionRejectsInvalid(t *testing.T) {
	for _, input := range []string{"-1s", "broken", "10", "NaN"} {
		t.Run(input, func(t *testing.T) {
			called := false
			app := &cli.App{Flags: clientFlags(1), Action: func(*cli.Context) error { called = true; return nil }}
			if err := app.Run([]string{"test", "--writer-flush-timeout", input}); err == nil {
				t.Fatal("invalid duration accepted")
			}
			if called {
				t.Fatal("command action ran with an invalid timeout")
			}
		})
	}
}

// TestWriterFlushScopeOption carries --writer-flush-scope into the VFS configuration and keeps file as the default.
func TestWriterFlushScopeOption(t *testing.T) {
	for _, tc := range []struct {
		args []string
		want string
	}{
		{nil, vfs.WriterFlushScopeFile},
		{[]string{"--writer-flush-scope", "file"}, vfs.WriterFlushScopeFile},
		{[]string{"--writer-flush-scope", "range"}, vfs.WriterFlushScopeRange},
	} {
		t.Run(tc.want, func(t *testing.T) {
			set := flag.NewFlagSet("scope-test", flag.ContinueOnError)
			for _, f := range clientFlags(1) {
				if err := f.Apply(set); err != nil {
					t.Fatal(err)
				}
			}
			if err := set.Parse(tc.args); err != nil {
				t.Fatal(err)
			}
			cfg := getVfsConf(cli.NewContext(nil, set, nil), meta.DefaultConf(), &meta.Format{}, &chunk.Config{})
			if cfg.WriterFlushScope != tc.want {
				t.Fatalf("scope=%q, want %q", cfg.WriterFlushScope, tc.want)
			}
		})
	}
}

// TestWriterFlushScopeOptionRejectsInvalid rejects unknown scopes before a command action can perform I/O.
func TestWriterFlushScopeOptionRejectsInvalid(t *testing.T) {
	for _, input := range []string{"", "chunk", "Range"} {
		t.Run(input, func(t *testing.T) {
			called := false
			app := &cli.App{Flags: clientFlags(1), Action: func(*cli.Context) error { called = true; return nil }}
			if err := app.Run([]string{"test", "--writer-flush-scope", input}); err == nil {
				t.Fatal("invalid scope accepted")
			}
			if called {
				t.Fatal("command action ran with an invalid scope")
			}
		})
	}
}
