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

	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
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
