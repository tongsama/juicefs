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
	"sort"
	"strings"
)

// kazRenamedFlags maps the old names of kaz flags that were added without the
// kaz- prefix to their current names. The old names stay as aliases so that
// existing mount scripts keep working; they are deprecated.
var kazRenamedFlags = map[string]string{
	"compaction-gc-mode":   "kaz-compaction-gc-mode",
	"compaction-scheduler": "kaz-compaction-scheduler",
	"writeback-fsync":      "kaz-writeback-fsync",
	"slice-flush-wait":     "kaz-slice-flush-wait",
	"slice-flush-idle":     "kaz-slice-flush-idle",
	"writer-reuse-window":  "kaz-writer-reuse-window",
	"writer-flush-timeout": "kaz-writer-flush-timeout",
	"writer-flush-scope":   "kaz-writer-flush-scope",
	"meta-write-batch":     "kaz-meta-write-batch",
}

// kazOldFlagNamesUsed returns, sorted, the old names of kazRenamedFlags that
// appear in args as -name, --name or --name=value.
func kazOldFlagNamesUsed(args []string) []string {
	found := map[string]bool{}
	for _, a := range args {
		if !strings.HasPrefix(a, "-") {
			continue
		}
		name := strings.TrimLeft(a, "-")
		if i := strings.IndexByte(name, '='); i >= 0 {
			name = name[:i]
		}
		if _, ok := kazRenamedFlags[name]; ok {
			found[name] = true
		}
	}
	names := make([]string, 0, len(found))
	for n := range found {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// kazWarnOldFlagNames logs one warning for each deprecated old flag name in args.
func kazWarnOldFlagNames(args []string) {
	for _, old := range kazOldFlagNamesUsed(args) {
		logger.Warnf("--%s is deprecated and will be removed; use --%s", old, kazRenamedFlags[old])
	}
}
