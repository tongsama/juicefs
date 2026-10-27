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

package meta

import (
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

// TestConfigDeleteWarnings catches silent synchronous deletion and preserves the disabled-deletion warning.
func TestConfigDeleteWarnings(t *testing.T) {
	// Install a local capture hook without changing output or discarding existing hooks.
	previousHooks := logger.ReplaceHooks(make(logrus.LevelHooks))
	activeHooks := make(logrus.LevelHooks)
	for level, hooks := range previousHooks {
		activeHooks[level] = append([]logrus.Hook(nil), hooks...)
	}
	logger.ReplaceHooks(activeHooks)
	hook := logtest.NewLocal(&logger.Logger)
	previousLevel := logger.GetLevel()
	logger.SetLevel(logrus.WarnLevel)
	t.Cleanup(func() {
		logger.ReplaceHooks(previousHooks)
		logger.SetLevel(previousLevel)
	})

	for _, tc := range []struct {
		name     string
		value    int
		concepts []string
	}{
		{"negative", -1, []string{"synchronous", "unlimited", "compaction", "positive"}},
		{"other_negative", -10, []string{"synchronous", "unlimited", "compaction", "positive"}},
		{"disabled", 0, []string{"disabled"}},
		{"workers", 10, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook.Reset()
			conf := DefaultConf()
			conf.MaxDeletes = tc.value
			conf.SelfCheck()
			var warnings []string
			for _, entry := range hook.AllEntries() {
				if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "max-deletes") {
					warnings = append(warnings, strings.ToLower(entry.Message))
				}
			}
			if tc.concepts == nil {
				if len(warnings) != 0 {
					t.Fatalf("positive worker count produced deletion warnings: %v", warnings)
				}
				return
			}
			if len(warnings) != 1 {
				t.Fatalf("max-deletes=%d: expected one deletion warning, got %v", tc.value, warnings)
			}
			for _, concept := range tc.concepts {
				if !strings.Contains(warnings[0], concept) {
					t.Errorf("warning must explain %q: %s", concept, warnings[0])
				}
			}
			if conf.MaxDeletes != tc.value {
				t.Errorf("SelfCheck changed deletion mode from %d to %d", tc.value, conf.MaxDeletes)
			}
		})
	}
}
