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

// capturePolicyLogs installs a local log capture at level without dropping existing hooks.
func capturePolicyLogs(t *testing.T, level logrus.Level) *logtest.Hook {
	t.Helper()
	previousHooks := logger.ReplaceHooks(make(logrus.LevelHooks))
	activeHooks := make(logrus.LevelHooks)
	for l, hooks := range previousHooks {
		activeHooks[l] = append([]logrus.Hook(nil), hooks...)
	}
	logger.ReplaceHooks(activeHooks)
	hook := logtest.NewLocal(&logger.Logger)
	previousLevel := logger.GetLevel()
	logger.SetLevel(level)
	t.Cleanup(func() {
		logger.ReplaceHooks(previousHooks)
		logger.SetLevel(previousLevel)
	})
	return hook
}

// hasLog reports whether a captured entry at level contains every fragment.
func hasLog(hook *logtest.Hook, level logrus.Level, fragments ...string) bool {
	for _, e := range hook.AllEntries() {
		if e.Level != level {
			continue
		}
		ok := true
		for _, f := range fragments {
			if !strings.Contains(e.Message, f) {
				ok = false
				break
			}
		}
		if ok {
			return true
		}
	}
	return false
}

// TestKazCompactionPolicyStartLogs reports once at start whether the policy is on.
func TestKazCompactionPolicyStartLogs(t *testing.T) {
	for _, tc := range []struct {
		name    string
		readMin int
		info    string
	}{
		{"off", 0, "kaz compaction policy: off"},
		{"on", 100, "kaz compaction policy: read-min=100"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newGCTestBackend(t, "memkv")
			b.conf.KazCompactionReadMin = tc.readMin
			hook := capturePolicyLogs(t, logrus.InfoLevel)
			b.startCompactionPolicy()
			defer b.stopCompactionPolicy()
			if !hasLog(hook, logrus.InfoLevel, tc.info) {
				t.Fatalf("missing INFO %q in %v", tc.info, hook.AllEntries())
			}
		})
	}
}
