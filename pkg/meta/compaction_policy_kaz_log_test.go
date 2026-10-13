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
	"time"

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

// TestKazCompactionPolicyStartLogs reports the policy state once at start and warns about the legacy scheduler.
func TestKazCompactionPolicyStartLogs(t *testing.T) {
	for _, tc := range []struct {
		name      string
		scheduler string
		mutate    func(*Config)
		info      []string
		warn      bool
	}{
		{"off", "priority", func(*Config) {}, []string{"kaz compaction policy: off"}, false},
		{"priority", "priority", func(c *Config) { c.KazCompactionReadMin = 100; c.KazCompactionCooldown = time.Minute }, []string{"read-min=100", "cooldown=1m0s"}, false},
		{"legacy", "legacy", func(c *Config) { c.KazCompactionCooldown = time.Minute }, []string{"cooldown=1m0s"}, true},
		{"legacy_read_min_only", "legacy", func(c *Config) { c.KazCompactionReadMin = 100 }, []string{"read-min=100"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := newGCTestBackend(t, "memkv")
			b.conf.CompactionScheduler = tc.scheduler
			tc.mutate(b.conf)
			hook := capturePolicyLogs(t, logrus.InfoLevel)
			b.startCompactionPolicy()
			defer b.stopCompactionPolicy()
			if !hasLog(hook, logrus.InfoLevel, tc.info...) {
				t.Fatalf("missing INFO %v in %v", tc.info, hook.AllEntries())
			}
			if got := hasLog(hook, logrus.WarnLevel, "priority"); got != tc.warn {
				t.Fatalf("legacy warning=%v, want %v", got, tc.warn)
			}
		})
	}
}

// TestCompactionPolicyOverflowLogs leaves a DEBUG trace when the held map is full.
func TestCompactionPolicyOverflowLogs(t *testing.T) {
	p, _, _, _ := newTestPolicy(5, time.Minute, 700)
	for i := 0; i < kazCompactionMaxDeferred; i++ {
		p.onCommit(Ino(i+1), 0, 99)
		p.request(Ino(i+1), 0, 99, 0, false)
	}
	hook := capturePolicyLogs(t, logrus.DebugLevel)
	p.onCommit(100000, 0, 99)
	p.request(100000, 0, 99, 0, false)
	if !hasLog(hook, logrus.DebugLevel, "kaz compaction overflow", "inode=100000") {
		t.Fatalf("missing overflow DEBUG in %v", hook.AllEntries())
	}
}
