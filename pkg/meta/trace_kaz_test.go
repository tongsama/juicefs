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
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

// Regular expressions and substring tests copied verbatim from the analysis
// scripts that parse these logs, so that a format change breaks this test.
var (
	// docs/inspection/cache_churn/2026-10-20/compaction_overnight.py:25-27 (RE_COMP)
	kazTestReComp = regexp.MustCompile(
		`^(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d\.\d+) .*slow compaction inode=(\d+) chunk=(\d+) once=(\w+) force=(\w+) ` +
			`slices=(\d+) bytes=(\d+) total=(\S+) queue_wait=(\S+) object=(\S+) metadata=(\S+)(?: mode=(\w+))?`)
	// docs/inspection/cache_churn/2026-10-20/compaction_overnight.py:30-31 (RE_SLOWWRITE)
	kazTestReSlowWrite = regexp.MustCompile(`^(\d{4}/\d\d/\d\d \d\d:\d\d:\d\d)\.\d+ .*slow metadata write.* inode=(\d+) chunk=(\d+) .*slices=(\d+)`)
	// docs/inspection/incident_recurrence/2026-10-02/compaction_queue_analysis.py:8 (seconds)
	kazTestReDuration = regexp.MustCompile(`([\d.]+)(ms|µs|ns|h|m|s)`)
)

// kazTestQueueAnalysisMatches applies the line filters of
// docs/inspection/incident_recurrence/2026-10-02/compaction_queue_analysis.py:16-19
// to one line, for inode 596153 / chunk key c596153_.
func kazTestQueueAnalysisMatches(s string) bool {
	if (strings.Contains(s, "compaction inode=") || strings.Contains(s, "metadata write inode=") || strings.Contains(s, "slice commit inode=")) && strings.Contains(s, "inode=596153") {
		if strings.Contains(s, "compaction") || strings.Contains(s, "phase=compact") || strings.Contains(s, "slow metadata write") {
			return true
		}
	}
	if strings.Contains(s, "slow metadata transaction") {
		return true
	}
	return strings.Contains(s, "metadata transaction key=") && strings.Contains(s, "c596153_")
}

// kazTestLines returns the captured messages at level and the full formatted
// lines (with the timestamp prefix the scripts anchor on).
func kazTestLines(t *testing.T, entries []*logrus.Entry, level logrus.Level) (msgs, lines []string) {
	t.Helper()
	for _, e := range entries {
		if e.Level != level {
			continue
		}
		b, err := logger.Format(e)
		if err != nil {
			t.Fatal(err)
		}
		msgs = append(msgs, e.Message)
		lines = append(lines, strings.TrimRight(string(b), "\n"))
	}
	return
}

// kazTestGolden checks msgs against golden lines in order. In a golden line
// "<D>" stands for a Go duration (the only part that varies between runs);
// everything else must match byte for byte.
func kazTestGolden(t *testing.T, msgs []string, golden []string) {
	t.Helper()
	if len(msgs) != len(golden) {
		t.Fatalf("got %d lines, want %d:\n%s", len(msgs), len(golden), strings.Join(msgs, "\n"))
	}
	for i, g := range golden {
		parts := strings.Split(g, "<D>")
		for j := range parts {
			parts[j] = regexp.QuoteMeta(parts[j])
		}
		re := regexp.MustCompile("^" + strings.Join(parts, `[0-9.]+(?:h|m|s|ms|µs|ns)(?:[0-9.]+(?:m|s|ms|µs|ns))*`) + "$")
		if !re.MatchString(msgs[i]) {
			t.Errorf("line %d:\n got  %q\n want %q", i, msgs[i], g)
		}
	}
}

// TestKazTraceLogFormats pins the diagnostic log lines of the kaz trace
// helpers to the text the code printed before they were moved out of the
// upstream functions, and to the patterns of the analysis scripts.
func TestKazTraceLogFormats(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.DebugLevel)

	// redis txn: lock wait, active, slow transaction.
	tx := newKazTxnTrace("jfs:c596153_0", time.Now().Add(-2*time.Second))
	tx.lockAcquired()
	tx.attempt()
	tx.attempt()
	tx.done(syscall.ENOENT)
	// A fast transaction must not report anything at done.
	fast := newKazTxnTrace("jfs:c596153_1", time.Now())
	fast.lockAcquired()
	fast.attempt()
	fast.done(nil)

	// compactChunk: queue wait (once), phases, slow compaction.
	c := newKazCompactTrace(596153, 2, true, false)
	c.queueWait()
	c.queueWait()
	c.queued()
	c.setSlices(5, 4096)
	c.objectBegin(77)
	c.objectDone()
	c.metadataBegin(77)
	c.metadataDone(77, 0)
	c.start = c.start.Add(-2 * time.Second)
	c.done()
	// Block mode shares the slow format, with mode= appended.
	kazLogSlowCompaction(596153, 3, false, true, 7, 8192, 3*time.Second, time.Millisecond, 2*time.Second, 500*time.Millisecond, "block")

	// baseMeta.Write: phases and slow write.
	w := newKazWriteTrace(596153, 4, 99)
	w.lockBegin()
	w.lockAcquired()
	w.writing()
	w.written(syscall.ENOSPC)
	w.start = w.start.Add(-2 * time.Second)
	ph := writePhases{numSlices: 12, backend: time.Second, stat: time.Millisecond, compact: 0}
	w.done(syscall.ENOSPC, &ph)

	debugMsgs, debugLines := kazTestLines(t, hook.AllEntries(), logrus.DebugLevel)
	warnMsgs, warnLines := kazTestLines(t, hook.AllEntries(), logrus.WarnLevel)

	// Golden text: the format strings of base.go/redis.go/compaction_block_kaz.go
	// before the move, filled with the inputs above.
	kazTestGolden(t, debugMsgs, []string{
		"metadata transaction key=jfs:c596153_0 phase=lock_wait",
		"metadata transaction key=jfs:c596153_0 phase=active lock_wait=<D>",
		"metadata transaction key=jfs:c596153_1 phase=lock_wait",
		"metadata transaction key=jfs:c596153_1 phase=active lock_wait=<D>",
		"compaction inode=596153 chunk=2 phase=queue_wait once=true force=false",
		"compaction inode=596153 chunk=2 phase=read queue_wait=<D>",
		"compaction inode=596153 chunk=2 slice=77 phase=object slices=5 bytes=4096",
		"compaction inode=596153 chunk=2 slice=77 phase=metadata object=<D>",
		"compaction inode=596153 chunk=2 slice=77 phase=done errno=errno 0",
		"metadata write inode=596153 chunk=4 slice=99 phase=lock_wait",
		"metadata write inode=596153 chunk=4 slice=99 phase=doWrite lock_wait=<D>",
		"metadata write inode=596153 chunk=4 slice=99 phase=done errno=no space left on device",
	})
	kazTestGolden(t, warnMsgs, []string{
		"slow metadata transaction key=jfs:c596153_0 total=<D> lock_wait=<D> active=<D> attempts=2 err=no such file or directory",
		"slow compaction inode=596153 chunk=2 once=true force=false slices=5 bytes=4096 total=<D> queue_wait=<D> object=<D> metadata=<D>",
		"slow compaction inode=596153 chunk=3 once=false force=true slices=7 bytes=8192 total=3s queue_wait=1ms object=2s metadata=500ms mode=block",
		"slow metadata write inode=596153 chunk=4 slice=99 slices=12 total=<D> lock_wait=<D> doWrite=1s stat=1ms compact=0s errno=no space left on device",
	})

	// Compaction, transaction and slow-write lines are picked up by the queue
	// analysis filters (its write filter keeps only the slow write report).
	for _, s := range append(append([]string{}, debugLines...), warnLines...) {
		if !strings.Contains(s, "compaction") && !strings.Contains(s, "transaction") && !strings.Contains(s, "slow metadata write") {
			continue
		}
		if !kazTestQueueAnalysisMatches(s) {
			t.Errorf("compaction_queue_analysis.py would skip %q", s)
		}
	}
	// The slow compaction lines parse with RE_COMP, mode only on the block line.
	modes := []string{"", "block"}
	n := 0
	for _, s := range warnLines {
		if !strings.Contains(s, "slow compaction") {
			continue
		}
		g := kazTestReComp.FindStringSubmatch(s)
		if g == nil {
			t.Fatalf("RE_COMP does not match %q", s)
		}
		if g[2] != "596153" || g[12] != modes[n] {
			t.Errorf("RE_COMP groups %q", g[1:])
		}
		for _, d := range []string{g[8], g[9], g[10], g[11]} {
			if !kazTestReDuration.MatchString(d) {
				t.Errorf("duration %q not parsed", d)
			}
		}
		n++
	}
	if n != 2 {
		t.Fatalf("got %d slow compaction lines", n)
	}
	matched := false
	for _, s := range warnLines {
		if g := kazTestReSlowWrite.FindStringSubmatch(s); g != nil {
			matched = g[2] == "596153" && g[3] == "4" && g[4] == "12"
		}
	}
	if !matched {
		t.Fatalf("RE_SLOWWRITE did not match the slow write line: %q", warnLines)
	}
}

// TestKazTraceDebugOff checks that with debug logging off nothing but the
// slow reports is printed.
func TestKazTraceDebugOff(t *testing.T) {
	hook := capturePolicyLogs(t, logrus.InfoLevel)
	tx := newKazTxnTrace("jfs:c1_0", time.Now())
	tx.lockAcquired()
	tx.attempt()
	tx.done(nil)
	if n := len(hook.AllEntries()); n != 0 {
		t.Fatalf("got %d entries", n)
	}
}

// BenchmarkKazTxnTrace measures the per-transaction cost of the trace on the
// fast path (debug off, not slow); it must not allocate.
func BenchmarkKazTxnTrace(b *testing.B) {
	previous := logger.GetLevel()
	logger.SetLevel(logrus.InfoLevel)
	defer logger.SetLevel(previous)
	key := "jfs:c1_0"
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result error
		func() {
			tr := newKazTxnTrace(key, time.Now())
			tr.lockAcquired()
			defer func() { tr.done(result) }()
			tr.attempt()
		}()
	}
}
