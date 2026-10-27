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

// Diagnostic traces that kaz adds to upstream hot paths (redis txn,
// baseMeta.Write, baseMeta.compactChunk). Each trace is a value type kept in
// a local variable of the traced function, so it stays on the stack; the
// upstream function only calls its methods at the points where it used to
// take a timestamp or print a line. The text of every line is parsed by the
// investigation scripts and must not change (see trace_kaz_test.go).

import (
	"syscall"
	"time"

	"github.com/sirupsen/logrus"
)

// kazSlowTrace is the duration from which a traced operation is reported as slow.
const kazSlowTrace = time.Second

// kazDebug reports whether debug lines are printed. Checking it before a
// Debugf call skips boxing the arguments, which logrus would drop anyway.
func kazDebug() bool {
	return logger.IsLevelEnabled(logrus.DebugLevel)
}

// kazTxnTrace times one redis metadata transaction: the wait for the local
// transaction lock, the active time after it, and the number of attempts.
type kazTxnTrace struct {
	key         string
	start       time.Time // start of the whole transaction (upstream's start)
	lockStart   time.Time
	activeStart time.Time
	lockWait    time.Duration
	attempts    int
}

// newKazTxnTrace logs that the transaction on key waits for the local lock
// and starts timing that wait. start is the transaction start taken by the
// caller, used for the slow report.
func newKazTxnTrace(key string, start time.Time) kazTxnTrace {
	if kazDebug() {
		logger.Debugf("metadata transaction key=%s phase=lock_wait", key)
	}
	return kazTxnTrace{key: key, start: start, lockStart: time.Now()}
}

// lockAcquired records the lock wait right after the lock is taken, starts
// the active time and logs it.
func (t *kazTxnTrace) lockAcquired() {
	t.lockWait = time.Since(t.lockStart)
	t.activeStart = time.Now()
	if kazDebug() {
		logger.Debugf("metadata transaction key=%s phase=active lock_wait=%s", t.key, t.lockWait)
	}
}

// attempt counts one try of the transaction body.
func (t *kazTxnTrace) attempt() {
	t.attempts++
}

// done reports a transaction that took at least kazSlowTrace, with its
// final result. It runs after the local lock is released.
func (t *kazTxnTrace) done(result error) {
	if total := time.Since(t.start); total >= kazSlowTrace {
		logger.Warnf("slow metadata transaction key=%s total=%s lock_wait=%s active=%s attempts=%d err=%v",
			t.key, total, t.lockWait, time.Since(t.activeStart), t.attempts, result)
	}
}

// kazCompactTrace times one upstream-mode chunk compaction: the wait in the
// compaction queue, the object (CompactChunk message) and metadata phases.
type kazCompactTrace struct {
	inode        Ino
	indx         uint32
	once, force  bool
	loggedWait   bool
	start        time.Time
	phaseStart   time.Time
	waited       time.Duration // time in the compaction queue
	objectTime   time.Duration
	metadataTime time.Duration
	sliceCount   int
	compactSize  uint32
}

// newKazCompactTrace starts timing a compaction of inode:indx at the call.
func newKazCompactTrace(inode Ino, indx uint32, once, force bool) kazCompactTrace {
	return kazCompactTrace{inode: inode, indx: indx, once: once, force: force, start: time.Now()}
}

// queueWait logs, once, that the compaction waits for another compaction
// of the same chunk to finish.
func (t *kazCompactTrace) queueWait() {
	if !t.loggedWait {
		if kazDebug() {
			logger.Debugf("compaction inode=%d chunk=%d phase=queue_wait once=%t force=%t", t.inode, t.indx, t.once, t.force)
		}
		t.loggedWait = true
	}
}

// queued records the queue wait once the chunk is marked as compacting and
// logs the start of the read phase.
func (t *kazCompactTrace) queued() {
	t.waited = time.Since(t.start)
	if kazDebug() {
		logger.Debugf("compaction inode=%d chunk=%d phase=read queue_wait=%s", t.inode, t.indx, t.waited)
	}
}

// setSlices records the number and total size of the slices to compact.
func (t *kazCompactTrace) setSlices(n int, size uint32) {
	t.sliceCount, t.compactSize = n, size
}

// objectBegin logs and starts timing the object phase writing slice id.
func (t *kazCompactTrace) objectBegin(id uint64) {
	if kazDebug() {
		logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=object slices=%d bytes=%d", t.inode, t.indx, id, t.sliceCount, t.compactSize)
	}
	t.phaseStart = time.Now()
}

// objectDone ends the object phase.
func (t *kazCompactTrace) objectDone() {
	t.objectTime = time.Since(t.phaseStart)
}

// metadataBegin logs and starts timing the metadata phase for slice id.
func (t *kazCompactTrace) metadataBegin(id uint64) {
	if kazDebug() {
		logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=metadata object=%s", t.inode, t.indx, id, t.objectTime)
	}
	t.phaseStart = time.Now()
}

// metadataDone ends the metadata phase and logs its result.
func (t *kazCompactTrace) metadataDone(id uint64, st syscall.Errno) {
	t.metadataTime = time.Since(t.phaseStart)
	if kazDebug() {
		logger.Debugf("compaction inode=%d chunk=%d slice=%d phase=done errno=%s", t.inode, t.indx, id, st)
	}
}

// done reports a compaction that took at least kazSlowTrace since the trace began.
func (t *kazCompactTrace) done() {
	if total := time.Since(t.start); total >= kazSlowTrace {
		kazLogSlowCompaction(t.inode, t.indx, t.once, t.force, t.sliceCount, t.compactSize, total, t.waited, t.objectTime, t.metadataTime, "")
	}
}

// kazLogSlowCompaction prints the slow-compaction report shared by the
// upstream and the kaz (block/delta) compactions. mode is empty for the
// upstream compaction, whose line has no mode= field.
func kazLogSlowCompaction(inode Ino, indx uint32, once, force bool, slices int, size uint32, total, queueWait, object, metadata time.Duration, mode string) {
	if mode == "" {
		logger.Warnf("slow compaction inode=%d chunk=%d once=%t force=%t slices=%d bytes=%d total=%s queue_wait=%s object=%s metadata=%s",
			inode, indx, once, force, slices, size, total, queueWait, object, metadata)
		return
	}
	logger.Warnf("slow compaction inode=%d chunk=%d once=%t force=%t slices=%d bytes=%d total=%s queue_wait=%s object=%s metadata=%s mode=%s",
		inode, indx, once, force, slices, size, total, queueWait, object, metadata, mode)
}

// kazWriteTrace times one baseMeta.Write: the wait for the open-file lock
// and the phases recorded in writePhases by writeLocked.
type kazWriteTrace struct {
	inode     Ino
	indx      uint32
	slice     uint64
	start     time.Time // also the start passed to timeit
	lockStart time.Time
	lockWait  time.Duration
}

// newKazWriteTrace starts timing a write of slice into inode:indx at the
// call and logs that it waits for the open-file lock.
func newKazWriteTrace(inode Ino, indx uint32, slice uint64) kazWriteTrace {
	t := kazWriteTrace{inode: inode, indx: indx, slice: slice, start: time.Now()}
	if kazDebug() {
		logger.Debugf("metadata write inode=%d chunk=%d slice=%d phase=lock_wait", inode, indx, slice)
	}
	return t
}

// lockBegin starts timing the wait for the open-file lock.
func (t *kazWriteTrace) lockBegin() {
	t.lockStart = time.Now()
}

// lockAcquired records the open-file lock wait.
func (t *kazWriteTrace) lockAcquired() {
	t.lockWait = time.Since(t.lockStart)
}

// writing logs the start of the backend write.
func (t *kazWriteTrace) writing() {
	if kazDebug() {
		logger.Debugf("metadata write inode=%d chunk=%d slice=%d phase=doWrite lock_wait=%s", t.inode, t.indx, t.slice, t.lockWait)
	}
}

// written logs the result of the write.
func (t *kazWriteTrace) written(st syscall.Errno) {
	if kazDebug() {
		logger.Debugf("metadata write inode=%d chunk=%d slice=%d phase=done errno=%s", t.inode, t.indx, t.slice, st)
	}
}

// done reports a write that took at least kazSlowTrace, with its phases.
func (t *kazWriteTrace) done(st syscall.Errno, ph *writePhases) {
	if total := time.Since(t.start); total >= kazSlowTrace {
		logger.Warnf("slow metadata write inode=%d chunk=%d slice=%d slices=%d total=%s lock_wait=%s doWrite=%s stat=%s compact=%s errno=%s",
			t.inode, t.indx, t.slice, ph.numSlices, total, t.lockWait, ph.backend, ph.stat, ph.compact, st)
	}
}
