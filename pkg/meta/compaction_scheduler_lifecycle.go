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

// startPriorityCompactor attaches the kaz compaction policy, logs the compaction mode and, for the priority
// scheduler, request-derived workers, independently of periodic background jobs.
func (m *baseMeta) startPriorityCompactor() {
	m.startCompactionPolicy()
	m.logKazCompactionMode()
	if m.conf.CompactionScheduler != "priority" || m.conf.ReadOnly || m.priorityCompactor.Load() != nil {
		return
	}
	s := newCompactionScheduler(func(inode Ino, chunk uint32, tier int) {
		m.backgroundCompactChunk(inode, chunk, tier)
	}, 11, 1024)
	if m.kazCompactMetrics != nil {
		s.setObserver(m.kazCompactMetrics.observeStart)
	}
	m.priorityCompactor.Store(s)
}

// stopPriorityCompactor detaches the kaz policy first, then joins scheduler
// callbacks before the metadata connection can be shut down.
func (m *baseMeta) stopPriorityCompactor() {
	m.stopCompactionPolicy()
	if s := m.priorityCompactor.Swap(nil); s != nil {
		s.close()
		s.wait()
	}
}

// requestBackgroundCompaction uses existing counts without another database read or global scan.
func (m *baseMeta) requestBackgroundCompaction(inode Ino, chunk uint32, count, tier int) {
	if m.conf.CompactionScheduler == "priority" {
		if s := m.priorityCompactor.Load(); s != nil {
			s.schedule(inode, chunk, count, tier)
			return
		}
	}
	go m.backgroundCompactChunk(inode, chunk, tier)
}
