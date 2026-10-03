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

// startPriorityCompactor attaches request-derived workers independently of periodic background jobs.
func (m *baseMeta) startPriorityCompactor() {
	if m.conf.CompactionScheduler != "priority" || m.conf.ReadOnly || m.priorityCompactor.Load() != nil {
		return
	}
	m.priorityCompactor.Store(newCompactionScheduler(func(inode Ino, chunk uint32, tier int) {
		m.compactChunk(inode, chunk, false, false, tier)
	}, 11, 1024))
}

// stopPriorityCompactor joins callbacks before the metadata connection can be shut down.
func (m *baseMeta) stopPriorityCompactor() {
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
	go m.compactChunk(inode, chunk, false, false, tier)
}
