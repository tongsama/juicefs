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
	"bytes"
	"syscall"
	"time"
)

// doKazReplaceChunk follows kvMeta.doCompactChunk with a list of new records.
func (m *kvMeta) doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []kazCreated, delayed []byte, delayedKey uint64) syscall.Errno {
	st := errno(m.txn(Background(), func(tx *kvTxn) error {
		buf2 := tx.get(m.chunkKey(inode, indx))
		if len(buf2) < len(origin) || !bytes.Equal(origin, buf2[:len(origin)]) {
			logger.Infof("chunk %d:%d was changed %d -> %d", inode, indx, len(origin), len(buf2))
			return syscall.EINVAL
		}
		buf2 = append(append([]byte{}, replaced...), buf2[len(origin):]...)
		if len(buf2) == 0 {
			tx.delete(m.chunkKey(inode, indx))
		} else {
			tx.set(m.chunkKey(inode, indx), buf2)
		}
		for _, c := range created {
			tx.set(m.sliceKey(c.Id, c.Size), packCounter(int64(c.Refs-1))) // create the key to tracking it, holding Refs-1
		}
		if delayed != nil {
			if len(delayed) > 0 {
				tx.set(m.delSliceKey(time.Now().Unix(), delayedKey), delayed)
			}
		} else {
			for _, s := range removed {
				if s.id > 0 {
					tx.incrBy(m.sliceKey(s.id, s.size), -1)
				}
			}
		}
		m.genLog(tx, time.Now(), "KAZCOMPACTCHUNK(%d,%d,%d,%d,%d)", inode, indx, len(origin)/sliceBytes, len(replaced)/sliceBytes, len(created))
		return nil
	}, inode))
	// there could be false-negative that the replacement is successful, double-check
	if st != 0 && st != syscall.EINVAL && len(created) > 0 {
		if refs, e := m.get(m.sliceKey(created[0].Id, created[0].Size)); e == nil {
			if len(refs) > 0 {
				st = 0
			} else {
				st = syscall.EINVAL
			}
		}
	}
	if st == syscall.EINVAL {
		_ = m.txn(Background(), func(tx *kvTxn) error {
			for _, c := range created {
				tx.incrBy(m.sliceKey(c.Id, c.Size), -1)
			}
			return nil
		})
	} else if st == 0 {
		for _, c := range created {
			m.cleanupZeroRef(c.Id, c.Size)
		}
		if delayed == nil {
			// a slice referenced by several removed records is announced once
			for _, s := range kazDedupDeletes(removed, func(s *slice) bool {
				var refs int64
				return m.client.txn(Background(), func(tx *kvTxn) error {
					refs = tx.incrBy(m.sliceKey(s.id, s.size), 0)
					return nil
				}, 0) == nil && refs < 0
			}) {
				m.enqueueCompactionDelete(s.id, s.size)
			}
		}
	}
	return st
}
