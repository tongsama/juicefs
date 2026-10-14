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

	"xorm.io/xorm"
)

// doKazReplaceChunk follows dbMeta.doCompactChunk with a list of new records.
func (m *dbMeta) doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []Slice, delayed []byte, delayedKey uint64) syscall.Errno {
	st := errno(m.txn(func(s *xorm.Session) error {
		var c2 = chunk{Inode: inode, Indx: indx}
		_, err := s.ForUpdate().MustCols("indx").Get(&c2)
		if err != nil {
			return err
		}
		if len(c2.Slices) < len(origin) || !bytes.Equal(origin, c2.Slices[:len(origin)]) {
			logger.Infof("chunk %d:%d was changed %d -> %d", inode, indx, len(origin), len(c2.Slices))
			return syscall.EINVAL
		}
		c2.Slices = append(append([]byte{}, replaced...), c2.Slices[len(origin):]...)
		if _, err := s.Cols("slices").Where("Inode = ? AND indx = ?", inode, indx).Update(c2); err != nil {
			return err
		}
		for _, c := range created {
			if err = mustInsert(s, sliceRef{c.Id, c.Size, 1}); err != nil { // create the key to tracking it
				return err
			}
		}
		if delayed != nil {
			if len(delayed) > 0 {
				if err = mustInsert(s, &delslices{delayedKey, time.Now().Unix(), delayed}); err != nil {
					return err
				}
			}
		} else {
			for _, s_ := range removed {
				if s_.id == 0 {
					continue
				}
				if _, err := s.Exec(m.sqlConv("update chunk_ref set refs=refs-1 where chunkid=? AND size=?"), s_.id, s_.size); err != nil {
					return err
				}
			}
		}
		m.genLog(Background(), s, time.Now().UnixNano(), "KAZCOMPACTCHUNK(%d,%d,%d,%d,%d)", inode, indx, len(origin)/sliceBytes, len(replaced)/sliceBytes, len(created))
		return nil
	}, inode))
	// there could be false-negative that the replacement is successful, double-check
	if st != 0 && st != syscall.EINVAL && len(created) > 0 {
		var ok bool
		if err := m.simpleTxn(Background(), func(s *xorm.Session) error {
			var e error
			ok, e = s.Get(&sliceRef{Id: created[0].Id})
			return e
		}); err == nil {
			if ok {
				st = 0
			} else {
				st = syscall.EINVAL
			}
		}
	}
	if st == syscall.EINVAL {
		_ = m.txn(func(s *xorm.Session) error {
			for _, c := range created {
				if err := mustInsert(s, &sliceRef{c.Id, c.Size, 0}); err != nil {
					return err
				}
			}
			return nil
		})
	} else if st == 0 && delayed == nil {
		for _, s := range removed {
			if s.id == 0 {
				continue
			}
			var ref = sliceRef{Id: s.id}
			var ok bool
			err := m.simpleTxn(Background(), func(s *xorm.Session) error {
				var e error
				ok, e = s.Get(&ref)
				return e
			})
			if err == nil && ok && ref.Refs <= 0 {
				m.enqueueCompactionDelete(s.id, s.size)
			}
		}
	}
	return st
}
