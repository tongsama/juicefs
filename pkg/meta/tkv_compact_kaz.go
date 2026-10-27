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
func (m *kvMeta) doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []kazCreated, delayed []byte, delayedKey uint64, onDead func(id uint64, size uint32)) syscall.Errno {
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
				// kaz: tell the caller which slices lost their last reference
				if onDead != nil {
					onDead(s.id, s.size)
				}
			}
		}
	}
	return st
}

// doWriteSlices appends slices to one chunk in a single transaction, skipping
// slices the chunk already holds (as doWrite does), and sets the chunk and the
// inode once.
func (m *kvMeta) doWriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
	if m.getFormat().ChangeLog && len(slices) > 1 {
		// genLog keys a record by the transaction ID, so several WRITE records in
		// one transaction could overwrite each other; keep one record per write.
		return errWriteSlicesFallback
	}
	return errno(m.txn(ctx, func(tx *kvTxn) error {
		*delta = dirStat{}
		*attr = Attr{}
		rs := tx.gets(m.inodeKey(inode), m.chunkKey(inode, indx))
		if rs[0] == nil {
			return syscall.ENOENT
		}
		m.parseAttr(rs[0], attr)
		if attr.Typ != TypeFile {
			return syscall.EPERM
		}
		if len(rs[1])%sliceBytes != 0 {
			logger.Errorf("Invalid chunk value for inode %d indx %d: %d", inode, indx, len(rs[1]))
			return syscall.EIO
		}
		val := append([]byte(nil), rs[1]...) // do not write into the value read in this transaction
		oldLength := attr.Length
		var added []SliceWrite
		for _, w := range slices {
			buf := marshalSlice(w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len)
			dup := false
			for i := 0; i < len(val); i += sliceBytes {
				if bytes.Equal(val[i:i+sliceBytes], buf) {
					dup = true
					break
				}
			}
			if dup {
				logger.Warnf("Write same slice for inode %d indx %d sliceId %d", inode, indx, w.Slice.Id)
				continue
			}
			if newleng := uint64(indx)*ChunkSize + uint64(w.Off) + uint64(w.Slice.Len); newleng > attr.Length {
				attr.Length = newleng
			}
			val = append(val, buf...)
			added = append(added, w)
		}
		if len(added) == 0 {
			*numSlices = len(val) / sliceBytes
			return nil
		}
		delta.length = int64(attr.Length - oldLength)
		delta.space = align4K(attr.Length) - align4K(oldLength)
		if err := m.checkQuota(ctx, delta.space, 0, attr.Uid, attr.Gid, m.getParents(tx, inode, attr.Parent)...); err != 0 {
			return err
		}
		now := time.Now()
		attr.Mtime = mtime.Unix()
		attr.Mtimensec = uint32(mtime.Nanosecond())
		attr.Ctime = now.Unix()
		attr.Ctimensec = uint32(now.Nanosecond())
		tx.set(m.inodeKey(inode), m.marshal(attr))
		tx.set(m.chunkKey(inode, indx), val)
		*numSlices = len(val) / sliceBytes
		for _, w := range added {
			m.genLog(tx, now, "WRITE(%d,%d,%d,%d,%d,%d,%d):%d", inode, indx, w.Off, w.Slice.Id, w.Slice.Len, attr.Mtime, attr.Mtimensec, *numSlices)
		}
		return nil
	}, inode))
}
