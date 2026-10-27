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
	"fmt"
	"syscall"
	"time"

	"xorm.io/xorm"
)

// doKazReplaceChunk follows dbMeta.doCompactChunk with a list of new records.
func (m *dbMeta) doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []kazCreated, delayed []byte, delayedKey uint64, onDead func(id uint64, size uint32)) syscall.Errno {
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
			if err = mustInsert(s, sliceRef{c.Id, c.Size, c.Refs}); err != nil { // create the key to tracking it
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
		// a slice referenced by several removed records is announced once
		for _, s := range kazDedupDeletes(removed, func(s *slice) bool {
			var ref = sliceRef{Id: s.id}
			var ok bool
			err := m.simpleTxn(Background(), func(s *xorm.Session) error {
				var e error
				ok, e = s.Get(&ref)
				return e
			})
			return err == nil && ok && ref.Refs <= 0
		}) {
			m.enqueueCompactionDelete(s.id, s.size)
			// kaz: tell the caller which slices lost their last reference
			if onDead != nil {
				onDead(s.id, s.size)
			}
		}
	}
	return st
}

// kazDeltaCapable tells that SQL counts shared slice references correctly (clone adds one per record).
func (m *dbMeta) kazDeltaCapable() bool { return true }

// doWriteSlices appends slices to one chunk in a single transaction: one
// upsert of the concatenated slices, one multi-row insert of their references
// and one update of the inode.
func (m *dbMeta) doWriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
	return errno(m.txn(func(s *xorm.Session) error {
		*delta = dirStat{}
		nodeAttr := node{Inode: inode}
		ok, err := s.ForUpdate().Get(&nodeAttr)
		if err != nil {
			return err
		}
		if !ok {
			return syscall.ENOENT
		}
		if nodeAttr.Type != TypeFile {
			return syscall.EPERM
		}
		// Single writes compact a chunk synchronously at maxSlices; a batch must not
		// jump past it (MySQL keeps a chunk's slices in a 64 KiB BLOB).
		ck := chunk{Inode: inode, Indx: indx}
		if _, err := s.MustCols("indx").Get(&ck); err != nil {
			return err
		}
		current := len(ck.Slices) / sliceBytes
		if current+len(slices) > maxSlices {
			return errWriteSlicesFallback
		}
		oldLength := nodeAttr.Length
		for _, w := range slices {
			if newleng := uint64(indx)*ChunkSize + uint64(w.Off) + uint64(w.Slice.Len); newleng > nodeAttr.Length {
				nodeAttr.Length = newleng
			}
		}
		delta.length = int64(nodeAttr.Length - oldLength)
		delta.space = align4K(nodeAttr.Length) - align4K(oldLength)
		if err := m.checkQuota(ctx, delta.space, 0, nodeAttr.Uid, nodeAttr.Gid, m.getParents(s, inode, nodeAttr.Parent)...); err != 0 {
			return err
		}
		now := time.Now().UnixNano()
		nodeAttr.setMtime(mtime.UnixNano())
		nodeAttr.setCtime(now)
		m.parseAttr(&nodeAttr, attr)

		buf := make([]byte, 0, len(slices)*sliceBytes)
		refs := make([]sliceRef, len(slices))
		for i, w := range slices {
			buf = append(buf, marshalSlice(w.Off, w.Slice.Id, w.Slice.Size, w.Slice.Off, w.Slice.Len)...)
			refs[i] = sliceRef{w.Slice.Id, w.Slice.Size, 1}
		}
		var insert bool
		if err = m.upsertSlice(s, inode, indx, buf, &insert); err != nil {
			return err
		}
		// A slice of beans is inserted with one multi-row INSERT.
		if n, err := s.Insert(refs); err != nil {
			return err
		} else if n != int64(len(refs)) {
			return fmt.Errorf("%d of %d slice references inserted", n, len(refs))
		}
		_, err = s.Cols("length", "mtime", "ctime", "mtimensec", "ctimensec").Update(&nodeAttr, &node{Inode: inode})
		if err == nil {
			*numSlices = current + len(slices)
		}
		if err == nil {
			for _, w := range slices {
				m.genLog(ctx, s, now, "WRITE(%d,%d,%d,%d,%d,%d,%d):%d", inode, indx, w.Off, w.Slice.Id, w.Slice.Len, attr.Mtime, attr.Mtimensec, *numSlices)
			}
		}
		return err
	}, inode))
}
