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
	"fmt"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

// doKazReplaceChunk follows redisMeta.doCompactChunk with a list of new records.
func (m *redisMeta) doKazReplaceChunk(inode Ino, indx uint32, origin, replaced []byte, removed []*slice, created []Slice, delayed []byte, delayedKey uint64) syscall.Errno {
	var rs []*redis.IntCmd // trash disabled: check reference of slices
	if delayed == nil {
		rs = make([]*redis.IntCmd, len(removed))
	}
	key := m.chunkKey(inode, indx)
	ctx := Background()
	st := errno(m.txn(ctx, func(tx *redis.Tx) error {
		n := len(origin) / sliceBytes
		vals, err := tx.LRange(ctx, key, 0, int64(n-1)).Result()
		if err != nil {
			return err
		}
		if len(vals) != n {
			return syscall.EINVAL
		}
		for i, val := range vals {
			if val != string(origin[i*sliceBytes:(i+1)*sliceBytes]) {
				return syscall.EINVAL
			}
		}
		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.LTrim(ctx, key, int64(n), -1)
			for i := len(replaced) / sliceBytes; i > 0; i-- {
				pipe.LPush(ctx, key, replaced[(i-1)*sliceBytes:i*sliceBytes])
			}
			for _, c := range created {
				pipe.HSet(ctx, m.sliceRefs(), m.sliceKey(c.Id, c.Size), "0") // create the key to tracking it
			}
			if delayed != nil {
				if len(delayed) > 0 {
					pipe.HSet(ctx, m.delSlices(), fmt.Sprintf("%d_%d", delayedKey, time.Now().Unix()), delayed)
				}
			} else {
				for i, s := range removed {
					if s.id > 0 {
						rs[i] = pipe.HIncrBy(ctx, m.sliceRefs(), m.sliceKey(s.id, s.size), -1)
					}
				}
			}
			m.genLog(ctx, pipe, time.Now(), "KAZCOMPACTCHUNK(%d,%d,%d,%d,%d)", inode, indx, n, len(replaced)/sliceBytes, len(created))
			return nil
		})
		return err
	}, key))
	// there could be false-negative that the replacement is successful, double-check
	if st != 0 && st != syscall.EINVAL && len(created) > 0 {
		if e := m.rdb.HGet(ctx, m.sliceRefs(), m.sliceKey(created[0].Id, created[0].Size)).Err(); e == nil {
			st = 0
		} else if e == redis.Nil {
			st = syscall.EINVAL
		}
	}
	if st == syscall.EINVAL {
		for _, c := range created {
			m.rdb.HIncrBy(ctx, m.sliceRefs(), m.sliceKey(c.Id, c.Size), -1)
		}
	} else if st == 0 {
		for _, c := range created {
			m.cleanupZeroRef(m.sliceKey(c.Id, c.Size))
		}
		if delayed == nil {
			for i, s := range removed {
				if s.id > 0 && rs[i].Err() == nil && rs[i].Val() < 0 {
					m.enqueueCompactionDelete(s.id, s.size)
				}
			}
		}
	}
	return st
}
