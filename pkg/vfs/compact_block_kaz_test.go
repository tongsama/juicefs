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

package vfs

import (
	"bytes"
	"fmt"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/object"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
)

// newKazBlockTestVFS builds a VFS in kaz block-compaction mode whose CompactChunk
// messages run vfs.Compact, recording the new slice ids and their lengths.
func newKazBlockTestVFS(t *testing.T, writeback bool) (*VFS, *sync.Map) {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	metaConf.KazCompactionMode = meta.KazCompactionModeBlock
	m := meta.NewClient("memkv://", metaConf)
	format := &meta.Format{Name: "test", UUID: uuid.New().String(), Storage: "mem", BlockSize: 4, Compression: "lz4", DirStats: true}
	require.NoError(t, m.Init(format, true))
	cconf := &chunk.Config{
		BlockSize: format.BlockSize * 1024, Compress: format.Compression, MaxUpload: 2, MaxDownload: 200,
		BufferSize: 30 << 20, CacheSize: 10 << 20, CacheDir: "memory",
	}
	if writeback {
		cconf.CacheDir = t.TempDir()
		cconf.Writeback = true
		cconf.WritebackThresholdSize = cconf.BlockSize + 1 // stage every block
	}
	blob, _ := object.CreateStorage("mem", "", "", "", "")
	registry := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, *cconf, registry)
	conf := &Config{Meta: metaConf, Format: *format, Version: "Juicefs", Chunk: cconf, FuseOpts: &FuseOptions{}}
	v := NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", registry), registry)
	written := &sync.Map{} // new slice id -> length
	m.OnMsg(meta.CompactChunk, func(args ...interface{}) error {
		err := Compact(*cconf, store, args[0].([]meta.Slice), args[1].(uint64), args[2].(uint8))
		if err == nil {
			var n uint32
			for _, s := range args[0].([]meta.Slice) {
				n += s.Len
			}
			written.Store(args[1].(uint64), n)
		}
		return err
	})
	return v, written
}

// TestKazBlockCompactionDataVFS writes a base and small updates, lets a read
// trigger background block compaction, and checks every byte afterwards.
func TestKazBlockCompactionDataVFS(t *testing.T) {
	for _, wb := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeback=%t", wb), func(t *testing.T) {
			v, written := newKazBlockTestVFS(t, wb)
			ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
			const B = 4096
			fe, fh, st := v.Create(ctx, 1, "image", 0644, 0, syscall.O_RDWR)
			require.Zero(t, st)
			want := make([]byte, 16*B)
			for i := range want {
				want[i] = byte(i%251 + 1)
			}
			require.Zero(t, v.Write(ctx, fe.Inode, want, 0, fh))
			require.Zero(t, v.Flush(ctx, fe.Inode, fh, 0))
			for i, off := range []int{2*B + 100, 5*B + 7, 11*B + 50, 2*B + 3000, 5*B + 4000} {
				upd := bytes.Repeat([]byte{byte(200 + i)}, 60)
				require.Zero(t, v.Write(ctx, fe.Inode, upd, uint64(off), fh))
				require.Zero(t, v.Flush(ctx, fe.Inode, fh, 0))
				copy(want[off:], upd)
			}
			buf := make([]byte, len(want))
			n, st := v.Read(ctx, fe.Inode, buf, 0, fh) // 6 records: requests compaction
			require.Zero(t, st)
			require.Equal(t, want, buf[:n])

			// Blocks 2, 5 and 11 are rewritten as three one-block slices.
			require.Eventually(t, func() bool {
				var slices []meta.Slice
				if v.Meta.Read(ctx, fe.Inode, 0, &slices) != 0 {
					return false
				}
				seen := 0
				for _, s := range slices {
					if _, ok := written.Load(s.Id); ok {
						seen++
					}
				}
				return seen == 3
			}, 10*time.Second, 20*time.Millisecond)
			written.Range(func(_, l any) bool {
				require.Equal(t, uint32(B), l.(uint32))
				return true
			})
			v.Release(ctx, fe.Inode, fh)

			_, fh, st = v.Open(ctx, fe.Inode, syscall.O_RDONLY)
			require.Zero(t, st)
			defer v.Release(ctx, fe.Inode, fh)
			buf = make([]byte, len(want))
			n, st = v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Zero(t, st)
			require.Equal(t, want, buf[:n])
		})
	}
}
