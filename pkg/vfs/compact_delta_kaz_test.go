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
	"math/rand"
	"path/filepath"
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

// newKazDeltaTestVFS builds a VFS in kaz delta-compaction mode on a temporary
// SQLite volume (memkv falls back to block mode) with 4 KiB blocks, a 16-byte
// gap (the 16 KiB default scaled to the block size) and whole reads of objects
// up to 1 MiB. CompactChunk messages run vfs.Compact and record each new slice
// id with its length.
func newKazDeltaTestVFS(t *testing.T, writeback bool) (*VFS, *sync.Map) {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	metaConf.KazCompactionMode = meta.KazCompactionModeDelta
	metaConf.KazCompactionDeltaGap = 16
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "delta.db"), metaConf)
	format := &meta.Format{Name: "test", UUID: uuid.New().String(), Storage: "mem", BlockSize: 4, Compression: "lz4", DirStats: true}
	require.NoError(t, m.Init(format, true))
	t.Cleanup(func() { _ = m.Shutdown() })
	cconf := &chunk.Config{
		BlockSize: format.BlockSize * 1024, Compress: format.Compression, MaxUpload: 2, MaxDownload: 200,
		BufferSize: 30 << 20, CacheSize: 10 << 20, CacheDir: "memory",
		KazChunkConfig: chunk.KazChunkConfig{KazWholeReadMax: 1 << 20},
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

// kazSyncMapLen counts the entries of m.
func kazSyncMapLen(m *sync.Map) int {
	n := 0
	m.Range(func(_, _ any) bool { n++; return true })
	return n
}

// TestKazDeltaDataVFS writes a 16-block base and three rounds of 99 scattered
// 60-byte updates, checking every byte after each write and after each delta
// compaction the writes trigger, then checks that tier slices were built and
// that a fresh open reads the same bytes.
func TestKazDeltaDataVFS(t *testing.T) {
	for _, wb := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeback=%t", wb), func(t *testing.T) {
			v, written := newKazDeltaTestVFS(t, wb)
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
			buf := make([]byte, len(want))
			check := func(fh uint64) bool {
				n, st := v.Read(ctx, fe.Inode, buf, 0, fh)
				require.Zero(t, st)
				return bytes.Equal(want, buf[:n])
			}

			rng := rand.New(rand.NewSource(7))
			for round := 1; round <= 3; round++ {
				before := kazSyncMapLen(written)
				for i := 0; i < 99; i++ {
					off := rng.Intn(len(want) - 60)
					upd := bytes.Repeat([]byte{byte(rng.Intn(255) + 1)}, 60)
					require.Zero(t, v.Write(ctx, fe.Inode, upd, uint64(off), fh))
					require.Zero(t, v.Flush(ctx, fe.Inode, fh, 0))
					copy(want[off:], upd)
					require.True(t, check(fh), "round %d write %d", round, i)
				}
				// Each round's 99 records trigger one delta compaction (reads
				// re-request it if a request was skipped).
				require.Eventually(t, func() bool {
					return check(fh) && kazSyncMapLen(written) > before
				}, 20*time.Second, 20*time.Millisecond, "round %d", round)
				require.True(t, check(fh), "round %d after compaction", round)
			}

			// A tier slice (records into one slice that is not block-shaped:
			// block rewrites are whole 4 KiB blocks) exists after the rounds.
			tiers := 0
			written.Range(func(_, l any) bool {
				if l.(uint32)%B != 0 {
					tiers++
				}
				return true
			})
			require.GreaterOrEqual(t, tiers, 1)
			var slices []meta.Slice
			require.Zero(t, v.Meta.Read(ctx, fe.Inode, 0, &slices))
			refs := map[uint64]int{}
			for _, s := range slices {
				if _, ok := written.Load(s.Id); ok {
					refs[s.Id]++
				}
			}
			shared := false
			for _, n := range refs {
				shared = shared || n > 1
			}
			require.True(t, shared, "no live tier slice with several records")
			v.Release(ctx, fe.Inode, fh)

			_, fh, st = v.Open(ctx, fe.Inode, syscall.O_RDONLY)
			require.Zero(t, st)
			defer v.Release(ctx, fe.Inode, fh)
			require.True(t, check(fh))
		})
	}
}
