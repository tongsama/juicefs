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
	"context"
	"io"
	"path/filepath"
	"strconv"
	"strings"
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

// kazGatedStorage wraps an object storage: Put waits until the gate is opened,
// and every Put and Delete is counted per slice id.
type kazGatedStorage struct {
	object.ObjectStorage
	gate chan struct{} // closed by open()
	once sync.Once
	mu   sync.Mutex
	puts map[uint64]int
	dels map[uint64]int
}

// kazKeySliceID returns the slice id of a "chunks/.../<id>_<indx>_<size>" key.
func kazKeySliceID(key string) (uint64, bool) {
	name := key[strings.LastIndexByte(key, '/')+1:]
	parts := strings.Split(name, "_")
	if len(parts) != 3 {
		return 0, false
	}
	id, err := strconv.ParseUint(parts[0], 10, 64)
	return id, err == nil
}

// Put blocks until the gate opens, then stores the object and counts it.
func (s *kazGatedStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	<-s.gate
	err := s.ObjectStorage.Put(ctx, key, in, getters...)
	if id, ok := kazKeySliceID(key); ok && err == nil {
		s.mu.Lock()
		s.puts[id]++
		s.mu.Unlock()
	}
	return err
}

// Delete counts the request and deletes the object.
func (s *kazGatedStorage) Delete(ctx context.Context, key string, getters ...object.AttrGetter) error {
	if id, ok := kazKeySliceID(key); ok {
		s.mu.Lock()
		s.dels[id]++
		s.mu.Unlock()
	}
	return s.ObjectStorage.Delete(ctx, key, getters...)
}

// open lets every waiting and later Put through.
func (s *kazGatedStorage) open() { s.once.Do(func() { close(s.gate) }) }

// totalPuts returns the number of successful Puts.
func (s *kazGatedStorage) totalPuts() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, c := range s.puts {
		n += c
	}
	return n
}

// keys lists the slice ids that have an object.
func (s *kazGatedStorage) keys(t *testing.T) map[uint64]bool {
	t.Helper()
	ch, err := object.ListAll(context.Background(), s.ObjectStorage, "", "", true, false)
	require.NoError(t, err)
	ids := make(map[uint64]bool)
	for o := range ch {
		require.NotNil(t, o)
		if id, ok := kazKeySliceID(o.Key()); ok && !o.IsDir() {
			ids[id] = true
		}
	}
	return ids
}

// kazPackTestEnv is a VFS with gated uploads for the staging-pack tests.
type kazPackTestEnv struct {
	v    *VFS
	m    meta.Meta
	blob *kazGatedStorage
	reg  *prometheus.Registry
}

// newKazPackTestVFS builds a delta-mode SQLite volume with 4 KiB blocks,
// writeback staging of every block, one upload slot and a closed upload gate.
func newKazPackTestVFS(t *testing.T, pack, skipDelete bool) *kazPackTestEnv {
	t.Helper()
	metaConf := meta.DefaultConf()
	metaConf.MountPoint = "/jfs"
	metaConf.KazCompactionMode = meta.KazCompactionModeDelta
	metaConf.KazCompactionDeltaGap = 16
	m := meta.NewClient("sqlite3://"+filepath.Join(t.TempDir(), "pack.db"), metaConf)
	format := &meta.Format{Name: "test", UUID: uuid.New().String(), Storage: "mem", BlockSize: 4, Compression: "lz4", DirStats: true}
	require.NoError(t, m.Init(format, true))
	t.Cleanup(func() { _ = m.Shutdown() })
	cconf := &chunk.Config{
		BlockSize: format.BlockSize * 1024, Compress: format.Compression, MaxUpload: 1, MaxDownload: 200,
		BufferSize: 30 << 20, CacheSize: 10 << 20, CacheDir: t.TempDir(), MaxRetries: 3,
		Writeback: true, WritebackThresholdSize: format.BlockSize*1024 + 1,
		KazChunkConfig: chunk.KazChunkConfig{KazWholeReadMax: 1 << 20, KazSkipUnuploadedDelete: skipDelete},
	}
	mem, _ := object.CreateStorage("mem", "", "", "", "")
	blob := &kazGatedStorage{ObjectStorage: mem, gate: make(chan struct{}), puts: map[uint64]int{}, dels: map[uint64]int{}}
	t.Cleanup(blob.open)
	reg := prometheus.NewRegistry()
	store := chunk.NewCachedStore(blob, *cconf, reg)
	m.OnMsg(meta.DeleteSlice, func(args ...interface{}) error {
		return store.Remove(args[0].(uint64), int(args[1].(uint32)))
	})
	conf := &Config{Meta: metaConf, Format: *format, Version: "Juicefs", Chunk: cconf, FuseOpts: &FuseOptions{}}
	conf.KazStagingPack = pack
	v := NewVFS(conf, m, store, prometheus.WrapRegistererWithPrefix("juicefs_", reg), reg)
	return &kazPackTestEnv{v: v, m: m, blob: blob, reg: reg}
}

// packEvents returns kaz_staging_pack_total{event}.
func (e *kazPackTestEnv) packEvents(t *testing.T, event string) float64 {
	t.Helper()
	mfs, err := e.reg.Gather()
	require.NoError(t, err)
	for _, mf := range mfs {
		if mf.GetName() != "kaz_staging_pack_total" {
			continue
		}
		for _, mt := range mf.Metric {
			for _, l := range mt.Label {
				if l.GetName() == "event" && l.GetValue() == event {
					return mt.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}

// referenced returns the slice ids referenced by any chunk record.
func (e *kazPackTestEnv) referenced(t *testing.T) map[uint64]bool {
	t.Helper()
	ids := make(map[uint64]bool)
	for _, ss := range e.listSlices(t) {
		for _, s := range ss {
			if s.Id != 0 {
				ids[s.Id] = true
			}
		}
	}
	return ids
}

// listSlices returns the records of every file. Unlike meta.Read it never
// starts a compaction, which would hold the chunk and make the packer skip.
func (e *kazPackTestEnv) listSlices(t *testing.T) map[meta.Ino][]meta.Slice {
	t.Helper()
	slices := make(map[meta.Ino][]meta.Slice)
	require.Zero(t, e.m.ListSlices(meta.Background(), slices, false, false, nil))
	return slices
}

// recordIDs returns the slice ids of every record of inode (hidden ones too)
// without starting a compaction.
func (e *kazPackTestEnv) recordIDs(t *testing.T, inode Ino) []uint64 {
	t.Helper()
	var ids []uint64
	for _, s := range e.listSlices(t)[inode] {
		if s.Id != 0 {
			ids = append(ids, s.Id)
		}
	}
	return ids
}

// waitStored waits until every referenced slice has an object.
func (e *kazPackTestEnv) waitStored(t *testing.T) {
	t.Helper()
	require.Eventually(t, func() bool {
		have := e.blob.keys(t)
		for id := range e.referenced(t) {
			if !have[id] {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond)
}

// kazPackFile is an open file with the bytes written to it.
type kazPackFile struct {
	inode Ino
	fh    uint64
	want  []byte
}

// writeScattered creates name and writes n fsynced 700-byte pieces at
// scattered offsets of chunk 0, one slice each.
func (e *kazPackTestEnv) writeScattered(t *testing.T, ctx Context, name string, n int) *kazPackFile {
	t.Helper()
	fe, fh, st := e.v.Create(ctx, 1, name, 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	f := &kazPackFile{inode: fe.Inode, fh: fh, want: make([]byte, 64<<10)}
	for i := 0; i < n; i++ {
		buf := bytes.Repeat([]byte{byte('a' + i)}, 700)
		off := uint64((i*37)%16)*4096 + 100
		require.Zero(t, e.v.Write(ctx, f.inode, buf, off, fh))
		require.Zero(t, e.v.Fsync(ctx, f.inode, 0, fh))
		copy(f.want[off:], buf)
	}
	end := 0
	for i := len(f.want) - 1; i >= 0; i-- {
		if f.want[i] != 0 {
			end = i + 1
			break
		}
	}
	f.want = f.want[:end]
	return f
}

// checkRead reads the file back through the open handle and a fresh handle.
func (e *kazPackTestEnv) checkRead(t *testing.T, ctx Context, f *kazPackFile) {
	t.Helper()
	got := make([]byte, len(f.want))
	n, st := e.v.Read(ctx, f.inode, got, 0, f.fh)
	require.Zero(t, st)
	require.Equal(t, len(f.want), n)
	require.True(t, bytes.Equal(f.want, got), "read back through the writer handle")
	_, fh, st := e.v.Open(ctx, f.inode, syscall.O_RDONLY)
	require.Zero(t, st)
	defer e.v.Release(ctx, f.inode, fh)
	got = make([]byte, len(f.want))
	n, st = e.v.Read(ctx, f.inode, got, 0, fh)
	require.Zero(t, st)
	require.Equal(t, len(f.want), n)
	require.True(t, bytes.Equal(f.want, got), "read back through a new handle")
}

// TestKazStagingPackReadBack: six small slices of one chunk wait for the single
// upload slot; when the uploads resume they are packed, fewer objects are put,
// and the file reads back the same.
func TestKazStagingPackReadBack(t *testing.T) {
	e := newKazPackTestVFS(t, true, false)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	f := e.writeScattered(t, ctx, "f", 6)
	e.blob.open()
	e.waitStored(t)
	require.GreaterOrEqual(t, e.packEvents(t, "packed"), 1.0)
	require.Less(t, e.blob.totalPuts(), 6)
	e.checkRead(t, ctx, f)
}

// TestKazStagingPackUnknownUploads: slices missing from the index (as after a
// restart) are uploaded one by one.
func TestKazStagingPackUnknownUploads(t *testing.T) {
	e := newKazPackTestVFS(t, true, false)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	f := e.writeScattered(t, ctx, "f", 6)
	idx := e.v.Conf.kazPackIdx
	require.NotNil(t, idx)
	idx.reset()
	e.blob.open()
	e.waitStored(t)
	require.Eventually(t, func() bool { return e.blob.totalPuts() == 6 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, e.packEvents(t, "packed"))
	require.GreaterOrEqual(t, e.packEvents(t, "skip_unknown"), 1.0)
	e.checkRead(t, ctx, f)
}

// TestKazStagingPackSharedSliceUploads: a slice also referenced by another file
// through copy_file_range keeps a reference after the pack, so its own object
// is still uploaded, and both files read back the same.
func TestKazStagingPackSharedSliceUploads(t *testing.T) {
	e := newKazPackTestVFS(t, true, false)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	f := e.writeScattered(t, ctx, "f", 6)
	require.Len(t, e.recordIDs(t, f.inode), 6)
	off := uint64((3*37)%16)*4096 + 100
	ge, gfh, st := e.v.Create(ctx, 1, "g", 0644, 0, syscall.O_RDWR)
	require.Zero(t, st)
	copied, st := e.v.CopyFileRange(ctx, f.inode, f.fh, off, ge.Inode, gfh, 0, 700, 0)
	require.Zero(t, st)
	require.Equal(t, uint64(700), copied)
	g := &kazPackFile{inode: ge.Inode, fh: gfh, want: append([]byte(nil), f.want[off:off+700]...)}
	gids := e.recordIDs(t, g.inode)
	require.Len(t, gids, 1)
	shared := gids[0]
	require.Contains(t, e.recordIDs(t, f.inode), shared)

	e.blob.open()
	e.waitStored(t)
	require.GreaterOrEqual(t, e.packEvents(t, "packed"), 1.0)
	require.NotContains(t, e.recordIDs(t, f.inode), shared, "the shared slice was packed out of f")
	require.True(t, e.blob.keys(t)[shared], "the shared slice must still be uploaded")
	e.checkRead(t, ctx, f)
	e.checkRead(t, ctx, g)
}

// TestKazStagingPackWithSkipDelete: with both flags on, the packed-away slices
// are never deleted from the object store (they were never put), and no object
// is left without a chunk record after the pack.
func TestKazStagingPackWithSkipDelete(t *testing.T) {
	e := newKazPackTestVFS(t, true, true)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	f := e.writeScattered(t, ctx, "f", 6)
	before := e.recordIDs(t, f.inode)
	require.Len(t, before, 6)
	e.blob.open()
	e.waitStored(t)
	require.GreaterOrEqual(t, e.packEvents(t, "packed"), 1.0)
	after := make(map[uint64]bool)
	for _, id := range e.recordIDs(t, f.inode) {
		after[id] = true
	}
	// Like `juicefs gc`: every object must belong to a referenced slice.
	require.Eventually(t, func() bool {
		ref := e.referenced(t)
		for id := range e.blob.keys(t) {
			if !ref[id] {
				return false
			}
		}
		return true
	}, 10*time.Second, 10*time.Millisecond, "leaked objects")
	e.blob.mu.Lock()
	for _, id := range before {
		if !after[id] {
			require.Zero(t, e.blob.dels[id], "DELETE of packed slice %d", id)
		}
	}
	e.blob.mu.Unlock()
	e.checkRead(t, ctx, f)
}

// TestKazStagingPackDisabled: without the flag every slice is uploaded by itself.
func TestKazStagingPackDisabled(t *testing.T) {
	e := newKazPackTestVFS(t, false, false)
	ctx := NewLogContext(meta.NewContext(10, 1, []uint32{2, 3}))
	f := e.writeScattered(t, ctx, "f", 6)
	require.Nil(t, e.v.Conf.kazPackIdx)
	e.blob.open()
	e.waitStored(t)
	require.Eventually(t, func() bool { return e.blob.totalPuts() == 6 }, 10*time.Second, 10*time.Millisecond)
	require.Zero(t, e.packEvents(t, "packed"))
	e.checkRead(t, ctx, f)
}
