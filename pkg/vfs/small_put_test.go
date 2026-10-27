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
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// measuredPutStorage observes the actual compressed bytes handed to object storage.
type measuredPutStorage struct {
	object.ObjectStorage
	mu            sync.Mutex
	raw, payload  []int
	gate, entered chan struct{}
}

// Put records successful requests while retaining the real memory backend for reads.
func (s *measuredPutStorage) Put(ctx context.Context, key string, in io.Reader, getters ...object.AttrGetter) error {
	if s.gate != nil {
		close(s.entered)
		<-s.gate
	}
	data, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	if err = s.ObjectStorage.Put(ctx, key, bytes.NewReader(data), getters...); err != nil {
		return err
	}
	n, err := strconv.Atoi(key[strings.LastIndex(key, "_")+1:])
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.raw = append(s.raw, n)
	s.payload = append(s.payload, len(data))
	s.mu.Unlock()
	return nil
}

// TestSmallPUTWritebackStaging verifies fsync commits raw local data while cloud upload is pending.
func TestSmallPUTWritebackStaging(t *testing.T) {
	v, store := newSmallPutVFS(t, "zstd", true)
	store.gate, store.entered = make(chan struct{}), make(chan struct{})
	var release sync.Once
	defer release.Do(func() { close(store.gate) })
	ctx := NewLogContext(meta.Background())
	fe, fh, eno := v.Create(ctx, 1, "staged", 0644, 0, syscall.O_RDWR)
	require.Zero(t, eno)
	defer v.Release(ctx, fe.Inode, fh)
	data := make([]byte, 512)
	require.Zero(t, v.Write(ctx, fe.Inode, data, 0, fh))
	done := make(chan syscall.Errno, 1)
	go func() { done <- v.Fsync(ctx, fe.Inode, 0, fh) }()
	select {
	case err := <-done:
		require.Zero(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("fsync did not finish after durable local staging")
	}
	select {
	case <-store.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("background cloud upload did not start")
	}
	raw, _ := store.sizes()
	require.Empty(t, raw, "cloud PUT is still blocked")
	var files []string
	require.NoError(t, filepath.WalkDir(filepath.Join(v.Conf.Chunk.CacheDir, "rawstaging"), func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !entry.IsDir() {
			files = append(files, path)
		}
		return nil
	}))
	require.Len(t, files, 1)
	staged, err := os.ReadFile(files[0])
	require.NoError(t, err)
	require.Len(t, staged, 516, "512 raw bytes plus a 4-byte checksum; tier zero has no footer")
	require.Equal(t, data, staged[:512])
	buf := make([]byte, 512)
	n, eno := v.Read(ctx, fe.Inode, buf, 0, fh)
	require.Zero(t, eno)
	require.Equal(t, 512, n)
	require.Equal(t, data, buf, "committed staged data must be readable before cloud upload")
	release.Do(func() { close(store.gate) })
	require.Eventually(t, func() bool { raw, _ := store.sizes(); return len(raw) == 1 }, 2*time.Second, time.Millisecond)
	raw, payload := store.sizes()
	require.Equal(t, []int{512}, raw)
	require.Less(t, payload[0], 512)
	t.Logf("raw=%v stage_file_bytes=%d payload=%v", raw, len(staged), payload)
}

// sizes returns stable snapshots so background upload cannot race with measurements.
func (s *measuredPutStorage) sizes() ([]int, []int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.raw...), append([]int(nil), s.payload...)
}

// newSmallPutVFS isolates metadata and storage and installs timers before writer startup.
func newSmallPutVFS(t *testing.T, compression string, writeback bool) (*VFS, *measuredPutStorage) {
	t.Helper()
	t.Setenv("_FUSE_STATE_PATH", filepath.Join(t.TempDir(), "state.json"))
	// The memkv driver persists Init settings to a shared test file even for distinct clients.
	const settings = "/tmp/juicefs.memkv.setting.json"
	previous, readErr := os.ReadFile(settings)
	require.True(t, readErr == nil || os.IsNotExist(readErr))
	t.Cleanup(func() {
		if readErr == nil {
			require.NoError(t, os.WriteFile(settings, previous, 0644))
		} else {
			err := os.Remove(settings)
			require.True(t, err == nil || os.IsNotExist(err))
		}
	})
	mc := meta.DefaultConf()
	mc.MaxDeletes = 0
	m := meta.NewClient("memkv://", mc)
	require.NoError(t, m.Reset()) // Reset only this in-memory client, including any loaded test settings.
	f := meta.Format{Name: "small-put-test", UUID: uuid.NewString(), Storage: "mem", BlockSize: 4096, Compression: compression}
	require.NoError(t, m.Init(&f, true))
	blob, err := object.CreateStorage("mem", "", "", "", "")
	require.NoError(t, err)
	s := &measuredPutStorage{ObjectStorage: blob}
	cc := &chunk.Config{BlockSize: 4 << 20, Compress: compression, MaxUpload: 2, MaxDownload: 2,
		BufferSize: 64 << 20, CacheDir: "memory", CacheEviction: "lru", CacheChecksum: "extend",
		AutoCreate: true, CacheFullBlock: true, PutTimeout: time.Second, GetTimeout: time.Second}
	if writeback {
		cc.CacheDir, cc.CacheSize, cc.Writeback = t.TempDir(), 32<<20, true
	}
	cc.SelfCheck(f.UUID) // Match mount's threshold normalization so writeback really stages blocks.
	if writeback {
		require.True(t, cc.Writeback)
		require.Equal(t, cc.BlockSize+1, cc.WritebackThresholdSize)
	}
	conf := &Config{Meta: mc, Format: f, Chunk: cc, FuseOpts: &FuseOptions{},
		SliceFlushWait: 30 * time.Second, SliceFlushIdle: 10 * time.Second}
	reg := prometheus.NewRegistry()
	store := chunk.NewCachedStore(s, *cc, reg)
	if writeback {
		// Wait for both cloud upload cleanup and queued cache writes before TempDir removal.
		t.Cleanup(func() {
			require.Eventually(t, func() bool {
				metrics, err := reg.Gather()
				if err != nil || store.UsedMemory() != 0 {
					return false
				}
				for _, metric := range metrics {
					if metric.GetName() == "object_request_uploading" {
						return metric.Metric[0].GetGauge().GetValue() == 0
					}
				}
				return false
			}, 3*time.Second, time.Millisecond)
			metrics, err := reg.Gather()
			require.NoError(t, err)
			var staged float64
			for _, metric := range metrics {
				if metric.GetName() == "staging_write_bytes" {
					staged = metric.Metric[0].GetCounter().GetValue()
				}
			}
			require.Positive(t, staged, "writeback fixture must exercise real disk staging")
		})
	}
	return NewVFS(conf, m, store, reg, reg), s
}

// synchronizedLogBuffer allows upload and commit goroutines to emit diagnostics concurrently.
type synchronizedLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

// Write serializes logging without changing the production logger or upload ordering.
func (b *synchronizedLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

// String snapshots diagnostics after the real fsync has completed.
func (b *synchronizedLogBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestSmallPUTDiagnostics requires raw slice, freeze cause and actual PUT payload to be distinguishable.
func TestSmallPUTDiagnostics(t *testing.T) {
	v, store := newSmallPutVFS(t, "zstd", false)
	var logs synchronizedLogBuffer
	oldOutput, oldLevel := logger.Out, logger.GetLevel()
	logger.SetOutput(&logs)
	logger.SetLevel(logrus.DebugLevel)
	defer func() { logger.SetOutput(oldOutput); logger.SetLevel(oldLevel) }()
	ctx := NewLogContext(meta.Background())
	fe, fh, eno := v.Create(ctx, 1, "diagnostics", 0644, 0, syscall.O_RDWR)
	require.Zero(t, eno)
	defer v.Release(ctx, fe.Inode, fh)
	require.Zero(t, v.Write(ctx, fe.Inode, make([]byte, 4096), 0, fh))
	require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
	raw, payload := store.sizes()
	require.Equal(t, []int{4096}, raw)
	require.Len(t, payload, 1)
	require.Less(t, payload[0], 1024)
	text := logs.String()
	require.Contains(t, text, "reason=explicit_flush")
	require.Contains(t, text, "raw_length=4096")
	require.Contains(t, text, "started_unix_ns=")
	require.Contains(t, text, fmt.Sprintf("payload_bytes=%d", payload[0]))
}

// TestSmallPUTAggregation measures when long timers can share a slice and when barriers split it.
func TestSmallPUTAggregation(t *testing.T) {
	for _, wb := range []bool{false, true} {
		for _, mode := range []string{"sequential", "overwrite", "block_overwrite", "gap", "chunks", "fsync", "read"} {
			t.Run(fmt.Sprintf("writeback=%t/%s", wb, mode), func(t *testing.T) {
				v, store := newSmallPutVFS(t, "none", wb)
				ctx := NewLogContext(meta.Background())
				fe, fh, eno := v.Create(ctx, 1, "image", 0644, 0, syscall.O_RDWR)
				require.Zero(t, eno)
				defer v.Release(ctx, fe.Inode, fh)
				wantRaw := []int{1024}
				if mode == "overwrite" {
					wantRaw = []int{512}
				}
				if mode == "block_overwrite" {
					wantRaw = []int{512, 4 << 20}
				}
				if mode == "gap" || mode == "chunks" || mode == "fsync" || mode == "read" {
					wantRaw = []int{512, 512}
				}
				for i := 0; i < 2; i++ {
					off := uint64(i * 512)
					if mode == "overwrite" {
						off = 0
					}
					if mode == "gap" {
						off = uint64(i * 8192)
					}
					if mode == "chunks" {
						off = uint64(i) * meta.ChunkSize
					}
					data := bytes.Repeat([]byte{byte(i + 1)}, 512)
					if mode == "block_overwrite" {
						off = 0
						if i == 0 {
							data = bytes.Repeat([]byte{1}, 4<<20)
						}
					}
					require.Zero(t, v.Write(ctx, fe.Inode, data, off, fh))
					if mode == "fsync" {
						require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
					}
					if mode == "read" {
						buf := make([]byte, 512)
						n, err := v.Read(ctx, fe.Inode, buf, off, fh)
						require.Zero(t, err)
						require.Equal(t, 512, n)
						require.Equal(t, data, buf)
					}
				}
				if mode == "sequential" || mode == "overwrite" {
					raw, _ := store.sizes()
					require.Empty(t, raw, "partial blocks must stay writable before a barrier")
				}
				require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
				lastOff := uint64(512)
				if mode == "overwrite" || mode == "block_overwrite" {
					lastOff = 0
				}
				if mode == "gap" {
					lastOff = 8192
				}
				if mode == "chunks" {
					lastOff = meta.ChunkSize
				}
				buf := make([]byte, 512)
				n, err := v.Read(ctx, fe.Inode, buf, lastOff, fh)
				require.Zero(t, err)
				require.Equal(t, 512, n)
				require.Equal(t, bytes.Repeat([]byte{2}, 512), buf, "latest write must remain visible")
				require.Eventually(t, func() bool { raw, _ := store.sizes(); return len(raw) == len(wantRaw) }, 3*time.Second, time.Millisecond)
				raw, payload := store.sizes()
				sort.Ints(raw)
				sort.Ints(payload)
				require.Equal(t, wantRaw, raw)
				require.Equal(t, wantRaw, payload)
				t.Logf("raw=%v payload=%v", raw, payload)
			})
		}
	}
}

// TestSmallPUTCompression distinguishes a compressed payload from its raw object-key length.
func TestSmallPUTCompression(t *testing.T) {
	for _, compression := range []string{"none", "lz4", "zstd"} {
		t.Run(compression, func(t *testing.T) {
			v, store := newSmallPutVFS(t, compression, false)
			ctx := NewLogContext(meta.Background())
			fe, fh, eno := v.Create(ctx, 1, "zeros", 0644, 0, syscall.O_RDWR)
			require.Zero(t, eno)
			defer v.Release(ctx, fe.Inode, fh)
			require.Zero(t, v.Write(ctx, fe.Inode, make([]byte, 128<<10), 0, fh))
			require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
			raw, payload := store.sizes()
			require.Equal(t, []int{128 << 10}, raw)
			require.Len(t, payload, 1)
			if compression == "none" {
				require.Equal(t, raw, payload)
			} else {
				require.Less(t, payload[0], 1024)
			}
			buf := make([]byte, 128<<10)
			n, err := v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Zero(t, err)
			require.Equal(t, len(buf), n)
			require.Equal(t, make([]byte, len(buf)), buf)
			t.Logf("raw=%v payload=%v", raw, payload)
		})
	}
}
