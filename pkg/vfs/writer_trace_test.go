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
	"regexp"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

// flushTraceBuffer safely captures diagnostics from concurrent real writers.
type flushTraceBuffer struct {
	sync.Mutex
	bytes.Buffer
}

// Write serializes concurrent diagnostic output.
func (b *flushTraceBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

// String snapshots the collected output.
func (b *flushTraceBuffer) String() string { b.Lock(); defer b.Unlock(); return b.Buffer.String() }

// Levels limits capture to the writer diagnostics checked by these tests.
func (b *flushTraceBuffer) Levels() []logrus.Level { return []logrus.Level{logrus.DebugLevel} }

// Fire captures messages independently of output changes made by background progress reporting.
func (b *flushTraceBuffer) Fire(entry *logrus.Entry) error {
	_, err := b.Write([]byte(entry.Message + "\n"))
	return err
}

// captureFlushTrace preserves existing hooks and restores logger state without changing its output.
func captureFlushTrace(t *testing.T) *flushTraceBuffer {
	t.Helper()
	logs := &flushTraceBuffer{}
	level := logger.GetLevel()
	original := logger.ReplaceHooks(make(logrus.LevelHooks))
	hooks := make(logrus.LevelHooks, len(original)+1)
	for level, entries := range original {
		hooks[level] = append([]logrus.Hook(nil), entries...)
	}
	hooks[logrus.DebugLevel] = append(hooks[logrus.DebugLevel], logs)
	logger.ReplaceHooks(hooks)
	logger.SetLevel(logrus.DebugLevel)
	t.Cleanup(func() {
		logger.ReplaceHooks(original)
		logger.SetLevel(level)
	})
	return logs
}

// TestWriterFlushTraceOutputReset reproduces progress completion replacing the logger output mid-barrier.
func TestWriterFlushTraceOutputReset(t *testing.T) {
	logs := captureFlushTrace(t)
	trace := beginWriterFlush(WithWriterFlushOrigin(meta.Background(), "output-reset"), 123)
	progress, _ := utils.MockProgress()
	progress.Done()
	trace.end(0)
	require.Contains(t, logs.String(), "origin=output-reset")
	require.Contains(t, logs.String(), "phase=end errno=0")
}

// TestWriterFlushOriginContext verifies credentials, values and shared cancellation survive annotation.
func TestWriterFlushOriginContext(t *testing.T) {
	old := logger.GetLevel()
	defer logger.SetLevel(old)
	logger.SetLevel(logrus.InfoLevel)
	ctx := meta.NewContext(12, 34, []uint32{56, 78})
	before := writerFlushBarrier.Load()
	require.Same(t, ctx, WithWriterFlushOrigin(ctx, "read"))
	require.Equal(t, before, writerFlushBarrier.Load())
	require.Zero(t, beginWriterFlush(ctx, 1).id)
	require.Equal(t, before, writerFlushBarrier.Load())
	require.Zero(t, testing.AllocsPerRun(100, func() { _ = WithWriterFlushOrigin(ctx, "read") }))
	logger.SetLevel(logrus.DebugLevel)
	wrapped := WithWriterFlushOrigin(ctx, "read").WithValue("test", 42)
	require.Equal(t, uint32(12), wrapped.Pid())
	require.Equal(t, uint32(34), wrapped.Uid())
	require.Equal(t, []uint32{56, 78}, wrapped.Gids())
	require.Equal(t, uint32(56), wrapped.Gid())
	require.Equal(t, ctx.CheckPermission(), wrapped.CheckPermission())
	require.Equal(t, 42, wrapped.Value("test"))
	require.Nil(t, ctx.Value("test"))
	require.Equal(t, "read", writerFlushOrigin(wrapped))
	wrapped.Cancel()
	require.True(t, ctx.Canceled())
	require.True(t, wrapped.Canceled())
	require.Equal(t, ctx.Done(), wrapped.Done())
	require.Equal(t, ctx.Err(), wrapped.Err())
}

// traceCountingMeta records successful metadata commits while using the real backend.
type traceCountingMeta struct {
	meta.Meta
	writes atomic.Int32
}

// Write delegates persistence and counts only completed slice commits.
func (m *traceCountingMeta) Write(ctx meta.Context, inode Ino, indx, off uint32, slice meta.Slice, mtime time.Time) syscall.Errno {
	err := m.Meta.Write(ctx, inode, indx, off, slice, mtime)
	if err == 0 {
		m.writes.Add(1)
	}
	return err
}

// TestWriterReuseWindow reads real data after revisiting a partial slice behind newer gap slices.
func TestWriterReuseWindow(t *testing.T) {
	for _, window := range []int{4, 16} {
		t.Run(fmt.Sprint(window), func(t *testing.T) {
			v, _ := createTestVFS(nil, "")
			v.Conf.WriterReuseWindow = window
			counter := &traceCountingMeta{Meta: v.Meta}
			v.writer.(*dataWriter).m = counter
			ctx := NewLogContext(meta.Background())
			fe, fh, eno := v.Create(ctx, 1, "reuse", 0644, 0, syscall.O_RDWR)
			require.Zero(t, eno)
			defer v.Release(ctx, fe.Inode, fh)
			expected := make([]byte, 8*8192+4096)
			var old *sliceWriter
			for i := 0; i < 9; i++ {
				data := bytes.Repeat([]byte{byte(i + 1)}, 4096)
				require.Zero(t, v.Write(ctx, fe.Inode, data, uint64(i*8192), fh))
				copy(expected[i*8192:], data)
				if i == 0 {
					fw := v.writer.(*dataWriter).find(fe.Inode)
					fw.Lock()
					old = fw.chunks[0].slices[0]
					fw.Unlock()
				}
			}
			fw := v.writer.(*dataWriter).find(fe.Inode)
			fw.Lock()
			before := old.lastMod
			fw.Unlock()
			data := bytes.Repeat([]byte{99}, 4096)
			require.Zero(t, v.Write(ctx, fe.Inode, data, 0, fh))
			copy(expected, data)
			fw.Lock()
			reused := fw.chunks[0].slices[0] == old && old.lastMod.After(before)
			count := len(fw.chunks[0].slices)
			frozen := old.freezed
			fw.Unlock()
			if window == 16 {
				require.False(t, frozen)
				require.True(t, reused)
				require.Equal(t, 9, count)
			} else {
				require.True(t, frozen)
			}
			require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
			if window == 16 {
				require.Equal(t, int32(9), counter.writes.Load())
			} else {
				require.Equal(t, int32(10), counter.writes.Load())
			}
			buf := make([]byte, len(expected))
			n, eno := v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Zero(t, eno)
			require.Equal(t, len(expected), n)
			require.Equal(t, expected, buf)
			// A new overwrite must still be committed by the read-before-write barrier.
			require.Zero(t, v.Write(ctx, fe.Inode, []byte("new"), 0, fh))
			copy(expected, []byte("new"))
			n, eno = v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Zero(t, eno)
			require.Equal(t, len(expected), n)
			require.Equal(t, expected, buf)
		})
	}
}

// TestWriterFlushTraceCorrelation pairs concurrent origins with their own begin/freeze/end barrier IDs.
func TestWriterFlushTraceCorrelation(t *testing.T) {
	logs := captureFlushTrace(t)
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		fe, fh, eno := v.Create(ctx, 1, fmt.Sprint("trace", i), 0644, 0, syscall.O_RDWR)
		require.Zero(t, eno)
		require.Zero(t, v.Write(ctx, fe.Inode, []byte("data"), 0, fh))
		wg.Add(1)
		go func(inode Ino, fh uint64, i int) {
			defer wg.Done()
			eno := v.writer.Flush(WithWriterFlushOrigin(meta.Background(), fmt.Sprint("origin", i)), inode)
			if eno != 0 {
				t.Errorf("flush: %s", eno)
			}
		}(fe.Inode, fh, i)
	}
	wg.Wait()
	lines := strings.Split(logs.String(), "\n")
	seen := map[string]bool{}
	for i := 0; i < 8; i++ {
		origin := fmt.Sprint("origin", i)
		id := ""
		phases := map[string]bool{}
		for _, line := range lines {
			if !strings.Contains(line, "origin="+origin+" ") {
				continue
			}
			match := regexp.MustCompile(`barrier=([0-9]+)`).FindStringSubmatch(line)
			require.Len(t, match, 2)
			require.NotEqual(t, "0", match[1])
			if id == "" {
				id = match[1]
			} else {
				require.Equal(t, id, match[1])
			}
			for _, phase := range []string{"phase=begin", "reason=explicit_flush", "phase=end"} {
				if strings.Contains(line, phase) {
					phases[phase] = true
				}
			}
			if strings.Contains(line, "phase=end") {
				require.Contains(t, line, "errno=0")
			}
		}
		require.False(t, seen[id], "distinct actual barriers must have distinct identities")
		seen[id] = true
		require.Len(t, phases, 3, "origin %s: %s", origin, logs.String())
	}
}

// TestWriterReuseWindowNormalization exercises Go callers outside CLI validation.
func TestWriterReuseWindowNormalization(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	for _, input := range []int{0, -1, 4, 16, 64, 65} {
		conf := *v.Conf
		conf.WriterReuseWindow = input
		w := NewDataWriter(&conf, v.Meta, v.writer.(*dataWriter).store, v.reader).(*dataWriter)
		expected := input
		if input <= 0 || input > 64 {
			expected = 4
		}
		require.Equal(t, expected, w.conf.WriterReuseWindow)
	}
}

// TestWriterFlushCallerErrors correlates real commit failures and Read/Fsync callers without masking errno.
func TestWriterFlushCallerErrors(t *testing.T) {
	logs := captureFlushTrace(t)
	v, _ := createTestVFS(nil, "")
	ctx := NewLogContext(meta.Background())
	fe, fh, eno := v.Create(ctx, 1, "caller-error", 0644, 0, syscall.O_RDWR)
	require.Zero(t, eno)
	defer v.Release(ctx, fe.Inode, fh)
	require.Zero(t, v.Write(ctx, fe.Inode, []byte("old"), 0, fh))
	buf := make([]byte, 3)
	n, eno := v.Read(ctx, fe.Inode, buf, 0, fh)
	require.Zero(t, eno)
	require.Equal(t, 3, n)
	require.Equal(t, "old", string(buf))
	v.writer.(*dataWriter).m = &failingWriteMeta{Meta: v.Meta, err: syscall.ENOSPC}
	require.Zero(t, v.Write(ctx, fe.Inode, []byte("new"), 0, fh))
	require.Equal(t, syscall.ENOSPC, v.Fsync(ctx, fe.Inode, 0, fh))
	buf = []byte("???")
	n, eno = v.Read(ctx, fe.Inode, buf, 0, fh)
	require.Equal(t, syscall.ENOSPC, eno)
	require.Zero(t, n)
	require.Equal(t, "???", string(buf))
	log := logs.String()
	require.Regexp(t, `origin=vfs.Read barrier=[1-9][0-9]* phase=begin`, log)
	require.Regexp(t, `origin=vfs.Fsync barrier=[1-9][0-9]* phase=end errno=28`, log)
	require.Regexp(t, `origin=vfs.Read barrier=[1-9][0-9]* phase=end errno=28`, log)
}

// TestWriterReuseRestrictions keeps gaps, intervening overlaps and complete blocks immutable.
func TestWriterReuseRestrictions(t *testing.T) {
	v, _ := createTestVFS(nil, "")
	v.Conf.WriterReuseWindow = 16
	ctx := NewLogContext(meta.Background())
	for _, full := range []bool{false, true} {
		t.Run(fmt.Sprint("full-block-", full), func(t *testing.T) {
			fe, fh, eno := v.Create(ctx, 1, fmt.Sprint("restriction", full), 0644, 0, syscall.O_RDWR)
			require.Zero(t, eno)
			defer v.Release(ctx, fe.Inode, fh)
			size := 4096
			if full {
				size = v.Conf.Chunk.BlockSize
			}
			initial := bytes.Repeat([]byte{1}, size)
			require.Zero(t, v.Write(ctx, fe.Inode, initial, 0, fh))
			fw := v.writer.(*dataWriter).find(fe.Inode)
			fw.Lock()
			first := fw.chunks[0].slices[0]
			before := first.lastMod
			fw.Unlock()
			expected := append([]byte(nil), initial...)
			if !full {
				gap := bytes.Repeat([]byte{2}, 4096)
				require.Zero(t, v.Write(ctx, fe.Inode, gap, 8192, fh))
				expected = append(expected, make([]byte, 8192)...)
				copy(expected[8192:], gap)
				fw.Lock()
				count := len(fw.chunks[0].slices)
				fw.Unlock()
				require.Equal(t, 2, count, "a gap must create another slice")
				overlap := bytes.Repeat([]byte{3}, 8192)
				require.Zero(t, v.Write(ctx, fe.Inode, overlap, 2048, fh))
				copy(expected[2048:], overlap)
			} else {
				require.Zero(t, v.Write(ctx, fe.Inode, []byte("new"), 0, fh))
				copy(expected, []byte("new"))
			}
			fw.Lock()
			last := fw.chunks[0].slices[len(fw.chunks[0].slices)-1]
			after := first.lastMod
			fw.Unlock()
			require.NotSame(t, first, last)
			require.Equal(t, before, after, "rejected candidates must remain unchanged")
			require.Zero(t, v.Fsync(ctx, fe.Inode, 0, fh))
			buf := make([]byte, len(expected))
			n, eno := v.Read(ctx, fe.Inode, buf, 0, fh)
			require.Zero(t, eno)
			require.Equal(t, len(expected), n)
			require.Equal(t, expected, buf)
		})
	}
}

// TestWriterSliceDiagnosticsNoAllocation keeps disabled diagnostic formatting off the write path.
func TestWriterSliceDiagnosticsNoAllocation(t *testing.T) {
	old := logger.GetLevel()
	logger.SetLevel(logrus.InfoLevel)
	defer logger.SetLevel(old)
	s := &sliceWriter{chunk: &chunkWriter{file: &fileWriter{inode: 10000}}, id: 10000, slen: 4096, started: time.Now(), lastMod: time.Now()}
	n := testing.AllocsPerRun(100, func() { s.logFreeze("age", writerFlushTrace{}); s.logFinish() })
	require.Zero(t, n, "INFO must not allocate DEBUG diagnostic arguments")
}
