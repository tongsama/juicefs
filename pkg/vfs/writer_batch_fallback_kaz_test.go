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
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/stretchr/testify/require"
)

// writeOnlyMeta embeds meta.Meta without adding WriteSlices, so it does not
// implement meta.KazSliceWriter; it counts Write calls and holds the first one.
type writeOnlyMeta struct {
	meta.Meta
	mu     sync.Mutex
	writes int
	hold   chan struct{} // the first Write waits until it is closed
	held   chan struct{} // closed when the first Write starts waiting
}

// Write counts a single-slice commit and holds the first one on the gate.
func (m *writeOnlyMeta) Write(ctx meta.Context, inode Ino, indx, off uint32, s meta.Slice, mtime time.Time) syscall.Errno {
	m.mu.Lock()
	m.writes++
	first := m.writes == 1
	m.mu.Unlock()
	if first {
		close(m.held)
		<-m.hold
	}
	return m.Meta.Write(ctx, inode, indx, off, s, mtime)
}

// TestWriterFallsBackWithoutSliceBatchWriter commits a would-be batch one slice
// at a time through Write when the metadata client has no WriteSlices.
func TestWriterFallsBackWithoutSliceBatchWriter(t *testing.T) {
	f := newRangeTestFile(t, WriterFlushScopeFile)
	f.v.Conf.MetaWriteBatch = 8
	f.v.Conf.WriterReuseWindow = 1
	wm := &writeOnlyMeta{Meta: f.v.Meta, hold: make(chan struct{}), held: make(chan struct{})}
	f.v.writer.(*dataWriter).m = wm
	f.write(t, 2<<20, []byte("s0"))
	f.write(t, 0, []byte("a1"))
	f.write(t, 1<<20, []byte("b2")) // freezes S0 (window 1), whose commit is held
	<-wm.held
	fsynced := make(chan syscall.Errno, 1)
	go func() { fsynced <- f.v.Fsync(f.ctx, f.ino, 0, f.fh) }()
	awaitAllDone(t, f.fileWriter(t), 0)
	close(wm.hold) // S1 and S2 are done, so they form one batch of two
	select {
	case eno := <-fsynced:
		require.Zero(t, eno)
	case <-time.After(rangeTestTimeout):
		t.Fatal("fsync did not finish")
	}
	wm.mu.Lock()
	writes := wm.writes
	wm.mu.Unlock()
	require.Equal(t, 3, writes, "every slice goes through Write")
	require.Equal(t, []byte("a1"), f.read(t, 0, 2))
	require.Equal(t, []byte("b2"), f.read(t, 1<<20, 2))
	require.Equal(t, []byte("s0"), f.read(t, 2<<20, 2))
}
