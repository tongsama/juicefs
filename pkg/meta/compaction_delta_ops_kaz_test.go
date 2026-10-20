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
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// kazDeltaOpsChunk is a delta chunk built for the chunk-operation tests: a
// 16-block base under one tier slice of kazDeltaTrigger records.
type kazDeltaOpsChunk struct {
	inode Ino
	name  string
	base  uint64
	tier  uint64
	ids   []uint64 // every slice id the setup created (base, small writes, tier)
	view  []byte   // the chunk's view after the delta compaction
}

// kazDeltaOpsSetup writes the base and 99 small writes over blocks 2, 5 and 11,
// runs one delta compaction and checks that it built a single tier slice with
// at least 3 records and kept the view.
func kazDeltaOpsSetup(t *testing.T, e *kazBlockEnv) *kazDeltaOpsChunk {
	t.Helper()
	c := &kazDeltaOpsChunk{inode: e.file(t), name: kazDeltaOpsName(t)}
	c.base = e.write(t, c.inode, 0, 16*kazEB)
	small := kazDeltaSmallWrites(t, e, c.inode, []uint32{2, 5, 11}, kazDeltaTrigger, 0)
	before := e.store.view(e.records(t, c.inode), 16*kazEB)
	after := kazRun(t, e, c.inode)
	c.view = e.store.view(after, 16*kazEB)
	require.Equal(t, before, c.view)
	require.Equal(t, 1, e.store.callCount())
	c.tier = e.store.callIDs()[0]
	require.GreaterOrEqual(t, len(kazTierRecords(after, c.tier)), 3)
	require.NoError(t, e.en.doCleanupSlices(Background(), nil))
	for _, id := range small {
		require.Equal(t, 1, e.deletedIDs()[id], "small write %d", id)
	}
	require.Zero(t, e.deletedIDs()[c.tier])
	require.Zero(t, e.deletedIDs()[c.base])
	c.ids = append(append([]uint64{c.base}, small...), c.tier)
	return c
}

// kazDeltaOpsName is the directory entry name kazBlockEnv.file gives the test's file.
func kazDeltaOpsName(t *testing.T) string {
	return strings.ReplaceAll(t.Name(), "/", "_")
}

// kazDeltaUnlink removes a root entry and waits until the background deletion of
// its data has finished, then runs the slice cleanup.
func kazDeltaUnlink(t *testing.T, e *kazBlockEnv, name string) {
	t.Helper()
	require.Zero(t, e.Unlink(Background(), RootInode, name))
	require.Eventually(t, func() bool { return len(e.maxDeleting) == 0 }, 10*time.Second, 5*time.Millisecond)
	require.NoError(t, e.en.doCleanupSlices(Background(), nil))
}

// kazDeltaNewFile creates an empty root file with the given name.
func kazDeltaNewFile(t *testing.T, e *kazBlockEnv, name string) Ino {
	t.Helper()
	var inode Ino
	var attr Attr
	require.Zero(t, e.Create(Background(), RootInode, name, 0600, 0, 0, &inode, &attr))
	return inode
}

// kazRequireDeletedOnce checks that every listed slice id was deleted exactly once.
func kazRequireDeletedOnce(t *testing.T, e *kazBlockEnv, ids []uint64) {
	t.Helper()
	got := e.deletedIDs()
	for _, id := range ids {
		require.Equal(t, 1, got[id], "slice %d deletions", id)
	}
}

// kazRequireAlive checks that none of the listed slice ids was deleted.
func kazRequireAlive(t *testing.T, e *kazBlockEnv, ids ...uint64) {
	t.Helper()
	got := e.deletedIDs()
	for _, id := range ids {
		require.Zero(t, got[id], "slice %d deletions", id)
	}
}

// TestKazDeltaChunkOps puts a chunk holding a delta tier through file deletion,
// truncate, upstream compaction, clone, CopyFileRange and JSON dump/load, and
// checks the view and that the tier slice is freed exactly once, only after its
// last reference goes (spec §6-4, Review Focus 4).
func TestKazDeltaChunkOps(t *testing.T) {
	for _, backend := range kazDeltaBackends {
		// (a) Unlink frees the tier slice and the base once.
		t.Run(backend+"/unlink", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			kazDeltaUnlink(t, e, c.name)
			kazRequireDeletedOnce(t, e, c.ids)
		})

		// (b) Truncate into the middle of the tier: the zero record goes above the
		// tier, the view past the length stays zero and no reference leaks.
		t.Run(backend+"/truncate", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			// Between the 17th and 18th record of block 11: 16 tier records go
			// past the length and less than half of the base is hidden.
			length := uint64(11*kazEB + 1650)
			var attr Attr
			require.Zero(t, e.Truncate(Background(), c.inode, 0, length, &attr, true))
			ss := e.records(t, c.inode)
			require.Zero(t, ss[len(ss)-1].id) // the truncate's zero record is on top
			want := append(append([]byte(nil), c.view[:length]...), make([]byte, 16*int(kazEB)-int(length))...)
			require.Equal(t, want, e.store.view(ss, 16*kazEB))
			after := kazRun(t, e, c.inode)
			require.Equal(t, want, e.store.view(after, 16*kazEB))
			require.Len(t, kazTierRecords(after, c.tier), kazDeltaTrigger-16)
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			kazRequireAlive(t, e, c.base, c.tier)
			ids := append(append([]uint64(nil), c.ids...), e.store.callIDs()[1:]...)
			// Grow the file back: the hidden part must still read as zero.
			require.Zero(t, e.Truncate(Background(), c.inode, 0, uint64(16*kazEB), &attr, true))
			require.Equal(t, want, e.store.view(e.records(t, c.inode), 16*kazEB))
			kazDeltaUnlink(t, e, c.name)
			kazRequireDeletedOnce(t, e, ids)
		})

		// (c) Upstream compaction rewrites the whole chunk and frees the tier once.
		t.Run(backend+"/upstream", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			e.compactChunk(c.inode, 0, false, true, 0)
			after := e.records(t, c.inode)
			require.Equal(t, c.view, e.store.view(after, 16*kazEB))
			require.Empty(t, kazTierRecords(after, c.tier))
			require.NoError(t, e.en.doCleanupSlices(Background(), nil))
			require.Equal(t, 1, e.deletedIDs()[c.tier])
			ids := append(append([]uint64(nil), c.ids...), e.store.callIDs()[1:]...)
			kazDeltaUnlink(t, e, c.name)
			kazRequireDeletedOnce(t, e, ids)
		})

		// (d) A clone keeps the tier alive after the source goes; the last copy frees it once.
		t.Run(backend+"/clone", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			var count, total uint64
			require.Zero(t, e.Clone(Background(), RootInode, c.inode, RootInode, "clone_"+c.name, CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total))
			var clone Ino
			var attr Attr
			require.Zero(t, e.Lookup(Background(), RootInode, "clone_"+c.name, &clone, &attr, false))
			require.Equal(t, c.view, e.store.view(e.records(t, clone), 16*kazEB))
			kazDeltaUnlink(t, e, c.name)
			kazRequireAlive(t, e, c.tier, c.base)
			require.Equal(t, c.view, e.store.view(e.records(t, clone), 16*kazEB))
			kazDeltaUnlink(t, e, "clone_"+c.name)
			kazRequireDeletedOnce(t, e, c.ids)
		})

		// (e) CopyFileRange of the tier's range keeps the tier alive after the
		// source goes; deleting the copy frees it once.
		t.Run(backend+"/copy", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			dst := kazDeltaNewFile(t, e, "copy_"+c.name)
			var copied, length uint64
			require.Zero(t, e.meta.CopyFileRange(Background(), c.inode, uint64(2*kazEB), dst, 0, uint64(10*kazEB), 0, &copied, &length))
			require.Equal(t, uint64(10*kazEB), copied)
			want := c.view[2*kazEB : 12*kazEB]
			require.Equal(t, want, e.store.view(e.records(t, dst), 10*kazEB))
			kazDeltaUnlink(t, e, c.name)
			kazRequireAlive(t, e, c.tier, c.base)
			require.Equal(t, want, e.store.view(e.records(t, dst), 10*kazEB))
			kazDeltaUnlink(t, e, "copy_"+c.name)
			kazRequireDeletedOnce(t, e, c.ids)
		})

		// (f) JSON dump/load: the loaded engine counts one reference per tier
		// record, so the slice survives until its last record is removed.
		t.Run(backend+"/dumpload", func(t *testing.T) {
			e := newKazDeltaEnv(t, backend)
			c := kazDeltaOpsSetup(t, e)
			_, err := e.meta.Load(true)
			require.NoError(t, err)
			var buf bytes.Buffer
			require.NoError(t, e.meta.DumpMeta(&buf, RootInode, 10, false, false, false))

			// The target is a fresh engine (for Redis it reuses the test database,
			// which newKazDeltaEnv resets; the source is no longer needed).
			d := newKazDeltaEnv(t, backend)
			require.NoError(t, d.meta.Reset())
			require.NoError(t, d.meta.LoadMeta(bytes.NewReader(buf.Bytes())))
			for id, b := range e.store.data { // the objects stay where they are
				d.store.data[id] = b
			}
			cur := d.records(t, c.inode)
			require.Equal(t, c.view, d.store.view(cur, 16*kazEB))
			r := d.en.(kazChunkReplacer)
			n := len(kazTierRecords(cur, c.tier))
			require.GreaterOrEqual(t, n, 3)
			for i := 0; i < n; i++ {
				cur = kazDropRecords(t, r, c.inode, cur, kazTierRecords(cur, c.tier)[:1])
				require.NoError(t, d.en.doCleanupSlices(Background(), nil))
				if i < n-1 {
					require.Zero(t, d.deletedIDs()[c.tier], "after dropping %d of %d records", i+1, n)
				}
			}
			require.Equal(t, 1, d.deletedIDs()[c.tier])
			require.Zero(t, d.deletedIDs()[c.base])
		})
	}
}
