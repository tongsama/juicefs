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
	"reflect"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestCompactionWanted matches the per-slice triggers for single writes and
// evaluates every count a batch passes through.
func TestCompactionWanted(t *testing.T) {
	for _, tc := range []struct {
		prev, now int
		want      bool
	}{
		{0, 1, false}, {98, 99, true}, {99, 100, false}, {198, 199, true},
		{350, 351, true}, {349, 350, false},
		// Batches: crossing a %100==99 count or exceeding 350 anywhere in (prev, now].
		{90, 105, true}, {100, 120, false}, {340, 360, true}, {0, 0, false},
	} {
		if got := compactionWanted(tc.prev, tc.now); got != tc.want {
			t.Errorf("compactionWanted(%d, %d) = %v, want %v", tc.prev, tc.now, got, tc.want)
		}
	}
	// Equivalent to the original single-write condition.
	for n := 1; n < 3000; n++ {
		if compactionWanted(n-1, n) != (n%100 == 99 || n > 350) {
			t.Fatalf("single write mismatch at %d", n)
		}
	}
}

// newWriteSlicesMeta prepares an initialized client with a session for WriteSlices tests.
func newWriteSlicesMeta(t *testing.T, m Meta) Meta {
	t.Helper()
	if err := m.Reset(); err != nil {
		t.Fatalf("reset meta: %s", err)
	}
	if err := m.Init(testFormat(), false); err != nil {
		t.Fatalf("init: %s", err)
	}
	if err := m.NewSession(false); err != nil {
		t.Fatalf("new session: %s", err)
	}
	m.OnMsg(DeleteSlice, func(args ...interface{}) error { return nil })
	m.OnMsg(CompactChunk, func(args ...interface{}) error { return nil })
	t.Cleanup(func() { _ = m.CloseSession() })
	return m
}

// newWriteSlicesMemKV returns a fresh MemKV client.
func newWriteSlicesMemKV(t *testing.T) Meta {
	m, err := newKVMeta("memkv", "jfs-write-slices-"+t.Name(), testConfig())
	if err != nil {
		t.Fatalf("create meta: %s", err)
	}
	return newWriteSlicesMeta(t, m)
}

// sliceShapes builds count 4 KiB slices at consecutive 4 KiB offsets of a chunk,
// allocating real slice IDs.
func sliceShapes(t *testing.T, m Meta, count int, gap uint32) []SliceWrite {
	t.Helper()
	ws := make([]SliceWrite, count)
	for i := range ws {
		var id uint64
		if st := m.NewSlice(Background(), &id); st != 0 {
			t.Fatalf("new slice: %s", st)
		}
		ws[i] = SliceWrite{Off: uint32(i) * (4096 + gap), Slice: Slice{Id: id, Size: 4096, Len: 4096}}
	}
	return ws
}

// chunkLayout returns the visible slices of a chunk with IDs replaced by their write order.
func chunkLayout(t *testing.T, m Meta, inode Ino, indx uint32, ws []SliceWrite) []Slice {
	t.Helper()
	order := make(map[uint64]uint64, len(ws))
	for i, w := range ws {
		order[w.Slice.Id] = uint64(i + 1)
	}
	var ss []Slice
	if st := m.Read(Background(), inode, indx, &ss); st != 0 {
		t.Fatalf("read: %s", st)
	}
	for i := range ss {
		ss[i].Id = order[ss[i].Id]
	}
	return ss
}

// createIn creates a file under a new directory and returns both inodes.
func createIn(t *testing.T, m Meta, name string) (dir, file Ino) {
	t.Helper()
	ctx := Background()
	var attr Attr
	if st := m.Mkdir(ctx, RootInode, name, 0755, 0, 0, &dir, &attr); st != 0 {
		t.Fatalf("mkdir %s: %s", name, st)
	}
	if st := m.Create(ctx, dir, "f", 0644, 0, 0, &file, &attr); st != 0 {
		t.Fatalf("create %s/f: %s", name, st)
	}
	return dir, file
}

// testWriteSlices checks that WriteSlices matches sequential Write calls, and reports
// partial failures with the index and errno of the first failing slice.
func testWriteSlices(t *testing.T, m Meta) {
	ctx := Background()
	mtime := time.Unix(1700000000, 123)

	t.Run("matches sequential writes", func(t *testing.T) {
		_, a := createIn(t, m, "seq")
		_, b := createIn(t, m, "batch")
		wa := sliceShapes(t, m, 5, 4096)
		wb := sliceShapes(t, m, 5, 4096)
		// Overlap: the last slice overwrites the first; order must be kept.
		wa[4].Off, wb[4].Off = 0, 0
		for _, w := range wa {
			if st := m.Write(ctx, a, 1, w.Off, w.Slice, mtime); st != 0 {
				t.Fatalf("write: %s", st)
			}
		}
		if n, st := m.WriteSlices(ctx, b, 1, wb, mtime); n != len(wb) || st != 0 {
			t.Fatalf("WriteSlices = %d, %s", n, st)
		}
		if la, lb := chunkLayout(t, m, a, 1, wa), chunkLayout(t, m, b, 1, wb); !reflect.DeepEqual(la, lb) {
			t.Fatalf("layout differs:\nseq   %+v\nbatch %+v", la, lb)
		}
		var aa, ab Attr
		if st := m.GetAttr(ctx, a, &aa); st != 0 {
			t.Fatal(st)
		}
		if st := m.GetAttr(ctx, b, &ab); st != 0 {
			t.Fatal(st)
		}
		// Offsets 0, 8192, 16384, 24576 and an overwrite at 0, in chunk 1.
		if aa.Length != ab.Length || ab.Length != ChunkSize+24576+4096 {
			t.Fatalf("length seq=%d batch=%d", aa.Length, ab.Length)
		}
		if ab.Mtime != mtime.Unix() || ab.Mtimensec != uint32(mtime.Nanosecond()) {
			t.Fatalf("mtime %d.%d, want %v", ab.Mtime, ab.Mtimensec, mtime)
		}
	})

	t.Run("single slice uses Write", func(t *testing.T) {
		_, f := createIn(t, m, "single")
		ws := sliceShapes(t, m, 1, 0)
		if n, st := m.WriteSlices(ctx, f, 0, ws, mtime); n != 1 || st != 0 {
			t.Fatalf("WriteSlices = %d, %s", n, st)
		}
		if l := chunkLayout(t, m, f, 0, ws); len(l) != 1 || l[0].Id != 1 {
			t.Fatalf("layout %+v", l)
		}
	})

	t.Run("quota stops at the first slice over the limit", func(t *testing.T) {
		dir, f := createIn(t, m, "quota")
		if err := m.HandleQuota(ctx, QuotaSet, "/quota", DirQuotaType,
			map[string]*Quota{"/quota": {MaxSpace: 3 * 4096, MaxInodes: 100}}, false, false, false); err != nil {
			t.Fatalf("set quota: %s", err)
		}
		m.getBase().loadQuotas()
		_ = dir
		ws := sliceShapes(t, m, 4, 0)
		n, st := m.WriteSlices(ctx, f, 0, ws, mtime)
		if n != 3 || st != syscall.EDQUOT {
			t.Fatalf("WriteSlices = %d, %s; want 3, EDQUOT", n, st)
		}
		if l := chunkLayout(t, m, f, 0, ws); len(l) != 3 {
			t.Fatalf("layout after partial failure %+v", l)
		}
	})

	t.Run("missing inode", func(t *testing.T) {
		ws := sliceShapes(t, m, 3, 0)
		if n, st := m.WriteSlices(ctx, Ino(1<<40), 0, ws, mtime); n != 0 || st != syscall.ENOENT {
			t.Fatalf("WriteSlices = %d, %s; want 0, ENOENT", n, st)
		}
	})

	t.Run("directory inode", func(t *testing.T) {
		dir, _ := createIn(t, m, "notfile")
		ws := sliceShapes(t, m, 3, 0)
		if n, st := m.WriteSlices(ctx, dir, 0, ws, mtime); n != 0 || st != syscall.EPERM {
			t.Fatalf("WriteSlices = %d, %s; want 0, EPERM", n, st)
		}
	})

	t.Run("open file", func(t *testing.T) {
		_, f := createIn(t, m, "opened")
		var attr Attr
		if st := m.Open(ctx, f, syscall.O_RDWR, &attr); st != 0 {
			t.Fatalf("open: %s", st)
		}
		defer m.Close(ctx, f)
		ws := sliceShapes(t, m, 4, 0)
		if n, st := m.WriteSlices(ctx, f, 0, ws, mtime); n != 4 || st != 0 {
			t.Fatalf("WriteSlices = %d, %s", n, st)
		}
		// The open-file chunk cache must not keep the pre-batch slice list.
		if l := chunkLayout(t, m, f, 0, ws); len(l) != 4 {
			t.Fatalf("stale chunk cache %+v", l)
		}
	})
}

// TestWriteSlicesMemKV runs the shared WriteSlices checks on MemKV.
func TestWriteSlicesMemKV(t *testing.T) {
	testWriteSlices(t, newWriteSlicesMemKV(t))
}

// failingBatchEngine fails batches with a fixed errno and counts single writes,
// to check which batch errors fall back to one-by-one writes.
type failingBatchEngine struct {
	engine
	err    syscall.Errno
	single atomic.Int32
}

// doWriteSlices fails without changing anything.
func (e *failingBatchEngine) doWriteSlices(ctx Context, inode Ino, indx uint32, slices []SliceWrite, mtime time.Time, numSlices *int, delta *dirStat, attr *Attr) syscall.Errno {
	return e.err
}

// doWrite counts single-slice writes and delegates to the real engine.
func (e *failingBatchEngine) doWrite(ctx Context, inode Ino, indx uint32, off uint32, s Slice, mtime time.Time, n *int, delta *dirStat, attr *Attr) syscall.Errno {
	e.single.Add(1)
	return e.engine.doWrite(ctx, inode, indx, off, s, mtime, n, delta, attr)
}

// TestWriteSlicesNoRetryOnUnknownError returns an unknown batch failure as is,
// and retries one by one only for errors raised before anything was written.
func TestWriteSlicesNoRetryOnUnknownError(t *testing.T) {
	m := newWriteSlicesMemKV(t)
	b := m.getBase()
	orig := b.en
	defer func() { b.en = orig }()
	for _, tc := range []struct {
		err        syscall.Errno
		wantN      int
		wantSt     syscall.Errno
		wantSingle int32
	}{
		{syscall.EIO, 0, syscall.EIO, 0},
		{syscall.EDQUOT, 3, 0, 3}, // the real engine accepts each slice individually
	} {
		fe := &failingBatchEngine{engine: orig, err: tc.err}
		b.en = fe
		_, f := createIn(t, m, fmt.Sprint("retry-", int(tc.err)))
		n, st := m.WriteSlices(Background(), f, 0, sliceShapes(t, m, 3, 0), time.Now())
		if n != tc.wantN || st != tc.wantSt || fe.single.Load() != tc.wantSingle {
			t.Errorf("%s: n=%d st=%s single=%d; want %d %s %d", tc.err, n, st, fe.single.Load(), tc.wantN, tc.wantSt, tc.wantSingle)
		}
		b.en = orig
	}
}

// TestWriteSlicesRedis runs the shared WriteSlices checks on Redis and requires
// the single-transaction implementation.
func TestWriteSlicesRedis(t *testing.T) {
	m, err := newRedisMeta("redis", "127.0.0.1:6379/11", testConfig())
	if err != nil {
		t.Skipf("redis not available: %s", err)
	}
	if _, ok := m.getBase().en.(sliceBatchWriter); !ok {
		t.Fatal("redis engine does not implement doWriteSlices")
	}
	testWriteSlices(t, newWriteSlicesMeta(t, m))
}
