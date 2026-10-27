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
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisCopySnapshotGate blocks one source LRANGE after Redis has returned its old snapshot.
// The second metadata client can then compact and physically delete those old slices.
type redisCopySnapshotGate struct {
	source    string
	read      chan struct{}
	release   chan struct{}
	gated     atomic.Bool
	reads     atomic.Int32
	aborts    atomic.Int32
	failWatch error
}

// DialHook preserves ordinary Redis connection establishment.
func (g *redisCopySnapshotGate) DialHook(next redis.DialHook) redis.DialHook { return next }

// afterRead gates exactly the first successful source read, allowing retries to proceed.
func (g *redisCopySnapshotGate) afterRead(ctx context.Context, cmd redis.Cmder) {
	args := cmd.Args()
	if cmd.Name() != "lrange" || len(args) < 2 || args[1] != g.source || cmd.Err() != nil {
		return
	}
	g.reads.Add(1)
	if g.gated.CompareAndSwap(false, true) {
		close(g.read)
		select {
		case <-g.release:
		case <-ctx.Done():
		}
	}
}

// ProcessHook gates the single-command reads used by CopyFileRange and Clone.
func (g *redisCopySnapshotGate) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if g.failWatch != nil && cmd.Name() == "watch" {
			for _, key := range cmd.Args()[1:] {
				if key == g.source {
					return g.failWatch
				}
			}
		}
		err := next(ctx, cmd)
		if err == nil {
			g.afterRead(ctx, cmd)
		}
		return err
	}
}

// ProcessPipelineHook gates BatchClone's read pipeline and observes EXEC conflicts.
func (g *redisCopySnapshotGate) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)
		if errors.Is(err, redis.TxFailedErr) {
			g.aborts.Add(1)
		}
		if err == nil {
			for _, cmd := range cmds {
				g.afterRead(ctx, cmd)
			}
		}
		return err
	}
}

// TestRedisCopyCompactionSnapshotRetry proves production copy methods reject a stale chunk snapshot across clients.
func TestRedisCopyCompactionSnapshotRetry(t *testing.T) {
	for _, tc := range []struct {
		method string
		chunk  uint32
	}{
		{"copy-file-range", 0}, {"clone", 0}, {"batch-clone", 0},
		{"copy-file-range", 1}, {"clone", 1}, {"batch-clone", 1},
		{"batch-clone-second-source", 1},
	} {
		method, chunk := tc.method, tc.chunk
		t.Run(fmt.Sprintf("%s/chunk-%d", method, chunk), func(t *testing.T) {
			b := newGCTestBackend(t, "redis")
			m := b.meta.(*redisMeta)
			var extraSource Ino
			if method == "batch-clone-second-source" {
				extraSource, _ = createGCTestFile(t, b, "extra-source")
			}
			src, oldIDs := createGCTestFile(t, b, "source")
			if chunk != 0 {
				oldIDs = nil
				for i := 0; i < 2; i++ {
					var id uint64
					var n int
					var d dirStat
					var a Attr
					if st := m.NewSlice(Background(), &id); st != 0 {
						t.Fatal(st)
					}
					if st := m.doWrite(Background(), src, chunk, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), &n, &d, &a); st != 0 {
						t.Fatal(st)
					}
					oldIDs = append(oldIDs, id)
				}
			}
			b.stopDeleteSliceTasks()
			deleted := make(map[uint64]bool)
			b.OnMsg(DeleteSlice, func(args ...interface{}) error { deleted[args[0].(uint64)] = true; return nil })
			mm, err := newRedisMeta("redis", os.Getenv("JUICEFS_TEST_COMPACTION_GC_REDIS"), testConfig())
			if err != nil {
				t.Fatal(err)
			}
			copier := mm.(*redisMeta)
			t.Cleanup(func() { _ = copier.Shutdown() })
			if _, err := copier.Load(false); err != nil {
				t.Fatal(err)
			}
			gate := &redisCopySnapshotGate{source: m.chunkKey(src, chunk), read: make(chan struct{}), release: make(chan struct{})}
			copier.rdb.AddHook(gate)
			var dst Ino
			var attr Attr
			if method == "copy-file-range" {
				if st := copier.Create(Background(), RootInode, "destination", 0600, 0, 0, &dst, &attr); st != 0 {
					t.Fatal(st)
				}
			}
			done := make(chan syscall.Errno, 1)
			go func() {
				ctx := Background()
				switch method {
				case "copy-file-range":
					var copied, length uint64
					done <- copier.CopyFileRange(ctx, src, uint64(chunk)*ChunkSize, dst, 0, 8192, 0, &copied, &length)
				case "clone":
					var count, total uint64
					done <- copier.Clone(ctx, RootInode, src, RootInode, "destination", CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total)
				case "batch-clone", "batch-clone-second-source":
					entries := []*Entry{{Inode: src, Name: []byte("destination")}}
					if extraSource != 0 {
						entries = append([]*Entry{{Inode: extraSource, Name: []byte("extra-destination")}}, entries...)
					}
					done <- copier.BatchClone(ctx, RootInode, RootInode, entries, CLONE_MODE_PRESERVE_ATTR, 022, nil)
				}
			}()
			// Always release a failed test so its blocked production goroutine can exit.
			released := false
			t.Cleanup(func() {
				if !released {
					close(gate.release)
					select {
					case <-done:
					case <-time.After(5 * time.Second):
						t.Error("copy did not exit after release")
					}
				}
			})
			select {
			case <-gate.read:
			case st := <-done:
				t.Fatalf("copy returned before source snapshot gate: %s", st)
			case <-time.After(5 * time.Second):
				t.Fatal("copy did not read source chunk")
			}
			m.compactChunk(src, chunk, false, false, 0)
			current, st := m.doRead(Background(), src, chunk)
			if st != 0 || len(current) != 1 {
				t.Fatalf("compaction replacement: %s %d", st, len(current))
			}
			for _, id := range oldIDs {
				m.deleteSlice_(id, 4096)
				if !deleted[id] {
					t.Fatalf("old object %d not deleted", id)
				}
			}
			close(gate.release)
			released = true
			select {
			case st := <-done:
				if st != 0 {
					t.Fatalf("copy failed after conflict retry: %s", st)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("copy did not retry")
			}
			if gate.reads.Load() < 2 || gate.aborts.Load() < 1 {
				t.Fatalf("stale transaction not retried: reads=%d aborts=%d", gate.reads.Load(), gate.aborts.Load())
			}
			if st := copier.Lookup(Background(), RootInode, "destination", &dst, &attr, false); st != 0 {
				t.Fatal(st)
			}
			destinationChunk := chunk
			if method == "copy-file-range" {
				destinationChunk = 0
			}
			copied, st := copier.doRead(Background(), dst, destinationChunk)
			if st != 0 || len(copied) != 1 || copied[0].id != current[0].id {
				t.Fatalf("copied deleted snapshot: %+v current=%+v errno=%s", copied, current, st)
			}
			for _, id := range oldIDs {
				if n, err := m.rdb.HExists(Background(), m.sliceRefs(), m.sliceKey(id, 4096)).Result(); err != nil || n {
					t.Fatalf("dead ref %d resurrected: exists=%v err=%v", id, n, err)
				}
			}
		})
	}
}

// TestRedisCopySourceWatchFailure preserves actual metadata errors and never acknowledges a failed copy.
func TestRedisCopySourceWatchFailure(t *testing.T) {
	for _, method := range []string{"copy-file-range", "clone", "batch-clone"} {
		t.Run(method, func(t *testing.T) {
			b := newGCTestBackend(t, "redis")
			m := b.meta.(*redisMeta)
			src, _ := createGCTestFile(t, b, "source")
			gate := &redisCopySnapshotGate{source: m.chunkKey(src, 0), failWatch: syscall.EIO}
			gate.gated.Store(true)
			m.rdb.AddHook(gate)
			var dst Ino
			var attr Attr
			var st syscall.Errno
			switch method {
			case "copy-file-range":
				if st = m.Create(Background(), RootInode, "destination", 0600, 0, 0, &dst, &attr); st != 0 {
					t.Fatal(st)
				}
				var copied, length uint64
				st = m.CopyFileRange(Background(), src, 0, dst, 0, 8192, 0, &copied, &length)
				if copied != 0 {
					t.Fatalf("failed copy acknowledged %d bytes", copied)
				}
			case "clone":
				var count, total uint64
				st = m.Clone(Background(), RootInode, src, RootInode, "destination", CLONE_MODE_PRESERVE_ATTR, 022, 1, &count, &total)
				if count != 0 {
					t.Fatalf("failed clone acknowledged %d entries", count)
				}
			case "batch-clone":
				var count uint64
				st = m.BatchClone(Background(), RootInode, RootInode, []*Entry{{Inode: src, Name: []byte("destination")}}, CLONE_MODE_PRESERVE_ATTR, 022, &count)
				if count != 0 {
					t.Fatalf("failed clone acknowledged %d entries", count)
				}
			}
			if st != syscall.EIO {
				t.Fatalf("WATCH error changed: %s", st)
			}
			if gate.reads.Load() != 0 {
				t.Fatalf("source read happened before failed WATCH: %d", gate.reads.Load())
			}
			if method == "copy-file-range" {
				ss, st := m.doRead(Background(), dst, 0)
				if st != 0 || len(ss) != 0 {
					t.Fatal("failed copy changed destination chunks")
				}
			} else if st := m.Lookup(Background(), RootInode, "destination", &dst, &attr, false); st != syscall.ENOENT {
				t.Fatalf("failed clone created destination: %s", st)
			}
		})
	}
}
