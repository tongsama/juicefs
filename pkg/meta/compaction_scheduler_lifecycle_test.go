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
	"sync"
	"testing"
	"time"
)

// TestPriorityCompactorReadAndShutdown exercises the real metadata Read notification and callback join.
func TestPriorityCompactorReadAndShutdown(t *testing.T) {
	b := newGCTestBackend(t, "memkv")
	b.conf.CompactionScheduler = "priority"
	inode, _ := createGCTestFile(t, b, "priority-read")
	var attr Attr
	for i := 2; i < 6; i++ {
		var id uint64
		if st := b.NewSlice(Background(), &id); st != 0 {
			t.Fatal(st)
		}
		var count int
		var delta dirStat
		if st := b.en.doWrite(Background(), inode, 0, uint32(i*4096), Slice{Id: id, Size: 4096, Len: 4096}, time.Now(), &count, &delta, &attr); st != 0 {
			t.Fatal(st)
		}
	}
	started, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	b.OnMsg(CompactChunk, func(...interface{}) error { once.Do(func() { close(started) }); <-release; return nil })
	b.startPriorityCompactor()
	defer b.stopPriorityCompactor()
	defer releaseOnce.Do(func() { close(release) })
	var visible []Slice
	if st := b.Read(Background(), inode, 0, &visible); st != 0 {
		t.Fatal(st)
	}
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("Read did not dispatch compaction")
	}
	stopped := make(chan struct{})
	go func() { b.stopPriorityCompactor(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("shutdown returned before its metadata callback finished")
	case <-time.After(30 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-stopped:
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown did not join callback")
	}
	raw, st := b.en.doRead(Background(), inode, 0)
	if st != 0 || len(raw) != 1 {
		t.Fatalf("compaction publish failed: errno=%v raw=%d", st, len(raw))
	}
	if b.priorityCompactor.Load() != nil {
		t.Fatal("closed scheduler remained attached")
	}
}
