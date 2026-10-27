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
	"fmt"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
)

// pendingFlushWriter models a completed data slice whose metadata call has not returned.
func pendingFlushWriter(t *testing.T, writeback bool) (*fileWriter, *chunkWriter) {
	t.Helper()
	w := &dataWriter{conf: &Config{Chunk: &chunk.Config{Writeback: writeback}}, files: make(map[Ino]*fileWriter)}
	f := w.Open(2, 4096, 0).(*fileWriter)
	f.Lock()
	c := f.findChunk(0)
	c.slices = []*sliceWriter{{chunk: c, id: 1, slen: 4096, length: 4096, done: true, freezed: true}}
	f.Unlock()
	t.Cleanup(func() {
		f.Lock()
		f.freeChunk(c)
		f.Unlock()
	})
	return f, c
}

// awaitFlushStart synchronizes on the real flush wait state instead of a fixed sleep.
func awaitFlushStart(t *testing.T, f *fileWriter) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	timeout := time.NewTimer(time.Second)
	defer timeout.Stop()
	for {
		f.Lock()
		started := f.flushwaiting > 0
		f.Unlock()
		if started {
			return
		}
		select {
		case <-tick.C:
		case <-timeout.C:
			t.Fatal("flush did not start")
		}
	}
}

// TestFlushReportsRealErrorWhileOtherCommitsWait prevents wait mode from hiding a failed write.
func TestFlushReportsRealErrorWhileOtherCommitsWait(t *testing.T) {
	for _, writeback := range []bool{false, true} {
		for _, eno := range []syscall.Errno{syscall.EIO, syscall.ENOSPC, syscall.EDQUOT} {
			t.Run(fmt.Sprintf("writeback=%t/%s", writeback, eno), func(t *testing.T) {
				f, _ := pendingFlushWriter(t, writeback)
				result := make(chan syscall.Errno, 1)
				go func() { result <- f.Flush(meta.Background()) }()
				awaitFlushStart(t, f)
				// commitThread records genuine upload/metadata failures in f.err.
				f.Lock()
				f.err = eno
				f.flushcond.Broadcast()
				f.Unlock()
				select {
				case got := <-result:
					if got != eno {
						t.Fatalf("flush errno=%v, want %v", got, eno)
					}
				case <-time.After(time.Second):
					t.Fatal("flush concealed a real failure behind another pending commit")
				}
			})
		}
	}
}

// TestFlushWaitsForCommitCompletion rejects early success in both data modes.
func TestFlushWaitsForCommitCompletion(t *testing.T) {
	for _, writeback := range []bool{false, true} {
		t.Run(fmt.Sprintf("writeback=%t", writeback), func(t *testing.T) {
			f, c := pendingFlushWriter(t, writeback)
			result := make(chan syscall.Errno, 1)
			go func() { result <- f.Flush(meta.Background()) }()
			awaitFlushStart(t, f)
			select {
			case got := <-result:
				t.Fatalf("flush returned before commit completed: %v", got)
			case <-time.After(20 * time.Millisecond):
			}
			f.Lock()
			f.freeChunk(c)
			f.Unlock()
			select {
			case got := <-result:
				if got != 0 {
					t.Fatalf("completed flush errno=%v", got)
				}
			case <-time.After(time.Second):
				t.Fatal("flush did not resume after commit completion")
			}
			f.Lock()
			defer f.Unlock()
			if f.flushwaiting != 0 {
				t.Fatal("flush waiter was not released")
			}
		})
	}
}

// TestFlushExplicitDeadline bounds only requests whose caller has opted into a deadline.
func TestFlushExplicitDeadline(t *testing.T) {
	f, c := pendingFlushWriter(t, false)
	f.w.conf.WriterFlushTimeout = 20 * time.Millisecond
	result := make(chan syscall.Errno, 1)
	go func() { result <- f.Flush(meta.Background()) }()
	select {
	case got := <-result:
		if got != syscall.EIO {
			t.Fatalf("deadline errno=%v, want EIO", got)
		}
	case <-time.After(time.Second):
		t.Fatal("explicit flush deadline was ignored")
	}
	f.Lock()
	defer f.Unlock()
	if f.chunks[c.indx] != c || c.slices[0].committed {
		t.Fatal("deadline must not discard pending commits")
	}
}

// TestFlushCompletedAtDeadline succeeds if the pending commit completes before the waiter reacquires its lock.
func TestFlushCompletedAtDeadline(t *testing.T) {
	f, c := pendingFlushWriter(t, false)
	f.w.conf.WriterFlushTimeout = 20 * time.Millisecond
	result := make(chan syscall.Errno, 1)
	go func() { result <- f.Flush(meta.Background()) }()
	awaitFlushStart(t, f)
	f.Lock()
	time.Sleep(30 * time.Millisecond)
	f.freeChunk(c)
	f.Unlock()
	select {
	case got := <-result:
		if got != 0 {
			t.Fatalf("already completed flush errno=%v, want success", got)
		}
	case <-time.After(time.Second):
		t.Fatal("completed flush did not return")
	}
}

// TestFlushCancellation preserves explicit request cancellation without discarding its pending data.
func TestFlushCancellation(t *testing.T) {
	f, c := pendingFlushWriter(t, false)
	ctx := meta.Background()
	result := make(chan syscall.Errno, 1)
	go func() { result <- f.Flush(ctx) }()
	awaitFlushStart(t, f)
	ctx.Cancel()
	f.Lock()
	f.flushcond.Broadcast()
	f.Unlock()
	select {
	case got := <-result:
		if got != syscall.EINTR {
			t.Fatalf("cancel errno=%v, want EINTR", got)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled flush did not return")
	}
	f.Lock()
	defer f.Unlock()
	if f.chunks[c.indx] != c {
		t.Fatal("cancellation discarded pending writes")
	}
}

// TestWriterFlushTimeoutModes verifies independent deadlines and explicit legacy retry-derived deadlines.
func TestWriterFlushTimeoutModes(t *testing.T) {
	for _, tc := range []struct {
		timeout time.Duration
		retries uint32
		want    time.Duration
	}{
		{0, 5, 0}, {0, 100, 0}, {time.Second, 5, time.Second},
		{AutoWriterFlushTimeout, 5, 5 * time.Minute},
		{AutoWriterFlushTimeout, 30, 512 * time.Second},
		{AutoWriterFlushTimeout, 150000, time.Duration(1<<63 - 1)},
		{AutoWriterFlushTimeout, ^uint32(0), time.Duration(1<<63 - 1)},
	} {
		w := &dataWriter{conf: &Config{WriterFlushTimeout: tc.timeout}, maxRetries: tc.retries}
		if got := w.flushTimeout(); got != tc.want {
			t.Errorf("timeout=%s retries=%d: got %s want %s", tc.timeout, tc.retries, got, tc.want)
		}
	}
}

// TestFlushDefaultSurvivesLegacyDeadline exercises the former five-minute failure window on demand.
func TestFlushDefaultSurvivesLegacyDeadline(t *testing.T) {
	if os.Getenv("JFS_TEST_LONG_FLUSH_WAIT") != "1" {
		t.Skip("set JFS_TEST_LONG_FLUSH_WAIT=1 to exercise the real five-minute deadline")
	}
	var writers []*fileWriter
	var chunks []*chunkWriter
	var results []chan syscall.Errno
	for _, writeback := range []bool{false, true} {
		f, c := pendingFlushWriter(t, writeback)
		result := make(chan syscall.Errno, 1)
		go func() { result <- f.Flush(meta.Background()) }()
		awaitFlushStart(t, f)
		writers = append(writers, f)
		chunks = append(chunks, c)
		results = append(results, result)
	}
	// This is a deliberate wall-clock integration test: advance beyond the old deadline.
	deadline := time.NewTimer(5*time.Minute + 4*time.Second)
	defer deadline.Stop()
	select {
	case got := <-results[0]:
		t.Fatalf("normal flush returned before commit completion: %v", got)
	case got := <-results[1]:
		t.Fatalf("writeback flush returned before commit completion: %v", got)
	case <-deadline.C:
	}
	for i, f := range writers {
		f.Lock()
		f.freeChunk(chunks[i])
		f.Unlock()
		select {
		case got := <-results[i]:
			if got != 0 {
				t.Fatalf("mode %d flush failed after eventual completion: %v", i, got)
			}
		case <-time.After(time.Second):
			t.Fatal("completed long flush did not resume")
		}
	}
}

// TestWriterFlushNegativeTimeout diagnoses invalid Go configuration and uses the safe wait policy.
func TestWriterFlushNegativeTimeout(t *testing.T) {
	conf := &Config{Meta: meta.DefaultConf(), Chunk: &chunk.Config{}, WriterFlushTimeout: -time.Second}
	w := NewDataWriter(conf, nil, nil, nil).(*dataWriter)
	if conf.WriterFlushTimeout != 0 || w.flushTimeout() != 0 {
		t.Fatal("unsupported negative timeout must be normalized to the no-deadline policy")
	}
}

// TestFlushCancellationGrace keeps pending commits alive during the configured cancellation grace.
func TestFlushCancellationGrace(t *testing.T) {
	f, _ := pendingFlushWriter(t, false)
	f.w.conf.Chunk.PutTimeout = 250 * time.Millisecond
	ctx := meta.Background()
	result := make(chan syscall.Errno, 1)
	go func() { result <- f.Flush(ctx) }()
	awaitFlushStart(t, f)
	ctx.Cancel()
	f.Lock()
	f.flushcond.Broadcast()
	f.Unlock()
	select {
	case got := <-result:
		t.Fatalf("flush canceled inside grace period: %v", got)
	case <-time.After(20 * time.Millisecond):
	}
	// Force a wakeup after the grace period without waiting for the three-second poll.
	time.Sleep(500 * time.Millisecond)
	f.Lock()
	f.flushcond.Broadcast()
	f.Unlock()
	select {
	case got := <-result:
		if got != syscall.EINTR {
			t.Fatalf("grace-expired cancel errno=%v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("flush ignored cancellation after its grace period")
	}
}
