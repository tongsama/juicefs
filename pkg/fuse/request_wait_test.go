//go:build linux

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

package fuse

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	gofs "github.com/hanwen/go-fuse/v2/fs"
	goFuse "github.com/hanwen/go-fuse/v2/fuse"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// TestGenFuseOptHasNoRequestDeadline prevents go-fuse from failing pending durable operations.
func TestGenFuseOptHasNoRequestDeadline(t *testing.T) {
	for _, deadline := range []time.Duration{0, vfs.AutoWriterFlushTimeout, time.Hour} {
		t.Run(deadline.String(), func(t *testing.T) {
			conf := &vfs.Config{WriterFlushTimeout: deadline}
			opt := GenFuseOpt(conf, "allow_other", 1, false, false, 128<<10)
			if opt.Timeout != 0 {
				t.Fatalf("FUSE request watchdog=%s with writer deadline=%s; watchdog can reply EINTR before storage/metadata commit completes", opt.Timeout, deadline)
			}
		})
	}
}

// pendingFsyncNode exposes a real FUSE fsync that cannot finish before simulated storage completion.
type pendingFsyncNode struct {
	gofs.Inode
	started  chan struct{}
	release  chan struct{}
	canceled chan struct{}
	once     sync.Once
	errno    syscall.Errno
}

// Getattr makes only the isolated test file accessible to the mounting user.
func (n *pendingFsyncNode) Getattr(ctx context.Context, file gofs.FileHandle, out *goFuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFREG | 0644
	out.Uid, out.Gid = uint32(os.Getuid()), uint32(os.Getgid())
	return 0
}

// Open supplies a handle so the kernel can issue a real fsync to the test node.
func (n *pendingFsyncNode) Open(ctx context.Context, flags uint32) (gofs.FileHandle, uint32, syscall.Errno) {
	return n, 0, 0
}

// Fsync distinguishes explicit completion/error from a server-induced request cancellation.
func (n *pendingFsyncNode) Fsync(ctx context.Context, file gofs.FileHandle, flags uint32) syscall.Errno {
	n.once.Do(func() { close(n.started) })
	select {
	case <-n.release:
		return n.errno
	case <-ctx.Done():
		close(n.canceled)
		return syscall.EINTR
	}
}

// TestFUSEFsyncCancelHelper issues only the inherited isolated-test descriptor's fsync.
func TestFUSEFsyncCancelHelper(t *testing.T) {
	if os.Getenv("JFS_TEST_FUSE_CANCEL_HELPER") != "1" {
		t.Skip("subprocess helper")
	}
	if err := syscall.Fsync(3); err != nil {
		t.Fatal(err)
	}
}

// TestFUSEWaitsForPendingFsync checks real kernel requests, real errors, and a calibrated watchdog failure.
func TestFUSEWaitsForPendingFsync(t *testing.T) {
	if _, err := os.Stat("/dev/fuse"); err != nil {
		t.Skipf("FUSE device unavailable: %v", err)
	}
	for _, tc := range []struct {
		name         string
		watchdog     bool
		kernelCancel bool
		errno        syscall.Errno
	}{
		{"completion", false, false, 0}, {"storage_error", false, false, syscall.ENOSPC},
		{"watchdog_reproduction", true, false, 0}, {"kernel_cancel", false, true, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := GenFuseOpt(&vfs.Config{}, "", 1, false, false, 128<<10)
			if opt.Timeout != 0 {
				t.Fatalf("unexpected automatic FUSE deadline: %s", opt.Timeout)
			}
			if tc.watchdog {
				opt.Timeout = 100 * time.Millisecond
			}
			root := &gofs.Inode{}
			mp := t.TempDir()
			server, err := gofs.Mount(mp, root, &gofs.Options{MountOptions: opt})
			if err != nil {
				t.Skipf("isolated FUSE mount unavailable: %v", err)
			}
			defer func() {
				if err := server.Unmount(); err != nil {
					t.Errorf("unmount isolated FUSE test: %v", err)
				}
			}()
			node := &pendingFsyncNode{started: make(chan struct{}), release: make(chan struct{}), canceled: make(chan struct{}), errno: tc.errno}
			var release sync.Once
			defer release.Do(func() { close(node.release) })
			root.AddChild("pending", root.NewPersistentInode(context.Background(), node, gofs.StableAttr{Mode: syscall.S_IFREG}), false)
			file, err := os.OpenFile(filepath.Join(mp, "pending"), os.O_RDWR, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			if tc.kernelCancel {
				// Kill only our helper process to make the kernel cancel its pending FUSE request.
				child := exec.Command(os.Args[0], "-test.run=^TestFUSEFsyncCancelHelper$", "-test.timeout=10s")
				child.Env = append(os.Environ(), "JFS_TEST_FUSE_CANCEL_HELPER=1")
				child.ExtraFiles = []*os.File{file}
				if err := child.Start(); err != nil {
					t.Fatal(err)
				}
				defer func() { _ = child.Process.Kill(); _ = child.Wait() }()
				select {
				case <-node.started:
				case <-time.After(3 * time.Second):
					t.Fatal("helper fsync did not reach the pending operation")
				}
				if err := child.Process.Kill(); err != nil {
					t.Fatal(err)
				}
				select {
				case <-node.canceled:
				case <-time.After(3 * time.Second):
					t.Fatal("kernel interrupt was not delivered without a watchdog")
				}
				return
			}
			result := make(chan error, 1)
			// The raw syscall exposes EINTR rather than automatically retrying it in os.File.Sync.
			go func() { result <- syscall.Fsync(int(file.Fd())) }()
			select {
			case <-node.started:
			case <-time.After(3 * time.Second):
				t.Fatal("FUSE fsync did not reach the pending operation")
			}
			if tc.watchdog {
				select {
				case err := <-result:
					if !errors.Is(err, syscall.EINTR) {
						t.Fatalf("watchdog fsync error=%v, want EINTR", err)
					}
				case <-time.After(4 * time.Second):
					t.Fatal("calibrated watchdog did not reproduce cancellation")
				}
				return
			}
			select {
			case err := <-result:
				t.Fatalf("pending fsync returned before completion: %v", err)
			case <-time.After(1200 * time.Millisecond):
			}
			release.Do(func() { close(node.release) })
			select {
			case err := <-result:
				if tc.errno == 0 && err != nil || tc.errno != 0 && !errors.Is(err, tc.errno) {
					t.Fatalf("completed fsync error=%v, want %v", err, tc.errno)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("FUSE fsync did not return after completion")
			}
		})
	}
}
