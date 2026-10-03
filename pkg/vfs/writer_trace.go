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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/sirupsen/logrus"
)

var writerFlushBarrier atomic.Uint64

// writerFlushContext carries diagnostics while delegating credentials and cancellation unchanged.
type writerFlushContext struct {
	meta.Context
	origin string
}

// WithValue preserves diagnostic origin when callers chain immutable metadata context values.
func (c *writerFlushContext) WithValue(k, v interface{}) meta.Context {
	return &writerFlushContext{Context: c.Context.WithValue(k, v), origin: c.origin}
}

// WithWriterFlushOrigin annotates a writer barrier only when DEBUG diagnostics are enabled.
func WithWriterFlushOrigin(ctx meta.Context, origin string) meta.Context {
	if !logger.IsLevelEnabled(logrus.DebugLevel) {
		return ctx
	}
	return &writerFlushContext{Context: ctx, origin: origin}
}

// writerFlushOrigin reads the private annotation without exporting context keys.
func writerFlushOrigin(ctx meta.Context) string {
	if c, ok := ctx.(*writerFlushContext); ok {
		return c.origin
	}
	return "unknown"
}

// writerFlushTrace identifies one actual flush invocation, including concurrent barriers.
type writerFlushTrace struct {
	id      uint64
	origin  string
	inode   Ino
	started time.Time
}

// beginWriterFlush allocates an identity only for enabled DEBUG diagnostics.
func beginWriterFlush(ctx meta.Context, inode Ino) writerFlushTrace {
	if !logger.IsLevelEnabled(logrus.DebugLevel) {
		return writerFlushTrace{}
	}
	t := writerFlushTrace{id: writerFlushBarrier.Add(1), origin: writerFlushOrigin(ctx), inode: inode, started: time.Now()}
	logger.Debugf("writer flush inode=%d origin=%s barrier=%d phase=begin", inode, t.origin, t.id)
	return t
}

// end records the original flush errno and duration without affecting completion semantics.
func (t writerFlushTrace) end(err syscall.Errno) {
	if t.id != 0 {
		logger.Debugf("writer flush inode=%d origin=%s barrier=%d phase=end errno=%d elapsed=%s", t.inode, t.origin, t.id, uint32(err), time.Since(t.started))
	}
}

// logFreeze formats a slice's freeze diagnostics separately from its completion scheduling.
func (s *sliceWriter) logFreeze(reason string, trace writerFlushTrace) {
	if !logger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}
	if trace.origin == "" {
		trace.origin = "none"
	}
	logger.Debugf("slice freeze inode=%d chunk=%d slice=%d off=%d raw_length=%d reason=%s age=%s idle=%s started_unix_ns=%d origin=%s barrier=%d",
		s.chunk.file.inode, s.chunk.indx, s.id, s.off, s.slen, reason, time.Since(s.started), time.Since(s.lastMod), s.started.UnixNano(), trace.origin, trace.id)
}

// logFinish reports the allocated slice identity without changing data completion.
func (s *sliceWriter) logFinish() {
	if !logger.IsLevelEnabled(logrus.DebugLevel) {
		return
	}
	logger.Debugf("slice finish inode=%d chunk=%d slice=%d off=%d raw_length=%d started_unix_ns=%d",
		s.chunk.file.inode, s.chunk.indx, s.id, s.off, s.slen, s.started.UnixNano())
}
