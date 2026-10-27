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

package cmd

import (
	"fmt"
	"time"

	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/utils"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// parseWriterFlushTimeout rejects invalid values rather than silently disabling a deadline.
func parseWriterFlushTimeout(value string) (time.Duration, error) {
	if value == "auto" {
		return vfs.AutoWriterFlushTimeout, nil
	}
	if value == "" {
		return 0, nil
	}
	timeout, err := time.ParseDuration(value)
	if err != nil || timeout < 0 {
		return 0, fmt.Errorf("invalid kaz-writer-flush-timeout %q: use 0s, auto, or a positive duration", value)
	}
	return timeout, nil
}

// applyKazVfsFlags carries the validated kaz writer settings (slice flush
// timing, reuse window, flush timeout and scope, metadata write batch) into the
// VFS config and exits on an invalid flush timeout.
func applyKazVfsFlags(c *cli.Context, cfg *vfs.Config) {
	flushTimeout, err := parseWriterFlushTimeout(c.String("kaz-writer-flush-timeout"))
	if err != nil {
		logger.Fatalf("%s", err)
	}
	cfg.SliceFlushWait = utils.Duration(c.String("kaz-slice-flush-wait"))
	cfg.SliceFlushIdle = utils.Duration(c.String("kaz-slice-flush-idle"))
	cfg.WriterReuseWindow = c.Int("kaz-writer-reuse-window")
	cfg.WriterFlushTimeout = flushTimeout
	cfg.WriterFlushScope = c.String("kaz-writer-flush-scope")
	cfg.MetaWriteBatch = c.Int("kaz-meta-write-batch")
	cfg.KazStagingPack = c.Bool("kaz-staging-pack")
	if cfg.KazStagingPack && c.String("kaz-compaction-mode") != "delta" {
		// packing relies on delta compaction to keep chunks small; without it the pack would only add slice records
		logger.Warnf("--kaz-staging-pack needs --kaz-compaction-mode delta; staging pack is disabled")
		cfg.KazStagingPack = false
	}
}

// kazRegisterMetaMsg registers the kaz metadata message handlers: durable
// metadata decisions retire the local copy of a slice.
func kazRegisterMetaMsg(m meta.Meta, store chunk.ChunkStore) {
	m.OnMsg(meta.RetireSlice, func(args ...interface{}) error {
		if retire, ok := store.(interface {
			Retire(id uint64, length int) error
		}); ok {
			return retire.Retire(args[0].(uint64), int(args[1].(uint32)))
		}
		// Custom stores without local retirement keep cleanup in physical Remove.
		return nil
	})
}
