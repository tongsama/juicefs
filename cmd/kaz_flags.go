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

	"github.com/urfave/cli/v2"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
)

// kazSetFlagUsage replaces the Usage text of the flag called name in flags.
// It is used to reword an upstream flag without editing the upstream literal.
// It panics when the flag is missing or has an unsupported type, so that an
// upstream rename, removal or type change shows up at the first command build
// instead of silently dropping the kaz wording.
func kazSetFlagUsage(flags []cli.Flag, name, usage string) {
	for _, f := range flags {
		if f.Names()[0] != name {
			continue
		}
		switch ff := f.(type) {
		case *cli.StringFlag:
			ff.Usage = usage
		case *cli.IntFlag:
			ff.Usage = usage
		default:
			panic(fmt.Sprintf("kaz: cannot reword usage of flag --%s: unsupported flag type %T", name, f))
		}
		return
	}
	panic(fmt.Sprintf("kaz: cannot reword usage of flag --%s: not found in the upstream flags", name))
}

// kazStorageFlags returns the upstream data storage flags with the kaz wording
// of --max-deletes, followed by the kaz data storage flags (block download
// header timeout and compaction policy). It appends to the upstream slice, so
// pass the upstream flag literal directly.
func kazStorageFlags(upstream []cli.Flag) []cli.Flag {
	kazSetFlagUsage(upstream, "max-deletes", "number of background threads to delete objects; 0 disables deletion, negative values delete synchronously in the caller (not unlimited concurrency)")
	return append(upstream, []cli.Flag{
		&cli.StringFlag{
			Name:  "kaz-get-header-timeout",
			Value: "0",
			Usage: "[kaz] give up and retry a block download when the object store sends no response headers within this time (0 disables it); unlike --get-timeout, which bounds the whole download, this bounds only the wait before the data starts",
		},
		&cli.StringFlag{
			Name: "kaz-compaction-gc-mode", Value: "legacy",
			Aliases: []string{"compaction-gc-mode"}, // deprecated old name
			Usage:   "[kaz] obsolete-slice cleanup notification: legacy waits for queue space; deferred uses durable references and bounded asynchronous hints (--compaction-gc-mode is a deprecated alias)",
			Action: func(c *cli.Context, value string) error {
				return (&meta.Config{MaxDeletes: c.Int("max-deletes"), NoBGJob: c.Bool("no-bgjob"), ReadOnly: c.Bool("read-only"), KazMetaConfig: meta.KazMetaConfig{CompactionGCMode: value}}).ValidateCompactionGC()
			},
		},
		&cli.StringFlag{
			Name: "kaz-compaction-scheduler", Value: "legacy",
			Aliases: []string{"compaction-scheduler"}, // deprecated old name
			Usage:   "[kaz] background compaction admission: legacy or bounded advisory priority scheduling (--compaction-scheduler is a deprecated alias)",
			Action: func(_ *cli.Context, value string) error {
				return (&meta.Config{KazMetaConfig: meta.KazMetaConfig{CompactionScheduler: value}}).ValidateCompactionScheduler()
			},
		},
		&cli.IntFlag{
			Name:  "kaz-compaction-read-min",
			Value: 5,
			Usage: "[kaz] when a read reloads a chunk's slice list from metadata (open-file chunk cache miss), queue background compaction of that 64 MiB chunk only if it has at least this many slice records (5-2499; 5 keeps the upstream behavior; compaction triggered by writes is unchanged)",
		},
		&cli.StringFlag{
			Name:  "kaz-compaction-mode",
			Value: "upstream",
			Usage: "[kaz] how compaction rewrites a chunk: upstream (the whole merged range as one new slice), block (only the blocks changed since the last compaction, one new slice per block; juicefs compact and gc --compact leave every chunk as one slice per block with no hidden data) or delta (like block, but scattered small changes over existing blocks are packed into one small slice per compaction; see --kaz-compaction-delta-*; on TKV engines (TiKV, etcd, BadgerDB, FoundationDB) delta falls back to block; delta also downloads small objects (1 MiB or less) whole on a cache miss instead of by range, which changes nothing with --compress lz4 or zstd because compressed blocks are always downloaded whole; do not run reflink copies (copy_file_range, cp --reflink) from unmodified clients while compaction runs); the synchronous compaction at 2500 slices stays upstream",
		},
		&cli.Float64Flag{
			Name:  "kaz-compaction-block-hidden-ratio",
			Value: 0.5,
			Usage: "[kaz] with --kaz-compaction-mode block, rewrite the whole chunk (one slice per block) when more than this share of the kept slices' data would be hidden by newer data (0 < ratio <= 1)",
		},
		&cli.IntFlag{
			Name:  "kaz-compaction-block-max-slices",
			Value: 0,
			Usage: "[kaz] with --kaz-compaction-mode block, rewrite the whole chunk when more than this many slice records would remain (0 = blocks per chunk + 1, 17 for 4 MiB blocks)",
		},
		&cli.IntFlag{
			Name:  "kaz-compaction-delta-tiers",
			Value: 8,
			Usage: "[kaz] with --kaz-compaction-mode delta, the maximum number of delta tiers kept on top of a block's base data; when exceeded, all deltas of the block are merged into one tier (1-16; 1 resends the whole delta every time); clients without delta mode (upstream or block) still read and write these chunks correctly but rewrite the touched blocks or the whole chunk",
		},
		&cli.IntFlag{
			Name:  "kaz-compaction-delta-max-records",
			Value: 500,
			Usage: "[kaz] with --kaz-compaction-mode delta, fold deltas into their blocks (block-wise rewrite) when a chunk would hold more than this many slice records (50-800)",
		},
		&cli.StringFlag{
			Name:  "kaz-compaction-delta-gap",
			Value: "16K",
			Usage: "[kaz] with --kaz-compaction-mode delta, join changes separated by a gap of at most this size, copying the original data of the gap into the delta, to reduce slice records (0-1M, 0 joins nothing; e.g. 16K)",
		},
		&cli.StringFlag{
			Name:  "kaz-compaction-delta-min-file-size",
			Value: "0",
			Usage: "[kaz] with --kaz-compaction-mode delta, use block mode for chunks of files shorter than this size (e.g. 1G; 0 does not restrict)",
		},
		&cli.IntFlag{
			Name:  "kaz-compaction-delta-trigger",
			Value: 99,
			Usage: "[kaz] with --kaz-compaction-mode delta, compact a chunk once this many slice records were added since its last compaction (10-400; max-records + trigger + 50 must not exceed 1000); a lower value compacts sooner and may clean more small staged slices before they upload, at the cost of more compactions and metadata reads; a higher value compacts less often",
		},
		&cli.Float64Flag{
			Name:  "kaz-compaction-delta-block-ratio",
			Value: 0.5,
			Usage: "[kaz] with --kaz-compaction-mode delta, rewrite a block whole instead of keeping its changes in the delta once the changed bytes reach this share of the block size (0 < ratio <= 1; 1 rewrites only fully changed blocks); a higher value keeps more in the small delta slice, a lower value rewrites more blocks",
		},
	}...)
}

// kazDataCacheFlags returns the upstream data cache flags with the kaz wording
// of --cache-eviction, followed by the kaz data cache flags (writer, staging
// and kaz-2q cache eviction settings). It appends to the upstream slice, so
// pass the upstream flag literal directly.
func kazDataCacheFlags(upstream []cli.Flag) []cli.Flag {
	kazSetFlagUsage(upstream, "cache-eviction", fmt.Sprintf("cache eviction policy [%s, %s, %s, %s]", chunk.EvictionNone, chunk.Eviction2Random, chunk.EvictionLRU, chunk.EvictionKaz2Q))
	return append(upstream, []cli.Flag{
		&cli.BoolFlag{
			Name:  "kaz-finish-canceled-get",
			Usage: "[kaz] keep a full-block download running after its read is canceled (e.g. dropped readahead) and store the block in the disk cache; requires the disk cache",
		},
		&cli.BoolFlag{
			Name:    "kaz-writeback-fsync",
			Aliases: []string{"writeback-fsync"}, // deprecated old name
			Value:   true,
			Usage:   "[kaz] fdatasync staged blocks and fsync their directories before acknowledging writes in writeback mode; disabling it is faster but staged data may be lost on an OS crash (--writeback-fsync is a deprecated alias)",
		},
		&cli.StringFlag{
			Name:    "kaz-slice-flush-wait",
			Aliases: []string{"slice-flush-wait"}, // deprecated old name
			Value:   "5s",
			Usage:   "[kaz] maximum duration before flushing a pending slice (--slice-flush-wait is a deprecated alias)",
		},
		&cli.StringFlag{
			Name:    "kaz-slice-flush-idle",
			Aliases: []string{"slice-flush-idle"}, // deprecated old name
			Value:   "1s",
			Usage:   "[kaz] idle duration before flushing a pending slice (--slice-flush-idle is a deprecated alias)",
		},
		&cli.IntFlag{
			Name: "kaz-writer-reuse-window", Value: 4,
			Aliases: []string{"writer-reuse-window"}, // deprecated old name
			Usage:   "[kaz] pending slice search distance before freezing old candidates (1..64); does not bypass flush barriers or merge gaps (--writer-reuse-window is a deprecated alias)",
			Action: func(_ *cli.Context, value int) error {
				if value < 1 || value > 64 {
					return fmt.Errorf("kaz-writer-reuse-window must be between 1 and 64")
				}
				return nil
			},
		},
		&cli.StringFlag{
			Name:    "kaz-writer-flush-timeout",
			Aliases: []string{"writer-flush-timeout"}, // deprecated old name
			Value:   "0s",
			Usage:   "[kaz] deadline for pending file writes; 0s waits until completion, auto uses the legacy retry-derived deadline, positive durations opt into timeout errors (--writer-flush-timeout is a deprecated alias)",
			Action: func(_ *cli.Context, value string) error {
				_, err := parseWriterFlushTimeout(value)
				return err
			},
		},
		&cli.StringFlag{
			Name:    "kaz-writer-flush-scope",
			Aliases: []string{"writer-flush-scope"}, // deprecated old name
			Value:   vfs.WriterFlushScopeFile,
			Usage:   "[kaz] pending writes committed before read and fallocate: file waits for the whole file, range only for the touched chunks; fsync and close always wait for the whole file (--writer-flush-scope is a deprecated alias)",
			Action: func(_ *cli.Context, value string) error {
				if value != vfs.WriterFlushScopeFile && value != vfs.WriterFlushScopeRange {
					return fmt.Errorf("invalid kaz-writer-flush-scope %q: use file or range", value)
				}
				return nil
			},
		},
		&cli.IntFlag{
			Name:    "kaz-meta-write-batch",
			Aliases: []string{"meta-write-batch"}, // deprecated old name
			Value:   0,
			Usage:   "[kaz] maximum slices of one chunk committed in one metadata transaction (0 disables, up to 1024); applies to every commit including fsync (--meta-write-batch is a deprecated alias)",
			Action: func(_ *cli.Context, value int) error {
				if value < 0 || value > 1024 {
					return fmt.Errorf("kaz-meta-write-batch must be between 0 and 1024")
				}
				return nil
			},
		},
		&cli.Float64Flag{
			Name:  "kaz-cache-probation-ratio",
			Value: 0.4,
			Usage: "[kaz] share of the cache capacity for newly cached blocks under --cache-eviction kaz-2q (0 < x < 1)",
		},
		&cli.StringFlag{
			Name:  "kaz-cache-promote-delay",
			Value: "10m",
			Usage: "[kaz] correlated-reference period before a cached block can be promoted under --cache-eviction kaz-2q",
		},
		&cli.Float64Flag{
			Name:  "kaz-cache-ghost-ratio",
			Value: 4,
			Usage: "[kaz] capacity of the evicted-key list as a multiple of the cache capacity under --cache-eviction kaz-2q (0 disables)",
		},
		&cli.StringFlag{
			Name:  "kaz-cache-state-interval",
			Value: "5m",
			Usage: "[kaz] save interval of the kaz-cache-state.v1 file under --cache-eviction kaz-2q",
		},
		&cli.StringFlag{
			Name:  "kaz-cache-compaction-placement",
			Value: "inherit",
			Usage: "[kaz] segment of a cached compaction output block under --cache-eviction kaz-2q: inherit (protected if any source block is protected), probation (always probation), replace (protected only if a protected source block was copied whole into it), share (protected if the bytes copied from protected source blocks are at least --kaz-cache-compaction-placement-share-ratio of its non-hole bytes); an output is cached only when a source block is cached",
		},
		&cli.Float64Flag{
			Name:  "kaz-cache-compaction-placement-share-ratio",
			Value: 0.5,
			Usage: "[kaz] threshold of --kaz-cache-compaction-placement share: the share of an output block's non-hole bytes copied from protected source blocks needed to cache it in protected (0 < x <= 1; used only with share)",
		},
		&cli.BoolFlag{
			Name:  "kaz-cache-demote-unread-first",
			Usage: "[kaz] under --cache-eviction kaz-2q, when the protected segment overflows, demote the protected blocks not read since they entered it (e.g. compaction outputs) first, oldest read first, and the read ones only after them; the same order applies to direct eviction from protected when probation is empty; off orders both by last read time only",
		},
		&cli.StringFlag{
			Name:  "kaz-cache-compaction-hot-age",
			Value: "0",
			Usage: "[kaz] under --cache-eviction kaz-2q, a source block that is itself a compaction output cached less than this ago does not count as a protected source when placing a compaction output (no share bytes, no read mark); an output left without another protected source goes to probation (0 disables)",
		},
	}...)
}
