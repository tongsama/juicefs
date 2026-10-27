/*
 * JuiceFS, Copyright 2022 Juicedata, Inc.
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
	"os"
	"path"
	"runtime"

	"github.com/juicedata/juicefs/pkg/chunk"
	"github.com/juicedata/juicefs/pkg/meta"
	"github.com/juicedata/juicefs/pkg/vfs"
	"github.com/urfave/cli/v2"
)

func globalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"debug", "v"},
			Usage:   "enable debug log",
		},
		&cli.BoolFlag{
			Name:    "quiet",
			Aliases: []string{"q"},
			Usage:   "show warning and errors only",
		},
		&cli.BoolFlag{
			Name:  "trace",
			Usage: "enable trace log",
		},
		&cli.StringFlag{
			Name:   "log-level",
			Usage:  "set log level (trace, debug, info, warn, error, fatal, panic)",
			Hidden: true,
		},
		&cli.StringFlag{
			Name:  "log-id",
			Usage: "append the given log id in log, use \"random\" to use random uuid",
		},
		&cli.BoolFlag{
			Name:  "no-agent",
			Usage: "disable pprof (:6060) agent",
		},
		&cli.StringFlag{
			Name:  "pyroscope",
			Usage: "pyroscope address",
		},
		&cli.BoolFlag{
			Name:  "no-color",
			Usage: "disable colors",
		},
	}
}

func addCategory(f cli.Flag, cat string) {
	switch ff := f.(type) {
	case *cli.StringFlag:
		ff.Category = cat
	case *cli.BoolFlag:
		ff.Category = cat
	case *cli.IntFlag:
		ff.Category = cat
	case *cli.Int64Flag:
		ff.Category = cat
	case *cli.Uint64Flag:
		ff.Category = cat
	case *cli.Float64Flag:
		ff.Category = cat
	case *cli.DurationFlag:
		ff.Category = cat
	case *cli.StringSliceFlag:
		ff.Category = cat
	default:
		panic(f)
	}
}

func addCategories(cat string, flags []cli.Flag) []cli.Flag {
	for _, f := range flags {
		addCategory(f, cat)
	}
	return flags
}

func storageFlags() []cli.Flag {
	return addCategories("DATA STORAGE", []cli.Flag{
		&cli.StringFlag{
			Name:  "storage",
			Usage: "customized storage type (e.g. s3, gs, oss, cos) to access object store",
		},
		&cli.StringFlag{
			Name:  "bucket",
			Usage: "customized endpoint to access object store",
		},
		&cli.StringFlag{
			Name:  "storage-class",
			Usage: "the storage class for data written by current client",
		},
		&cli.StringFlag{
			Name:  "tag",
			Usage: "custom tag when uploading object storage (e.g. --tag key=value)",
		},
		&cli.StringFlag{
			Name:  "get-timeout",
			Value: "60s",
			Usage: "the timeout to download an object",
		},
		&cli.StringFlag{
			Name:  "kaz-get-header-timeout",
			Value: "0",
			Usage: "[kaz] give up and retry a block download when the object store sends no response headers within this time (0 disables it); unlike --get-timeout, which bounds the whole download, this bounds only the wait before the data starts",
		},
		&cli.StringFlag{
			Name:  "put-timeout",
			Value: "60s",
			Usage: "the timeout to upload an object",
		},
		&cli.IntFlag{
			Name:  "io-retries",
			Value: 10,
			Usage: "number of retries after network failure",
		},
		&cli.IntFlag{
			Name:  "max-uploads",
			Value: 20,
			Usage: "number of connections to upload",
		},
		&cli.IntFlag{
			Name:  "max-downloads",
			Value: 200,
			Usage: "number of connections to download",
		},
		&cli.IntFlag{
			Name:  "max-stage-write",
			Value: 1000, // large enough for normal cases, also prevents unlimited concurrency in abnormal cases
			Usage: "number of threads allowed to write staged files, other requests will be uploaded directly (this option is only effective when 'writeback' mode is enabled)",
		},
		&cli.IntFlag{
			Name:  "max-deletes",
			Value: 10,
			Usage: "number of background threads to delete objects; 0 disables deletion, negative values delete synchronously in the caller (not unlimited concurrency)",
		},
		&cli.StringFlag{
			Name: "compaction-gc-mode", Value: "legacy",
			Usage: "obsolete-slice cleanup notification: legacy waits for queue space; deferred uses durable references and bounded asynchronous hints",
			Action: func(c *cli.Context, value string) error {
				return (&meta.Config{CompactionGCMode: value, MaxDeletes: c.Int("max-deletes"), NoBGJob: c.Bool("no-bgjob"), ReadOnly: c.Bool("read-only")}).ValidateCompactionGC()
			},
		},
		&cli.StringFlag{
			Name: "compaction-scheduler", Value: "legacy",
			Usage: "background compaction admission: legacy or bounded advisory priority scheduling",
			Action: func(_ *cli.Context, value string) error {
				return (&meta.Config{CompactionScheduler: value}).ValidateCompactionScheduler()
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
		&cli.StringFlag{
			Name:  "upload-limit",
			Usage: "bandwidth limit for upload in Mbps",
		},
		&cli.StringFlag{
			Name:  "download-limit",
			Usage: "bandwidth limit for download in Mbps",
		},
		&cli.BoolFlag{
			Name: "check-storage",
			// AK/SK should have been checked before creating volume, here checks client access to the storage
			Usage: "test storage before mounting to expose access issues early",
		},
	})
}

func getDefaultCacheDir() string {
	var defaultCacheDir = "/var/jfsCache"
	switch runtime.GOOS {
	case "linux":
		if os.Getuid() == 0 {
			break
		}
		fallthrough
	case "darwin":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Warnf("%v", err)
			homeDir = defaultCacheDir
		}
		defaultCacheDir = path.Join(homeDir, ".juicefs", "cache")
	case "windows":
		homeDir, err := os.UserHomeDir()
		if err != nil {
			logger.Fatalf("%v", err)
			return ""
		}
		defaultCacheDir = path.Join(homeDir, ".juicefs", "cache")
	}
	return defaultCacheDir
}

func dataCacheFlags() []cli.Flag {
	var defaultCacheDir = getDefaultCacheDir()
	return addCategories("DATA CACHE", []cli.Flag{
		&cli.StringFlag{
			Name:  "buffer-size",
			Value: "300M",
			Usage: "total read/write buffering in MiB",
		},
		&cli.StringFlag{
			Name:  "max-readahead",
			Usage: "max buffering for read ahead in MiB per read session",
		},
		&cli.IntFlag{
			Name:  "prefetch",
			Value: 1,
			Usage: "prefetch N blocks in parallel",
		},
		&cli.BoolFlag{
			Name:  "kaz-finish-canceled-get",
			Usage: "[kaz] keep a full-block download running after its read is canceled (e.g. dropped readahead) and store the block in the disk cache; requires the disk cache",
		},
		&cli.BoolFlag{
			Name:  "writeback",
			Usage: "upload blocks in background",
		},
		&cli.StringFlag{
			Name:  "writeback-threshold-size",
			Value: "0",
			Usage: "blocks smaller than this size will be staged, 0 means all staged.",
		},
		&cli.BoolFlag{
			Name:  "writeback-fsync",
			Value: true,
			Usage: "fdatasync staged blocks and fsync their directories before acknowledging writes in writeback mode; disabling it is faster but staged data may be lost on an OS crash",
		},
		&cli.StringFlag{
			Name:  "slice-flush-wait",
			Value: "5s",
			Usage: "maximum duration before flushing a pending slice",
		},
		&cli.StringFlag{
			Name:  "slice-flush-idle",
			Value: "1s",
			Usage: "idle duration before flushing a pending slice",
		},
		&cli.IntFlag{
			Name: "writer-reuse-window", Value: 4,
			Usage: "pending slice search distance before freezing old candidates (1..64); does not bypass flush barriers or merge gaps",
			Action: func(_ *cli.Context, value int) error {
				if value < 1 || value > 64 {
					return fmt.Errorf("writer-reuse-window must be between 1 and 64")
				}
				return nil
			},
		},
		&cli.StringFlag{
			Name:  "writer-flush-timeout",
			Value: "0s",
			Usage: "deadline for pending file writes; 0s waits until completion, auto uses the legacy retry-derived deadline, positive durations opt into timeout errors",
			Action: func(_ *cli.Context, value string) error {
				_, err := parseWriterFlushTimeout(value)
				return err
			},
		},
		&cli.StringFlag{
			Name:  "writer-flush-scope",
			Value: vfs.WriterFlushScopeFile,
			Usage: "pending writes committed before read and fallocate: file waits for the whole file, range only for the touched chunks; fsync and close always wait for the whole file",
			Action: func(_ *cli.Context, value string) error {
				if value != vfs.WriterFlushScopeFile && value != vfs.WriterFlushScopeRange {
					return fmt.Errorf("invalid writer-flush-scope %q: use file or range", value)
				}
				return nil
			},
		},
		&cli.IntFlag{
			Name:  "meta-write-batch",
			Value: 0,
			Usage: "maximum slices of one chunk committed in one metadata transaction (0 disables, up to 1024); applies to every commit including fsync",
			Action: func(_ *cli.Context, value int) error {
				if value < 0 || value > 1024 {
					return fmt.Errorf("meta-write-batch must be between 0 and 1024")
				}
				return nil
			},
		},
		&cli.StringFlag{
			Name:  "upload-delay",
			Value: "0s",
			Usage: "delayed duration for uploading blocks",
		},
		&cli.StringFlag{
			Name:  "upload-hours",
			Usage: "(start-end) hour of a day between which the delayed blocks can be uploaded",
		},
		&cli.StringFlag{
			Name:  "cache-dir",
			Value: defaultCacheDir,
			Usage: "directory paths of local cache, use colon to separate multiple paths",
		},
		&cli.StringFlag{
			Name:  "cache-mode",
			Value: "0600", // only owner can read/write cache
			Usage: "file permissions for cached blocks",
		},
		&cli.StringFlag{
			Name:  "cache-size",
			Value: "100G",
			Usage: "size of cached object for read in MiB",
		},
		&cli.Int64Flag{
			Name:  "cache-items",
			Value: 0,
			Usage: "max number of cached items (0 will be automatically calculated based on the `free‑space‑ratio`.)",
		},
		&cli.Float64Flag{
			Name:  "free-space-ratio",
			Value: 0.1,
			Usage: "min free space (ratio)",
		},
		&cli.BoolFlag{
			Name:  "cache-partial-only",
			Usage: "cache only random/small read",
		},
		&cli.BoolFlag{
			Name:  "cache-large-write",
			Usage: "cache full blocks after uploading",
		},
		&cli.StringFlag{
			Name:  "verify-cache-checksum",
			Value: "extend",
			Usage: "checksum level (none, full, shrink, extend)",
		},
		&cli.StringFlag{
			Name:  "cache-eviction",
			Value: chunk.Eviction2Random,
			Usage: fmt.Sprintf("cache eviction policy [%s, %s, %s, %s]", chunk.EvictionNone, chunk.Eviction2Random, chunk.EvictionLRU, chunk.EvictionKaz2Q),
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
		&cli.StringFlag{
			Name:  "cache-scan-interval",
			Value: "1h",
			Usage: "interval to scan cache-dir to rebuild in-memory index",
		},
		&cli.StringFlag{
			Name:  "cache-expire",
			Value: "0s",
			Usage: "cached blocks not accessed for longer than this option will be automatically evicted (0 means never)",
		},
	})
}

func metaFlags() []cli.Flag {
	return addCategories("META", []cli.Flag{
		&cli.StringFlag{
			Name:  "subdir",
			Usage: "mount a sub-directory as root",
		},
		&cli.StringFlag{
			Name:  "backup-meta",
			Value: "1h",
			Usage: "interval to automatically backup metadata in the object storage (0 means disable backup)",
		},
		&cli.BoolFlag{
			Name:  "backup-skip-trash",
			Usage: "skip files in trash when backup metadata",
		},
		&cli.StringFlag{
			Name:  "heartbeat",
			Value: "12s",
			Usage: "interval to send heartbeat; it's recommended that all clients use the same heartbeat value",
		},
		&cli.BoolFlag{
			Name:  "read-only",
			Usage: "allow lookup/read operations only",
		},
		&cli.BoolFlag{
			Name:  "no-bgjob",
			Usage: "disable background jobs (clean-up, backup, etc.)",
		},
		&cli.StringFlag{
			Name:  "atime-mode",
			Value: "noatime",
			Usage: "when to update atime, supported mode includes: noatime, relatime, strictatime",
		},
		&cli.IntFlag{
			Name:  "skip-dir-nlink",
			Value: 20,
			Usage: "number of retries after which the update of directory nlink will be skipped (used for tkv only, 0 means never)",
		},
		&cli.StringFlag{
			Name:  "skip-dir-mtime",
			Value: "100ms",
			Usage: "skip updating attribute of a directory if the mtime difference is smaller than this value",
		},
		&cli.BoolFlag{
			Name:  "sort-dir",
			Usage: "sort entries within a directory by name",
		},
		&cli.BoolFlag{
			Name:  "fast-statfs",
			Value: false,
			Usage: "Use local counters for statfs instead of querying metadata service",
		},
		&cli.StringFlag{
			Name:  "network-interfaces",
			Usage: "comma-separated list of network interfaces to use for IP discovery (e.g. eth0,en0), empty means all",
		},
	})
}

func clientFlags(defaultEntryCache float64) []cli.Flag {
	return expandFlags(
		metaFlags(),
		metaCacheFlags(defaultEntryCache),
		storageFlags(),
		dataCacheFlags(),
	)
}

func shareInfoFlags() []cli.Flag {
	return addCategories("METRICS", []cli.Flag{
		&cli.StringFlag{
			Name:  "metrics",
			Value: "127.0.0.1:9567",
			Usage: "address to export metrics",
		},
		&cli.StringFlag{
			Name:  "custom-labels",
			Usage: "custom labels for metrics",
		},
		&cli.StringFlag{
			Name:  "consul",
			Value: "127.0.0.1:8500",
			Usage: "consul address to register",
		},
		&cli.BoolFlag{
			Name:  "no-usage-report",
			Usage: "do not send usage report",
		},
	})
}

func metaCacheFlags(defaultEntryCache float64) []cli.Flag {
	return addCategories("META CACHE", []cli.Flag{
		&cli.StringFlag{
			Name:  "attr-cache",
			Value: "1.0s",
			Usage: "attributes cache timeout",
		},
		&cli.StringFlag{
			Name:  "entry-cache",
			Value: fmt.Sprintf("%.1fs", defaultEntryCache),
			Usage: "file entry cache timeout",
		},
		&cli.StringFlag{
			Name:  "dir-entry-cache",
			Value: "1.0s",
			Usage: "dir entry cache timeout",
		},
		&cli.StringFlag{
			Name:  "negative-entry-cache",
			Usage: "cache timeout for negative entry lookups",
		},
		&cli.BoolFlag{
			Name:  "readdir-cache",
			Usage: "enable kernel caching of readdir entries, with timeout controlled by attr-cache flag (require linux kernel 4.20+)",
		},
		&cli.StringFlag{
			Name:  "open-cache",
			Value: "0s",
			Usage: "The cache time to reuse open file without checking update (0 means disable this feature)",
		},
		&cli.Uint64Flag{
			Name:  "open-cache-limit",
			Value: 10000,
			Usage: "max number of open files to cache (soft limit, 0 means unlimited)",
		},
	})
}

func expandFlags(compoundFlags ...[]cli.Flag) []cli.Flag {
	var flags []cli.Flag
	for _, flag := range compoundFlags {
		flags = append(flags, flag...)
	}
	return flags
}
