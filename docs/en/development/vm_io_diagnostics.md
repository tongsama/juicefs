# Diagnosing stalled VM image writes

This local change targets the v1.4.1 client. It propagates the writer preflush error from `VFS.Read`: a read that cannot flush pending writes returns zero bytes and the flush errno, rather than fetching previously committed data. Reader errors retain their existing handling. Normal reads still flush before fetching data. Failed write errors remain sticky for the existing file writer, as before.

The added logs diagnose slow slice commits without changing metadata locks, compaction thresholds, or writeback durability. The subsequent wait-policy change below makes the whole-file flush deadline opt-in. The existing `--slice-flush-wait` and `--slice-flush-idle` options retain their 5s and 1s defaults. These slice timers are different from the whole-file flush deadline.

## Logs

Operations taking at least one second emit WARN summaries at the normal log level:

| Message | Fields and interpretation |
| --- | --- |
| `slow slice commit` | `inode`, `chunk`, `slice`, `metadata`, `errno`: total time spent inside `Meta.Write`, including synchronous compaction. |
| `slow metadata write` | `total`, `lock_wait`, `doWrite`, `stat`, `compact`, `slices`, `errno`: open-file lock acquisition, backend update, parent/user/group statistics, and synchronous compaction durations. |
| `slow metadata transaction` | `key`, `total`, `lock_wait`, `active`, `attempts`, `err`: Redis local transaction lock wait versus transaction execution. `active` includes WATCH/GET/EXEC, pool waits, retry backoff and quota work; it is not a pure Redis network RTT. The key hash selects one of 1,024 locks, so other keys can contend too. |
| `slow compaction` | `inode`, `chunk`, `once`, `force`, `slices`, `bytes`, `total`, `queue_wait`, `object`, `metadata`: compaction admission/existing-job wait, object callback, and backend metadata replacement plus old-slice cleanup (which can include object DELETE). `object` includes VFS memory pressure waits, object reads and uploads. |

Durations do not necessarily add up to `total`: setup, cache invalidation, deferred cleanup and diagnostic logging also contribute. The compaction summary can describe an early exit or failed attempt; it is not proof of successful object upload or metadata replacement. Existing error logs carry the failure details. Forced recursive compaction can include subsequent passes in the outer `total`.

DEBUG logs report entry into processing phases. Enable the existing debug logging option for a bounded reproduction when a call is stuck and has no completion summary. Use a local, responsive log destination: debug output increases volume and some phase messages occur while an inode/transaction lock is held.

Correlate `inode`, `chunk`, and `slice` for VFS and base metadata writes. Redis transaction logs identify `key`; multiple operations on the same key can interleave, so use a complete goroutine dump to resolve ambiguity. A transaction's DEBUG `phase=active` means its local lock has been acquired; absence of a WARN does not prove completion.

To extract the relevant messages from a captured log:

```bash
rg 'slow (slice commit|metadata write|metadata transaction|compaction)|phase=' /path/to/juicefs.log
```

## What the source establishes

`baseMeta.Write` holds the open-file inode lock across backend `doWrite`, statistics updates, and synchronous compaction. Redis also serializes transactions using a hash of the first watched key; `doWrite` watches the inode key. Replacing only the outer lock with a per-chunk lock therefore cannot establish concurrent Redis writes to the same inode.

Background `compactChunk` does not itself acquire the open-file inode lock. However, a write with at least 2,500 raw slices invokes compaction synchronously while retaining that lock, and can wait for an already running background compaction. A later raw list length of 682 for one chunk does not exclude a threshold crossing during the earlier failure or in another chunk.

Pending slices with `done=true`, `err=0`, and `committed=false` show that VFS commit processing has not finished. They do not prove that the backend RPUSH has not happened: `Meta.Write` can remain in statistics or compaction after the backend update succeeds, and later slices can simply be queued behind the first one.

## Next reproduction

Use the diagnostic build on an isolated test image with the same metadata/object topology. Capture the complete log and goroutine dump around a flush timeout, all hot-chunk raw slice counts, and Redis RTT from the client. Compare low-latency Redis with the WAN path, and a local object backend with rclone/Drive, one variable at a time.

The Read regression fix prevents a successful stale read after preflush failure. It does not remove the metadata backlog. Before the wait-policy change, the five-minute-minimum flush deadline could still return EIO. Those failure mechanisms still require measurements on the affected deployment before changing concurrency or compaction policy.

## Local verification (2026-10-01)

The four metadata-failure cases (EIO, ENOSPC, EDQUOT, ENOENT) returned `"old"` with no error before the Read fix. The fixed regression test checks zero bytes, the original flush errno, an untouched buffer, and handle cleanup. The targeted race check passed. The VFS and high-level FS suites passed with a temporary local Redis.

The requested broad Makefile targets were attempted. `test.pkg` requires absent GlusterFS headers/libraries. `test.meta.core` reaches an unconfigured TiKV service in `TestLoadDump` even with `SKIP_NON_CORE=true`; it does not fully complete in this environment. Targeted Redis, SQLite and MemKV suites, quota checks and canceled-KV-transaction checks cover the modified common metadata path.

Additional broad checks found `pkg/chunk` test linking failures in the existing mockey library (`runtime.duffcopy`/`runtime.duffzero`) with Go 1.26.4 and a FUSE `FstatDeleted` time-attribute mismatch. Both reproduce from an unchanged archive of HEAD `febf149a`; they are not introduced by this patch. Full broad test success is therefore not claimed.


## Incident confirmed on 2026-10-01

The user's log `/home/kwatanabe/.juicefs/diagnostics/vm-io-20261001-183557.log` identifies the immediate cause of the observed fsync EIO. The running client is PID 989530. The log continues to grow, so the sequence below describes observed events rather than the final state of the mount.

| Local time (JST) | Observed event |
| --- | --- |
| 18:46:13.311124 | Background compaction for inode 596153, chunk 0, output slice 3721413 enters the backend/cleanup phase after object generation. |
| 18:55:31.001431 | Write of slice 3727383 succeeds in `doWrite` in 42.074811ms, producing exactly 2,500 raw slices. |
| 18:55:31.001475–001486 | This Write enters synchronous compaction and waits for the existing background compaction. |
| 19:00:31.024267 | Whole-file flush reaches its five-minute deadline; the same slice is pending with done=true, err=0, committed=false. |
| 19:00:31.033106 | `fsync(596153,1)` returns EIO after 300.074009 seconds. |
| 19:04:28.187113 | Existing background compaction finally finishes: total 18m26.307667724s, backend/cleanup 18m14.875939104s. |
| 19:04:28.187220 | Synchronous compaction leaves its queue wait after 8m57.185735463s, then begins another pass. |

The timeout goroutine dump confirms that the foreground commit thread is sleeping in `compactChunk(once=true)`, invoked by `baseMeta.Write`. The background compaction for the same inode/chunk is inside `redisMeta.doCompactChunk -> deleteSlice -> deleteSlice_ -> cachedStore.Remove -> rSlice.Remove -> cachedStore.delete`. It is waiting on object DELETE, not on an inode lock acquisition or a Redis WATCH operation.

The supplied `--max-deletes=-1` is decisive. `startDeleteSliceTasks` starts workers only when MaxDeletes > 0. `deleteSlice` skips deletion only for MaxDeletes == 0, and falls back to synchronous deletion when the queue is nil. A negative value therefore performs deletion in the caller, rather than providing unlimited deletion concurrency. Redis compaction updates the chunk list and reference counts before deleting obsolete slices, but keeps the compaction marked active until the deletion loop completes. With hundreds or 1,000 old slices and object DELETE calls often around 0.7–1 seconds each, this loop holds up subsequent synchronous compaction long enough to exceed the flush deadline.

For the next controlled test, replace `--max-deletes=-1` with the CLI default `--max-deletes 10`. Keep the other settings initially for comparison. Positive values use background deletion workers with a bounded queue; this removes the demonstrated in-line deletion path, but sustained queue saturation and slow object processing remain possible. Do not interpret this configuration correction as proof that all causes of VM corruption have been resolved.

The preceding advice to retain every mount option overlooked the negative deletion setting and is superseded by this incident analysis. The Read preflush-error fix remains relevant. On the incident build it did not prevent the synchronous compaction deadline failure shown here; the subsequent wait-policy change addresses the deadline conversion. No production mount, Redis metadata, object storage, or VM disk has been modified by the investigation.


## Wait-first policy added after the incident

The user explicitly selected completion-based waiting as the default for this investigation branch. `--writer-flush-timeout` defaults to `0s`. A default flush waits for its pending slices/metadata commits regardless of elapsed time; this includes synchronous compaction waiting on an existing job whose old-slice cleanup is blocked by a full deletion queue. A waiting flush emits a summary warning every five minutes without completing the request with EIO. Queue capacity, compaction ordering, and object cleanup durability have not been weakened.

A real writer error (including EIO, ENOSPC, or EDQUOT) is still returned, even if another chunk has pending commits. A request is never reported successful before its pending commits finish. Timeout and cancellation do not discard or roll back in-flight writes. Explicit request cancellation retains the existing `PutTimeout × 2` grace and returns EINTR if the operation is still pending. Completing commits take precedence over an elapsed opt-in deadline when the waiter reacquires its lock.

`--writer-flush-timeout auto` explicitly selects the retry-derived legacy deadline, with a five-minute minimum. Very large retry counts saturate at the maximum duration instead of overflowing into a short deadline. Positive durations opt into a finite deadline; reaching it with pending commits returns EIO as before. Invalid or negative CLI durations are rejected before command execution. Go callers use `WriterFlushTimeout=0` for unlimited waiting and `AutoWriterFlushTimeout` for legacy behavior; other negative values emit a warning and normalize to zero at writer initialization.

This changes the default for all users of this VFS configuration, including Go callers using zero-valued configuration, not just the FUSE mount command. Existing deployments requiring a finite wait must opt in. QEMU/device/guest timeouts, genuine object or metadata failures after retries, and local staging capacity failures can still surface separately. Waiting indefinitely can make a VM, read, fsync, close, or normal unmount wait indefinitely during a persistent outage. Successful JuiceFS writeback staging retains its existing semantics; this patch does not make fsync wait for every asynchronous cloud upload.

Negative `--max-deletes` values now warn at startup that they perform synchronous deletion in the caller rather than unlimited concurrent deletion. CLI help and the shared option reference explain positive/zero/negative modes and queue capacity.

Regression coverage includes: pending flush completion and real errors in both writeback modes; opt-in deadlines; successful completion at a deadline; cancellation and its grace; a saturated production deletion queue with resume/cancel; and real MemKV metadata replacement followed by a 2,500-slice synchronous compaction blocked on deletion-queue cleanup. The metadata compaction test simulates object creation via the existing message callback convention and checks final visible slice metadata. A separately enabled `JFS_TEST_LONG_FLUSH_WAIT=1` test waits beyond the real former five-minute deadline and then releases pending commits in both writeback modes.


The wait-policy validation passed the VFS and FS suites; selected Redis/SQLite/MemKV, quota and cancellation suites; three runs of the new VFS/metadata/CLI tests under the race detector; and the explicitly enabled 304-second default-wait regression in both writeback modes. Normal builds succeed. The full `make test.cmd` target could not run because sudo requires authentication; the modified CLI paths passed their targeted normal/race tests. Earlier broad-suite dependency/baseline limitations above still apply.

## PostgreSQL source review (2026-10-01; no runtime tests)

At the user's request, the PostgreSQL path was reviewed without starting a database or executing tests. PostgreSQL registers `newSQLMeta` and uses the same `dbMeta.doWrite` and `dbMeta.doCompactChunk` implementation as SQLite. The constructor selects pgx, and `Name()` maps it back to `postgres` so PostgreSQL-specific branches remain active.

The default no-deadline flush, real-writer-error propagation, Read preflush check, compaction admission and deletion workers/queue all live in common VFS/baseMeta code. They do not depend on Redis-specific methods and apply to PostgreSQL as well. SQL slice appending uses the SQLite/PostgreSQL `ON CONFLICT` path, and the slice count feeds the same common 2,500-slice compaction threshold.

The lock mechanisms differ: SQLite forces transaction serialization through one client-side lock slot; PostgreSQL uses the supplied inode lock slot(s) plus `SELECT ... FOR UPDATE` on inode/chunk rows. `dbMeta.doWrite` updates slice data, slice references and inode attributes inside the transaction. The pinned Xorm `Transaction()` returns success only after `session.Commit()` succeeds. SQL compaction also updates the chunk and references transactionally, then looks up obsolete references and calls `deleteSlice` after that transaction has returned. Thus a full deletion queue does not keep that SQL transaction open, although the common compaction flag and an outer synchronous Write's open-file lock can still remain held while cleanup waits.

With `--max-deletes 10`, the same 102,400-slice queue and ten background deletion workers are used. If saturated, PostgreSQL cleanup waits for capacity, and a default VFS flush keeps waiting for pending commits without converting elapsed time into EIO. The negative-deletion warning applies through common Config.SelfCheck too.

This review does not mean PostgreSQL can never return EIO. The pre-existing SQL transaction retry loop is bounded at 50 attempts; `shouldRetry` has PostgreSQL-specific handling, and a final database/driver/commit failure passes through `errno` and VFS writer error handling. Those retry limits, SQL statements and lock mechanisms were not changed. Database/driver-side timeouts or connection failures can still end an operation separately from the removed default VFS flush deadline. Redis-specific transaction phase logs are not emitted for SQL, but common metadata Write, compaction, slice commit logs and transaction-duration metrics remain available.

No PostgreSQL-specific incompatibility in the wait-policy change was found by this source review. PostgreSQL runtime behavior, deployed database settings and performance remain unverified; no tests, build, database connections or migrations were run for this review.
