# Bad days

Stage 10, size L. Follows [../01-protocol.md](../01-protocol.md); the common
acceptance criteria there apply in full, and this page adds the bad days' own.
Foundation items are referenced as `A7`, `B5` and so on (see
[../00-foundation.md](../00-foundation.md)).

## 1. Summary

Nine boards, nine failures: the broker is down (`Bad-Down`), a second broker on
one data directory (`Bad-Lock`), a taken port (`Bad-Port`), a full disk
(`Bad-Disk`), a restart after a crash (`Bad-Recovered`), a rebalance loop
(`Bad-Rebalance-Storm`), a wrong SCRAM password (`Bad-Auth`), two producers on
one `transactional.id` (`Bad-Fenced`) and a slow `acks=all` (`Bad-Slow`). Each is
a board that tells the person what happened, what the broker did about it and the
one next step. A board that is wrong here costs more than anywhere else: it is
read at the moment the person trusts nothing else.

So this stage is not about screens. It is about making each bad day **real,
reproducible, tested and honest**: a failure-injection test that causes it, a
statement of what the broker must do, a statement of what the console must show,
and every sentence of the board checked against the code. The audit found that
four of the nine boards describe behaviour the code does not have (recovery that
logs a line, a lock message, a port message, a full disk that spares `acks=1`),
and two real durability defects sit under them (BAD-1, BAD-4).

Peers: [traffic](traffic.md) (every board's "wire, filtered to" table),
[groups](groups.md) (the storm), [access](access.md) (users), [cluster](cluster.md)
(recovery history, disk meter, health pill), [topics](topics.md) (retention as the
disk remedy), [trace](trace.md). Foundation `A7` (the console without a broker),
`A2` (the vocabulary of refusals), `B5` (crash and restart helpers).

Packages owned: `internal/storage/log` (`recovery.go`, `segment.go`, sync and
fsync hooks), `internal/storage/meta` (lock timeout), `server/server.go`
(start-up, `Close`, the data-directory lock), `cmd/doze-kafka` (start-up error
messages, shutdown), `internal/broker` (`produce.go` acks and
`KAFKA_STORAGE_ERROR`, `txn.go` fencing, group session expiry, `auth.go` and
`scram.go` failure handling) and the chaos and soak harness
(`internal/broker/soak_test.go`, `task chaos`, `task soak`,
`.github/workflows/soak.yml`).

**Estimate:** L, about 19 working days (38 half-days, section 11). **Actual:**
_fill in when done._

### What was run for this document

Evidence tags below are `run` only where it was reproduced on `main` at
`970d829`, darwin/arm64, with a binary built to
`$SCRATCH/dkbin`:

- port taken, data directory in use and registry port taken, each with a first
  broker on `--listen 127.0.0.1:39092`;
- **full disk**: a 12 MiB HFS+ volume made without privileges
  (`hdiutil attach -nomount ram://24576`, `newfs_hfs -v tiny /dev/rdisk4`,
  `mount -t hfs /dev/disk4 $DIR`) with `--data-dir $DIR/data --segment-bytes
  1048576`, filled by kcat and a franz-go client with incompressible records
  (compressible zeros do not fill a disk; the first attempt "succeeded" for that
  reason), then `kill -9` and a restart on the still-full volume;
- `retention.ms=1000` as the remedy, with `df` before and after;
- two `InitProducerId` for one transactional id over an open transaction.

The same recipe on Linux is `mount -t tmpfs -o size=12m tmpfs $DIR` (needs root or
`unshare -rm`); section 7 puts both behind one helper.

## 2. Contract

Kafka APIs the bad days touch (the key and name are from
`internal/protocol/versions.go`; "max v" is the Kafka 4 column, which is what the
boards must not exceed):

| API | max v (K4) | Bad day | Now | Target | Note |
|---|---|---|---|---|---|
| Produce (0) | 9 | Disk, Slow, Recovered, Fenced | F | F | `acks=all` fsyncs before the reply. Full disk answers `UNKNOWN_SERVER_ERROR`, not `KAFKA_STORAGE_ERROR`: BAD-4. A duplicate retry is acknowledged without a sync: BAD-1. |
| Fetch (1) | 13 | Disk, Recovered | F | F | Keeps working on a full disk (run). Boards show v16: BAD-36. |
| ListOffsets (2), Metadata (3) | 12 | Disk | F | F | Work on a full disk (run). |
| OffsetCommit (8) | 9 | Disk, Storm | F | F | Writes `meta.db`; a failure answers `UNKNOWN_SERVER_ERROR` (BAD-4, BAD-40). |
| JoinGroup (11), SyncGroup (14), Heartbeat (12), LeaveGroup (13) | 9, 5, 4, 5 | Storm | F | F | Session timeout is not bounded: BAD-29. LeaveGroup reason is dropped: BAD-31. |
| ConsumerGroupHeartbeat (68) | 1 | Storm | F | F | The storm board describes only the classic protocol: BAD-39. |
| SaslHandshake (17), SaslAuthenticate (36) | 1, 2 | Auth | F | F | Failure closes the connection after the answer (held by `TestAuth_*`). Traffic rows carry no detail: BAD-23. |
| ApiVersions (18) | 3 | Auth | F | F | Board shows v4: BAD-36. |
| InitProducerId (22), AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28) | 4, 3, 3, 3, 3 | Fenced | F | F | Takeover raises the epoch by 2, not 1: BAD-26. |
| DescribeProducers (61), DescribeTransactions (65), ListTransactions (66) | 0 | Fenced | F | F | The only way to see an owner; no fence history: BAD-28. |
| CreateTopics (19), IncrementalAlterConfigs (44) | | Disk | F | F | The remedy is an `AlterConfigs`, a `meta.db` write: BAD-35. |
| DescribeGroups (15) | | Storm | F | F | |

Console routes the stage touches. It adds **no page**: the bad days are states of
existing pages, shown through the vocabulary of `A2` and the pill of `A7`.

| Route | Boards | Now | Target | Note |
|---|---|---|---|---|
| `GET /cluster` | Down, Lock, Port, Disk, Recovered | F | F | Gains: health pill with the storage-fault ledger, disk meter, recovery history (all new, BAD-8, BAD-9, BAD-10). |
| `GET /traffic`, `/traffic/live`, `/traffic/request` | Auth, Disk, Slow, Storm, Fenced | F | F | "The wire, filtered to" is a saved filter on these. Needs the summaries in BAD-23 and BAD-33. |
| `GET /groups/{id}`, `/groups/{id}/live` | Storm | F | F | State strip, rejoin counts: BAD-30, BAD-31, BAD-32 (belongs to [groups](groups.md); this stage supplies the data). |
| `GET /access` | Auth | F | F | "Users on the Access page" is `new`; credentials are in memory only. Owned by [access](access.md). |
| `GET /topics/{name}`, `POST /topics/{name}/config` | Disk | F | F | The retention remedy. |
| `GET /api/counts`, `/api/palette` | all | F | F | The pill reads `/api/counts`: it must keep answering when storage is failing, and the client keeps the last answer when it stops (A7). |

Declined: a console button that frees disk space or deletes segments; a recovery
that quarantines a corrupt partition and carries on (Kafka takes the log directory
offline; here a partition that cannot be recovered fails its requests, which is
honest enough for a development broker); rate limiting of failed sign-ins (Kafka
has none).

## 3. Where it stands

Coverage today (baseline, full suite): `internal/storage/log` 80.5%,
`internal/storage/meta` 80.7%, `server` 79.0%, `cmd/doze-kafka` 79.8%,
`internal/broker` 89.0% (27.5% under `-short`).

Function-level gaps in the code this stage owns (`go test -coverprofile`):
`recoverSegment` 72.7%, `truncate` 75.0%, `openSegment` 70.6%, `syncDir` 80.0%,
`rewrite` 81.8%, `openIndex` 62.5%, `SyncPartition` 0.0% inside its own package
(covered only from `internal/broker`), `SetTopicLimits` 0.0%, `maintain` 76.0%,
`Close` 85.2%, `meta.Open` 66.7%, `server.NewFromConfig` 62.7%, `lockDataDir`
83.3%, `closeStorage` 80.0%.

| Layer | What exists |
|---|---|
| Recovery | `internal/storage/log/recovery_test.go`: `TestRecoveryCrashMatrix` (damage the log or its index by hand, reopen, check offsets and appends), `TestValidateBatches`. `durability_audit_test.go` (11): Bounds agrees with recovery, recovery fsyncs its truncation, failed fsync retried for log and index, roll waits for an in-flight fsync, close and periodic sync flush, a failed append leaves no duplicate offset. `sync_test.go` (5): group commit shares fsyncs and does not hide a failure. |
| Produce acks | `internal/broker/produce_internal_test.go`: `TestProduceAcksFsync`, `TestProduceFsyncFailureIsReportedNotAcknowledged` (injected `BeforeSync` failure: `acks=all` gives `KAFKA_STORAGE_ERROR`, `acks=1` is unaffected), `TestAcksZeroWithAnInvalidBatchFailsTheRequest`. |
| Lock and start | `meta/store_test.go: TestSecondOpenOfTheSameFileFailsWithAReason` (200 ms override). `server/disk_test.go: TestSecondBrokerOnTheSameDataDirIsRefused` (real 2 s), `server/audit_test.go: TestChurn_...`. `cmd/doze-kafka`: `run_test.go`, `process_test.go` (not exhaustively read; none asserts the port-clash or lock text end to end). |
| Fencing | `txn_test.go`: `TestAnotherProducerWithTheSameIDFencesTheFirst`, `TestATransactionPastItsTimeoutIsAborted`; `txn_api_test.go`: coordinator rules, restart finishing a half-done transaction. They assert that the old producer fails, not which code, which epoch or what it is told. |
| Sessions | `groupexpiry_test.go` (classic and KIP-848 expiry), `group_timers_internal_test.go` (7: stale timers evict nobody), `groupevents_test.go` (3 timeline tests). |
| SASL | `auth_test.go`: 11 (round trips, wrong password, unknown user, unsupported mechanism, unauthenticated, TLS). |
| Chaos | `soak_test.go`: `TestChaosSlice` (6 s, every `go test`): the real binary, 4 idempotent `acks=all` producers + a transactional one + a compacted-topic producer + a live consumer, SIGKILL and SIGTERM alternating three times, then every acknowledged record must be on disk in order. `task chaos` / `task soak` (default 2 m), `.github/workflows/soak.yml` (Mondays, 10 m, race build). |
| Browser | None for a bad day. `e2e/tests/` has no spec that stops the broker, fills a disk or fences a producer. |
| Real clients | `compat/`: SCRAM-over-TLS cells exercise sign-in success only. |

What the chaos harness cannot see: SIGKILL leaves the page cache intact, so it
passes whether or not `acks=all` fsyncs (BAD-13). It only checks `acks=all`; no
`acks=1` assertion, no disk fault, no fsync fault, no kill during a rebalance.

## 4. Findings

Evidence: `run` = reproduced; `read` = from the code. Source: `spec` = wire
definition, `doc` = a named doc, `memory` = recollection (goes on the recording
queue, section 8), `code` = the claim is about doze-kafka itself and the code is
the authority.

**Stage 1 ("Durability first") closed BAD-1, 2, 3, 4, 13 and 29.** What was done and
what moved from the text below:

- **BAD-1** fixed. `appendIdempotent` now reports that it answered from data the log
  already holds, and `handleProduce` syncs such a partition at `acks=all`.
  `TestDuplicateRetryAtAcksAllIsSyncedBeforeTheAck`.
- **BAD-2** decided: the batch stays and the producer is told (document and show), the
  same shape as `NOT_ENOUGH_REPLICAS_AFTER_APPEND`. A rollback was rejected: other
  producers' unsynced batches may follow the failed one before its sync, and a truncate
  would take them too. `TestFsyncFailedBatchIsEitherRolledBackOrDocumented` pins it
  (a characterisation test, green on main as well; the board text "was not appended"
  is still wrong for a failed sync and belongs to the console stage).
- **BAD-3** fixed by taking the segment offline after a failed fsync (not by
  truncating to the last synced length, which would remove records a consumer may
  already have read). `TestFsyncFailureDoesNotFalselySucceed` and, through the binary,
  `TestFaultSeam_AFailedFsyncTakesThePartitionOffline` (new seam point `log.fsync`).
- **BAD-4**: the foundation had already mapped I/O errors in Produce, Fetch,
  OffsetCommit and DeleteRecords. Stage 1 closed the rest (CreateTopics,
  CreatePartitions, IncrementalAlterConfigs, DeleteGroups, OffsetDelete) and `acks=0`,
  where Kafka closes the connection for any refused partition (recorded, Q9).
  Q2 is recorded: code 56 at `acks=all` and `acks=1`, and Kafka's broker then goes
  away, which this one does not (a deliberate, documented difference).
- **BAD-13** `TestPowerCutKeepsEveryAcksAllRecord` (three tail variants), in
  `internal/broker`. The chaos harness (`soak_test.go`) is unchanged: still SIGKILL
  only, to be given the power-cut and acks=1 modes by the Bad days stage.
- **BAD-29** fixed with Kafka's default bounds (6 s, 30 min), recorded as Q5. The same
  fix is GRP-1 of the Groups stage, which should now be read as done.
- Q1 stays a design choice, not a Kafka claim; Q2, Q5 and the new Q9 are recorded
  (`conformance/claims/bad-days.md`).

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| BAD-1 | P0 | A retry of an idempotent `acks=all` batch that the log already holds (`seqDuplicate`) is acknowledged **without an fsync**. `appendIdempotent` returns `wrote=false` for a duplicate, so the partition is never put in `written`, and only `written` partitions are synced (`produce.go:75-79`, `:110-113`, `:282`). Sequence: the first send is appended, the fsync fails or the connection drops before the sync, the producer retries, the duplicate is answered with the original offset and `ErrorCode 0` while the bytes are still in the page cache. Acknowledged and not durable. | `internal/broker/produce.go:75-79,110,262-290` | read | code | Broker internal test `TestDuplicateRetryAtAcksAllIsSyncedBeforeTheAck`: idempotent batch at `acks=all` with `BeforeSync` failing (gets `KAFKA_STORAGE_ERROR`); `BeforeSync` healed; resend the same sequence; assert `SyncOps` rose by 1 and the reply is `0`. Then revert the fix and watch it fail. |
| BAD-2 | P1 | When the `acks=all` fsync fails the batch is **already in the log**: LEO advanced, offsets assigned, visible to every consumer, while the producer is told `KAFKA_STORAGE_ERROR` with `BaseOffset -1`. A non-idempotent retry stores it twice; a consumer reads records the producer was told failed. `Bad-Disk` says the opposite ("The batch was not appended and no offset was assigned"), true for a failed write but false for a failed sync. | `produce.go:110-129`, `partition.go:117-171` | read | code | Raw-`kmsg` test with `BeforeSync` failing: produce `acks=all`, assert error, then Fetch from 0 and state the expected result (decision: roll the batch back with `Truncate` to the pre-append size under the partition lock, or document and show it; the test pins the choice). |
| BAD-3 | P1 | After a failed fsync the segment sets `logDirty` again and the next sync simply retries (`segment.go:119-124`, test `TestFailedFsyncIsRetriedForLogAndIndex`). On Linux a failed `fsync` can drop the dirty pages and clear the error, so the retry returns success for data that never reached the disk; the broker would then acknowledge `acks=all`. Kafka does not retry: it takes the log directory offline. | `internal/storage/log/segment.go:112-125` | read | memory | Unit test with a fault file that, on a failed `fsync`, discards unsynced bytes and succeeds the next call (the documented kernel behaviour); assert the partition refuses further `acks=all` acks, or truncates to the last synced length, instead of acknowledging. Recording queue Q1. |
| BAD-4 | P1 | Every storage fault except a failed `acks=all` fsync is answered `UNKNOWN_SERVER_ERROR` (-1), which is not retriable. `storageFault` hard-codes it (`broker.go:213`). On a full disk (run) Produce at `acks=1` and at `acks=all`, CreateTopics, and (by code) OffsetCommit, Fetch-read and DeleteRecords all say -1; franz-go reports `retriable=false`, kcat "Unknown broker error". Only the one fsync path says `KAFKA_STORAGE_ERROR` (56, retriable). The board's headline code is never what a full disk produces. | `internal/broker/broker.go:213`, `produce.go:144,231,251,274`, `offsets.go:64`, `topics.go:101,114` | run | memory | `TestFullDiskProduceIsKafkaStorageError` (broker, injected write error `syscall.ENOSPC` via the seam of BAD-34): Produce at `acks=0/1/all` gives `KAFKA_STORAGE_ERROR`. Which code real Kafka gives is Q2. |
| BAD-5 | P2 | `Bad-Disk` says "`acks=1` produces did not fail the same way: they sit in the page cache until the next sync, so they can be lost. Do not trust them now." False: a full disk fails the `write`, not the sync, so `acks=1` fails at once with the same code (run: kcat `acks=1` "Delivery failed: Unknown broker error", log `request failed: produce: append ... no space left on device`). A silent loss only follows a failed periodic sync, which BAD-7 covers. | board `Bad-Disk`; `produce.go:231` | run | code | Browser/board text test: the copy is generated from the settings table; assert no sentence claims `acks=1` is accepted when the write fails. Broker test as BAD-4. |
| BAD-6 | P2 | A failed `WriteAt` (ENOSPC mid-batch) does not truncate back; only an index-write failure does (`segment.go:84-103`). Run: after the disk filled the active file was 987,136 bytes where whole 200 kB batches would give 800,288 or 1,000,360. In process the next append overwrites from `sizeBytes`, so reads are fine, but a shorter later append leaves the old partial tail after it, and nothing says so. A restart removed it (silently, BAD-10). | `internal/storage/log/segment.go:84-103` | run | code | `TestFailedWriteLeavesNoPartialTail` with a write seam that writes half and returns ENOSPC: file size equals `sizeBytes`; then a smaller append; reopen; offsets contiguous. |
| BAD-7 | P2 | A failed periodic fsync is one `Warn` per tick ("periodic fsync failed") and nothing else: `acks=1` records stay unsynced, no counter, no state, no escalation, the broker carries on indefinitely and the console cannot say "writes at risk". | `internal/storage/log/manager.go:552-556` | read | code | `TestPeriodicFsyncFailureIsRecorded`: fail `BeforeSync` for three ticks; assert the ledger of BAD-8 holds the count and last error and clears after a success. |
| BAD-8 | P2 | Nothing in the broker records that storage is failing: no counter, last error, time or topic. The console's "writes failing since 14:35:48", "3 batches refused in the last minute" and the per-topic tag on `Bad-Disk` have no data. `storageFault` only logs. Needs a small ledger (per op and per topic: count, first, last, last error, cleared-at) fed from `storageFault`, the `acks=all` path and the periodic sync, read by the pill. New backend. | `broker.go:213`, `produce.go:122` | read | code | `TestStorageFaultLedger` (broker): inject, assert counts, assert it clears; `console` handler test that `/api/counts` carries the pill state and the page renders "writes failing" from it. |
| BAD-9 | P2 | No free-space figure exists anywhere (`statfs`/`GetDiskFreeSpaceEx` is not called). `Bad-Disk`'s "Volume 100% used", the 90% warning and "Biggest" per topic are all `new`. Per-topic size exists (`Cluster`/`Topics` sizes); volume use does not, and needs a Windows implementation. | `console/` (absent) | read | code | `TestDiskUsageOfDataDir` per platform (build-tagged), and a fake in the console test double. |
| BAD-10 | P2 | Recovery says nothing. `recoverSegment` truncates and rewrites the index and returns only the log-end offset; no log line, no counter, no return value for bytes dropped (`recovery.go:22-45`). Run: a restart that dropped a 186,848-byte partial tail printed only the banner. `Bad-Recovered`'s "truncated 2,317 bytes ... last intact offset 4,789", "Index rebuilt", "Took 38 ms" and "Recovery is logged to the terminal at start-up" **cannot be produced from any real output**. Needs `recoverSegment` to return a report (dropped bytes and batches, last offset, index rewritten, duration), `openPartition` to log it at `Warn` and keep it in a bounded list. | `internal/storage/log/recovery.go:22`, `partition.go:89` | run | code | `TestRecoveryReportsWhatItDropped` (log package): craft a torn tail of N bytes, open, assert the report says N and the logged line names topic-partition, segment file, bytes and offset; `TestRecoveryReportsNothingWhenClean`. |
| BAD-11 | P2 | The board's frame is not how the broker works. Partitions are opened and recovered **lazily on first access** (`manager.go:177,215`), not "every active segment of all 15 partitions before it opened the port", and there is no clean-shutdown marker, so the broker cannot know "the previous run ended without a clean shutdown". "Checked 15 partitions ... Took 38 ms" would need an eager pass (cost: up to 8 MiB tail read per partition) or an honest rewording: "recovered when first used". Decide, then test. | `manager.go:177-185,215-262`; board `Bad-Recovered` | read | code | `TestCleanShutdownLeavesAMarkerAndAnUncleanStartIsReported` (server): kill -9 leaves no marker; start logs "previous run did not shut down cleanly"; Close removes/sets it. |
| BAD-12 | P2 | `Bad-Down` says "A hard kill (kill -9, power) can lose the last half second of `acks=1` records". For kill -9 that is false: `append` is a `WriteAt` straight to the file with no user-space buffer, so the bytes are in the kernel (`segment.go:86`). `Bad-Recovered` and `docs/storage-and-durability.md:108` both say a killed process loses nothing. Only power loss loses the sub-second. The two boards contradict each other. | board `Bad-Down`; `segment.go:84-90` | read | doc | Chaos variant `acks=1` under SIGKILL asserts zero loss (new test, section 7); doc test B14 for the sentence. |
| BAD-13 | P1 | The chaos harness is blind to durable acks. It kills with SIGKILL, which preserves the page cache; deleting the `SyncPartition` call would still pass `TestChaosSlice`, `task chaos` and the weekly soak. There is no power-loss model (drop everything not fsynced), so "recovery drops only unacknowledged data" is not tested. | `internal/broker/soak_test.go:14,66-72` | read | code | `TestPowerCutKeepsEveryAcksAllRecord`: record the length of each file at its last successful fsync (hook), on "cut" truncate every file to that length (variants: zero-fill the rest, keep a random prefix of the unsynced tail), reopen, and assert every `acks=all` record is present and an `acks=1` record survives only if it preceded a sync. Mutation by hand: remove `SyncPartition`, watch it fail. |
| BAD-14 | P3 | Recovery verifies CRCs only over the last 8 MiB of the active segment (`recoveryCRCWindow`); earlier batches get structure checks only. Above 8 MiB written per fsync interval, damage in an older unsynced region that still parses would be served. Unlikely (zero-filled and torn tails fail the structure check); stated in the code comment, not in the docs. | `recovery.go:11,57` | read | code | `TestRecoveryCatchesBitFlipJustBeyondTheCRCWindow` documents the limit (expects the batch to be kept, with a comment). |
| BAD-15 | P2 | The lock message on `Bad-Lock` is not what a second broker prints. Run, `--data-dir ../run1/d` held by a first broker: `level=ERROR msg=fatal err="data directory ../run1/d is in use by another doze-kafka (or its lock could not be taken: timeout); stop the other broker or choose another --data-dir"`, exit 1, after 2 s. The board prints `doze-kafka: meta: ../harbour/data/meta.db is locked by another process; is another doze-kafka already using this --meta-db (or --data-dir)?` on two lines, without the `time= level= msg=fatal err=` frame. That second message (`store.go:147`) appears only when two brokers with different data directories share one `--meta-db`. The lock file is `.doze-kafka.lock`, taken first (`server.go:511-526`); `meta.db` is locked second. | `server/server.go:520`, `meta/store.go:147`; board `Bad-Lock` | run | code | `cmd/doze-kafka` process test `TestSecondBrokerOnSameDataDirPrintsTheLockLine` with the dir lock timeout injected (BAD-16): assert exit 1, the exact `err=` text and that no `listening` line was printed. A second test for the shared `--meta-db` case. |
| BAD-16 | P3 | The data-directory lock timeout is a literal `2 * time.Second` (`server.go:520`), so its test takes 2 s and cannot be shortened (the meta one has `lockTimeout`). The text leaks bbolt's word ("could not be taken: timeout"), and says nothing about who holds it: the lock file holds no pid, so "Lock holder: this broker, pid 48213", "Up since 12:22" on the board cannot be produced for the second terminal; the first broker can show its own pid. | `server/server.go:516-526` | read | code | Make it a package var like `lockTimeout`; write the holder's pid and start time into the lock file on success and read it on failure (best effort), message "held by pid N since T". Test both. |
| BAD-17 | P2 | `Bad-Port` text and its "one next step". Run: `level=ERROR msg=fatal err="listen on 127.0.0.1:39092: listen tcp 127.0.0.1:39092: bind: address already in use (is another broker running there? choose another with --listen)"`, exit 1: one line with the hint in parentheses, not two, and not prefixed `doze-kafka:`. Worse, the suggested remedy `./doze-kafka --listen 127.0.0.1:9093 --data-dir ./data-b` **fails** when the first broker runs under `--listen`: its console defaults to `127.0.0.1:9093` and its registry to `127.0.0.1:8081` (`config.go:128,131`), so the second dies on 9093 with the same message (run), then would die on 8081. The board mentions only the registry. The hint must say `--listen`, `--console-addr`, `--schema-registry-addr`, or the board must give a port set that works. | `cmd/doze-kafka/main.go:241`, `internal/config/config.go:128-131`; board `Bad-Port` | run | code | Process test `TestSecondBrokerOnATakenPort`: two `--listen` brokers, assert the text and exit code; `TestTwoBrokersWithTheBoardsRemedy` runs the command the board prints and expects it to start (it does not today). |
| BAD-18 | P3 | The board shows only the `--listen` path. The default is name mode, where the same clash reads `doze-kafka: cannot listen on <ip>:9092 for kafka.<name>.doze: ...\n  something else already holds ...; free it, or name this instance differently (--name)` (`listen.go:158`), and a second instance under the same name reads `doze-kafka: kafka.<name>.doze is already served by pid N (<owner>).` (`listen.go:134`). Both are better messages than the board's. The board and `docs/troubleshooting.md` should show the default path first. | `cmd/doze-kafka/listen.go:131-160` | read | code | Table test of `openListener` errors per mode (`listen_test`-style, injected lease). |
| BAD-19 | P3 | A start that fails after the listener (console or registry port taken) has already created the data directory, `meta.db` and `.doze-kafka.lock` (run: `--data-dir ../run1/d2x` appeared) before it exits 1. `Bad-Port` says "Nothing was opened: no data directory lock, no console, no registry" and that holds only for the broker's own port. | `main.go:266-300` | run | code | Process test: registry port taken; assert exit 1 and that the board's claim is worded as "before serving" or that the order is changed to bind all listeners first. |
| BAD-20 | P2 | Shutdown cannot be forced and cannot be bounded. `signal.NotifyContext` keeps capturing SIGINT/SIGTERM after the first (the default action is restored only by `stop()`, which is deferred), so a second Ctrl-C does nothing; and `protocol.Server.Serve` waits on `s.wg.Wait()` with no deadline (`server.go:221`), so a handler blocked in a hung `fsync` (the worst `Bad-Slow`) holds the process open for good, with only `kill -9` left. | `cmd/doze-kafka/main.go:118-123`, `internal/protocol/server.go:221` | read | code | Binary test with the fsync hang seam (BAD-34): SIGTERM, assert exit within a bounded time or a second SIGTERM exits 1 and logs what was not flushed. |
| BAD-21 | P3 | "shutting down: flushing logs to disk" is logged after `ServeListener` returned, i.e. after the flush is finished (`main.go:326`); `docs/getting-started.md:95` presents it as proof. A failed final flush returns an error and exit 1 (good, tested) but the order of the lines says the opposite. | `cmd/doze-kafka/main.go:324-327` | read | code | Process test: SIGTERM, assert the line precedes "stopped" (new) and a failed flush prints `fatal` after it. |
| BAD-22 | P3 | `Bad-Down` shows a start-up line `doze-kafka listening on 127.0.0.1:9092   Data /Users/...` that does not exist: the banner is `doze-kafka is up.` and a table (`kafka`, `console`, `registry`, `data`, `serving`) (run). The default is not `127.0.0.1:9092` at all but the instance name on its own loopback address (`listen.go:1-30`); the console is `http://kafka.<instance>.doze` port-less, not `http://kafka.doze`. | board `Bad-Down`; `cmd/doze-kafka/announce` | run | code | Board copy generated from the banner function (B7/B14), asserted by a test that prints the banner. |
| BAD-23 | P2 | `Bad-Auth` rows cannot be built from the traffic ring: `SaslHandshake` and `SaslAuthenticate` use `sumNothing` (`versions.go:106,121`), so Detail is empty; the board's "asks for SCRAM-SHA-512", "final step", "connection closed after the answer", "Principal none yet" have no source. The handshake mechanism is in the request; the step is countable per connection; the close is `CloseAfterResponse`. | `internal/protocol/versions.go:106,121`, `summary.go` | read | code | `TestSaslSummaries` (protocol): handshake reports mechanism, authenticate reports step number and `closed` on failure; traffic row test. |
| BAD-24 | P3 | The failure log line is `level=WARN msg="SASL authentication failed" mechanism=SCRAM-SHA-512 err="scram: invalid credentials"` (`auth.go:162`). The board prints `warn: SASL authentication failed mechanism=SCRAM-SHA-512` and omits `err`. The real line names no client, no remote address and no user, so an operator with two failing apps cannot tell them apart from the log (the traffic row can). | `internal/broker/auth.go:162` | read | code | `TestSaslFailureLogNamesTheClient`. |
| BAD-25 | P3 | `scram.go:119-121` says an unknown user gets "the same answer as a wrong password: do not confirm which users exist". The text is the same but the **round** differs: an unknown user fails the first `SaslAuthenticate` (client-first), a wrong password fails the second (client-final). A client can tell. `Bad-Auth` itself shows "final step" and says "The reply does not say which of the two was wrong". Whether Kafka differs is not known (Q4). | `internal/broker/scram.go:117-121`, `auth.go:147` | read | memory | Raw-`kmsg` test records which round fails for both cases; compared with the recording. |
| BAD-26 | P2 | A second `InitProducerId` over an open transaction raises the epoch by **two**: `abortFencing` increments (`txn.go:199`) and then `initTxnProducer` increments again (`txn.go:186`). Run: first owner `0`, second owner `2`; the old owner's `EndTxn` then answers `PRODUCER_FENCED` (correct). `Bad-Fenced` shows `4 -> 5` and a produce at "epoch 5". Whether Kafka also jumps by two is not known (Q3). | `internal/broker/txn.go:186,199` | run | memory | `TestTakeoverEpochMatchesTheRecording` (replay of Q3); until recorded, the board must say "raises the epoch" without a number. |
| BAD-27 | P2 | `abortFencing` bumps `e.rec.Epoch` and sets `PrepareAbort` in memory **before** `persistTxn`; if the store write fails (disk full) it returns `CoordinatorNotAvailable` but the in-memory entry keeps the new epoch and state, which the store does not have. The entry and the disk disagree until the next successful write or a restart. | `internal/broker/txn.go:198-207,148-153` | read | code | `TestFencingWithAFailingMetaWriteLeavesNoPhantomEpoch` (fault `PutTxn`): after the failure, state and epoch equal the stored values, the old producer is not fenced, a retry succeeds. |
| BAD-28 | P2 | The fence leaves no history. `TxnRecord` has producer id, epoch, state, timeout, partitions, offsets and timestamps, but no previous epoch, no fenced-at, no owner address, no count of aborted records (`meta/txns.go:12-22`). The board's "Epoch 4 -> 5 at 14:36:01.350", "owner 127.0.0.1:55201", "Aborted: 1 open transaction, 3 records on p1" are not stored. The timeline rows are rebuildable from the traffic ring filtered by transactional id (`sumProduce/sumEndTxn` put it in `Group`), and the owner address from the connection; the aborted count is not. | `meta/txns.go`, `txn.go:152-207` | read | code | `TestFenceIsRemembered` (broker): after a takeover `DescribeTransactions` or the console endpoint reports previous epoch, time and aborted partitions; restart keeps it. |
| BAD-29 | P1 | JoinGroup accepts any `session.timeout.ms`; there is no `INVALID_SESSION_TIMEOUT` (`group.go:393`; no occurrence in `internal/broker`). A client with a 1 s session works here and is refused by a stock Kafka broker (`group.min.session.timeout.ms` / `group.max.session.timeout.ms`, values to be recorded). That is exactly "cause 2" of `Bad-Rebalance-Storm`, and the cause is invisible: a client developed against this broker ships a setting that fails in production. | `internal/broker/group.go:393,456` | read | memory | `TestJoinGroupRefusesASessionTimeoutOutsideTheBounds` (raw `kmsg`, both sides of each bound, classic and the `JoinGroup` v0 to v9 range); bounds from Q5. |
| BAD-30 | P2 | One 2,000-entry ring holds every group's events (`traffic.New(2000, ...)`, `groupLimit = capacity`, filtered per group on read). A storm of one group (4 events per rebalance per member) fills it in a few minutes and evicts every other group's history; a restart empties it. The storm board's "generation 7 to 31 in 250 s" needs the history back to generation 7. | `internal/traffic/traffic.go:84,106`, `server.go:trafficRows` | read | code | `TestOneNoisyGroupDoesNotEvictAnother` (traffic): 3,000 events for group A, 10 for B; B keeps its 10; per-group cap. |
| BAD-31 | P2 | The LeaveGroup reason is discarded. `LeaveGroup` v5 carries a per-member `Reason` ("the consumer is being closed", the Java client's poll-timeout text); `sumLeave` records only the group (`summary.go:197-202`) and the coordinator's event is `"client X left the group"` (`group.go:656`). The board's "processing took longer than max.poll.interval.ms" row detail is exactly that string. `JoinGroup` already keeps its reason (`sumJoin`). | `internal/protocol/summary.go:197`, `internal/broker/group.go:656` | read | spec | `TestLeaveGroupReasonIsRecorded` (protocol summary + coordinator event). |
| BAD-32 | P3 | `Bad-Rebalance-Storm`'s per-member "Rejoins" counts members by name `depot-router-1/2/3`. A classic member id is assigned by the broker and a client that restarts gets a new one, so a crash-looping pod looks like a stream of new members, each with 1 rejoin. The count must be keyed by client id plus remote address (or `group.instance.id`). | `group.go` events, board | read | code | `TestRejoinCountsFollowTheClientNotTheMemberId` (traffic/console). |
| BAD-33 | P2 | The `Bad-Disk`/`Bad-Slow` tables need the acks level and the sync share on the wire. `sumProduce` shows `acks=0` only (`summary.go:66`); there is no measurement of fsync time inside a request (the board says so: "today only the total is on the wire"). A tiny change (acks in Detail) and a small one (measure `SyncPartition` and pass it to the observer). | `summary.go:66`, `produce.go:112` | read | code | `TestProduceRowShowsAcks`; `TestProduceRowCarriesTheSyncTime` using the slow-sync seam (BAD-34): a 100 ms injected sync shows 100 ms +- tolerance. |
| BAD-34 | P2 | There is no way to inject a fault into the real binary. `ManagerOptions.BeforeSync` and the `fsyncFile` variable are reachable only from tests inside `internal/storage/log` and `internal/broker`; `server.Options` and the CLI expose neither, there is no write hook, and nothing simulates ENOSPC or a hung disk. So the soak harness, the e2e suite and `compat` cannot drive a bad day. The tests in section 7 need a seam. | `internal/storage/log/segment.go:33`, `manager.go` options | read | code | Add the seam (section 7, "Seam"), then the tests of section 5. |
| BAD-35 | P2 | `Bad-Disk`'s remedy is partly true. Run: with the disk full, `retention.ms=1000` on the topic freed space in under 3 s (df 100% to 11%) and produce resumed, and the alter itself worked (`meta.db` had headroom). But: (a) it frees only sealed segments **older than `retention.ms`**, so the board's `retention.ms=3600000` frees nothing for data written in the last hour; (b) a partition nobody has touched since start is swept once a minute (`closedRetentionInterval`), not "within about a second"; (c) the remedy writes `meta.db` (bbolt), which can fail on a full disk and was not made to (400 consecutive commits succeeded; not forced). Board says "takes effect at once". | `manager.go:22-29,533-560`, `retention.go:34-45` | run | code | `TestRetentionAlterFreesSpaceOnAFullDisk` (tmpfs, Linux CI, and the hdiutil recipe on macOS); `TestClosedPartitionRetentionAfterAlter` times the sweep; a `meta.db`-full case with the write seam. |
| BAD-36 | P3 | The boards show request versions the broker never offers (Kafka 4 column): `Fetch v16` (offered max 13), `InitProducerId v5` (4), `EndTxn v4` (3), `ApiVersions v4` (3). A traffic row can never read that. `Produce v9`, `OffsetCommit v9`, `JoinGroup v9`, `Heartbeat v4`, `LeaveGroup v5`, `SyncGroup v5`, `SaslAuthenticate v2`, `SaslHandshake v1`, `Metadata v12` are correct. | boards; `versions.go:93-130` | read | code | Board fixture test (B13): every version in a board's request table is within `protocol.APIs`. |
| BAD-37 | P3 | The docs say nothing about any of the bad days beyond durability. `docs/troubleshooting.md` and `docs/limitations.md` have no entry for a full disk (what fails, what keeps working, what frees space), for the lock, for a taken port in both modes, for what recovery did, or for the fsync cost. | `docs/` | read | doc | B14 doc check: each bad-day board links to a troubleshooting anchor that exists. |
| BAD-38 | P2 | `Bad-Recovered` says producers "that retry resend the dropped batch and it lands at offset 4,790. An idempotent producer will not duplicate it." True only if the producer never saw the ack. After a power cut a producer at `acks=1` may have been acked for a batch recovery dropped and then sent later batches; those carry sequences the log no longer has a predecessor for, so the next idempotent send is `OUT_OF_ORDER_SEQUENCE_NUMBER` (`produce.go:296`), fatal for the Java producer. Kafka's answer to the same state (and `UNKNOWN_PRODUCER_ID`) is not recorded. | `produce.go:277-300`, board | read | memory | Q6; `TestProducerAfterATruncatedTailGetsTheRecordedError`. |
| BAD-39 | P3 | `Bad-Rebalance-Storm` is drawn for the classic protocol (`PreparingRebalance`/`CompletingRebalance`). A KIP-848 member does not rebalance that way: the group epoch moves and members converge (events `epoch`, `assigned`). A storm on a `group.protocol=consumer` group has a different shape and the board does not say which it is looking at. | `groupevents.go`, board | read | code | `TestStormShapeIsDescribedPerProtocol` (console): both protocols render a state strip from their own events. |
| BAD-40 | P2 | No test makes a `meta.db` write fail. OffsetCommit, InitProducerId, ReserveProducerIDs, PutTxn, AlterConfigs, CreateTopics all go through `boltStore.update`; on failure the first two answer `UNKNOWN_SERVER_ERROR`, transactions `COORDINATOR_NOT_AVAILABLE`, and none of the paths is exercised. Disk-full cannot be forced in practice (bbolt allocates ahead), so only the seam can show it. | `meta/store.go:~196`, `offsets.go:64`, `txn.go:108-110` | read | code | Fault the store (a `Store` wrapper whose `update` returns ENOSPC) and assert each API's answer; `meta` test for a failing `db.Update`. |

**Verified true** (checked against code or run, so a board may keep them):
`Bad-Lock` "waited 2 seconds" (`server.go:520`); "exited with status 1 before
opening any partition" (the lock is taken before `log.OpenManager`); "a lock left
behind by a crashed run is released by the operating system" (bbolt `flock`); the
first broker unaffected. `Bad-Port` "The error is the operating system's own,
passed through unchanged" (`listen.go:105`) and "exited before opening the data
directory" for the broker port. `Bad-Disk` "Reads still work" (run: Fetch,
ListOffsets, Metadata, OffsetCommit answered at 0 bytes free), "The active segment
is never deleted" (`retention.go:34`), "a producer asking for `acks=all` is told
no". `Bad-Slow` "On macOS the sync is a full device-cache flush (`F_FULLFSYNC`)":
Go's `os.File.Sync` calls it (`$GOROOT/src/internal/poll/fd_fsync_darwin.go:13`);
`--fsync-interval` default 500 ms (`config`); `acks=1` answered before the sync.
`Bad-Auth` closing the connection after the answer (`auth.go:156-161`), "SCRAM,
`--sasl-user name:password`, `[credentials]`", "ASCII only, no SASLprep" (not
checked against the SCRAM code, see the checklist). `Bad-Fenced`
`PRODUCER_FENCED` on EndTxn and `INVALID_PRODUCER_EPOCH` on a produce
(`txn.go:123-126,373`), "records of the aborted transaction stay in the log,
hidden from read_committed" (`TestATransactionPastItsTimeoutIsAborted`),
"A transaction past its timeout is aborted and its producer fenced the same way".

## 5. What to check

### 5.1 The nine bad days: injection, broker, console

For each board: how the failure is **caused in a test**, what the **broker must
do**, and what the **console must show**. "Seam" is the fault seam of section 7
(BAD-34); "helper" is `B5` (`Kill()`, `Restart()`).

| Board | Inject (test name, layer) | The broker must | The console must show |
|---|---|---|---|
| Bad-Down | `TestBrokerDownIsShownAndRecovers`, browser: helper `Kill()` the subprocess mid-poll, wait, `Restart()`. Plus `TestKillMinusNineMidProduceLosesNoAckedRecord`, binary: SIGKILL during an `acks=all` stream. | Start from the same `--data-dir` with all topics, offsets, schemas and transactions back; a clean stop (SIGTERM) flushes (`TestManagerCloseFlushesEveryPartition`); a kill loses no `acks=all` record and no `acks=1` record on a process kill, and may lose the sub-second only on power loss (BAD-12). | One pill "Not reachable" with the last good time, frozen content, one retry every 2 s, no toast per poll (A7). The "Last known" panel is the last `/api/counts` payload kept in the browser. "Last good answer: a Produce from checkout-api" is the newest traffic row the client saw. After restart: recovery without reload. |
| Bad-Lock | `TestSecondBrokerOnSameDataDir...`, process: start broker A on a temp dir, start B on the same dir (and a third on a separate dir with A's `--meta-db`); assert B exits 1 within the timeout, prints the lock text, wrote nothing, and A's offsets did not move. | Refuse in 2 s, exit 1, create nothing, release on any exit of A (`TestChurn_...`). | A is unchanged ("Nothing changed here"). The `new` "Noticed a refused start" needs a trace the second process leaves next to the lock (BAD-16); until it exists, cut it from the board. |
| Bad-Port | `TestSecondBrokerOnATakenPort`, process, both modes: `--listen` on a port held by a plain `net.Listen`; name mode with the lease held. | Exit 1 before the data directory is opened (true for the broker port; BAD-19 for the others), the OS error verbatim, the hint. | A is unchanged; Cluster shows its own pid, listen, advertised, console, registry, data (all exist). The one next step must be a command that works (BAD-17). |
| Bad-Disk | `TestFullDiskIsSurvivable`, broker + binary, on the size-limited volume (tmpfs on Linux, hdiutil on macOS) and by the write seam (`ENOSPC`) for the pieces a volume cannot force: produce at `acks=0/1/all`, fetch, list offsets, offset commit, create topic, alter config, retention remedy, a restart while full, a restart after freeing. Plus `meta.db` writes failing (BAD-40). | Never crash or hang; reads and group traffic keep working while `meta.db` has room; every write answers a retriable `KAFKA_STORAGE_ERROR` and leaves no partial tail and no offset (BAD-2, 4, 6); a restart on a full disk starts; freeing space (retention) resumes writes without a restart; nothing acknowledged is lost. | Pill "writes failing since T", the refused-batch count, the refusing topics, the error name in the vocabulary (Failed, red), reads still green; volume use and the biggest topics (BAD-8, 9). The "safe / not safe" lists are static copy; the `acks=1` sentence is corrected (BAD-5). |
| Bad-Recovered | `TestRecoveryAfterAPowerCut`, log + broker: the power-cut model of BAD-13 and a torn-tail matrix (every byte count 1..batch size) on a partition and on a transaction control marker; plus `TestKillMinusNineThenRestartReport`, binary. | Keep exactly the whole batches up to the first torn one; truncate and fsync the cut (`TestRecoveryFsyncsItsTruncation` exists); rebuild the index; **report** what it did (BAD-10, 11); resume at the next offset; offsets and groups from `meta.db` intact; keep idempotent state consistent (BAD-38). | A list: partition, segment, bytes dropped, last intact offset, index rebuilt, when, how long: from the recovery report, kept in a bounded list that survives until restart (history across restarts only if persisted; the board marks it `new`). Wording "recovered when first used" unless BAD-11 chooses the eager pass. |
| Bad-Rebalance-Storm | `TestRebalanceLoopIsVisible`, client library: three franz-go consumers, one with `SessionTimeout` shorter than its heartbeat can keep up with (a paused heartbeat via the helper's pause, since the broker never sees `max.poll.interval`), one crash-looping (close and reopen every 10 s). Both protocols (BAD-39). | Held joins until members arrive or the rebalance timeout; generation climbs by one per rebalance; expiry events carry the reason; sessions below the minimum are refused (BAD-29); one group's storm does not evict another group's history (BAD-30). | Generation range and rate, state strip from group events, per-client rejoin counts (BAD-32), the LeaveGroup reason (BAD-31); the three "likely settings" are static copy from the settings table (A3). The broker is blameless; the page says so only when the events show clients leaving. |
| Bad-Auth | `TestWrongPasswordIsRefusedAndClosed`, raw `kmsg` + client library (franz-go, librdkafka via compat): wrong password, unknown user, unsupported mechanism, TLS on/off. | `SASL_AUTHENTICATION_FAILED`, connection closed after the answer, nothing reached any topic, other connections unaffected, a log line naming the client (BAD-24), same text for unknown user and wrong password. | The refusal in the vocabulary ("Refused"), the filtered wire with mechanism and step (BAD-23), "Principal none yet". "Users on the Access page" is `new`: names only, from `cfg.Credentials`; last sign-in and failure counts are not recorded anywhere and are cut or built (access stage). |
| Bad-Fenced | `TestTakeoverFencesTheOldOwner`, raw `kmsg` and franz-go: owner A with an open transaction, owner B inits, A produces, adds partitions, ends, offset-commits; A idle until timeout; restart between. | B owns at a higher epoch; every request from A carries the right code (`INVALID_PRODUCER_EPOCH` on produce, `PRODUCER_FENCED` on coordinator calls); A's open transaction is aborted (markers, hidden from `read_committed`); persisted, and consistent when the store write fails (BAD-27). | The timeline (from the traffic ring by transactional id), owner, previous and new epoch, aborted count (BAD-28). Numbers from the store, not from the board (BAD-26). |
| Bad-Slow | `TestSlowFsyncOnlyHurtsAcksAll`, broker + binary: `BeforeSync` sleeps 100 ms; produce at `acks=1/all`, fetch, heartbeat, offset commit concurrently. | `acks=all` takes the sync time, `acks=1`, Fetch and Heartbeat do not; group commit shares one sync across concurrent producers (`TestGroupCommitSharesFsyncs`); shutdown still completes (BAD-20). | The wire filtered to Produce with the acks level (BAD-33); the sync share, built from the measured time; the "honest limits" line stays until it is measured. |

### 5.2 Message text checklist (every sentence read against the code)

| Board | Text | Code says | Match |
|---|---|---|---|
| Lock | `doze-kafka: meta: ../harbour/data/meta.db is locked by another process;\nis another doze-kafka already using this --meta-db (or --data-dir)?` | `server.go:522`: `data directory %s is in use by another doze-kafka (or its lock could not be taken: %w); stop the other broker or choose another --data-dir`, logged as `level=ERROR msg=fatal err="..."`. `store.go:147` is the second message, one line. | No (BAD-15) |
| Lock | "waited 2 seconds" | `Timeout: 2 * time.Second` in both | Yes |
| Port | `doze-kafka: listen on 127.0.0.1:9092: listen tcp ...: bind: address already in use\n(is another broker running there? choose another with --listen)` | `main.go:241`, one line, logged as `msg=fatal err=` | No (BAD-17) |
| Port | "`./doze-kafka --listen 127.0.0.1:9093 --data-dir ./data-b`" works | fails: 9093 is the first broker's console | No (BAD-17) |
| Down | `doze-kafka listening on 127.0.0.1:9092   Data /...` | the "is up" table; name mode by default | No (BAD-22) |
| Recovered | the recovery line and numbers | none is logged | No (BAD-10) |
| Disk | `KAFKA_STORAGE_ERROR`, "no space left on device" | `UNKNOWN_SERVER_ERROR`; text only in the log and, for `appendPlain`, in `ErrorMessage` | No (BAD-4) |
| Disk | "`acks=1` ... can be lost" | `acks=1` fails the same way | No (BAD-5) |
| Auth | `warn: SASL authentication failed mechanism=SCRAM-SHA-512` | `level=WARN msg="SASL authentication failed" mechanism=... err="scram: invalid credentials"` | Partly (BAD-24) |
| Auth | `SASL_AUTHENTICATION_FAILED` on SaslAuthenticate | yes | Yes |
| Fenced | `PRODUCER_FENCED` / `INVALID_PRODUCER_EPOCH` and their messages | codes yes; the message text `There is a newer producer ...` is kerr's, not the broker's | Yes |
| Fenced | `4 -> 5` | `0 -> 2` (run) | No (BAD-26) |
| Slow | "~118 ms fsync" | not measured | Not producible (BAD-33) |
| Storm | `REBALANCE_IN_PROGRESS` on Heartbeat, `ILLEGAL_GENERATION` on a stale commit, `UNKNOWN_MEMBER_ID` after removal | read in `group.go`; held by `group_classic_*` tests | Yes (read) |

### 5.3 Is a full disk survivable? (run, 12 MiB volume)

- The process never crashed or hung across the whole run, including a `kill -9`
  at 0 bytes free and a restart on the still-full volume.
- Reads kept working: Fetch from the middle of the log, ListOffsets, Metadata.
- OffsetCommit and IncrementalAlterConfigs succeeded at 0 bytes free: bbolt had
  pre-allocated room (`meta.db` 262,144 bytes). Whether a `meta.db` write ever
  fails was not forced (BAD-40).
- Produce failed at every acks level with `UNKNOWN_SERVER_ERROR` (BAD-4);
  CreateTopics failed the same way and was undone (the name was not left
  registered); an auto-creating produce logged `auto-create topic failed ...
  no space left on device` on each retry.
- A partial batch stayed on disk until the restart (BAD-6).
- The retention remedy worked without a restart (BAD-35).
- Not run: a full disk while compaction is rewriting (the cleaner writes a swap
  file next to the segment, so it needs headroom exactly when there is none), and
  a full disk during a transaction's marker writes (`finishTxn` returns
  `COORDINATOR_NOT_AVAILABLE` and the state stays `PrepareCommit`; retried only
  on restart? to be read and tested: `TestMarkerWriteFailureIsRetried`).

### 5.4 Does recovery drop only unacknowledged data?

With the documented fsync interval (`--fsync-interval`, default 500 ms):

- `acks=all`: each acknowledged batch was fsynced (`SyncPartition`) except the
  duplicate-retry path (BAD-1). An fsync covers the whole file, so everything
  before it is durable too. Recovery cuts at the first invalid batch, which can
  only be after the last sync.
- `acks=1`: loss is bounded by the sync interval and applies to power loss only
  (BAD-12). If the interval is `0` the bound is "the OS decides" and a clean
  shutdown.
- A hole in the middle of the unsynced region (pages written out of order) drops
  everything after it, including valid later batches; all of it is unsynced, so
  still only unacknowledged data. Held by `TestRecoveryCrashMatrix` for shape, not
  for the sync boundary (BAD-13).
- What is not established: Q1 (the failed-fsync retry), BAD-38 (a producer
  acknowledged at `acks=1` for dropped data).

### 5.5 Could `Bad-Recovered`'s numbers come from real output?

No (BAD-10, BAD-11). Today recovery logs nothing at any level, returns no report,
runs lazily, and cannot say a run was unclean. Run: a restart that discarded
186,848 bytes printed the banner and `listening`, nothing more. The numbers on
the board (2,317 bytes, offset 4,789, 14 clean partitions, 38 ms, "index rebuilt")
each need a field in the report of BAD-10. "Recovery history on the Cluster page"
is honest as `new`: the board's last paragraph already says so.

### 5.6 Other checks

- [ ] `acks=all` for idempotent and transactional producers is the client default
      (`enable.idempotence`): the claim on `Bad-Slow` is about client defaults, so
      name the client versions or drop the sentence (`doc`).
- [ ] The SCRAM claim "ASCII only, no SASLprep" is read against `scram.go`
      (not yet done; `memory`).
- [ ] The wire panel on each board is a saved traffic filter, not a second query
      path.
- [ ] Windows: `syncDir` is best effort (`segment.go:158`), the data-directory lock
      works through bbolt, the full-disk helper is Linux and macOS only (document
      the skip).
- [ ] The console pill works with auth on (the console has its own credential,
      `provisionConsoleCredential`).

## 6. Console

Foundation provides the inspector, wire dock and neighbours (`A1`, `A4`, `A5`) and
the vocabulary and the broker-down state (`A2`, `A7`); they are used by name here.

| Board | Template + handler | Status | Built from |
|---|---|---|---|
| `Bad-Down` | `layout.html` (pill), `static/shell.js` (retry, frozen state); `cluster.html` for the "last known" panel; `errors.html` | buildable | The last `/api/counts` and `/traffic/live` payloads kept in the browser; `doctor` and `./data` are static copy from the banner function. No server work. |
| `Bad-Lock` | `cluster.html` banner (first broker's side); the second terminal is documentation (`docs/troubleshooting.md`) | needs backend | Lock holder pid and start time (BAD-16); "Noticed a refused start" needs a trace file next to the lock (cut it if not built). |
| `Bad-Port` | `cluster.html` "Holder of 9092" panel | buildable | Own pid, listen, advertised, console, registry, data: all exist (`Config()`, `readyInfo`). The terminal text is documentation and must be corrected (BAD-17). |
| `Bad-Disk` | `cluster.html` (volume, biggest), `topics.html` (per-topic tag), `layout.html` (pill), `traffic.html` (filtered wire) | needs backend | Storage-fault ledger (BAD-8), disk usage (BAD-9), Produce summary with acks (BAD-33); the codes (BAD-4). Per-topic sizes exist. |
| `Bad-Recovered` | `cluster.html` (recovery list) | needs backend | Recovery report (BAD-10) and, for "previous run was unclean", the shutdown marker (BAD-11). History across restarts is `new` and needs a file. |
| `Bad-Rebalance-Storm` | `groups.html` (state strip, members' rejoins), `traffic.html` (filtered wire) | needs backend | Group events with per-group retention (BAD-30), LeaveGroup reason (BAD-31), client-keyed rejoins (BAD-32). Strip rebuilt from events, not from the 2,000-row ring as the board says. |
| `Bad-Auth` | `traffic.html` (filtered wire), `access.html` (`new` users list), the refusal panel (`A2`) | needs backend | SASL summaries (BAD-23). "Users" names from `cfg.Credentials` (never passwords); counts and last sign-in need a small store (cut unless access stage builds it). |
| `Bad-Fenced` | `trace.html`/transaction inspector (`A1`), `traffic.html` | needs backend | Traffic ring by transactional id (exists), owner from the connection, fence record (BAD-28), epoch (BAD-26). |
| `Bad-Slow` | `traffic.html`, request inspector | needs backend | Acks in the row (BAD-33), measured sync time (BAD-33); the "what the time is" bar stays `new` until measured. |

Counts: **0 real, 2 buildable, 7 needs backend.**

Teaching lines: the three `acks` explanations, the three storm "likely settings",
the `transactional.id` advice and the retention remedy are generated from the
settings table and the ledger (`A3`, `B7`), not typed into the templates. The
safe/not-safe lists on `Bad-Disk` are static copy and are checked by BAD-5's test.

## 7. Tests to add

**Seam (BAD-34), first.** `ManagerOptions` gains a `Faults` struct of function
fields, nil in production: `BeforeWrite(path string, n int) error` (can return a
short write plus `ENOSPC`), `BeforeSync(path string) error` (error or sleep),
`AfterSync(path string)` (for the power-cut model) and a way to fail a
`meta.update`. A build tag `faultinject` compiles a tiny controller into the
binary: `DOZE_KAFKA_FAULTS=<file>` names a JSON file re-read each second
(`{"write":"enospc","sync_delay":"100ms","sync":"eio"}`), so the soak
supervisor and the e2e suite can flip a fault on a running subprocess. The default
build contains none of it (checked by `lightness`, B9, and `api`, B10).

Unit / store:

- `TestRecoveryReportsWhatItDropped`, `TestRecoveryReportsNothingWhenClean`,
  `TestRecoveryTornTailEveryLength` (every cut point of the last batch),
  `TestRecoveryRewritesAnIndexItDoesNotMatch` (BAD-10) in
  `internal/storage/log/recovery_test.go`.
- `TestFailedWriteLeavesNoPartialTail` (BAD-6), `TestPeriodicFsyncFailureIsRecorded`
  (BAD-7), `TestFsyncFailureDoesNotFalselySucceed` (BAD-3), `TestPowerCutModel`
  (BAD-13) in the same package.
- `TestSecondOpenOfTheSameFileFailsWithAReason` extended: the exact message, the
  injectable data-dir lock (BAD-16), the holder pid.
- `TestBrokerStorageFaultLedger` (BAD-8).

Raw `kmsg`:

- `TestDuplicateRetryAtAcksAllIsSyncedBeforeTheAck` (BAD-1);
  `TestFsyncFailedBatchIsEitherRolledBackOrDocumented` (BAD-2);
  `TestFullDiskProduceIsKafkaStorageError` at `acks=0/1/all` (BAD-4).
- `TestJoinGroupRefusesASessionTimeoutOutsideTheBounds` over JoinGroup v0 to v9
  (BAD-29); `TestLeaveGroupReasonIsRecorded` (BAD-31).
- `TestTakeoverFencesTheOldOwner` asserting codes per API and the epoch (BAD-26,
  27, 28); `TestFencingWithAFailingMetaWriteLeavesNoPhantomEpoch`.
- `TestSaslSummaries`, `TestSaslFailureLogNamesTheClient`, `TestUnknownUserAndWrongPasswordRounds`
  (BAD-23, 24, 25).

Client library:

- `TestFullDiskWithFranzGo`: a franz-go producer sees a retriable error and
  succeeds after space is freed, with no duplicates (idempotent) (BAD-4).
- `TestRebalanceLoopIsVisible` (both protocols), `TestOneNoisyGroupDoesNotEvictAnother`.
- `TestKillMinusNineAtAcks1LosesNothing` (BAD-12).

Binary / process (`cmd/doze-kafka`, via `B5`):

- `TestSecondBrokerOnSameDataDirPrintsTheLockLine`, `TestSecondBrokerOnATakenPort`,
  `TestTwoBrokersWithTheBoardsRemedy`, `TestFailedStartLeavesNothingBehind`
  (BAD-15, 17, 19).
- `TestSigtermTwice`, `TestSigtermWithAHungDisk` (BAD-20), `TestShutdownLogOrder`
  (BAD-21).
- Chaos: extend `runSoak` with acks=1 producers (loss on SIGKILL must be zero),
  a power-cut mode (needs the seam), a disk-full mode (flip the fault file for 5
  seconds, expect retriable errors, then full recovery of acknowledged data), a
  slow-sync mode (latency of `acks=1`, Fetch and Heartbeat unchanged), and a kill
  during a rebalance and during `EndTxn`.

Compat cell: `SCRAM wrong password` per client (10 clients): each reports its own
authentication error and exits, no hang, no retry storm (a client retrying a bad
password forever is a finding about that client, recorded).

Fuzz: none new (no parser of client bytes is added); `FuzzDecodeRequest` already
covers the SASL frames. `TestRecoveryTornTailEveryLength` is exhaustive instead.

E2E (Playwright, `e2e/tests/baddays.spec.ts`, route gate on): broker killed and
restarted (pill, frozen rows, no repeating toast, recovery without reload); full
disk through the fault file (pill, Failed vocabulary, refused count, recovery);
fenced producer timeline; wrong password row; recovery list after a kill; storm
strip. 960 px and 390 px renders.

Differential (Docker, section 8): Q1 to Q7 replayed.

Coverage targets: `internal/storage/log` **86%** (`recoverSegment` to 95%,
`truncate`, `openSegment`, `syncDir`, `rewrite`, `openIndex`, `Close`,
`maintain`, `SyncPartition`, the new report functions); `internal/storage/meta`
**84%** (`Open` to 100%, the fault path); `server` **85%** (`NewFromConfig` to
85%, `lockDataDir`, `closeStorage`); `cmd/doze-kafka` **83%** (`run` error paths,
signal handling); `internal/broker` **90%** (the acks, `storageFault`, `txn.go`
fencing and SASL branches above).

## 8. Recording queue

Scenarios against `apache/kafka:4.0.0` in Docker; none of these is acted on until
recorded. A full disk in the container: `docker run --tmpfs
/var/lib/kafka/data:size=16m ...`.

| # | Claim | Scenario to record |
|---|---|---|
| Q1 | A failed `fsync` is not retried by Kafka; the log directory goes offline | Not reproducible by a client; record only the observable result of Q2 with a failing disk. The doze-side behaviour is decided by the test of BAD-3 (a design choice, not a Kafka claim). |
| Q2 | A full data disk answers Produce with `KAFKA_STORAGE_ERROR` (56) at `acks=1` and `acks=all`, and what the log directory does next | Tmpfs of 16 MiB, produce until failure at each acks level, record the code, `ErrorMessage`, whether Fetch and Metadata keep working, whether the broker stays up. |
| Q3 | Second `InitProducerId` over an open transaction: the epoch the new owner gets, the epoch on the abort marker, `EndTxn`/`Produce`/`AddPartitionsToTxn` codes for the old owner (and `InitProducerId` v4/v5 differences, transaction version) | Two raw clients, one transactional id, record every response. |
| Q4 | Unknown SCRAM user versus wrong password: which round fails and the `ErrorMessage` text for both; the same for `PLAIN` | Raw handshake and authenticate with each. |
| Q5 | JoinGroup with `session.timeout.ms` below `group.min.session.timeout.ms` and above the max: `INVALID_SESSION_TIMEOUT`, the default bounds | Join with 1000, 6000, 1800000, 1800001; record the code. |
| Q6 | After a log is truncated under a producer (stop the broker, cut the tail, start): the idempotent producer's next send (`OUT_OF_ORDER_SEQUENCE_NUMBER` or `UNKNOWN_PRODUCER_ID`) | Container with a mounted data volume, `truncate -s` the segment between a stop and a start. |
| Q7 | LeaveGroup v5 reason handling, and the broker's response to a heartbeat during rebalance (`REBALANCE_IN_PROGRESS`), stale commit (`ILLEGAL_GENERATION`), removed member (`UNKNOWN_MEMBER_ID`) | A three-member group driven by raw requests. |
| Q8 | `acks=all` default with idempotence enabled on current clients | Doc check against the client versions the compat matrix uses. |

## 9. Owed to integration

- **Foundation `A7`** supplies the disconnected pill and frozen state used by
  `Bad-Down` and by every other board when the broker dies; `A2` supplies Refused,
  Denied, Not implemented, Failed for the codes on these boards. `B5` supplies
  `Kill()` and `Restart()`; this stage needs one more verb, `Pause()`
  (SIGSTOP/SIGCONT, to simulate a stalled heartbeat).
- **[Groups](groups.md)** owns the group page; this stage supplies the events
  (BAD-30, 31, 32) and the storm's data. **[Access](access.md)** owns the users
  list. **[Cluster](cluster.md)** owns the recovery list, disk meter and health
  pill. **[Traffic](traffic.md)** owns the summaries (SASL, acks, sync time).
  **[Topics](topics.md)** owns the retention form used as the disk remedy.
- A stage that loses its data on a bad day is cross-cutting; the "pages work when
  the broker is down" criterion is verified once in this stage's e2e spec and
  re-run by each page's stage.

Asks of the foundation:

- **B15, a fault seam** for the log, the store and the binary (section 7), with
  the `faultinject` build tag and the tmpfs/hdiutil volume helper.
- **A13, the health pill's states** (Healthy, Writes failing, Not reachable,
  Recovered) as data with their vocabulary words.
- `B5` gains `Pause()`.

## 10. Acceptance criteria

On top of the common ones in [../01-protocol.md](../01-protocol.md):

- [ ] BAD-1 to BAD-40 each have a test that failed before its fix, named in the
      commit; the P0 and P1 ones (BAD-1, 2, 3, 4, 13, 29) first.
- [ ] `go test ./internal/storage/log -run 'Recovery|PowerCut|FailedWrite|Fsync'`
      passes under `-race`; `TestPowerCutKeepsEveryAcksAllRecord` fails with
      `SyncPartition` removed (mutation by hand, recorded in the commit).
- [ ] `go tool task coverage`: `internal/storage/log` >= 86%, `meta` >= 84%,
      `server` >= 85%, `cmd/doze-kafka` >= 83%, `internal/broker` >= 90%.
- [ ] `go tool task mutate:check -- ./internal/storage/log/... ./internal/storage/meta/...`
      shows no new surviving mutant in `recovery.go`, `segment.go`, `partition.go`
      `Sync`; any survivor is listed with a reason.
- [ ] `go tool task chaos SOAK_DURATION=2m` includes the power-cut, disk-full,
      slow-sync and acks=1 modes and passes; `TestChaosSlice` includes the acks=1
      assertion.
- [ ] `go tool task test:e2e` runs `baddays.spec.ts` (9 scenarios, one per board)
      with the route gate on; the accessibility sweep is clean on the states;
      960 px and 390 px pass.
- [ ] `go tool task test:compat`: the wrong-password cell passes for all 10
      clients, plain and over TLS.
- [ ] A produced message for each of the nine boards is compared with the board's
      text by `TestBoardTextMatchesCode` (the checklist of 5.2 as a test); zero
      mismatches, or the board is corrected.
- [ ] Every board is implemented (2 buildable, 7 with their backend), or the
      document names the dropped `new` items: "Noticed a refused start", "Users
      with last sign-in", "recovery history across restarts", "sync share" if not
      built.
- [ ] A full disk is survivable: `TestFullDiskIsSurvivable` passes on Linux (tmpfs)
      and macOS (hdiutil), skipped with a message on Windows; the process never
      exits, reads work, writes answer `KAFKA_STORAGE_ERROR`, a retention alter
      frees space, and the next produce succeeds.
- [ ] Recovery is visible: after a kill with a torn tail the log has one `Warn`
      line naming topic-partition, segment, bytes and last offset; the Cluster
      page lists it.
- [ ] `docs/troubleshooting.md` has an entry per board (anchors checked), and
      `docs/limitations.md` states the full-disk and fsync behaviour; `CHANGELOG.md`
      lists the error-code change (BAD-4), the session timeout bounds (BAD-29), and
      the lock/port messages.
- [ ] Q1 to Q8 are recorded and replayed, or each is listed as still `memory`
      with the test written and skipped.

## 11. Files and order of work

Half-day units. Overall: **L, about 19 days (38 half-days).**

| # | Work | Half-days |
|---|---|---|
| 1 | Fault seam (`Faults`, `faultinject` tag, fault file, volume helper tmpfs/hdiutil) and the `Pause()` ask. BAD-34 | 3 |
| 2 | Power-cut model and the red tests for BAD-13, BAD-1, BAD-2, BAD-3 | 3 |
| 3 | Fix BAD-1 (sync duplicates), BAD-2 (rollback or document), BAD-3 (no false success after a failed fsync) | 3 |
| 4 | Storage-fault codes: `KAFKA_STORAGE_ERROR` through `storageFault`, partial-tail truncate, periodic fsync failure ledger. BAD-4, 5, 6, 7, 8, 40 | 4 |
| 5 | Recovery report, log line, shutdown marker or reword; restart test. BAD-10, 11, 14 | 3 |
| 6 | Lock and port: injectable timeout, holder pid, messages, remedy command, start order, shutdown (second signal, drain bound, log order). BAD-15 to 22 | 4 |
| 7 | SASL summaries and log; unknown-user round; takeover epoch and fence record; `abortFencing` ordering. BAD-23 to 28 | 4 |
| 8 | Groups: session bounds, per-group event retention, LeaveGroup reason, client-keyed rejoins. BAD-29 to 32, 39 | 3 |
| 9 | Produce row: acks and sync time; disk usage per platform; retention remedy tests. BAD-9, 33, 35 | 3 |
| 10 | Console: pill states, Cluster recovery list and disk meter, filtered-wire links, the nine boards' states | 5 |
| 11 | Chaos modes (power cut, disk full, slow sync, acks=1, kill in rebalance/EndTxn); e2e spec; compat cell | 3 |
| 12 | Recordings Q2 to Q7 and replay; docs, troubleshooting, limitations, changelog; board text fixes (BAD-36, 37) | 2 |

Order: 1, 2 (reds), then 3 (the P0/P1 durability fixes land before anything
else), 4, 5, 6, 7, 8, 9 in parallel by package, then 10, 11, 12. Items 3 and 4
change what a client sees (error codes) and are the two entries in the changelog
that matter.

Prior art. **Apache Kafka** takes a log directory offline on an I/O error and
answers `KAFKA_STORAGE_ERROR` for its partitions (KIP-112, JBOD; recorded as Q2),
refuses session timeouts outside `group.min/max.session.timeout.ms`, and treats a
failed `fsync` as fatal rather than retrying. **PostgreSQL** made `fsync` failure
a PANIC after the 2018 "fsyncgate" discussion for the same reason as BAD-3.
**Redpanda** exposes a disk-space alert and stops accepting writes under a
configurable free-space threshold (`rpk cluster health`), the model for the disk
meter. **FoundationDB's** simulated disks and **SQLite's** crash tests (a VFS
that drops unsynced writes) are the model for the power-cut test of BAD-13.
**doze-aws** `plan/00-foundation.md` has the same "failure injection" idea as a
shared helper; the registry's recorded corpus in this repository is the oracle's
shape.
