# Stage: Topics

The largest stage: the topic page is the specimen of the whole console (board
Sy-01), and everything under it is the storage engine, the topic registry and
the produce/fetch path. Follows [the protocol](../01-protocol.md); the shared
pieces (inspector, dock, neighbours, Refusal, teaching lines) are the
foundation's A1 to A12 and B1 to B10, named here and not redefined
([foundation](../00-foundation.md)).

## 1. Summary

**Surface.** Topic list and home, create, the messages tape and the record
inspector, the composer and "send again", go to offset and go to time,
partitions and growing them, the config tab and editing it, the compacted and
transactional views, purge and delete. Boards: `Tp-Home`, `Tp-01` to `Tp-30`,
and `Sy-01-Anatomy` (the topic page is the specimen).

**Size.** About 11,000 lines of Go under it (`internal/storage/log` 3.7k,
`internal/storage/meta` 0.9k, the broker's topic files 1.9k, `console` topic
files 3.3k) and 13 test files in the log package, 30 in the broker. It is
the stage with the most existing behaviour and the most existing tests, so most
findings are about places the tests do not look, and about the board claiming
what the code does not do.

**Peers.** [traffic](traffic.md) supplies the dock, "written by" and the
"why it happened" lines; [groups](groups.md) supplies readers and lag;
[schemas](schemas.md) supplies the decoded value and the composer's encode;
[access](access.md) supplies "which rule decided" (Tp-25);
[cluster](cluster.md) supplies the transaction detail Tp-22 links to and the
settings table for configs; [trace](trace.md) is entered from "Follow".

**Packages owned.** `internal/storage/log`, `internal/storage/meta` (topic
registry; offsets and ACL tables belong to groups and access), the broker's
`topics.go`, `topiclimits.go`, `topicconfig.go`, `produce.go`, `fetch.go`,
`listoffsets.go`, `deleterecords.go`, `createpartitions.go`, `configs.go`,
`alterconfigs.go`, `idempotency.go`, `producerid.go`, and the console's
`handlers_topics.go`, `backend.go` (topic half), `produceform.go`,
`topicconfig.go`, `gototime.go`, `partitions.go`, `payload.go`, `search.go`,
`resourceurl.go`, `templates/topics.html`, `static/editor.js`.

**What the work finds.** Method: read every board's text as a claim, read the
code beneath, then run a built binary (`topbrk`, a copy of `dkbin`, on its own
port) with franz-go scripts under the scratchpad. 40 findings (3 P1, 19 P2, 18 P3), 12 reproduced
against the binary. The headline ones:

- `CreateTopics` ignores the replication factor (TOP-1): replication factor 3
  is created and reported as 1; the board promises `INVALID_REPLICATION_FACTOR`.
- `DescribeConfigs` returns 7 keys for a topic where Kafka returns about 30
  (TOP-2), so the Config tab as drawn cannot be built from the wire.
- On a compacted topic every compaction gap is drawn as a "transaction marker"
  (TOP-14, 46 such rows in a 201-offset test topic).
- A valid-CRC batch with garbage record bodies is stored and later breaks every
  reader and every time lookup (TOP-4).
- Retention judges a segment by the time it was written, not by its records'
  timestamps (TOP-3), so the Tp-30 story ("retention changed, old records gone")
  is only true for data produced in real time.
- The boards' defaults are wrong: retention is unlimited (not 7 d) and
  `segment.bytes` is 128 MiB (not 1 GiB) (TOP-19).

Durability is in good shape: `acks=all` fsyncs before replying and reports
`KAFKA_STORAGE_ERROR` when the fsync fails (`produce.go:117`), with a set of tests that
inject fsync failures (`sync_test.go`, `durability_audit_test.go`). The weak spots are recovery coverage for sealed segments
(TOP-23) and no SIGKILL test (B5).

## 2. Contract

### Kafka APIs

Key and name from `internal/protocol/versions.go`. "Now" is the tier today,
"Target" the tier this stage ends at. Versions are the advertised range for the
Kafka 4 profile.

| Key | API | Versions | Now | Target | Note |
|---|---|---|---|---|---|
| 0 | Produce | 3-9 | F | F | acks 0/1/all honoured; acks=all fsyncs. Gap: no recorded per-record error detail (KIP-467 style) for `INVALID_RECORD` (TOP-10). |
| 1 | Fetch | 4-13 | F | F | Long-poll capped at 30 s; read_committed honoured; no fetch sessions (answers `FETCH_SESSION_ID_NOT_FOUND`). |
| 2 | ListOffsets | 0-7 | F | F | earliest, latest, max timestamp, by time; read_committed latest = LSO. |
| 3 | Metadata | 0-12 | F | F | Shared; the topic list and partition count come from here. |
| 19 | CreateTopics | 0-7 | F | F | Gaps: replication factor unchecked (TOP-1), `validateOnly` skips the exists and collision checks (TOP-2a below, TOP-5), `.`/`_` collision unchecked (TOP-5). |
| 20 | DeleteTopics | 0-6 | F | F | Also deletes committed offsets of every group for the topic (`store.go:373`). |
| 21 | DeleteRecords | 0-2 | F | F | `-1` empties; start kept in a `log-start` file. |
| 22 | InitProducerId | 0-4 | F | F | Idempotence and transactions; see [cluster](cluster.md). |
| 32 | DescribeConfigs | 0-4 | F (partial) | F | 7 default keys only (TOP-2); no synonyms. |
| 33 | AlterConfigs | 0-2 | F | F | Whole-config replace. |
| 44 | IncrementalAlterConfigs | 0-1 | F | F | SET, DELETE, APPEND, SUBTRACT. |
| 37 | CreatePartitions | 0-3 | F | F | Grow only; cap `--max-partitions`. |
| 61 | DescribeProducers | 0 | F | F | Used by the Partitions tab for "open transaction on p1" (Tp-16). |
| 65 / 66 | DescribeTransactions / ListTransactions | 0 | F | F | Tp-22 to the transaction detail. |
| 35 | DescribeLogDirs | not advertised | S | F | New (TOP-27): sizes per partition; Kafbat UI and Redpanda Console also probe it. |
| 75 | DescribeTopicPartitions | not advertised | S | S | The boards' wire samples show it; decline it and change the samples (TOP-26). Metadata serves the same answer. |
| 23 | OffsetForLeaderEpoch | not advertised | S | S | Unchanged; already in limitations. |
| 9 / 16 | OffsetFetch / ListGroups | | | | Readers tab; owned by [groups](groups.md). |

Console-side tiers for cosmetic configs: every topic config Kafka defines is
validated and stored (`topicconfig.go:80`); five act (`retention.ms`,
`retention.bytes`, `segment.bytes`, `max.message.bytes`,
`message.timestamp.type`), and with `cleanup.policy=compact` also
`min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms`,
`delete.retention.ms` and `compression.type` (TOP-7). Everything else is C and
must say so wherever the console shows it.

### Console routes

From `console/console.go:227-239`. Target adds the rows marked new.

| Route | Method | Mutates | Now | Target | Note |
|---|---|---|---|---|---|
| `/` , `/topics` | GET | no | F | F | Home list. Needs size, retention, readers, compact, subject columns (Tp-Home). |
| `/new/topic` | GET, POST | POST | F | F | Add `cleanup.policy`; cap from the broker, not `max=1000` (TOP-9). |
| `/topics/{name}` | GET | no | F | F | Becomes the five-tab page (Messages, Partitions, Readers, Config, Access). |
| `/topics/{name}/delete` | POST | yes | F | F | Confirm sentence must name offsets and subject (TOP-21). |
| `/topics/{name}/purge` | POST | yes | F | F | Confirm per Tp-24. |
| `/topics/{name}/partitions` | POST | yes | F | F | Form asks "add N"; Tp-17 asks the total (TOP-30a). |
| `/topics/{name}/config` | POST | yes | F | F | Five fields today; generic per-key edit target. |
| `/topics/{name}/tape` | GET | no | F | F | Polled partial; hash on bounds only (TOP-14). |
| `/topics/{name}/produce` | POST | yes | F | F | Refusals lose the broker message (TOP-10). |
| `/topics/{name}/resend` | POST | yes | F | F | Server-side copy; fails above 1 MiB (TOP-11). |
| `/topics/{name}/message`, `/message/raw` | GET | no | F | F | Inspector body; raw download. |
| `/topics/{name}/partitions-table` | GET | no | none | F | New: Tp-16 data. |
| `/topics/{name}/cleaner` | GET | no | none | F | New: Tp-21 data (TOP-28). |
| `/topics/{name}/compose/preview` | POST | no | none | F | New: Tp-11 live check and encoded preview. |
| `/topics/{name}/config/{key}` | GET, POST | POST | none | F | New: per-key edit with before/after (Tp-19). |

## 3. Where it stands

**Coverage, 2026-10-11 (this stage's full runs).**

| Package | Statement coverage | Functions below 70% worth naming |
|---|---|---|
| `internal/storage/log` | 80.5% | `RequireKeys`, `LargestBatch`, `StampLogAppendTime`, `LastClean`, `cleanerReadFailed` (all 0% in the package; covered only through the broker), `SetTopicLimits`, `ClearTopicLimits`, `SyncPartition`, `setSegmentBytes`, `CodecByName` (0%), `openIndex` 62%, `writeLogStart` 56%, `markTransactional` 69% |
| `internal/storage/meta` | 80.7% | `Open` 67% (the lock-timeout branch), `RegistryAll`/`RegistryApply` 0% (schemas stage), `PutTxn`/`ListTxns` 0% (cluster stage) |
| `internal/broker` (all) | 89.1% (27.5% under `-short`) | topic files: `maybeAutoCreate` 58%, `materialize` 64%, `appendPlain` 60%, `appendIdempotent` 70%, `compactionPolicy` 73%, `alterTopic` 76%, `handleDescribeConfigs` 75%, `growTopic` 77%, `readPartition` 78%, `handleListOffsets` 81%, `isFloat` 20%, `anyValue` 0%, `alterError.Error` 0% |
| `console` (all) | 87.8% | topic files: `saslMechanism` 0%, `newReader` 44%, `newBackend` 63%, `searchTopic` 73%, `topicResend` 79%, `topicMessageRaw` 78% |

**Tests that exist.**

- Log package: `TestRecoveryCrashMatrix`, `TestBoundsAgreesWithRecoveryAfterEveryKindOfDamage`,
  `TestRecoveryFsyncsItsTruncation`, `TestFailedFsyncIsRetriedForLogAndIndex`,
  `TestRollFsyncsTheSegmentItSeals`, `TestGroupCommit*`, the retention, log-start,
  timestamp, compaction and transaction suites (14 files).
- Broker: `topic_config_test.go`, `topic_names_test.go`, `topic_recreate_test.go`,
  `topics_persist_test.go`, `deleterecords_test.go`, `createpartitions_test.go`,
  `timestamps_test.go`, `retention_test.go`, `compaction_test.go`,
  `idempotency_test.go`, `idempotency_bound_test.go`, `produce_features_test.go`,
  `persistence_surfaces_test.go`.
- Console: `tape_test.go`, `messages_test.go`, `payload_test.go`, `gototime_test.go`,
  `txn_test.go`, `audit_test.go`.
- Browser: `topics.spec.ts` (create, purge/delete, partitions, go to time,
  transactions on the tape), `compose.spec.ts` (7 tests), `messages.spec.ts` (drawer,
  binary, send again, go to offset, search).
- Fuzz: `FuzzDecodeRecords` (the record reader) is the only target on this stage's
  bytes. Nothing fuzzes `scanLog` (recovery), `ValidateBatches`, or the console's
  form parsers.
- Compat: every cell creates and reads topics; none asserts a `DescribeConfigs`
  result or a refusal. **No recording of a real Kafka broker exists**, so every
  "Kafka says" below is `read` or `memory` until the recording queue (section 8)
  is run.

## 4. Findings

Evidence: `run` reproduced against a built binary (script under the scratchpad
`tp/`), or `read`. Source: `spec` (kmsg definitions), `doc`, `memory`
(recording queue, section 8), `board` (a claim in the design). IDs are stable.
The first test named is the one to write red first.

### 4a. Create, delete, names, configs (the registry and the admin APIs)

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| TOP-1 | P1 | `CreateTopics` never reads `ReplicationFactor`: factors 3, 0 and -2 all return code 0 and `ReplicationFactor: 1`. Tp-02 says a higher factor is refused `INVALID_REPLICATION_FACTOR`; a one-broker Kafka refuses it. | `internal/broker/topics.go:51-125` | run | board, memory | Raw kmsg: create with factor 3, 0, -2 expects code 38 each and nothing created (layer: broker). |
| TOP-2 | P1 | `DescribeConfigs` on a topic returns 7 keys (cleanup.policy, index.interval.bytes, message.timestamp.type, retention.bytes, retention.ms, segment.bytes, and max.message.bytes only if set). Kafka returns about 30, with source and default flags. Tp-18's 14-row table, Connect and Terraform readers cannot be satisfied. Also the board's "max.message.bytes 1048588 acts" is false: unset, nothing is enforced (a 3 MiB batch was accepted, run). | `internal/broker/configs.go:75-83` | run | board, memory | Table-driven: every key in `topicConfigs` is returned with a default, and `IsDefault` is true until overridden (layer: broker, after foundation B7). |
| TOP-39 | P1 | More forgiving than Kafka: the broker accepts three topic configs that `apache/kafka:4.3.1` refuses. `kafka-configs.sh --alter --add-config` on a real 4.3.1 broker answers `InvalidConfigurationException: Unknown topic config name` for `message.downconversion.enable`, `message.format.version` and `message.timestamp.difference.max.ms` (all removed in Kafka 4.0); doze-kafka stores each (`stored(...)` records in the settings table). Found on 2026-10-12 while checking `kafkaTopicConfigs` against a 4.3.1 container (its `--describe --all` lists 33 names; the test's list had 36). Not fixed in that pass: it changes what a client sees, so it goes red-first here, with a recording. | `internal/settings/settings.go:401,402,405`; `internal/broker/settings_table_test.go` (`notKafka431TopicConfigs` names them) | run | run (CLI against the image), not yet a recording | Record `IncrementalAlterConfigs` and `CreateTopics` with each of the three names; replay expects `INVALID_CONFIG` and nothing stored; then drop the three records and move the names to the never-configs in the test (broker + settings). |
| TOP-2a | P2 | `validateOnly` returns before the existence and case-collision checks: validate-only on existing topic `mm` and on `case` (collides with `Case`) both return 0. | `topics.go:85-90` | run | memory | Raw kmsg validateOnly against an existing name expects `TOPIC_ALREADY_EXISTS` (broker). |
| TOP-5 | P2 | Two names Kafka refuses as colliding (`a.b` then `a_b`) are both created. The case-insensitive refusal (`Case` vs `case`) that does exist is Kafka-stricter and is in no document. | `meta/store.go:264-270` | run | memory | Store test for `.`/`_` collision; docs line in limitations (meta). |
| TOP-8 | P3 | Board says "the same message as Kafka" six times; none matches. Code: `invalid topic name: "bad/name" contains "/"; only ASCII letters...`; `invalid topic name: 250 characters is longer than the maximum of 249`; `2000 partitions is above this broker's limit of 1000 (--max-partitions)`; `invalid value for retention.ms: "abc" is not a number`; `unknown topic config "retention.mss"`; `the topic already has 1 partitions`. Board: `Topic name is invalid: 'harbour/orders' contains one or more characters other than ASCII alphanumerics, '.', '_' and '-'`; `Number of partitions is 1500; at most 1000 are allowed.`; `Invalid value abc for configuration retention.ms: Not a number of type LONG`; `Unknown topic config name: retention.mss`; `Topic currently has 3 partitions, which is higher than the requested 2.` Fix the code to Kafka's text once recorded (R3, R4), or cut "same message" from the board. | `meta/names.go:28-41`, `topicconfig.go:20-46`, `topics.go:78-82`, `createpartitions.go:62-66` | run | board, memory | Replay of R3/R4 compares strings (differential). |
| TOP-9 | P2 | The create form hard-codes `max="1000"`; the cap is `--max-partitions` (1 to any). With `--max-partitions 50` the form allows 1000 and the broker refuses; with 5000 the form blocks. The board calls 1000 "the broker's cap" and "fixed". | `console/templates/topics.html:26` | read | board | Console test with `MaxPartitions` option 50 renders `max="50"` (needs the foundation ask in section 9). |
| TOP-30 | P2 | The only partitions UI asks "add N" (`console/partitions.go:34`); Tp-17 asks for the new total and shows two refusals (fewer, same) that the add-N form can never produce. Pick one. | `console/partitions.go`, Tp-17 | read | board | Browser: enter total 2 on a 3-partition topic shows `INVALID_PARTITIONS` with the broker's words. |
| TOP-20 | P3 | Board marks `max.message.bytes` and `message.timestamp.type` as `new` in the create form; both are already there (`topicSettings`). Only `cleanup.policy` is missing. | `console/topicconfig.go:36-41` | read | board | Console: create form posts `cleanup.policy=compact` and the topic compacts. |
| TOP-6 | P3 | A topic created with `cleanup.policy=compact` on defaults never compacts a development-sized log: the cleaner needs a sealed segment (128 MiB) and `segment.ms` does nothing. Tp-03's one line ("compact keeps the latest record of each key") omits "once a segment has rolled". | `compact.go:228-232`, board Tp-03 | read | board | E2E: compact topic with `segment.bytes=1024`, 200 keyed records over 5 keys; within 5 s the earliest-to-latest count is far above the live count, and the Tp-03 note names the roll condition. |
| TOP-7 | P3 | `docs/limitations.md` says "five settings act, the rest are kept". Compaction settings also act on a compacted topic: `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms`, `delete.retention.ms`, `compression.type` (codec of rewritten batches). Tp-18 labels `min.cleanable.dirty.ratio` "kept; doze cleans at 0" and `min.compaction.lag.ms` "kept, no effect", both false when set. | `topiclimits.go:94-110`, `docs/limitations.md:37-44` | read | board, doc | Broker: set `min.compaction.lag.ms=3600000`; a sealed segment is not cleaned (compaction). |
| TOP-19 | P3 | The boards show retention "7 d" as the default (Tp-02, 05, 08, 18, Home) and `segment.bytes` 1 GiB (Tp-16, 18). Real: retention unlimited (`retention.ms=-1`, run) and 128 MiB (`config.go:132`). Tp-29 "Default config: 7 d retention, delete policy". | boards; `config.go:132` | run | board | Console renders teaching lines from the settings table (B7), test fails on a hard-coded "7 d". |
| TOP-26 | P3 | Wire samples on the boards name versions not advertised: Fetch v16, ListOffsets v8, DescribeTopicPartitions v0. Advertised maxima are Fetch 13, ListOffsets 7, and key 75 is absent. | `protocol/versions.go:94-95`, Tp-14/15/16 | read | board | The dock samples are real rows from the recorder (traffic); no fixture text. |
| TOP-18 | P3 | Three statements disagree on when a retention change acts: Tp-03 "deletes ... at once", the settings note "Changes apply at once: a shorter retention deletes what now falls outside it" (`templates/topics.html:334`), Tp-19 "checked about once a minute". Real: open partitions are swept each second (`manager.go:20`), unopened ones each minute (`manager.go:25`), and the active segment is never deleted. | `topics.html:334` | read | doc | Teaching line generated from the table (A3); test greps the template for "at once". |

### 4b. Produce, fetch, offsets (the data path)

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| TOP-4 | P2 | `ValidateBatches` checks header shape and CRC only. A batch with a valid CRC and an undecodable record body (count 2, body bytes junk) is stored. Every fetching consumer then fails on it and `ListOffsets` by time returns a storage fault. On a compacted topic `RequireKeys` happens to decode it and refuses; on any other topic it is stored. Kafka validates records on append (`CORRUPT_MESSAGE`). | `storage/log/batch.go:96-117`, `produce.go:150-161` | read | memory | Raw kmsg: forge batch with correct CRC and truncated record; expect code 2 and the log unchanged (broker + log). |
| TOP-11 | P2 | Broker has no default `max.message.bytes`, so another client can store a 3 MiB record; the console's own producer clients cap a batch near 1 MiB, so Send again fails: `curl -X POST .../resend` on that record answers `MESSAGE_TOO_LARGE.` (run). The composer fails the same way. | `console/backend.go:94-110` | run | doc | Console: set `ProducerBatchMaxBytes` from the topic's limit or the broker cap; resend a 3 MiB record succeeds. |
| TOP-10 | P2 | A refused produce shows the bare code ("MESSAGE_TOO_LARGE.", run). franz-go drops `ErrorMessage`, so Tp-12's broker sentences ("2,452 B against max.message.bytes 1,024 on ...") cannot appear. A raw `kmsg` Produce for the composer is needed. | `console/backend.go:547-565` | run | spec | Console: oversize send shows the topic and both numbers in the refusal. |
| TOP-16 | P3 | With acks 0 the broker drops the connection on a refused batch (correct), but the console toasts "Sent - no confirmation requested" and the record is gone, so Tp-12's three refusals vanish for `acks=0`. | `handlers_topics.go:214` | run | read | Browser: acks 0 oversize to a limited topic shows "not delivered", not "Sent". |
| TOP-12 | P2 | `PickPartition` treats an empty (non-nil) key as keyless and round-robins it (`len(key) > 0`). Tp-07 says an empty key is "hashed like any key". Java and franz-go hash any non-null key. | `console/backend.go:759` | read | board, memory | Unit: empty key twice goes to the same partition (console). |
| TOP-13 | P2 | The composer cannot send an empty key (`key != ""` is the only test), and "Edit a copy" of an empty-key record prefills "" which sends no key. On a compacted topic that is `INVALID_RECORD` for a record the topic holds. | `console/produceform.go:68`, `handlers_topics.go:553` | read | board | Console: edit-a-copy of an empty-key record sends an empty key (checkbox "empty, not null"). |
| TOP-31 | P3 | Fetch long-poll subscription: `defer sub.Close()` runs once, so a request that loops on partial results is fine, but every error partition ends the wait for all (`failed`), so a consumer fetching one broken partition and one live one never long-polls. Matches Kafka only if Kafka also returns early; unrecorded. | `fetch.go:57-65` | read | memory | Record R9; add a fetch test mixing an unknown and a live partition. |
| TOP-32 | P3 | After an `acks=all` fsync fails, the batch stays in the log and is readable, and for an idempotent producer its sequence is recorded; the client is told it failed. A non-idempotent retry duplicates it. Documented nowhere. | `produce.go:117-131` | read | doc | Broker: fsync fault injected, record visible to a reader, retry yields two copies; limitation line. |
| TOP-33 | P3 | Every call to `fetchWindow` builds a new franz-go client (dial, SASL, TLS, Metadata) and closes it: the live tape poll, the drawer, resend, each 1,000-record search chunk and 2 more per transactional render. Under SCRAM-over-TLS that is 3 round trips a read, and each shows in Traffic as a fresh `doze-kafka-console-fetch` client. | `backend.go:456-478` | read | read | Benchmark in `bench_test.go`: 100 reads share one connection after the change. |

### 4c. Storage engine

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| TOP-3 | P2 | Age retention compares the segment's last-append (or file mtime after a restart) to the cutoff, not record timestamps. Data produced with old `CreateTime` (replays, MirrorMaker, backfills) is kept; the same data in Kafka is deleted at the next sweep. Tp-30's story is only true for live-produced data. | `storage/log/retention.go:38`, `segment.go:76,91` | read | memory | Log unit: records stamped 2 days ago, retention 1 d, two segments; sealed one dropped (R6 confirms Kafka). |
| TOP-23 | P3 | Recovery scans only the active segment; CRC is checked only in its last 8 MiB (`recoveryCRCWindow`). A sealed segment damaged on disk (bit rot, a truncated copy) is served as is. Reasonable for a development broker; undocumented. | `recovery.go:8-25`, `partition.go:63-85` | read | doc | Log: truncate a sealed segment's file mid-batch and reopen; the read reports corruption, not a short answer. |
| TOP-24 | P3 | An unreadable or garbled `log-start` file is read as 0, which un-hides records `DeleteRecords` (and Purge) removed from the first segment. | `logstart.go:23-30` | read | read | Log: garble `log-start` after DeleteBefore; reopen must not resurrect, or must say so in the log. |
| TOP-22 | P3 | The first timestamp lookup on a partition scans every segment's batch headers while holding the partition lock, stalling produce and fetch there for the length of the scan. Documented as "proportional to size", not as a stall. | `timestamps.go:44-60,140-155` | read | doc | Log benchmark: 1 GiB log, lookup, concurrent append latency bounded (scan outside the lock). |
| TOP-25 | P3 | `hashKey` is a 64-bit FNV; two distinct keys that collide let the cleaner drop an older record of one as superseded by the other. Vanishing probability, but the code keeps no proof; the comment says "key hash to offset". | `compact.go:113` | read | read | Log: force a colliding pair via an injectable hash; both keys keep their latest record. |
| TOP-34 | P2 | A mid-segment `DeleteRecords` moves the start but leaves the bytes on disk until the segment rolls or the partition is emptied; the offsets map in `log-start` and the open-transaction table (`dropBelow` only runs on segment drops) can point below the start. A read_committed fetch with LSO below the start answers empty. | `logstart.go:84-117`, `txn.go:137` | read | read | Log+broker: open txn at offset 5, DeleteRecords to 8 (inside the txn), read_committed fetch from 8 returns data or a clear error. Unverified; test first. |

### 4d. Console behaviour that contradicts data

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| TOP-14 | P2 | Every offset with no record is drawn as a "transaction marker" and the "Committed only" toggle appears. On a compacted topic (201 offsets, 5 live keys) the page has 46 marker rows. Tp-21 says gaps are normal. | `handlers_topics.go:545,604`, `txn.go` (log) | run | read | Console: compacted fixture, `strings.Count(body, "transaction marker") == 0` (tape_test). |
| TOP-15 | P2 | Everything at or above the last stable offset is marked "open", including non-transactional records and other producers' committed records that merely sit behind an open transaction. The board's sentence ("records from 4,788 belong to checkout-1's open transaction") repeats it. `kgo.Record.Attrs.IsTransactional()` and `ProducerID` can tell them apart. | `handlers_topics.go:627` | read | board | Console: open txn plus a plain record after it; the plain record is "waiting", not "open" (txn_test). |
| TOP-17 | P3 | The header says "N records" for latest minus earliest; on the compacted test topic that is "201 records" for 5 live keys. The tooltip admits the gap; the count is still the headline. Tp-21 wants "612 records held for 388 keys". | `topics.html:168`, `backend.go:173` | run | read | Needs the cleaner source (TOP-28). |
| TOP-21 | P2 | Delete's confirm says "removes every partition's data". It also deletes the committed offsets of every group on the topic (`meta.DeleteTopic`) and leaves the schema subject; the board's four-line confirm is the true one. A6 requires the sentence to match the effect. | `topics.html:55`, `meta/store.go:373-388` | read | doc | A6 table test: delete topic with 2 groups, sentence says 2 groups lose offsets, and they do. |
| TOP-35 | P3 | Tp-28 describes search as "Key contains", case sensitive, "the last 14,429"; the code searches key, value and headers case-insensitively over at most 5,000 records (`searchBudget`) and says how far it read. Board text is wrong, code is fine. | `search.go:20-29,43-56` | read | board | Board edit; console test pins the budget sentence. |
| TOP-36 | P3 | Tp-13 says "Only the offset and the timestamp are new". A resent record is a plain (non-idempotent, non-transactional) batch: producer id, epoch, sequence and the transactional flag are dropped, and `tx checkout-1` is not carried. Key, value and headers are exact. | `handlers_topics.go:293-330` | read | board | Console: resend of a transactional record has no producer id; the board line says so. |

### 4e. Needs backend (the board asks for data that does not exist)

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| TOP-27 | P2 | Tp-16 and Tp-Home need bytes on disk, segment count, active segment and last write per partition. Nothing exposes them: `DescribeLogDirs` is not implemented (`docs/limitations.md:46`) and the console is a pure client. New work: implement API 35 (sizes, offset lag) and an optional `LogSource` interface (like `TrafficSource`) for segments, `cleanedTo` and mtime. | none | read | board | Broker raw kmsg DescribeLogDirs; console fake `LogSource` renders the table. |
| TOP-28 | P2 | Tp-21's cleaner card (idle/running, last cleaned, records removed last pass and "since start", dirty ratio, tombstones kept, "388 keys") has no source. `Partition.LastClean()` holds one pass in memory (0% covered) and nothing totals them. New: counters on the partition and a keys-held count (one scan, cached by `cleanedTo`). | `compact.go:144` | read | board | Log: two passes, counters add up; restart resets and says "since start". |
| TOP-29 | P2 | Tp-06 "Written by checkout-api, tx checkout-1" and the record's "Type CreateTime" need provenance: the recorder keeps the last 2,000 requests, so only recent records have a writer, and a transactional id exists only via producer id to coordinator. `Type` is free (`Attrs.TimestampType()`). | none | read | board | Console fake: record with a matching recorder row shows the client; without one shows nothing, never a guess. |
| TOP-37 | P2 | Tp-30's "Retention was changed to 1 d at 14:38 (the change)" needs a history of config changes; the registry stores only the current config. Cut the sentence or add an append-only change log (name, key, old, new, time, client) in `meta`. | `meta/store.go:329-362` | read | board | Meta: SetTopicConfig appends a change row; survives restart. |
| TOP-38 | P2 | Tp-11 "Valid against ...-value v3 ... live check; today the broker checks on send" and "Resulting schema id", and Tp-08 "id 9 is not registered": the encode in `registry.encode` runs only on send; the decode path does not distinguish "not framed" from "framed with an unknown id". | `console/registry.go:359-376` | read | board | Console: preview endpoint returns the encoded bytes and id; decode of framing with unknown id says so. |

Counts: 40 findings. P1: 3 (TOP-1, TOP-2, TOP-39). P2: 19. P3: 18. Reproduced (`run`): 12
(TOP-1, 2, 2a, 5, 8, 10, 11, 14, 16, 17, 19, and TOP-39 against a real Kafka). The sub-id `TOP-2a` is a separate
finding; IDs are not contiguous because they were assigned in the order found.

## 5. What to check

Organised by operation family. Each line is a check to make with a named test
or a command; none restates the common criteria.

**Create and names**
- Name rules (`meta/names.go`): `.`, `..`, 249 and 250 characters, a name with a
  multibyte character, a space, a NUL, a name that is a Windows device name
  (`con`, `nul`): directories are `con-0`, so allowed, but run on Windows CI.
  Table test over the full ASCII range against `[a-zA-Z0-9._-]`.
- Directory length: topic 249 plus `-999` is 253, under the 255 limit; at
  `--max-partitions` above 9999 the suffix grows. Test the limit.
- Case-insensitive file systems: macOS and Windows data dirs. The collision rule
  exists in the registry; check it also covers a registry rebuilt from disk.
- Create racing delete and auto-create: `TestCreateDeleteRace` (new) with `-race`.
- Crash between `meta.CreateTopic` and `materialize`: kill after the registry write
  (fault injection in `meta.afterCreateCommit`), restart, produce succeeds.

**Delete, recreate, purge**
- Delete then recreate gets a new id, offset 0, and no committed offsets;
  produce with a stale producer id after recreate (idempotency dropped).
- Stray directories after a crash during delete are removed on the next create
  (`RemoveStrayTopicDirs`) and not adopted.
- Purge on a topic with an open transaction (TOP-34), on a compacted topic, and
  with a group whose committed offset is below the new start.

**Produce**
- `acks` 0, 1, all, -2 (invalid): code and what the connection does.
- A batch of every codec (none, gzip, snappy, lz4, zstd) round-trips through
  produce, compaction rewrite and fetch (compression is "stored verbatim").
- Batch limits: just under, equal, one over `max.message.bytes` (the limit is
  on the batch, bytes on the wire, compressed), and the 100 MiB request cap.
- Idempotent retry after restart (sequence relearned from the last 16 MiB); a
  producer idle past the window.
- `message.timestamp.type=LogAppendTime`: a consumer sees the broker time on every
  record in a multi-record batch (the broker rewrites only `maxTimestamp`).
- Compacted topic: batch with one keyless record in ten refuses all ten.

**Fetch and offsets**
- Fetch at: start-1, start, end, end+1, mid-batch, inside a compaction gap, in the
  tail gap of a sealed segment; `ErrOffsetOutOfRange` mapping on every one.
- `maxBytes` smaller than the first batch (returned whole), request budget across
  partitions, read_committed with LSO below start (TOP-34).
- Long-poll wake on produce to another partition of the same topic; wake on
  topic delete; 30 s cap; client gone during the wait (goroutine count).
- `ListOffsets`: time before first, after last (-1), on an empty partition, on a
  compacted partition, with `LogAppendTime`, with out-of-order timestamps in one
  batch; v0 old-style offsets list.

**Storage engine**
- Torn writes: zero-filled tail, a header cut at every byte, a body cut at every
  byte, a CRC flipped in the tail window and outside it, an index longer than the
  log, an index shorter than the log, a leftover `.cleaned` and `.swap`. The
  crash matrix and `Bounds` agreement tests exist; add the same matrix for a
  sealed segment (TOP-23).
- fsync: group commit under 64 concurrent `acks=all` producers returns after one
  or two fsyncs (`syncOps`); an fsync failure on the index only; a directory fsync
  failure on roll.
- Rolling at `segment.bytes` exactly; a batch larger than the segment size.
- Windows: removing a segment another handle has open (the partition closes it
  first); renaming over an open `.log` in `swapSegment`.
- Retention: by bytes counts the active segment; by age uses write time (TOP-3);
  `retention.ms=0` becomes 1 ms and never touches the active segment.
- Compaction: tombstone age, the newest segment, an open transaction, an aborted
  transaction's marker, key map limit (`MaxMapEntries`), a crash during
  `swapSegment` (`recoverSwaps`).
- Time lookups across a restart and across a compaction swap (a new `Segment`
  object resets its scan).

**Config**
- `AlterConfigs` replaces everything; `IncrementalAlterConfigs` SET, DELETE,
  APPEND, SUBTRACT, a duplicate name, an unknown op, a broker resource, validate-only.
- A config that survives restart: stored in `meta.db` (`SetTopicConfig`) and
  applied at open (`InitialTopicLimits`). Test: set `segment.bytes`, SIGKILL,
  restart, the next roll uses it.
- `cleanup.policy` toggled from delete to compact on a topic with 10 segments:
  the cleaner starts without a restart.

**Console**
- Message display: null vs empty vs binary vs 5% control characters vs a value
  of exactly `displayLimit`, `resendLimit` and `hexLimit` bytes; invalid UTF-8 split
  at the 4096-byte sample; a value that is text in the drawer and binary in the
  table (`looksBinarySample`).
- Registry framing: a text value that starts with byte 0 and four more bytes;
  an unknown id; a schema whose subject was deleted.
- Links: topic names containing `.`, `-`, `_`, uppercase (the alphabet is
  already safe); names that predate validation, such as `a/b` created through the
  data directory, must not break the list page.
- Numbers: `Count` on a compacted partition (TOP-17), 1000 partitions page weight
  (153 KB, 275 ms in the run), tape poll with 1000 partitions.

## 6. Console

Status is for the whole board: `real` (shipped, wording or detail to adjust),
`buildable` (the data exists on the wire or in the registry; the screen does not),
`needs backend` (data or behaviour does not exist). The inspector, dock,
neighbours, Refusal, teaching lines and confirms are the foundation's (A1 to A6)
and are used by name.

| Board | Template and handler | Status | Built from / what is missing |
|---|---|---|---|
| Tp-Home | `topics_home` / `topics` | needs backend | Rows from Metadata + ListOffsets (real). Size on disk needs DescribeLogDirs (TOP-27); Retention and Compact from DescribeConfigs; Read by from groups (buildable); Schema subject from the registry's subject list; "dead letters" heuristic is a rule to define (topic ends `.dlq` or `-dlq`). |
| Tp-01 | `topics_home` empty state | buildable | `uiCopy topics.none` exists; add the `kafka-topics.sh` preview built from the form (A3 for flags) and the "auto-create is on" sentence from `num.partitions`/`auto.create.topics.enable` (DescribeConfigs broker). |
| Tp-02 | `topic_create` / `topicCreate` | buildable | Names-taken check from the list; command preview from form state; replication-factor line needs TOP-1 fixed or cut; cap from Options (TOP-9). |
| Tp-03 | `topic_settings_fields` | buildable | Add `cleanup.policy` field; the two `new` tags on `max.message.bytes` and `message.timestamp.type` are stale (TOP-20); teaching lines from B7; the compact warning line is true (INVALID_RECORD). |
| Tp-04 | `fail` -> Refusal | buildable | Broker error name and message pass through `refusal()` already; fix strings (TOP-8); `TOPIC_ALREADY_EXISTS` is turned into a bad-input today (`backend.go:292`). |
| Tp-05 | `topic_detail`, `tape_and_table` | needs backend | Tape and table are real per partition. New: merged cross-partition table (buildable: fetch the tail of each partition, merge by timestamp), time axis marks (ListOffsets by time at N points, buildable), size and last write (TOP-27), written by (TOP-29). |
| Tp-06 | `message_drawer` | needs backend | Timestamp, key, sizes, headers, Text/Hex/Base64, Follow, Send again, Edit a copy, Copy menu are real or buildable. "Written by / tx" needs TOP-29; "Copy as kafka-console-producer" is buildable client-side but see recording queue R8 for what it can say losslessly. |
| Tp-07 | `message_drawer` | real | `bytesView`, hex dump, "Null is not empty" legend. Registry marker explanation: the first byte test exists in `registry.decode`. |
| Tp-08 | `message_drawer` + `registry.decode` | buildable | Decoded/Raw toggle is real; the unknown-id note needs the decode path to say why (TOP-38). |
| Tp-09 | `message_drawer` | needs backend | Tombstone view is real. "What compaction will do with SKU-BUOY-0007" needs the key's history per offset and the segment boundaries (TOP-28). |
| Tp-10 | `#compose-panel` / `topicProduce` | buildable | Composer exists. Add "What will be sent", the murmur2 line from the real partitioner, JSON validity as typed, the page-cache sentence per acks. Empty key (TOP-12, TOP-13). |
| Tp-11 | composer + `registry.encode` | buildable | New preview endpoint calling `encode` (TOP-38). |
| Tp-12 | composer refusal states | buildable | Needs raw Produce for the broker message (TOP-10) and acks=0 handling (TOP-16). The header-JSON error is in `parseProduceForm`. |
| Tp-13 | `topicResend` | buildable | Side-by-side from two `FetchWindow` reads (TOP-33); "readers will see it twice" = groups whose committed offset is above the original (groups stage); wording TOP-36. |
| Tp-14 | `tapeData` jump | real | Out-of-range note exists (`handlers_topics.go:446-452`); text adjusts to the board. |
| Tp-15 | `atParam`, `OffsetAtTime` | buildable | `ListOffsets` already returns the record's timestamp (discarded at `gototime.go:79`); add "before the first record" and "after the last" wording; "no time index on disk" is true (TOP-22). |
| Tp-16 | new Partitions tab | needs backend | Start, End, High mark, Last stable are ListOffsets (real, isolation 1 for LSO); Size, Segments, Active segment, Last write need TOP-27; "why p1's LSO is behind" from DescribeProducers + ListTransactions. |
| Tp-17 | `topic_addparts` | buildable | Total-based form (TOP-30), refusal text, "who picks them up" from `cg848`/classic group lists; fix the "metadata.max.age.ms 5 min" claim only after R-queue confirms the client default. |
| Tp-18 | `topic_config` | needs backend | All keys with source and the "acts/kept" column need DescribeConfigs to return every key (TOP-2) and the settings table (B7). |
| Tp-19 | `topic_config` edit row | buildable | Before/after from the table; IncrementalAlterConfigs exists; effect sentence from the table, correcting TOP-18 and TOP-3. |
| Tp-20 | config refusals | buildable | Did-you-mean from the existing edit-distance helper (`errors.go:232`); strings TOP-8. |
| Tp-21 | new cleaner card | needs backend | TOP-28; "latest/in active segment/kept/delete" row note needs a key-history scan. |
| Tp-22 | `tape_and_table` committed-only | real | `txnStatus` and the toggle exist; fix TOP-14 and TOP-15; "Open the transaction" links to [cluster](cluster.md). |
| Tp-23 | `topic_groups` | buildable | Readers from groups; "Who is writing" needs the recorder filtered by topic (TRF, A4). |
| Tp-24 | confirms | buildable | A6 table: purge (partitions, record sum, groups behind) and delete (offsets of N groups, subject kept, clients affected); sentences TOP-21. |
| Tp-25 | denied state | needs backend | Refusal is real; "which rule decided" needs the authorizer to explain a decision (ACC); "as `__console`" row is true. |
| Tp-26 | tape live/paused | real | Pause, counted new records, selection stays; `n` key if absent. |
| Tp-27 | empty-topic state | buildable | Inline first-record form reuses the composer; "created ... by an auto-create from checkout-api" from the recorder. |
| Tp-28 | search | real | Search exists; board copy wrong (TOP-35). |
| Tp-29 | not-found | buildable | Suggestions exist (`errors.go:168`); "why it happened" needs the recorder (TRF). Tp-01 says auto-create is on yet Tp-29's producer was refused: show it only for clients that set `allow.auto.create=false`. |
| Tp-30 | log-start moved | needs backend | Tape and log start real; group table from committed vs earliest (groups); config-change event TOP-37. |
| Sy-01-Anatomy | `topic_detail` | buildable | The specimen: list pane, name and one action, facts sentence, neighbours (A5), five tabs, Try it, wire dock (A4). Delete moves under "...". The "coloured number" rule needs a definition (nonzero DLQ, high lag). |

Totals: 5 real (Tp-07, 14, 22, 26, 28), 18 buildable, 9 needs backend.

**Teaching lines** come from the settings table (B7). Per topic key it needs:
name, unit, range, Kafka default, doze default, acts/stored, what enforces it.
The 12 keys this stage has facts for: the 5 that act, the 5 compaction ones
(act when compacted), and `min.insync.replicas` and `flush.messages` (stored).
Defaults in doze differ from Kafka for `retention.ms` (-1), `segment.bytes`
(128 MiB), `max.message.bytes` (no limit), `min.cleanable.dirty.ratio` (0 until
set) and `segment.bytes` minimum (1024); the table states each difference.

## 7. Tests to add

Every finding gets its red test first (protocol step 2). By layer:

**Unit and store (`internal/storage/log`, `internal/storage/meta`)**
- `TestRetentionUsesRecordTimestamps` (TOP-3), `TestSealedSegmentDamageIsReported`
  (TOP-23), `TestGarbledLogStartDoesNotResurrect` (TOP-24),
  `TestFirstTimeLookupDoesNotBlockAppends` (TOP-22), `TestCleanerHashCollision`
  (TOP-25), `TestDeleteRecordsInsideOpenTransaction` (TOP-34),
  `TestCleanerCountersAccumulate` (TOP-28), `TestPartitionStatsReportSegments`
  (TOP-27), `TestRequireKeysAndLargestBatchAndStamp` (the three 0% functions),
  `TestValidateBatchesDecodesRecords` (TOP-4), `TestTopicNameCollisionDotUnderscore`
  and `TestConfigChangeHistorySurvivesRestart` (meta, TOP-5, TOP-37).
- Property: random torn tails of a random log recover to a prefix of whole
  batches with an index that agrees (extends `TestRecoveryCrashMatrix`).

**Raw kmsg (broker)**
- `TestCreateTopicsRejectsBadReplicationFactor` (TOP-1),
  `TestCreateTopicsValidateOnlyChecksExisting` (TOP-2a),
  `TestDescribeConfigsReturnsEveryKey` (TOP-2), `TestProduceRefusesUndecodableRecords`
  (TOP-4), `TestFetchMixedErrorAndLivePartition` (TOP-31),
  `TestAcksAllFsyncFailureLeavesRecordReadable` (TOP-32),
  `TestCompactionSettingsAct` (TOP-7), `TestDescribeLogDirs` (TOP-27),
  `TestListOffsetsVersions0To7` for every special timestamp, and the string
  tests for TOP-8 after recording.

**Client library (franz-go and Sarama)**
- franz-go: produce 5 codecs to a compacted topic with a tombstone, consume
  read_committed across an abort; `kadm`-style `DescribeTopicConfigs` returns all
  keys; `DeleteRecords` then consume with `auto.offset.reset` earliest and latest.
- Sarama: `GetOffset` by time before first and after last; `CreateTopic` with
  replication factor 3 returns the error.

**Compat cells (`compat/`)**
- `java`: `kafka-topics.sh --create --replication-factor 3` refused; `kafka-configs.sh
  --describe --entity-type topics` lists the same key set as the recorded Kafka
  (R5); `kafka-delete-records.sh`.
- `kcat`: `-b ... -t compacted -K:` with a tombstone (`-Z`); `-o s@<time>` start
  from timestamp.
- `connect` and `spring`: topic auto-create with configs and a later describe.

**Fuzz (`task fuzz`)**
- New: `FuzzScanLog` (arbitrary bytes as a segment; never panics, `validEnd` on a
  batch boundary), `FuzzValidateBatches`, `FuzzParseProduceForm`,
  `FuzzParseAt` and `FuzzScaled` (console parsers: never panic, round trip).

**Console unit (`console/`)**
- `TestPickPartitionEmptyKey` (TOP-12), `TestEditCopyKeepsEmptyKey` (TOP-13),
  `TestCompactedGapsAreNotMarkers` (TOP-14), `TestWaitingIsNotOpen` (TOP-15),
  `TestCreateFormUsesBrokerCap` (TOP-9), `TestRefusalCarriesBrokerMessage`
  (TOP-10), `TestResendOverOneMiB` (TOP-11), `TestAcksZeroRefusalIsShown` (TOP-16),
  `TestConfirmSentencesMatchEffects` (A6; TOP-21), `TestNoHardCodedDefaults`
  (TOP-19: greps templates for "7 d", "1 GiB"), `TestEveryTopicFormFieldHasASettingsRecord` (A3).

**Browser (`e2e/tests/`)**
- `topics.spec.ts` additions: create with `cleanup.policy=compact` and see the
  keyless refusal; every refusal state of Tp-04 and Tp-20; confirm text for delete
  and purge (Tp-24); empty-topic first record (Tp-27); not-found suggestions (Tp-29).
- New `topic-partitions.spec.ts` (Tp-16, Tp-17), `topic-config.spec.ts` (Tp-18 to
  Tp-20), `topic-compacted.spec.ts` (Tp-21, Tp-09: gaps are not markers),
  `topic-anatomy.spec.ts` (Sy-01: tabs, keys, inspector, 960 and 390 widths).
- With the route gate on (B8): `GET /topics/{name}/partitions-table`, `/cleaner`,
  `/config/{key}` and the three new POSTs listed in section 2.

**Differential vs real Kafka (B2)**
- Scenarios R1 to R17 in section 8, each recorded once against
  `apache/kafka:4.3.1` and replayed. Target: replay reports zero "answered
  differently" for the F rows, or each difference is fixed or in limitations.

**Coverage targets (never below baseline)**

| Package | Baseline | Target | Functions that must be covered to get there |
|---|---|---|---|
| `internal/storage/log` | 80.5% | 87% | `RequireKeys`, `LargestBatch`, `StampLogAppendTime`, `LastClean`, `cleanerReadFailed`, `SetTopicLimits`, `ClearTopicLimits`, `SyncPartition`, `setSegmentBytes`, `CodecByName`, `writeLogStart` error branches, `openIndex` error branches |
| `internal/storage/meta` (topic half) | 80.7% | 86% | `Open` lock-timeout, change-history, collision |
| `internal/broker` | 89.0% | 91% | `alterError.Error`, `isFloat`, `anyValue`, `maybeAutoCreate`, `materialize` rollback, `appendPlain` error, `growTopic` assignment branches, `handleDescribeConfigs` broker resource |
| `console` | 87.8% | 90% | `saslMechanism`, `newReader`, `newBackend` TLS and SASL, `searchTopic`, `topicResend`, `topicMessageRaw`, new handlers |

**Mutation (`mutate:check`, B3)**: baseline for `internal/storage/log` and
`internal/storage/meta` first; the broker's topic files as one group
(`topics.go`, `produce.go`, `fetch.go`, `listoffsets.go`, `deleterecords.go`,
`createpartitions.go`, `configs.go`, `alterconfigs.go`, `topiclimits.go`,
`topicconfig.go`); `console` topic files as another. Mutants to kill by name:
the `> limit` in the `max.message.bytes` check, `p.segments[0].lastModified`
comparison, `info.recordCount <= 0`, `fetchOffset > p.logEndOffset`, the `len(p.segments) > 1`
guards in retention (the active segment must survive), `offset > p.logEndOffset` in
`DeleteBefore`.

## 8. Recording queue

Each runs on `apache/kafka:4.3.1` through the B2 harness, normalised, stored as
`conformance/recordings/topics-<n>.json`, and replayed in Go. Until recorded,
the claim is `memory` and its test is written and skipped.

| # | Claim in this document or a board | Scenario |
|---|---|---|
| R1 | CreateTopics with factor 3, 0, -2 on one broker is `INVALID_REPLICATION_FACTOR` with a given message (TOP-1) | Create three topics with those factors; record code and message. |
| R2 | `validateOnly` on an existing topic is `TOPIC_ALREADY_EXISTS`; `Case`/`case` coexist; `a.b` then `a_b` is `INVALID_TOPIC_EXCEPTION` (TOP-2a, TOP-5) | Sequence of creates and validate-only creates. |
| R3 | The exact messages for an invalid name, a 250-character name, unknown config, bad value (`abc`, 512 for `segment.bytes`, `2` for the dirty ratio) (TOP-8) | One CreateTopics per case; record `ErrorMessage`. Also `segment.bytes` minimum in 4.0 (1 MiB claimed in `topicconfig.go:68`; the board says 1024). |
| R4 | CreatePartitions with fewer and with the same count: code and text (TOP-8) | Two calls on a 3-partition topic. |
| R5 | `DescribeConfigs` key list, defaults, `IsDefault`, source, and `max.message.bytes` 1048588 for a fresh topic (TOP-2) | Describe after create with and without overrides; Alter then describe. |
| R6 | Age retention uses the largest record timestamp of a segment; the retention check interval default; `retention.ms` change deletes at the next check (TOP-3, TOP-18, Tp-19/30) | `segment.bytes` small, backdated timestamps, `log.retention.check.interval.ms=1000`, watch log start. |
| R7 | A batch with valid CRC and garbage body is refused `CORRUPT_MESSAGE` (TOP-4) | Hand-built batch via raw Produce. |
| R8 | Empty-key partitioning equals murmur2 of zero bytes; the generated `kafka-console-producer` command reproduces key, value, headers and null exactly (Tp-06, TOP-12) | Produce the same record with the generated command and a client; fetch both, compare bytes; list what is inexpressible (binary, newline, empty vs null key). |
| R9 | `ListOffsets` for time before first, after last (-1), max timestamp on a compacted partition, read_committed latest = LSO; Fetch with one broken and one live partition does or does not wait (TOP-31) | Scripted per case. |
| R10 | `DeleteRecords`: offset above HW, offset -1, inside an open transaction; fetch below start; read_committed LSO after (TOP-34) | Scripted. |
| R11 | Compaction semantics on the wire: offsets after cleaning, tombstone retention, newest segment untouched, `min.compaction.lag.ms` honoured (TOP-7, Tp-09/21) | Small segments, keyed writes, wait for cleaner, fetch from 0. |
| R12 | `acks=0` with a refused batch closes the connection (TOP-16) | Raw produce with `acks=0` to a limited topic. |
| R13 | `INVALID_RECORD` text and per-record errors for a keyless record on a compacted topic (Tp-12) | One produce. |
| R14 | `MESSAGE_TOO_LARGE` text and that the limit is on the batch; default 1048588 (TOP-2, TOP-11) | Produce just under and over, batched and single. |
| R15 | `LogAppendTime`: what a consumer sees for each record's timestamp in a multi-record batch | Produce 3 records with old timestamps to a LogAppendTime topic; fetch. |
| R16 | DeleteTopics removes committed offsets of groups on the topic (Tp-24, TOP-21) | Commit, delete, recreate, OffsetFetch. |
| R17 | Metadata auto-create by a Java producer and the produce refusal when `allow.auto.create=false` (Tp-29) | Two producers, one flag each. |

## 9. Owed to integration

- **Traffic** ([traffic](traffic.md)): a recorder filter by topic with a cursor
  (A4). Tp-05, 06, 23, 27, 29 and every dock depend on it. "Written by" and
  "created by an auto-create from" read it and must show nothing when the row
  has aged out of the 2,000.
- **Groups** ([groups](groups.md)): readers, lag, "behind the start" (Tp-23, 30),
  "will process it twice" (Tp-13), and what a deleted topic does to a group.
- **Schemas** ([schemas](schemas.md)): the topic-naming-strategy map (Tp-Home
  column), `encode` with the unknown-id answer (TOP-38).
- **Access** ([access](access.md)): "which rule decided" (Tp-25), the Access tab.
- **Cluster** ([cluster](cluster.md)): the transaction detail Tp-22 opens; the
  `LogSource` and `DescribeLogDirs` work (TOP-27) also feeds the Cluster storage
  board (Cl-04); agree one interface.
- **Map** ([map](map.md)): topic lists and neighbours.

**Asks of the foundation**
1. B7 must include the broker defaults that differ from Kafka and the console
   cap (`--max-partitions`) as a value the console can read; pass `MaxPartitions`
   in console `Options` (TOP-9).
2. A2 needs a raw-`kmsg` request path (not franz-go's producer) so the Refusal can
   carry the broker's `ErrorMessage` (TOP-10).
3. A6's table must include "groups that lose offsets" for delete and "groups
   behind the new start" for purge as computed values.
4. A one-line `LogSource` interface beside `TrafficSource` (decision recorded in
   section 11) so the embedded `doze` module can leave it nil.
5. B5 helper `Kill()` before the SIGKILL tests in section 10.

## 10. Acceptance criteria

On top of the common ones in [the protocol](../01-protocol.md).

**Contract**
- [ ] `docs/api-support.md` rows for CreateTopics, DescribeConfigs, DescribeLogDirs
      (new F) agree with the code; `go test ./internal/broker -run TestVersionMatrix`.
- [ ] `docs/limitations.md` lists: no broker default `max.message.bytes`, the
      case-insensitive name collision, retention basis (after TOP-3), acks=all
      fsync failure leaves the record (TOP-32), compaction settings that act (TOP-7).
- [ ] `grep -n "7 d\|1 GiB" console/templates/*.html` finds no hard-coded default.

**Behaviour**
- [ ] TOP-1 to TOP-5, TOP-34 closed with the red test first (`git log` shows test commit before fix).
- [ ] `go test ./internal/broker -run 'TestCreateTopics|TestDescribeConfigs' -count=1` green; no test skipped for R1 to R5 once recorded.
- [ ] SIGKILL variant: acknowledged `acks=all` write survives `kill -9` and a
      restart (`go test ./internal/broker -run TestCrash_AcknowledgedWriteSurvivesAProcessKill`, uses B5; process-kill survival, not a power cut).
- [ ] A data-dir test with a path containing a space creates, produces, restarts, reads (`go test ./internal/broker -run TestTopicsDataDirWithSpace`).
- [ ] 1,000-partition topic: create under 1 s, page under 1 s, fd count unchanged (`TestWideTopicBudget`).
- [ ] Total partitions or topics bounded or documented as unbounded (TOP-5 follow-up decision in the changelog).

**Proof**
- [ ] `go tool task coverage`: `internal/storage/log` >= 87%, `internal/storage/meta` >= 86%, `internal/broker` >= 91%, `console` >= 90%.
- [ ] `go tool task mutate:check -- ./internal/storage/log/... ./internal/storage/meta/...` no new survivor; broker and console topic groups recorded and checked.
- [ ] `go tool task fuzz` runs `FuzzScanLog`, `FuzzValidateBatches`, `FuzzParseProduceForm`, `FuzzParseAt`, `FuzzScaled` for 60 s each, clean.
- [ ] `go tool task test:compat` cells `java`, `kcat`, `kafkajs`, `python`, `spring`, `connect` pass plain and SCRAM over TLS; new topic-admin assertions in `java` and `kcat`.
- [ ] Differential: R1 to R17 recorded; the replay prints `answered differently: 0` for F rows; `memory`-sourced claims remaining: 0 (count recorded in the changelog).
- [ ] `go tool task lightness`: idle heap and binary size unchanged, or the commit says why (the `LogSource` counters and change log are small).

**Console**
- [ ] Boards implemented: Tp-Home, Tp-01 to Tp-30, Sy-01 (32), or each dropped one named here with a reason.
- [ ] Every Refusal in Tp-04, 12, 17, 20, 25 shows the broker's name and message (`TestRefusalCarriesBrokerMessage`).
- [ ] Confirm sentences for purge and delete asserted against effect (`TestConfirmSentencesMatchEffects`).
- [ ] Teaching lines on every create and config field from B7 (`TestEveryTopicFormFieldHasASettingsRecord`).
- [ ] `go tool task test:e2e` including `topic-partitions.spec.ts`, `topic-config.spec.ts`, `topic-compacted.spec.ts`, `topic-anatomy.spec.ts`; accessibility sweep clean on the topics pages; keys `j k Enter / g t p c n Esc` work on list, tape and forms; pages render at 960 and 390.
- [ ] Broker down, unreachable mid-poll, empty data dir: the topic pages render the A7 states.
- [ ] `docs/api-support.md`, `docs/limitations.md`, `docs/console.md`, `docs/storage-and-durability.md`, `CHANGELOG.md` updated; findings table has no open row.

## 11. Files and order of work

Half-day units (hd). Overall: **L, about 28 days (56 hd)**, of which about 14 hd
are the findings, 12 hd new backend, 30 hd boards and tests.

| Step | hd | Work | Files |
|---|---|---|---|
| 1 | 2 | Red tests for TOP-1, 2, 2a, 4, 5, 12 to 16 (skipped where they await a recording). | `internal/broker/*_test.go`, `console/*_test.go` |
| 2 | 2 | B2 scenarios R1 to R5, R7, R13, R14 recorded; fix message strings (TOP-8). | `conformance/`, `topics.go`, `topicconfig.go`, `meta/names.go` |
| 3 | 3 | Fix TOP-1, 2a, 5; DescribeConfigs full key set with defaults from B7 (TOP-2); record validation (TOP-4). | `topics.go`, `configs.go`, `storage/log/batch.go`, `meta/store.go` |
| 4 | 2 | Retention by record timestamp (TOP-3) after R6; compaction settings docs (TOP-7); `log-start` and sealed-segment checks (TOP-23, 24). | `retention.go`, `segment.go`, `logstart.go`, `recovery.go` |
| 5 | 2 | Time lookup outside the lock (TOP-22); DeleteRecords in an open transaction (TOP-34) after R10. | `timestamps.go`, `logstart.go`, `txn.go` |
| 6 | 4 | `DescribeLogDirs` (API 35) and `LogSource`: per-partition size, segments, active segment, last write; cleaner counters (TOP-27, 28). | `broker/`, `protocol/versions.go`, `storage/log/` |
| 7 | 2 | Config change history in `meta` (TOP-37); `docs/api-support.md` row. | `meta/store.go` |
| 8 | 3 | Console data path: one shared reader client (TOP-33), raw Produce with broker message (TOP-10), acks 0 (TOP-16), batch size (TOP-11), empty key (TOP-12, 13). | `backend.go`, `produceform.go`, `handlers_topics.go` |
| 9 | 2 | Tape fixes: gaps vs markers (TOP-14), open vs waiting (TOP-15), count (TOP-17). | `handlers_topics.go`, `topics.html` |
| 10 | 6 | Boards Tp-Home, Tp-01 to 04, Sy-01: list columns, create form, anatomy, tabs, facts sentence. | `topics.html`, `handlers_topics.go`, `topicconfig.go` |
| 11 | 6 | Boards Tp-05 to 13: tape with lanes and time axis, inspector, composer with preview, resend side by side. | `topics.html`, `static/editor.js`, `registry.go`, `handlers_topics.go` |
| 12 | 4 | Boards Tp-14 to 20: jump, partitions tab, add partitions, config tab and editor. | `gototime.go`, `partitions.go`, `topicconfig.go`, `topics.html` |
| 13 | 4 | Boards Tp-21 to 30: compacted, transactions, readers, confirms, denied, paused, empty, filter, not found, retention gone. | `topics.html`, `handlers_topics.go`, `search.go` |
| 14 | 4 | E2E specs, compat additions, fuzz targets, coverage floor, mutation baselines. | `e2e/tests/`, `compat/`, `*_test.go` |
| 15 | 4 | R6, R8 to R12, R15 to R17 recorded and replayed; fix differences; documents and changelog. | `conformance/`, `docs/` |

Dependencies: step 6 before 12 and 13 (Partitions tab, cleaner card); step 8
before 11 (composer refusals); the traffic filter (A4) before the dock rows of
10 to 13; B7 before 3 (DescribeConfigs) and every teaching line.

**Decision for step 6.** Implement `DescribeLogDirs` (a real Kafka API that Kafka
UIs probe) for sizes, and add a small `LogSource` interface passed in
`console.Options` for segments, cleaner state and last write, because those have
no Kafka API. The embedded module leaves it nil and the tabs say "not available
here".

**Prior art.**
- Redpanda Console's topic page has Messages, Partitions, Configuration and
  Consumers tabs, per-partition low/high watermarks and sizes, a message
  viewer with a deserializer choice and a "publish message" dialog; the Config
  tab lists every topic config with a default marker and edits any of them. That
  is the shape Tp-16 and Tp-18 follow; sizes there come from the broker's
  log-dir API, which is why doze-kafka needs API 35.
- Kafbat UI (the Kafka UI fork) offers seek by offset or timestamp and a live
  tail, and edits all topic settings; both UIs are compat cells here.
- Apache Kafka: `kafka-delete-records.sh` for DeleteRecords, `kafka-configs.sh
  --describe` for the full key list (the expectation in TOP-2), and the
  `kafka-run-class kafka.tools.DumpLogSegments` view of a segment, the nearest
  thing to the Partitions tab's segment row. Relevant KIPs, named only where
  certain: KIP-32 (record timestamps, `message.timestamp.type`), KIP-33 (time
  index; doze-kafka deliberately has none, see TOP-22), KIP-58 (compaction lag),
  KIP-71 (`compact,delete`), KIP-98 (idempotence and transactions), KIP-113
  (`DescribeLogDirs`), KIP-195 (`CreatePartitions`), KIP-516 (topic ids),
  KIP-467 (per-record errors on produce, relevant to TOP-10).
- Confluent Control Center and Conduktor show the same compaction ratio and
  "dirty" state per topic from JMX; doze-kafka has no JMX, hence the `LogSource`.

## Foundation asks this stage builds

From [00-foundation.md section E](../00-foundation.md#e-asks-from-the-stages): asks stage 0 did not build because this stage is their only consumer.

- E18: a `LogSource` hook in the console `Options` beside `Traffic` and `ClientMetrics`.
