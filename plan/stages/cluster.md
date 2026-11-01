# Stage: Cluster (CLU)

The Cluster section of the console (ten boards: Cl-Home and Cl-01 to Cl-09) and the broker
code beneath it: configuration reporting, the API table, the transaction coordinator, the
compaction cleaner, storage, listeners and advertised hosts, the `.doze` zone, and the
version string. Read [01-protocol.md](../01-protocol.md) for the method and
[00-foundation.md](../00-foundation.md) for the inspector, dock and neighbours (`A1`, `A4`,
`A5`), the refusal vocabulary (`A2`), the settings table (`B7`) and the route table (`B8`).

Evidence tags: `run` = reproduced against a binary built from `main` at `970d829`
(`go build -o $SCRATCH/dkbin ./cmd/doze-kafka`); `read` = from the code. Source tags as in
the protocol document.

## 1. Summary

**What the surface is.** One page today (`GET /cluster`, `console/templates/cluster.html`,
34 lines, built from `console/backend_cluster.go`, 148 lines). It shows the node, the cluster
id, how the console's own connection is secured, the supported APIs as chips, open
transactions as text rows and the eight broker config keys `DescribeConfigs` returns. The
design makes it ten boards: an overview, configuration, the API table with a profile switch,
the statement of limits, storage, a transaction list and detail with an Abort action, the
compaction cleaner, the `.doze` names and the listeners.

**The central finding.** The console is a pure Kafka client (franz-go over TCP; `Options`
has only the traffic and client-metrics sources as in-process hooks). Everything the current
page shows is a wire answer. Of the ten boards, only the API list and the transaction list
can be built that way. Everything else the design shows (version, uptime, cluster id format,
sizes on disk, last fsync, configuration provenance, listener protocol, user count, the zone,
cleaner status, an Abort) has no source today. This stage adds one in-process interface
(`ClusterSource`, section 6) and the broker accessors behind it, and says per number which
are real data and which are invented (section 4, table in 4.3).

**Where the design disagrees with the code (all verified).**
- The console is shown at `http://kafka.doze`. `cmd/doze-kafka/names.go:13` says `kafka.doze`
  is deliberately not claimed, and `names_test.go:48` asserts it. The real URL is
  `http://kafka.<instance>.doze`.
- The default listener is shown as `127.0.0.1:9092`, advertised host `127.0.0.1`, console
  `127.0.0.1:9093`. By default (name mode) the broker binds the name's own loopback address
  and advertises `kafka.<instance>.doze`; `127.0.0.1:9092` is not bound
  (`cmd/doze-kafka/listen.go` header, `internal/config/config.go:96-110`). The design's
  addresses are what `--listen 127.0.0.1:9092` gives.
- `--zone=false` does not exist. `DOZE_ZONE=off` without `--listen` is fatal (`run`).
- Node id 0, cluster id `doze-7Hk2...`, version `0.9.2`, "secrets are never shown": the
  code says node 1, cluster id `doze-kafka`, version `dev` (latest tag `v1.2.0`), and
  `doze-kafka config print` prints passwords (`run`).
- "Abort transaction ... sends the same decision as EndTxn with abort": that decision does
  not fence the producer. The timeout path does. They are different behaviours (CLU-15).

**Size.** 38 findings (section 4). Backend work is the larger half: a config snapshot with
provenance, storage statistics, a cleaner status, an abort path, transaction-id expiry,
`ListTransactions` conformance, a persisted cluster id, a version/uptime accessor. Console
work is ten boards on seven routes.

**Packages owned.** `console` (cluster files), `cmd/doze-kafka` (config, names, listen,
doctor), `internal/config`, `internal/protocol` (API table only), `internal/broker` (txn,
txnapis, configs, alterconfigs, topiclimits), `internal/storage/log` (compaction, sync,
sizes), `internal/storage/meta` (txn records, cluster id). Peers: [traffic](traffic.md)
(the wire dock, connected clients), [topics](topics.md) (per-topic config, retention,
compaction settings), [groups](groups.md) (isolation level of consumers), [access](access.md)
(SASL users, super user), [connect](connect.md) (Cn-06 Docker and Cn-07 SASL/TLS repeat the
listener facts), [map](map.md), [bad days](bad-days.md) (Bad-Disk, Bad-Port, Bad-Fenced).

## 2. Contract

### 2.1 Kafka APIs

Tier: F functional, C cosmetic, S stub (refused honestly). "Now" is what `main` does.

| API (key) | Used for | Now | Target | Note |
|---|---|---|---|---|
| ApiVersions (18) | API table; the console's own handshake | F | F | Ranges are doze's own (`versions.go` header). Add a test that v4+ answers `UNSUPPORTED_VERSION` with the list (read `router.go:38`, no test found for it). |
| DescribeCluster (60) | node, cluster id | F | F | Node id is 1 (`broker.go:219`), cluster id the constant `doze-kafka` (`broker.go:222`). Cluster id becomes a persisted 22-character id (CLU-5). |
| Metadata (3) | partition and topic counts | F | F | Owned by [topics](topics.md). |
| DescribeConfigs (32), broker | broker config | F (8 keys) | F | Reports `log.*`, `num.partitions`, `auto.create.topics.enable`, `allow.everyone.if.no.acl.found`, `super.users` only (`configs.go:88-99`). Resource name is not checked against the node id (CLU-16). |
| AlterConfigs (33), IncrementalAlterConfigs (44), broker | refusal | S | S | Answers `INVALID_REQUEST` "broker configuration is read-only here ..." (`alterconfigs.go:25`). The board calls this "Refused" (grey-purple, "Kafka would say no too"); real Kafka accepts dynamic broker configs, so the kind is "Not implemented" (CLU-30). |
| ListTransactions (66) | open transactions | F | F | No `UnknownStateFilters` (CLU-20). |
| DescribeTransactions (65) | detail | F | F | `StartTimestamp` only; no last-change time (CLU-14). |
| DescribeProducers (61) | first offset of an open transaction | F | F | `CurrentTxnStartOffset` is already filled (`txnapis.go:384`). |
| InitProducerId (22), AddPartitionsToTxn (24), AddOffsetsToTxn (25), EndTxn (26), TxnOffsetCommit (28) | coordinator | F | F | Findings 12-18. |
| FindCoordinator (10), type transaction | coordinator lookup | F | F | Always this node. |
| ListOffsets (2) `read_committed` latest | last stable offset | F | F | Board Cl-06 LSO comes from here. |
| SaslHandshake (17), SaslAuthenticate (36) | listeners board | F | F | Advertised only with SASL. ACL APIs 29-31 are advertised always (CLU-17). |
| GetTelemetrySubscriptions (71), PushTelemetry (72) | profile note | F | F | Advertised only with `--client-telemetry`. |
| DescribeLogDirs (35) | storage sizes | S (not advertised) | F, optional | See 2.3. |
| WriteTxnMarkers (27) | `kafka-transactions abort` | S (not advertised) | F, optional | See 2.3. |
| OffsetForLeaderEpoch (23), quota APIs, DescribeUserScramCredentials, ElectLeaders, reassignment APIs | probes | S (not advertised) | S | Listed in `limitations.md` only for the first; the rest are not named anywhere (CLU-31). |

### 2.2 Console routes

Today one route owns the surface: `GET {prefix}/cluster` (`console.go:270`). The design needs
these (all GET unless stated; each goes in the B8 route table):

| Route | Mutates | Board | Now/Target |
|---|---|---|---|
| `GET /cluster` | no | Cl-Home | exists / rebuilt |
| `GET /cluster/config` and `GET /cluster/config.toml` (copy as TOML, secrets redacted) | no | Cl-01 | new |
| `GET /cluster/apis?profile=1..4` | no | Cl-02 | new |
| `GET /cluster/limits` | no | Cl-03 | new |
| `GET /cluster/storage` and `GET /cluster/storage/{topic}/{partition}` (inspector) | no | Cl-04 | new |
| `GET /cluster/transactions` and `GET /cluster/transactions/{id}` (inspector) | no | Cl-05, Cl-06 | new |
| `POST /cluster/transactions/{id}/abort` | yes | Cl-06 | new, behind a confirm (A6) |
| `GET /cluster/compaction` and `GET /cluster/compaction/{topic}/{partition}` | no | Cl-07 | new |
| `GET /cluster/names` | no | Cl-08 | new |
| `GET /cluster/listeners` | no | Cl-09 | new |
| `GET /api/counts` (exists) | no | rail counts | extend: configuration, APIs, transactions, compaction |

### 2.3 Declined or deferred

- Altering broker configuration over the wire: declined, stays S. The board's refusal card
  (Cl-01) is kept, with the "Not implemented" kind.
- `DescribeLogDirs` (35): the real Kafka API for sizes (`kafka-log-dirs.sh`; `doc`, queue R13).
  Implementing it makes Kafbat's disk column work and gives storage sizes a recordable
  oracle, but it cannot carry last fsync or cleaner status, so the console needs
  `ClusterSource` regardless. Recommendation: build `ClusterSource` first (C3), add 35 as a
  separate F item after R13 is recorded; if added, remove it from `limitations.md`.
- `WriteTxnMarkers` (27): see CLU-15; optional second step.

## 3. Where it stands

### 3.1 Coverage today (baseline, full suite)

| Package | Baseline | Target | Why |
|---|---|---|---|
| `cmd/doze-kafka` | 79.8% | 84% | zone source, config provenance, redaction, listeners snapshot |
| `console` | 87.8% | 90% | seven new pages, the abort handler, the false-empty paths |
| `internal/broker` | 89.0% | 90% (txn files each at least 92%) | `handleAlterConfigs` (0% in a txn/config-only run), `applyIncremental` 26.7%, `handleAddOffsetsToTxn` 46.2%, `handleEndTxn` 60.0%, `groupFence` 60.0%, `initTxnProducer` 61.5%, `persistTxn` 60.0% |
| `internal/config` | 61.6% | 75% | `InstanceName`, `DeriveName`, `ResolveConsoleAddr`, `ResolveMetaDB`, `holdsPartitionData`, `validHeaderName` are all 0.0%; `Validate` 36.2% (measured with this package's own tests; `cmd/doze-kafka` tests reach some of them) |
| `internal/protocol` | 71.9% | 73% | only the API-table helpers added here |
| `internal/storage/log` | 80.5% | 84% | `LastClean` 0.0%, `RequireKeys` 0.0%, `cleanerReadFailed` 0.0%, `SetTopicLimits`/`ClearTopicLimits` 0.0%, `SyncPartition` 0.0%, `EnforceCompaction` 61.5%, `compactionLocked` 66.7%, `setSegmentBytes` 0.0% |
| `internal/storage/meta` | 80.7% | 82% | cluster id, txn record expiry |

(Function figures from `go test -coverprofile` on `internal/config`, `internal/storage/log`,
and `internal/broker -run 'Txn|Transaction|Compact|DescribeConfigs|AlterConfig|Cluster|Exactly|Fenc|Producer'`
on 2026-10-11; a function shown as 0% there may be covered by another test file, so every
target function is re-measured with the full suite before work starts.)

### 3.2 Tests that touch the surface

- Broker: `txn_test.go` (4), `txn_api_test.go` (4: coordinator rules, offsets applied only on
  commit, restart finishes half-done, retried batch after restart), `compaction_test.go` (7),
  `version_matrix_test.go`, `advertised_test.go`, `telemetry_test.go`.
- Log: `compact_test.go` (11), `durability_audit_test.go`.
- cmd: `names_test.go` (10), `doctor_test.go` (3), `dnsready_test.go` (5), `cli_test.go` (10),
  `env_test.go` (4), `run_test.go` (3), `registry_test.go` (7).
- Console: `console/txn_test.go` is about the message tape, not the Cluster page.
  `surfaces_test.go` renders `/cluster` against a broker. Browser: `smoke.spec.ts:19` (one test,
  asserts `Produce`, `IncrementalAlterConfigs`, `log.retention.ms`, "no authentication"),
  `smoke.spec.ts` route sweep includes `cluster`, `narrow.spec.ts`. No spec reads a transaction row.
- Compat cells: none targets the Cluster page; Kafbat UI and Redpanda Console cells read
  `DescribeCluster`/`DescribeConfigs`/`DescribeLogDirs` and tolerate the close.
- Not tested anywhere: the open-transaction row on the page, the false-empty paths, redaction,
  `LastClean`, the timeout deadline (the console cannot compute it), any zone text.

## 4. Findings

Severity per [01-protocol.md](../01-protocol.md). "Test first" names the layer.

### 4.1 Numbers on the boards: real, needs accessor, or invented

| Board number | Source today | Verdict |
|---|---|---|
| Node 0 | `broker.go:219` returns 1 (`run`: "node 1 · 127.0.0.1:19092") | invented; board says 0, code 1 |
| Cluster id `doze-7Hk2mQ9tTqK1xW4bZpL0aA` | constant `doze-kafka` (`broker.go:222`, `run`) | invented; a Kafka cluster id is a 22-character base64 UUID, this one is 27 and prefixed |
| Version `0.9.2` | `main.version`, `dev` for source builds; latest tag `v1.2.0` (`git tag`) | invented; real data exists only in `package main` and is not passed to the console |
| "speaks Kafka 1 to 4, emulating 4" | `printVersion`, `cfg.Version` | real, needs plumbing |
| Up 2 h 14 m | none | needs accessor (process start time) |
| Topics 7, partitions 16, groups 4, connections 5 | Metadata, ListGroups, traffic | real (connections from the [traffic](traffic.md) stage) |
| Open transactions 1, producer id 4012, epoch 5, age, timeout | ListTransactions + DescribeTransactions | real. Producer ids come from a counter in 1000-blocks (`producerid.go:16`), so 4012 is plausible; after a restart the remainder of a block is skipped, so ids are not dense |
| Data dir 6.4 MiB, partition dirs 6.2 MiB, `meta.db` 212 KiB, per-segment sizes | none; `Segment.sizeBytes` is private (`segment.go:20`); `DescribeLogDirs` not implemented | needs accessor |
| `.index` sizes, segment counts | none exposed | needs accessor |
| Last fsync "0.2 s ago" | `Partition.Sync` records only a sequence number (`partition.go:314`), no time | needs a timestamp |
| Compaction: sealed, last cleaned, removed, dirty ratio, tombstones, "cleaning now / 98 so far", key map | `Partition.LastClean()` exists (`compact.go:144`), has no caller outside tests, is memory-only | needs status struct |
| Config provenance (flag / file / default), "2 / 3 / 11" | none (`loadConfig` applies file then flags, no record of which) | needs accessor; the counts are invented |
| Listener protocol `SASL_PLAINTEXT`, mechanisms, users 5, super user | console knows only its own dial options (`backend_cluster.go:60-66`) | needs accessor |
| "5 users", `acl-super-user User:__console` | `cfg.Credentials`, `cfg.ACLSuperUsers` | real in config, not visible to the console |
| 41 APIs, "2 for telemetry, 2 for SASL" | `versions.go` has 41 rows; with both off 37, default 39 (`run`: 39 chips) | real if computed from the table with the conditions |
| LSO 4,810, first 4,810, held 2 | DescribeProducers `CurrentTxnStartOffset` and ListOffsets `read_committed` | real |
| `kafka.harbour.doze:9092`, `schema.harbour.doze`, `http://kafka.harbour.doze` | `names.Qualified("kafka"/"schema", instance)` in `names.go:206`, `registry.go:65` | real names. `http://kafka.doze` is not real |

### 4.2 Findings

| ID | Sev | Finding | Where | Ev | Src | Test to write first |
|---|---|---|---|---|---|---|
| CLU-1 | P2 | Boards Cl-Home, Cl-01, Cl-08 show the console at `http://kafka.doze`. That name is deliberately never claimed ("kafka.doze, the machine-wide apex, is deliberately NOT claimed"). The console's real URL is `http://kafka.<instance>.doze` (port 9093 on the name's address if no front door). | `cmd/doze-kafka/names.go:13`, `names_test.go:48-59`, `docs/configuration.md:133` | read | doc | Console unit: render `/cluster/names` from a fake `ClusterSource` and assert no `kafka.doze` apex is ever printed, only the source's URL. |
| CLU-2 | P2 | Board Cl-08 gives `doze-kafka --zone=false`. No such flag exists; the zone is left with `--listen`, and `DOZE_ZONE=off` alone is fatal: `fatal err="doze-kafka: nothing to listen on: .doze gave no name and no --listen was set"`. Cl-08 also says "With the zone off ... The broker serves the address you gave it" and "on Windows ... that is the default" (the last is true, `config.go:148`). Foundation A8 repeats `--zone=false`. | `cli.go` flag list (no `zone`), `listen.go` `errNothingToListenOn`, `names.go:111` | run | doc | cmd: `loadConfig([]string{"--zone=false"})` must fail with "unknown flag"; Playwright: Cl-08 opt-out card text equals the text generated from the flag table. Foundation ask F1. |
| CLU-3 | P2 | Board shows the default broker as `127.0.0.1:9092` ("bound 127.0.0.1:9092 here"), advertised host `127.0.0.1 (derived)`, console `127.0.0.1:9093`, and Cl-08 lists `127.0.0.1:9092` as "direct, always" resolving. In name mode the broker binds the name's own address and advertises the name (or that address where it does not resolve); `127.0.0.1:9092` is not bound ("`127.0.0.1:9092` is no longer bound by default"). The listener table is only true under `--listen 127.0.0.1:9092`. | `listen.go` header and `openListener`, `config.go:96-110` | read | doc | cmd: table-driven `listenersSnapshot(cfg, binding)` for name mode (resolves / does not resolve), `--listen 127.0.0.1:9092`, `--listen :9092 --advertised-host h`, asserting each row; Playwright at `--listen` and at name mode (A8). |
| CLU-4 | P1 | `doze-kafka config print` writes SASL passwords in clear: `[credentials] bob = "hunter2"` (reproduced with `--sasl-user bob:hunter2`). Board Cl-01 promises "secrets are never shown" and a "Copy as TOML" that "writes the same thing" as `config print`. Built on `WriteTOML` as it is, the console would show every user's password to anyone who can open it. | `internal/config/file.go:243-245` | run | read | config unit: `WriteTOML` with `Redact: true` replaces every credential value and never prints the TLS key's contents; console handler test: body of `/cluster/config.toml` contains no password for a broker started with a credential; `config print` prints a comment line saying passwords are included (decision in 11). **Closed:** D6: `config print` redacts passwords; the console is not built from `WriteTOML`. |
| CLU-5 | P2 | Cluster id is the constant `"doze-kafka"` for every broker everywhere; Kafka's is a random 22-character id fixed at format time, and tools that join clusters by id (Kafka Connect, Confluent tooling, Kafbat's cluster key) see all doze brokers as one. Board shows a 27-character `doze-` string. Needs an id generated once and stored in `meta.db`. | `broker.go:222`, `configs.go:142`, `metadata.go` | run | doc | meta: id is created on first open, identical after reopen, 22 URL-safe base64 chars; raw kmsg: Metadata and DescribeCluster return the same id; restart keeps it. Record R6. |
| CLU-6 | P2 | Board says node 0; the broker is node 1 everywhere (Metadata brokers, FindCoordinator, DescribeCluster). `DescribeConfigs` for broker "0" is answered like broker "1" (resource name never checked). | `broker.go:219`, `configs.go:56-62` | run | read | Console text test: the Cluster page shows the number Metadata returns. Fix the board, not the broker (changing the node id changes what every client caches). Raw kmsg: DescribeConfigs broker "99" (CLU-16). |
| CLU-7 | P2 | Version `0.9.2` is invented. The version exists only in `package main` (`var version = "dev"`, set by `-ldflags -X main.version`), and the console is never told it. A source build prints `dev`. Board Cl-Home/Cl-01 show a doze-kafka version and "speaks Kafka 1 to 4"; also no license line anywhere (LICENSE is Apache-2.0). | `main.go:38-40`, `.goreleaser.yaml:30`, `console.Options` | run | read | Console unit: `Options.Version=""` renders "development build", `"v1.2.0"` renders it; a test greps the templates for a literal version number. Pass version and license (SPDX `Apache-2.0`) through `ClusterSource.Identity()`. |
| CLU-8 | P2 | "Up 2 h 14 m" has no source: the broker records no start time. | `internal/broker/broker.go` | read | memory | Broker unit: `Identity().StartedAt` is set at `New`, and a restart resets it. |
| CLU-9 | P2 | Data-directory, per-partition and `meta.db` sizes, segment counts and index sizes have no accessor (`Segment.sizeBytes` is private; `DescribeLogDirs` is not implemented). A directory walk per page load is unbounded with many partitions (the manager comment cites 5,000 partitions at shutdown). Board says "Sizes are read from the directory, not estimated." | `segment.go:20`, `manager.go:446` | read | doc | Log unit: `Manager.Stats()` returns per-partition `{Bytes, Segments, IndexBytes, Records span}` equal to a walk of the temp dir after produce, roll, compaction and retention; bench: 5,000 partitions stat under 100 ms or cached with a TTL; console test: second request within the TTL does not walk. |
| CLU-10 | P2 | "Last fsync" per partition has no source: `Partition.Sync` stores a sequence (`syncedSeq`), not a time, and a partition not open since start has no value. Board's "idle ones stay at their last flush" is false after a restart (value would have to be "none since start"). | `partition.go:314-345` | read | memory | Log unit: after `Sync`, `Partition.LastSyncAt()` is within a second; unopened partition reports zero and the page says "not opened since start". |
| CLU-11 | P2 | Cl-04 prose is hard-coded where the data differs: "Retention is unbounded, a segment rolls at 128 MiB" is the broker default while the same board lists `retention 1 d` on a topic and Cl-07 shows a topic `segment.bytes` of 256 KiB; "Index entry every 4 KiB of log" ignores `--index-interval-bytes`; "Records 4,815 (offsets 0 to 4,814)" counts offsets, and compaction and transaction markers leave gaps (the Topics page already warns about this, `topics.html:168`). | board Cl-04 | read | doc | Console test: with a topic override and a custom `--index-interval-bytes` the sentence names the effective values; a partition with a compaction gap shows "offsets 0 to N" without "records". Sentences generated from B7. |
| CLU-12 | P2 | Cl-04 has a "Reveal ./data" action. A console route that opens the OS file manager is a local command execution reachable by any page that passes the Host guard; and in the embedded and Docker cases the console is not on the machine the user is looking at. Replace with "Copy path" (the absolute path exists in `readyInfo`). | board Cl-04; `main.go:273` | read | memory | Route-table test (B8): no cluster route executes a process. |
| CLU-13 | P2 | The Cluster page hides failures as emptiness. `openTransactions` returns nil when `ListTransactions` errors, ignores `lresp.ErrorCode`, and skips any `DescribeTransactions` row with an error; the page then says "None. No transaction is open." Broker configuration disappears silently when `DescribeConfigs` fails. With ACLs and a console user lacking `Describe` on a transactional id, open transactions are hidden and reported as none. | `backend_cluster.go:98-120`, `cluster.html:25-27` | read | doc | Console unit with a fake kafka that returns `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` / a broker error: the page shows a Denied/Failed card (A2) and never "None". |
| CLU-14 | P2 | The timeout the board describes cannot be computed from the wire. Cl-06 says "ends 14:37:16" (Began 14:36:16 + 60 s) and "The timer counts from the last state change, not from the start" in the same card. The broker aborts when `now - UpdatedMs > TimeoutMs` and every `persistTxn` (AddPartitions, AddOffsets, TxnOffsetCommit) moves `UpdatedMs`, so the real deadline is last change + timeout. `DescribeTransactions` exposes only `StartTimestamp`. The page's "open 6 s of 1m" uses start. `docs/transactions.md` also says "last changed state". Whether Kafka counts from the start is unrecorded (R1). | `txn.go:341-346`, `txnapis.go` (describe), `docs/transactions.md` "Timeouts and fencing" | read | memory | Broker unit with a fake clock: add a partition at t0 and another at t0+3 s with a 5 s timeout; assert the abort time (skipped pending R1). Console: the deadline comes from a `ClusterSource.Transactions()` field `DeadlineMs`, not from `StartTimestamp + Timeout`. |
| CLU-15 | P1 | "Abort transaction" as written ("sends the same decision as EndTxn with abort") is a weaker operation than the timeout abort. `EndTxn` abort from another connection requires the right producer id and epoch (`owns`), moves the state to `CompleteAbort` and does not raise the epoch; the live producer is not fenced, its next `AddPartitionsToTxn` begins a new transaction at the same epoch and a later commit looks successful to the application while the aborted part is gone. The timeout path (`abortFencing`) raises the epoch first. Real Kafka's tool is `kafka-transactions.sh abort --topic --partition --start-offset`, which works per partition (KIP-664, `doc`/`memory`, R2). Also "names the partitions that get a marker": a partition added but never written gets none (`AppendMarker` returns `wrote=false`), so the list must come from `OpenTransactions` per partition, not from the transaction's partition list. | `txn.go:117-130,198-209,262-300`, `log/txn.go:251` | read | memory | Broker unit: abort-by-id uses `abortFencing`, the old epoch's next `AddPartitionsToTxn` and `Produce` return `PRODUCER_FENCED`/`INVALID_PRODUCER_EPOCH`, read_committed LSO advances, markers exist only where records were written; restart variant (state `PrepareAbort` stored first, finished by `RecoverTransactions`). Console: confirm sentence equals the markers written (A6). Raw kmsg: `EndTxn(abort)` at the old epoch does not fence (documents today's behaviour; compare with R3). |
| CLU-16 | P2 | `DescribeConfigs` for a broker resource does not check the name: `"99"`, `"abc"` and `"1"` return the same list; the list is 8 keys and none of Kafka's identity keys (`broker.id`, `listeners`, `advertised.listeners`, `log.dirs`, `sasl.enabled.mechanisms`, `transaction.max.timeout.ms`, `message.max.bytes`). UIs (Kafbat, Redpanda Console) show this list. Real behaviour for an unknown id is unrecorded (R5). | `configs.go:56-62,88-99` | read | memory | Raw kmsg: broker "99" and "x" (skipped pending R5); table test that every key reported is named in the settings table (B7). |
| CLU-17 | P3 | Board Cl-02 groups `DescribeACLs · CreateACLs · DeleteACLs` (29-31) under "Security (only with SASL)". In the API table they have no condition, so they are advertised without SASL. Count "41" holds only with SASL and telemetry on; default is 39 (`run`). The board's "of which 2 only because telemetry, 2 only because SASL" is right; the grouping is wrong. | `versions.go:106,121` (only two rows carry `WhenSASL`) | run | read | Console unit: the API page groups, counts and conditions are computed from `protocol.APIs` for each of the four profiles and each SASL/telemetry combination (golden). |
| CLU-18 | P3 | Cl-02 says an unimplemented API or out-of-range version "gets no response and the connection is closed". The router returns `ErrNotAdvertised` (`router.go:42`); what the connection does is in `conn.go`. No test in `internal/protocol` or `cmd/doze-kafka` pins "closed, not hung" for an unadvertised key and for a version above the range of an advertised one. `limitations.md` promises a clean close. | `router.go:38-45` | read | doc | Raw kmsg (`protocol/apis_test.go`): DescribeLogDirs key 35, OffsetForLeaderEpoch 23, and Fetch v14 each get EOF within 1 s; ApiVersions v4 gets `UNSUPPORTED_VERSION` with the list. |
| CLU-19 | P3 | Board Cl-07 quotes `INVALID_RECORD "Compacted topic cannot accept message without key in topic partition harbour.depot.dispatch-0."` as the broker's message. The broker sends `log: a compacted topic cannot accept a message without a key` (no partition, with a Go package prefix). Kafka's text is unrecorded (R7). | `log/compact.go:40`, `produce.go:174-179` | read | memory | Raw kmsg produce keyless to a compacted topic: error code `INVALID_RECORD`, message has no `log:` prefix and names the partition (message text pending R7). |
| CLU-20 | P2 | `ListTransactions` ignores unknown state filters. The response struct has `UnknownStateFilters` (KIP-664 / `spec`); a request with `"Bogus"` returns an empty list and no error. `kafka-transactions list` shows states the user typed wrongly as "no transactions". | `txnapis.go:318-345`, `kmsg/generated.go:46039` | read | spec | Raw kmsg: `StateFilters=["Bogus","Ongoing"]` returns `UnknownStateFilters=["Bogus"]`. |
| CLU-21 | P2 | Transaction records never expire. Every distinct `transactional.id` ever used stays in the `txnManager` map and in `meta.db` for ever (no `transactional.id.expiration.ms`, Kafka's default 7 days, `doc`/R11). Kafka Streams and test suites use one id per task or per run, so the "all states" list and the map grow without bound; `ListTransactions` with no filter returns all of them, unpaged. | `txn.go:64-90`, `meta/txns.go` | read | doc | Broker unit with a fake clock: an `Empty`/`Complete*` entry older than the expiry is removed from map and store and `DescribeTransactions` answers `TRANSACTIONAL_ID_NOT_FOUND`; an `Ongoing` one never expires by this rule; restart variant. |
| CLU-22 | P2 | The board's "Who is affected: g-order-validator ... only while this is open, if it uses read_committed" cannot be known from the broker: a group's isolation level is a per-Fetch field, not group state. The sentence needs the traffic recorder to have seen a Fetch with `isolation_level=1` from the group's members. | board Cl-06; `internal/traffic` | read | spec | Traffic unit: recorder keeps the last isolation level per client and topic (traffic stage); console test: with no Fetch seen the sentence says "any consumer using read_committed". |
| CLU-23 | P3 | Board texts about transactions that differ from the code: Cl-05 "A producer appears from its first write" (it appears at `AddPartitionsToTxn`/`AddOffsetsToTxn`, before any record; a transaction with partitions added and nothing written holds nobody back); Cl-05 "has held 2 partitions" is the added list, not the partitions with records; Cl-06 "After a restart ... Its timeout starts again from the restart" is true (`RecoverTransactions` stamps `UpdatedMs`) and so is "A stored commit decision is carried out". | `txnapis.go:118`, `txn.go:303-325` | read | doc | Console test with a fake: partition added, no records: the card reads "added, nothing written" and LSO = end. |
| CLU-24 | P2 | Compaction status is not exposed and is mis-documented. `Partition.LastClean()` is the only accessor, it has no non-test caller, and its comment says "the most recent pass that did anything or looked"; the code sets `lastClean` only after a pass that rewrote at least one segment (`compact.go:329-333`; every early return leaves it zero). After a restart it is zero although the `cleaner-offset` file says the log was cleaned (so the board's "never" would be false). Not available: dirty ratio, tombstones held/dropped, "cleaning now" and "98 so far", sealed count. | `compact.go:144,222-346` | read | doc | Log unit: `Partition.CleanerStatus()` returns `{CleanedTo, Sealed, DirtyBytes, DirtyRatio, Running, LastRun, Removed, TombstonesHeld}`; equals the values `Compact` computes (shared helper, not a copy); a no-op pass records `LastLooked`; a restart restores `CleanedTo` and shows "cleaned before this run". |
| CLU-25 | P3 | Compaction runs only on open partitions (`manager.snapshot()` in `EnforceCompaction`; the once-a-minute closed sweep does retention only). The board's "4 partitions are compacted ... cleans as soon as one has rolled" is true only for partitions in use; `limitations.md` and `storage-and-durability.md` say so. The status needs a "not opened since start" state. | `manager.go:508-531,533-570` | read | doc | Log unit: a compacted partition never opened reports `Opened=false`; opening it triggers a pass (existing claim in `limitations.md`; test is missing). |
| CLU-26 | P2 | Statement of limits (Cl-03) is a second copy of `docs/limitations.md`, and both are stale. (a) "Topic configs: Five act ... The rest are validated, stored and reported, and change nothing" (board and `limitations.md:37`) is false: `topiclimits.go:94-110` also applies `cleanup.policy=compact`, `min.cleanable.dirty.ratio`, `min.compaction.lag.ms`, `max.compaction.lag.ms`, `delete.retention.ms` and `compression.type`; Cl-07 itself relies on them. (b) Limits that exist in code and appear in neither: `--max-partitions` 1000 per topic (Kafka has no such limit; documented only in `configuration.md:113`), the 16 MiB idempotence window is in `limitations.md` but not on the board, the telemetry window is memory-only. (c) The board says "The console adds nothing to it and removes nothing from it" while it omits (b). | board Cl-03; `limitations.md:37-44`; `topiclimits.go`; `config.go:135-140` | read | doc | Console test: the limits page is rendered from `docs/limitations.md` (embedded, one source), asserts every `##`/bullet appears, and a code test enumerates the topic configs the broker acts on (`TopicLimits`, `compactionPolicy`) and fails if `limitations.md` names a different set. |
| CLU-27 | P2 | Config provenance does not exist. `loadConfig` applies the file then the flags with no record of which key came from where; the board's source column and "2 from a flag, 3 from file, 11 are defaults" cannot be produced. The board's 16 settings are a hand-picked subset of ~40 flags (`cli.go:300-420`) and have no settings record yet (B7, A3). | `cli.go:195-300`, `config/file.go:65-120` | read | doc | Config unit: `Sources` records `flag`/`file`/`default` per key for a config file plus `--data-dir`; `WriteTOML` omits nothing the table lists; a test fails for a flag with no settings record (A3). |
| CLU-28 | P2 | The console has no access to the broker's own listener, authentication or zone facts. `ClusterInfo.AuthMech`/`TLS` are the console's dial options (`backend_cluster.go:60-66`), not the broker's SASL mechanisms (board: "SCRAM-SHA-512 · PLAIN off · SCRAM-SHA-256 off", "5 users", "super user"). In the embedded topology the console dials a unix socket and shows `DisplayAddr`; there is nothing to snapshot. Needs an optional `ClusterSource` (nil = pages Listeners, Names, Config and Storage say "not available in this embedding"). | `console.Options`, `backend_cluster.go` | read | doc | Console test with `ClusterSource=nil`: each page renders the "not available" card; `server/` embedding test passes without it. |
| CLU-29 | P3 | Cl-09 Docker table: "In a container, default: Metadata says 127.0.0.1:9092 -> hangs on next request". A container that bootstraps to `host.docker.internal:9092` and is told `127.0.0.1:9092` gets connection refused on its own loopback, and clients retry until their timeout, which reads as a hang. The text should say refused, then timeout. Also the generated "Compose file for it" must use the page's actual `--advertised-host`. Table values must come from the same `listenersSnapshot` as CLU-3. | board Cl-09 | read | memory | `compose.spec.ts` exists; add: the generated compose snippet is run by the compat harness against `--listen :9092 --advertised-host host.docker.internal`. |
| CLU-30 | P3 | Cl-01 shows `AlterConfigs on broker 0` as "Refused" (grey-purple means "real Kafka would say no too"). Kafka (KRaft) supports dynamic broker configs, so this is "Not implemented" (A2). The code uses the generic `INVALID_REQUEST`. The error names node 0. | `alterconfigs.go:25,104`, board Cl-01 | read | memory | Raw kmsg: AlterConfigs and IncrementalAlterConfigs on broker "1" return `INVALID_REQUEST` with the read-only message (pins today's answer); console: the card kind is "Not implemented". R8 decides whether the code should change. |
| CLU-31 | P3 | Cl-02 names only "OffsetForLeaderEpoch, DescribeLogDirs, quota APIs" as not implemented; `limitations.md` names two. Other APIs a UI probes (`DescribeUserScramCredentials`, `DescribeClientQuotas`, `ElectLeaders`, `ListPartitionReassignments`, `DescribeLogDirs`) are absent from both. "Quota APIs" in the board is true by omission only. | `limitations.md:46-48` | read | doc | Table test: every Kafka key in `kmsg` not in `protocol.APIs` is listed by the page with a one-line reason, generated. |
| CLU-32 | P3 | Cl-Home and the wire dock on every Cluster board show "what this page asked the broker (the console is excluded from Traffic)". The recorder drops the console's requests by client id prefix (`traffic.go:126,168,282`), so a dock built on it would be empty. Needs a per-request marker, not an include of all console traffic. | `internal/traffic/traffic.go` | read | doc | Traffic unit (traffic stage): a request carrying the page's correlation header/clientID suffix is kept in a side ring, absent from Traffic lists. |
| CLU-33 | P3 | The existing `cluster.html` says "Every API the broker does not implement closes the connection cleanly on that call" and links `limitations.md` on GitHub `main`; fine, but the chip list shows `InitProducerID` where Kafka calls it `InitProducerId` (name from `kmsg.NameForKey`) and times as `1m0s` (`Duration.String`), where the board writes "1 m". Chip name differs from every Kafka document. | `backend_cluster.go:77-83,113` | run | read | Console unit: names equal the key names in `docs/api-support.md`; durations use the shared formatter (A9). |
| CLU-34 | P2 | No test for `RunTransactions` abort of a transaction with a stored consumer-offset set or a restart in `PrepareAbort` with a deleted topic path through the console's view; `handleAddOffsetsToTxn` is 46.2%, `handleEndTxn` 60.0%, `initTxnProducer` 61.5%, `groupFence` 60.0% (partial run). Untested behaviours: `AddOffsetsToTxn` with an invalid or unauthorised group, `InvalidTransactionTimeout` (`timeoutMs <= 0` or above 15 min), epoch exhaustion (`MaxInt16-1` new producer id), `ConcurrentTransactions` during prepare, `EndTxn` repeat in each Complete state. | `txn.go:152-195`, `txnapis.go:136-170,273-280` | read | spec | Broker unit/raw kmsg, one per bullet in the finding (name them `TestInitProducerIdTimeoutBounds`, `TestEpochExhaustionGetsANewProducerId`, `TestAddOffsetsToTxnRefusals`, `TestEndTxnRepeatsPerState`). Mutation (B3) on `owns` and `abortFencing`. |
| CLU-35 | P3 | `ListTransactions` and `DescribeTransactions` take unbounded id and filter lists from the network (`for _, id := range dr.TransactionalIDs`), each with a lock round trip. Bounded by the 100 MiB frame cap only. | `txnapis.go:282-346` | read | spec | Fuzz/raw: 100,000 ids answered in under 1 s without allocating beyond a bound; page `ListTransactions` after CLU-21. |
| CLU-36 | P3 | `docs/transactions.md` "Timeouts and fencing" states the last-change rule as Kafka's ("counted from when the transaction last changed state"); unverified (R1). `docs/limitations.md` has no entry for "transaction ids never expire" (CLU-21) or the cleaner running only on open partitions in the Limits list. | docs | read | doc | Doc check in the stage's acceptance box; recording R1 moves the sentence to `recorded`. |
| CLU-37 | P2 | `doze-kafka doctor` and the Names board must report the same facts, and today `doctor` prints `answers on kafka.<name>:9092` even when the name does not resolve here and the broker bound/advertises the address (the banner and `env` handle this via `resolvesHere`; `doctorTo` does not). | `doctor.go:46-52`, `listen.go:219-250` | read | doc | cmd: `doctorTo` with a name that resolves elsewhere prints the address; one shared function feeds doctor, env, the banner and the console snapshot. |
| CLU-38 | P3 | No e2e spec reads a transaction row, the API table for each profile, the zone cards, or the abort confirm; the one cluster spec asserts four strings. | `e2e/tests/smoke.spec.ts:19` | read | doc | See section 7. |

Not found: a bound on any of the cluster pages' inputs (they take only `profile` and path ids; both are validated in the new handlers); no goroutine or fd leak on the txn path (`RunTransactions` stops on `ctx`, the ticker is deferred-stopped); no lock-order inversion in `txn.go` (entry, then idempotency, then partition, as the header says). I looked by reading every lock site in `txn.go`/`txnapis.go` and running `go test -race` on the broker transaction tests as part of `task check`.

## 5. What to check

- A `transaction.timeout.ms` of 1 ms, 15 min and 15 min + 1 ms on `InitProducerId` (v0 to v4), and a negative value.
- Two producers with one `transactional.id`, the first mid-transaction: second `InitProducerId` aborts, raises the epoch, first gets `PRODUCER_FENCED` on `AddPartitionsToTxn` and `INVALID_PRODUCER_EPOCH` on `Produce`.
- Abort while a `read_committed` consumer is blocked at the LSO: it moves on, and the consumer sees nothing from the aborted batch.
- Restart in `Ongoing`, `PrepareCommit`, `PrepareAbort`; SIGKILL variant of each.
- `ListTransactions` with each state filter, with `ProducerIdFilters`, with an unauthorised id (omitted, not an error).
- The page under `--listen 127.0.0.1:0`, a non-default prefix, a data dir with a space, SASL off/on, TLS off/on, and the zone on/off (A8).
- The page with the broker down (A7), with `ClusterSource=nil` (embedding), with an empty data dir.
- Cleaner: keyless produce refused on both group of settings; tombstone survives one pass and goes after `delete.retention.ms`; open transaction untouched; restart mid-swap rebuilds.
- Names: every claim in Cl-08 checked against `doze-names` (`Qualified`, `Ingress`, `Lease.Route`, `Disabled`). Verified: `kafka.<instance>.doze` and `schema.<instance>.doze` are real qualified names (`doze-names/names.go:104`, `registry.go:65`); the front door is one shared HTTP ingress selecting by Host (`ServeIngress`); `DOZE_ZONE=off|0|false|no` disables (`names.go:186-198`); the console Host guard allows localhost, IP literals, `*.doze` and `--console-allow-host`; not verified here and left to the doze-names owners: "one small HTTP listener on port 80", "a dynamic range hashed per name", the macOS daemon translation. The board must label those as doze-names' claims.
- Windows: `go vet` for windows passes; the zone cards say "names are not available on Windows" (config default `127.0.0.1:9092`, `config.go:148`).

## 6. Console

Pieces from the foundation (`A1` inspector, `A2` refusals, `A3` teaching lines, `A4` wire dock, `A5` neighbours, `A6` confirms, `A7` broker-down state) are used by name. New shared piece owned here, **C3 `ClusterSource`**: an optional interface in `console.Options`, implemented in `cmd/doze-kafka`, nil in embeddings:

- `Identity()`: version, license, kafka major, started-at, node id, cluster id.
- `ConfigSnapshot()`: effective settings with provenance, secrets redacted by construction.
- `Listeners()`: bound/advertised address and protocol per listener, SASL mechanisms, user count, super users, TLS state.
- `Zone()`: instance name, claimed names, resolves-here per name, front-door state, doctor findings.
- `Storage()`: per-partition bytes, segments, index bytes, offsets span, last sync, plus `meta.db` size.
- `Cleaner()`: per-partition `CleanerStatus`.
- `Transactions()`: rows with `DeadlineMs`; `AbortTransaction(id)`.

| Board | Template / handler | Status | Built from |
|---|---|---|---|
| Cl-Home | `cluster_home` in `cluster.html` / `c.cluster` | needs backend | Wire: DescribeCluster, Metadata, ListGroups, ListTransactions. `Identity()` for version, uptime, cluster id. `Storage()` total. Wire dock needs CLU-32. **As built before this stage** (evidence run, foundation "Deviations from the boards"): the page counts "Compacted partitions" from each topic's `cleanup.policy` (the cleaner's own status stays CLU-24) and draws "What this broker is not" from DescribeCluster (one broker) and `docs/limitations.md`; **the wire dock is CUT and assigned here (CLU-32)**: the recorder drops the console's own requests (`isConsole`), so it has nothing to show. |
| Cl-01 Config | `cluster_config` / `c.clusterConfig` | needs backend | `ConfigSnapshot()` with provenance (CLU-27), redaction (CLU-4); refusal card from a live `AlterConfigs` attempt (A2, "Not implemented"). Teaching lines from B7. |
| Cl-02 Apis | `cluster_apis` / `c.clusterAPIs` | buildable | `protocol.APIs` filtered by profile, SASL, telemetry (CLU-17); live `ApiVersions` for "this broker". |
| Cl-03 Limits | `cluster_limits` / `c.clusterLimits` | buildable | embedded `docs/limitations.md` as the single source (CLU-26). The board's headline sentence and the three groups map to the three `##` sections. |
| Cl-04 Storage | `cluster_storage` / `c.clusterStorage`; inspector for a partition | needs backend | `Storage()` (CLU-9, CLU-10); Topics config via `DescribeConfigs`; "Records" via `ListOffsets` with the gap caveat (CLU-11); no Reveal (CLU-12). |
| Cl-05 Transactions | `cluster_txns` / `c.clusterTxns` | buildable | ListTransactions + DescribeTransactions (exists in small form); failures shown, not hidden (CLU-13); "From first write" wording (CLU-23). |
| Cl-06 Txn-Detail | inspector `txn_detail` / `c.clusterTxn`, `c.clusterTxnAbort` | needs backend | DescribeProducers (first), ListOffsets read_committed (LSO) real; deadline (CLU-14), abort (CLU-15), "who is affected" (CLU-22) new. |
| Cl-07 Compaction | `cluster_compaction` / `c.clusterCompaction` | needs backend | `Cleaner()` (CLU-24, CLU-25). "Wire" strip from the [traffic](traffic.md) recorder. |
| Cl-08 Zone | `cluster_names` / `c.clusterNames` | needs backend | `Zone()`; text fixed per CLU-1, CLU-2, CLU-3, CLU-37; "Copy dns-setup command" is static text. |
| Cl-09 Listeners | `cluster_listeners` / `c.clusterListeners` | needs backend | `Listeners()` (CLU-3, CLU-28, CLU-29); Docker table from the same snapshot. |

Board count: 10 boards. `real` 0, `buildable` 3 (Cl-02, Cl-03, Cl-05), `needs backend` 7 (Cl-Home, Cl-01, Cl-04, Cl-06, Cl-07, Cl-08, Cl-09). The current single page is a smaller `real` version of Cl-Home, Cl-02 and Cl-05.

**Teaching lines.** Cl-01 (unit, default, "applied or only stored" per setting) comes from the B7 settings table; Cl-04's retention/segment sentences and Cl-07's cleaner sentences ("min.cleanable.dirty.ratio is 0 here, Kafka's is 0.5") come from `topiclimits.go` constants exposed as data, not typed into the template. The `0.5` is `doc` and goes on the queue (R12).

**Abort action.** Menu item under "...", confirm names the verb, the id, the partitions where a marker will be written (computed, CLU-15), says the producer will be fenced and its epoch raised, focuses Cancel; the POST returns the new state; denied/failed use A2. Hidden if `ClusterSource` is nil. Logged at `Info` with the transactional id.

## 7. Tests to add

Unit/store
- `meta`: `TestClusterIDIsStable`, `TestTxnRecordExpiry`.
- `log`: `TestManagerStatsMatchesDisk` (after produce, roll, compact, retention), `TestLastSyncAt`, `TestCleanerStatusMatchesCompact`, `TestCleanerStatusSurvivesRestart`, `TestUnopenedPartitionReportsNotOpened`, `TestRequireKeys`, `TestCleanerReadFailed`, `TestSetClearTopicLimits`.
- `config`: `TestSourcesProvenance`, `TestWriteTOMLRedacts`, `TestInstanceName`, `TestDeriveName` (table), `TestResolveConsoleAddr`, `TestResolveMetaDB`, `TestValidateTable` (every error branch).
- `broker`: `TestAbortByIDFencesProducer`, `TestAbortMarkersOnlyWhereWritten`, `TestAbortRestartFinishes`, `TestTxnTimeoutFromLastChange` (skipped pending R1), `TestTxnIDExpiry`, plus the four in CLU-34.

Raw kmsg
- `ListTransactions` unknown filters; `DescribeConfigs` broker "99" (skipped pending R5); AlterConfigs/IncrementalAlterConfigs on broker; unadvertised key and out-of-range version close within 1 s; `ApiVersions` v4; keyless produce on compacted topic; EndTxn abort at old epoch (documents CLU-15).

Client library
- franz-go: producer in a transaction, console-style abort-by-id, producer's next call returns `ErrProducerFenced`; Sarama transactional producer likewise; a Java client in `compat/` does the same with `transactional.id`.

Compat cells
- Existing Kafbat UI and Redpanda Console cells assert the cluster, broker config and log-dir calls still behave (and `DescribeLogDirs` if C-optional is built). New cell: `kafka-transactions.sh list` and `describe` against doze (Java, plain and SCRAM/TLS), asserting state, producer id, epoch, start offset.

Fuzz
- `FuzzListTransactionsRequest` and `FuzzDescribeTransactionsRequest` through the existing request-decode corpus; `FuzzConfigFile` (TOML load) if absent from the seven targets.

Browser (e2e)
- `cluster.spec.ts` (new), one test per board: overview sentence matches Metadata; config table with a redaction check on a broker with a credential; API page for each profile and counts 37/39/41; storage sizes within 10% of `du`; transaction list with a live open transaction started by a test producer, detail, abort with confirm, producer fenced; compaction table after a roll; zone and listener cards at name mode and at `--listen`; broker-down overlay; 960 and 390 widths; keyboard `j k Enter / r`.

Differential vs a real broker (apache/kafka:4.3.1)
- R1, R2, R3, R4, R5, R6, R7, R8, R10, R11, R13 below; each becomes a `conformance/` scenario when B2 exists.

Coverage targets: per section 3.1, with functions named there.

## 8. Recording queue

| # | Claim (`doc`/`memory` today) | Scenario that would record it |
|---|---|---|
| R1 | Kafka counts `transaction.timeout.ms` from the transaction start, not the last state change. | InitProducerId timeout 5 s; AddPartitionsToTxn at t0 and t0+3 s; poll DescribeTransactions/ListTransactions and a final AddPartitions every 500 ms; record the time of the abort and the error. |
| R2 | `kafka-transactions.sh abort --topic --partition --start-offset` writes a marker per partition via WriteTxnMarkers and requires ClusterAction; whether the producer epoch is raised. | Run the tool against a hanging transaction; record DescribeTransactions before and after, the producer's next AddPartitionsToTxn, and ListTransactions. |
| R3 | `EndTxn(abort)` at the current epoch from a second connection leaves the first producer able to begin a new transaction. | Two connections, same pid/epoch; abort from B; A sends AddPartitionsToTxn and Produce. |
| R4 | Unknown `StateFilters` are returned in `UnknownStateFilters`; `DescribeTransactions` of an unknown id is `TRANSACTIONAL_ID_NOT_FOUND`. | ListTransactions with `["Bogus"]`; DescribeTransactions `["nope"]`. |
| R5 | `DescribeConfigs` for broker "99" and for a non-numeric name; the key set and `ConfigSource` of a KRaft broker. | DescribeConfigs for brokers "", "1", "99", "x" with no names; record keys and sources. |
| R6 | A cluster id is a 22-character base64 UUID. | Metadata and DescribeCluster `ClusterID` from `apache/kafka:4.3.1`. |
| R7 | The message text and per-record error for a keyless record on a compacted topic; whether the whole batch is rejected. | Produce one keyless and one keyed record in one batch. |
| R8 | AlterConfigs/IncrementalAlterConfigs on a broker resource, including a static key and an unknown broker id. | Same requests against node 1. |
| R9 | A request for an unsupported version of a non-ApiVersions API closes the connection. | Fetch v14, Produce v2 against `apache/kafka:4.3.1` (read EOF vs error response). |
| R10 | `DescribeProducers` `CurrentTxnStartOffset` and `CoordinatorEpoch` values; `StartTimestamp` after completion. | Open, commit, describe at each stage. |
| R11 | Transactional ids expire after 7 days (`transactional.id.expiration.ms`) and then disappear from ListTransactions. | Start a broker with `transactional.id.expiration.ms=2000`, finish a transaction, list at 1 s, 3 s. |
| R12 | `min.cleanable.dirty.ratio` default 0.5; `delete.retention.ms` default 24 h; tombstones are kept until segment cleaned once and older than that. | `DescribeConfigs` of a compacted topic; produce key/tombstone; observe fetch after cleaning. |
| R13 | `DescribeLogDirs` response shape: size per partition, `isFuture`, offset lag; sizes after compaction. | `kafka-log-dirs.sh --describe`. |

## 9. Owed to integration

- [traffic](traffic.md): a per-page request marker for the dock (CLU-32); the last isolation level seen per client and topic (CLU-22); connected-client count for the overview.
- [topics](topics.md): per-topic retention and segment size for the storage rows; topic delete must remove the partition from `Storage()` and `Cleaner()`.
- [groups](groups.md): the "who is affected" sentence uses group membership and the committed offsets.
- [access](access.md): SASL user count and super user for Cl-09 come from the same config snapshot; the console user's rights decide whether open transactions are visible (CLU-13).
- [connect](connect.md): Cn-06 (Docker) and Cn-07 (SASL/TLS) use `Listeners()`; one function, one test.
- [bad days](bad-days.md): Bad-Disk and Bad-Fenced reuse `Storage()` and the abort path.
- [map](map.md): none.

**Asks of the foundation**
- F1: correct A8's `--zone=false` (no such flag; use `--listen`) and the "zone on/off" run in A8.
- F2: add `ClusterSource` (C3) as an `Options` hook beside `Traffic` and `ClientMetrics`.
- F3: B7 settings table must hold the 16 settings Cl-01 shows plus the cleaner settings of Cl-07; add a "from" (provenance) column.
- F4: a B8 rule that no route spawns a process (CLU-12).
- F5: a lightness budget entry for the storage stats cache (CLU-9).

## 10. Acceptance criteria

Common criteria in [01-protocol.md](../01-protocol.md) apply. This stage adds:

- [ ] `go test ./console -run Cluster` passes with a fake `ClusterSource` and with `nil`.
- [ ] Coverage (`go tool task coverage`): `cmd/doze-kafka` >= 84%, `console` >= 90%, `internal/broker` >= 90% and `txn.go`, `txnapis.go` each >= 92%, `internal/config` >= 75%, `internal/storage/log` >= 84%, `internal/storage/meta` >= 82%, `internal/protocol` >= 73%.
- [ ] `go tool task mutate:check -- ./internal/broker/... ./internal/storage/log/... ./internal/config/...` has no new survivor in `owns`, `abortFencing`, `finishTxn`, `endTxn`, `Compact`, `Validate`, `WriteTOML`.
- [ ] `go test ./internal/broker -run 'TestAbortByID|TestTxnIDExpiry|TestInitProducerIdTimeoutBounds|TestEpochExhaustion|TestAddOffsetsToTxnRefusals|TestEndTxnRepeatsPerState'` passes under `-race`, and each fails with its fix reverted.
- [ ] `go test ./internal/protocol -run 'TestUnadvertised|TestApiVersionsAboveRange'` passes.
- [ ] `go test ./internal/config -run 'Sources|Redact'` passes; a test fails for a flag with no settings record.
- [ ] `go test ./internal/storage/log -run 'Stats|CleanerStatus|LastSync'` passes.
- [ ] Console text: no template contains the strings `kafka.doze` (apex), `--zone`, `0.9.2`, `doze-7Hk2`; a grep test enforces it.
- [ ] `/cluster/config.toml` never contains a configured password (test with a credential).
- [ ] The Limits page is rendered from `docs/limitations.md`; `limitations.md` names the ten topic configs that act, `--max-partitions`, and transaction-id expiry.
- [ ] `cluster.spec.ts` passes with the route gate on, at 960 and 390 px, with the accessibility sweep, and with the broker stopped.
- [ ] `go tool task test:compat` cells: Kafbat UI, Redpanda Console, the new `kafka-transactions` cell (plain and SCRAM/TLS) pass.
- [ ] `go tool task fuzz` runs the two new transaction request targets clean.
- [ ] `docs/api-support.md` (ACL rows noted "always advertised"; DescribeLogDirs/WriteTxnMarkers rows per decision), `docs/limitations.md`, `docs/transactions.md` (timeout sentence), `docs/console.md` Cluster section, `CHANGELOG.md` updated.
- [ ] Recording queue: R1 to R13 each `recorded` or listed with the reason; the count still at `memory`/`doc` is written here: 13 today.
- [ ] Boards implemented: Cl-Home, Cl-01 to Cl-09 (10 of 10), or the dropped ones named (Reveal action dropped, CLU-12).
- [ ] Findings CLU-1 to CLU-38 closed or moved with a reason; `plan/README.md` status row updated; actual time recorded.

## 11. Files and order of work

Half-day units (hd). **Overall: L, about 14 days (28 hd).**

| # | Work | Files | hd |
|---|---|---|---|
| 1 | Red tests for CLU-4, 6, 13, 15, 17, 18, 20, 21, 24 (the ones that fail today) | broker, config, protocol, console tests | 3 |
| 2 | Redaction and provenance: `config.Sources`, `WriteTOML` redaction, settings records for the 16 + cleaner settings (F3) | `internal/config`, `cmd/doze-kafka/cli.go` | 3 |
| 3 | `ClusterSource` interface, `Options` hook, implementation in `cmd/doze-kafka` (identity, version, started-at, listeners, zone) with one shared address function for doctor/env/banner/console (CLU-3, 28, 37) | `console/console.go`, `cmd/doze-kafka/listen.go`, `names.go`, `doctor.go` | 3 |
| 4 | Persisted cluster id (CLU-5), version and license plumbing (CLU-7), start time (CLU-8) | `meta`, `broker.go`, `main.go` | 1.5 |
| 5 | Storage stats and last-sync (CLU-9, 10) with a TTL cache; bench for 5,000 partitions | `internal/storage/log`, `cmd/doze-kafka` | 3 |
| 6 | Cleaner status (CLU-24, 25), shared dirty-ratio helper | `internal/storage/log/compact.go` | 2.5 |
| 7 | Transaction work: `ListTransactions` conformance (CLU-20), id expiry (CLU-21), deadline field (CLU-14), abort-by-id with fencing (CLU-15), remaining tests (CLU-34) | `broker/txn.go`, `txnapis.go`, `meta/txns.go` | 4 |
| 8 | Console: Cl-02, Cl-03 (rendered from `limitations.md`), Cl-05, failure states (CLU-13) | `console/backend_cluster.go`, `handlers_cluster.go`, `cluster.html` | 3 |
| 9 | Console: Cl-Home, Cl-01, Cl-04, Cl-07, Cl-08, Cl-09 | same | 4 |
| 10 | Console: Cl-06 and Abort with confirm (A6) | same | 1.5 |
| 11 | Docs and board corrections (CLU-1, 2, 11, 17, 19, 23, 26, 29, 30, 31, 33, 36), foundation asks F1-F5 | `docs/*`, design notes | 1.5 |
| 12 | e2e `cluster.spec.ts`, compat cell for `kafka-transactions`, fuzz targets, mutation baseline | `e2e`, `compat`, `Taskfile.yml` | 3 |
| 13 | Recordings R1-R13 once B2 exists; unskip CLU-14, 16, 19 and the R-dependent tests; decide `DescribeLogDirs` and `WriteTxnMarkers` | `conformance/` | 3 |

Order: 1, 2, 3, 4 first (the boards cannot lie once these exist); 5, 6, 7 in parallel; then 8 to 10; 11 to 13 close. Optional, not in the 28 hd: `DescribeLogDirs` (35) about 2 hd after R13, `WriteTxnMarkers` (27) about 4 hd plus a Java cell for `kafka-transactions.sh abort`.

**Prior art.**
- Apache Kafka: `kafka-transactions.sh` (`list`, `describe`, `abort`, `find-hanging`), added by KIP-664 (`doc`/`memory`, R2); `kafka-log-dirs.sh` over DescribeLogDirs for sizes; `kafka-configs.sh --describe --entity-type brokers`; `transactional.id.expiration.ms` for id expiry (`doc`).
- Redpanda Console and Kafbat UI both have compat cells here; whether each calls DescribeLogDirs is `memory` and is checked from the compat logs before deciding on key 35.
- Within this repo: `Traffic` and `ClientMetrics` are already in-process hooks on `console.Options`, which is the shape `ClusterSource` follows; `doze-names` owns the zone, and its claims on Cl-08 stay doze-names' to verify.

## Foundation asks this stage builds

From [00-foundation.md section E](../00-foundation.md#e-asks-from-the-stages): asks stage 0 did not build because this stage is their only consumer.

- E10: the storage-stats cache the Cluster page reads, inside the idle-heap budget.
- E18: a `ClusterSource` hook in the console `Options` beside `Traffic` and `ClientMetrics`.
- E14: the error strings quoted in `console/templates` held to the code, as `docs/errors_test.go` holds the documents'.
