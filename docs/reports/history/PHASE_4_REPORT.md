# Phase 4 report — Consumer groups (classic)

**Goal:** `FindCoordinator`, `JoinGroup`/`SyncGroup`/`Heartbeat`/
`LeaveGroup`, `OffsetCommit`/`OffsetFetch`. Gate: a franz-go group consumer joins,
gets an assignment, consumes, commits, and resumes from the committed offset after a
restart; two consumers in one group split partitions.

## The shape

In the classic protocol the broker is a **rendezvous point, not an assignor** — the
elected client leader runs the assignor and hands the result back as opaque bytes
(see [the consumer-group docs](../../consumer-groups.md)). So the work is a
correct state machine + timers that shuttle bytes, not partition math.

- **Coordinator state machine** (`broker/group.go`): the `groupManager` owns all
  in-memory group state — membership, generation, elected leader, negotiated
  protocol, and the deferred-response channels. Lifecycle is
  `Empty → PreparingRebalance → CompletingRebalance → Stable`. State transitions all
  happen under one mutex; `JoinGroup`/`SyncGroup` handlers block on a per-request
  channel *after* releasing the lock, and completions/timer callbacks deliver on
  those channels via buffered non-blocking sends. (The design sketches an actor
  goroutine; a single mutex + deferred channels is the equivalent and is what
  shipped.)
- **Deferred join/sync**: `JoinGroup` parks until every known member re-joins (or the
  `joinCollectTimeout` backstop fires), then the whole cohort is released at once with
  a bumped generation; only the leader's response carries every member's subscription
  metadata. `SyncGroup` parks followers until the leader uploads assignments, then
  fans each member its own slice.
- **The two-step join (KIP-394)**: a first-contact member (empty member id) is
  answered `MEMBER_ID_REQUIRED` with a freshly generated id and must retry — without
  it franz-go's join loop misbehaves and groups grow unbounded from clients that
  vanish mid-handshake.
- **Timers**: a per-member session timer (reset on every join/sync/heartbeat) evicts
  the dead and triggers a rebalance; a join-collect timer backstops a member that
  never rejoins. A single consumer completes its join **instantly** (it's the only
  member, so the cohort is complete on arrival) — no `group.initial.rebalance.delay`
  stall.
- **Generation fencing**: `Heartbeat`, `SyncGroup`, and `OffsetCommit` all carry the
  generation; a stale one is rejected (`ILLEGAL_GENERATION` /
  `REBALANCE_IN_PROGRESS`), forcing rejoin. `OffsetCommit` fencing lets a group-less
  simple consumer (empty group / negative generation) through.

## Handlers

- **`FindCoordinator`** (`broker/groups.go`): always returns **self**. Answers both
  the v0–v3 single-key form and the v4+ batched `CoordinatorKeys` form.
- **`JoinGroup` / `SyncGroup` / `Heartbeat` / `LeaveGroup`** (`broker/groups.go`):
  thin wire layer — decode, drive the `groupManager`, encode. `LeaveGroup` fills the
  top-level error (v0–v2) *and* the per-member list (v3+).
- **`OffsetCommit` / `OffsetFetch`** (`broker/offsets.go`): the **only** durable group
  state. Commit writes `(group, topic, partition) → (offset, metadata)` to bbolt,
  fenced by generation/member. Fetch reads them back, returning **-1** for any
  uncommitted partition so the client applies its `auto.offset.reset`. Both the
  v0–v7 single-group form and the v8+ batched `Groups` form are handled; a nil topic
  list means "all committed offsets for the group".
- **`meta.ListOffsets(group)`** (`storage/meta/store.go`): new store method backing
  the fetch-all path — scans the `group\0…` key prefix and splits each key back into
  `(topic, partition)`.
- **Profiles advertise** the classic group set across majors 1–4
  (`protocol/versions.go`). Maxes are capped just below where the wire form switches
  to topic-IDs (`OffsetCommit`/`OffsetFetch` v10) — the handlers key on topic names.

## Acceptance

- **`go test -race ./...` PASS** (group state machine is race-clean).
- **`TestPhase4_GroupConsumeCommitAndResume`**: a franz-go group consumer joins,
  consumes 20 records, and commits; the broker is **restarted over the same data
  dir**; a fresh consumer in the same group **resumes from the committed offset** —
  it sees only the 10 records produced after the commit, with **no replay** of the
  first batch.
- **`TestPhase4_TwoConsumersSplitPartitions`**: two members of one group each get a
  partition of a 2-partition topic (verified via `OnPartitionsAssigned` before any
  data is produced), and together consume every record; each member does real work.
- Repeated `-count=5` runs are stable (no rebalance-timing flakiness).

## Deviations / notes

- **Single mutex, not an actor goroutine.** The design doc models each group as a
  goroutine fed by a command channel; the implementation serializes on one mutex with
  deferred-response channels. Same guarantees, less machinery. Documented in
  `group.go`.
- **Static membership (KIP-345)** is deferred — the `instanceID` field is threaded
  through (join, member info, generated ids) but a disconnect still rebalances rather
  than reclaiming the slot within the session timeout.
- **Group-API version maxes are capped below the topic-ID cutover** (OffsetCommit/
  OffsetFetch v10) since the handlers resolve by topic name; franz-go negotiates down
  cleanly.
- **KIP-848 (new protocol)** is untouched — Phase 6. Kafka-4 Java clients still need
  `group.protocol=classic` until then, as the roadmap notes.
- Validation is franz-go only in CI (kcat/Java are the manual smoke clients from the
  roadmap; not run here).
