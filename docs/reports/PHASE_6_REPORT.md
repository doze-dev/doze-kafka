# Phase 6 report — KIP-848 broker-driven consumer groups (v2)

**Goal:** `ConsumerGroupHeartbeat (68)` + `ConsumerGroupDescribe
(69)` + a server-side assignor + incremental reconciliation (member epoch),
advertised under declared `version = 4`. Gate: a client at the default
`group.protocol=consumer` — no `classic` override — joins, consumes, commits, and
rebalances against a second member.

## The inversion

Classic (Phase 4) makes the broker a **relay**: the elected client leader computes
assignments and the broker shuttles opaque bytes. KIP-848 makes the broker the
**assignor and reconciliation driver**. A single `ConsumerGroupHeartbeat` carries
join (epoch 0), poll (epoch > 0), and leave (epoch < 0); the **member epoch**
replaces the generation. The client generates its own member UUID (v1+); the broker
echoes it.

## Reconciliation is incremental and safe

The load-bearing property: **the broker never hands partition P to member B while
member A still owns it.** Each heartbeat reports the member's currently-owned
partitions; the broker only delivers a target partition once no other member reports
owning it. So a handoff is:

1. Second member joins → group epoch bumps → assignor recomputes targets.
2. Member A's new (smaller) target excludes P; the broker sends A an assignment
   without P. franz-go revokes P and reports the smaller owned set on its next
   heartbeat.
3. P is now free; B's next heartbeat delivers it.

This is driven by assignment *content* plus the owned set, exactly what franz-go's
client-side reconciliation consumes — the two-member split converges in a couple of
heartbeat intervals with no dual ownership.

## What ships (`internal/broker/consumergroup848.go`)

- **`cg848Manager`**: ephemeral per-group state (epoch, members, each member's
  subscribed/owned/target/last-sent) under one mutex, with a per-member session
  timer that evicts and rebalances on expiry. Membership is rebuilt on rejoin; only
  committed offsets are durable (reused from Phase 4's OffsetCommit/Fetch).
- **Server-side assignor**: `uniform` (round-robin a topic's partitions across its
  subscribers — franz's default) and `range` (contiguous per-subscriber slices).
  Partitions of each topic are distributed only among members subscribed to it.
- **`ConsumerGroupHeartbeat` handler**: join/poll/leave, member-epoch fencing
  (`FENCED_MEMBER_EPOCH` for an unknown/stale member so the client cleanly rejoins),
  topic-ID ↔ name resolution via the metadata store, and a keepalive path (nil
  assignment when nothing changed).
- **`ConsumerGroupDescribe` handler**: group state/epoch/assignor plus per-member
  subscriptions and current/target assignments; `GROUP_ID_NOT_FOUND` for unknown
  groups.
- **Advertised only on the Kafka-4 profile** (`versions.go`): 68/69 at v0–1, so a
  client at default `group.protocol=consumer` negotiates the new path against a
  `version = 4` broker while `version = 1/2/3` stay classic-only.
- New error codes: `FENCED_MEMBER_EPOCH` (110), `UNSUPPORTED_ASSIGNOR` (112),
  `STALE_MEMBER_EPOCH` (113), `GROUP_ID_NOT_FOUND` (69).

## Acceptance

- **`go test -race ./...` PASS**, stable across `-count=5` (the manager, session
  timers, and reconciliation are concurrency-heavy).
- **`TestPhase6_NewProtocolConsumeCommitResume`** (gate): a franz-go client with
  `kgo.GroupProtocol("consumer")` — the new protocol, no classic override — joins,
  gets a **broker-computed** assignment, consumes, and commits; after a broker
  restart a fresh member of the group **resumes from the committed offset** with no
  replay.
- **`TestPhase6_NewProtocolTwoConsumersSplit`**: two members each get a partition of
  a two-partition topic (verified via `OnPartitionsAssigned` before producing) and
  together consume every record — exercising the assignor and the safe handoff.
- **Both protocols now covered**: because franz-go defaults to `consumer` and uses
  the new protocol whenever the broker advertises it, the Phase 4 tests were pinned
  to `kgo.GroupProtocol("classic")` so classic and KIP-848 are each exercised
  explicitly.

## Deviations / notes

- **Uniform assignor is round-robin per topic**, not the sticky, cross-topic-balanced
  assignor of the reference implementation. It produces an even split and is stable
  under the incremental handoff; stickiness (minimizing partition movement across
  rebalances) is a refinement, not a correctness requirement for local dev.
- **Member epoch is set to the group epoch on every accepted heartbeat** rather than
  strictly lagging until a member is fully reconciled. franz-go drives reconciliation
  off assignment content, so this is sufficient and simpler; fencing still works
  (mismatched epoch → `FENCED_MEMBER_EPOCH` → clean rejoin).
- **Regex subscriptions and `SubscribedTopicRegex`** are not implemented — only
  explicit `SubscribedTopicNames`. Rack-aware assignment and static-membership
  reclaim (`InstanceID`) are accepted on the wire but not acted on.
- **franz-go gates KIP-848 behind a beta opt-in** in v1.21.5: the client only uses
  the new protocol when given a `WithContext` carrying
  `opt_in_kafka_next_gen_balancer_beta` *and* a sticky/range balancer — otherwise it
  silently falls back to classic even with `GroupProtocol("consumer")`. The Phase 6
  tests set both (see `kip848Opts` in `phase6_test.go`), so they genuinely drive the
  broker's ConsumerGroupHeartbeat path; `TestAdmin_ConsumerGroupDescribe` /
  `TestAdmin_ListGroups` confirm the group actually lands in the KIP-848 manager.
- **Client validation is franz-go only** (its KIP-848 client is a faithful,
  independent implementation); the roadmap's stock-Java-client check remains a manual
  smoke test where a JVM is available.

## Definition of done

This completes the roadmap through **Phase 6**: classic *and* KIP-848 consumer
groups, chosen per client via the advertised APIs, both franz-go-verified — on top of
v1 (Phases 0–5) and optional SASL/TLS auth.
