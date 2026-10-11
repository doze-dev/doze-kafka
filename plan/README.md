# The road to the dream console, one stage at a time

> **A working document.** This folder is removed in the commit that tags the
> release. What it produces outlives it elsewhere: findings become tests,
> contracts become `docs/api-support.md` and `docs/limitations.md`, behaviour
> changes go to the changelog, and the recording queues move into the
> conformance harness.

The design is at <https://claude.ai/artifact/DFbFCyDEEbRsyFP7DRQuvx>: 137 boards
in 12 pages. This folder is how we get there **without building screens on top
of code nobody has checked**. Each stage takes one surface of the console
together with the broker code beneath it, finds what is wrong or unproven in
both, fixes it test-first, and only then builds the screens. A stage is not
started until the one before it has met its acceptance criteria, and nothing is
called done on the strength of somebody's memory of how Kafka behaves.

## How it was made

Every stage document was written by comparing the design's boards with the code
(what exists, what could be built from data we already have, what needs new
backend) **and** by auditing the existing code in the stage's packages: bounds,
error codes, persistence, races, hard-coded values, text that contradicts the
code, and the tests that are missing. Eleven findings in the Topics stage and
the durability, lock, port and full-disk cases in Bad days were reproduced
against a built binary; the rest are marked `read` and are reproduced before they
are fixed (the protocol, step 2).

## The documents

| Document | What it is |
|---|---|
| [00-foundation.md](00-foundation.md) | Work every stage depends on: the console shell, the test machinery (including a recording of a **real Apache Kafka**, which does not exist yet), the move to the latest `doze-names`, and the asks the stages made of it. Done first, once. |
| [01-protocol.md](01-protocol.md) | The seven steps every stage goes through, the evidence standard, the common acceptance criteria, the severity scale, and the eleven sections every stage document has. |
| [stages/](stages/) | One document per surface, all with the same eleven sections. |
| [90-integration.md](90-integration.md) | What is tested only once every stage is done: the harbour end to end, the board review, the widened client matrix, soak and chaos, budgets, the security matrix, platforms, documents. |

## Baseline

Measured on 2026-10-11 at `main` `7b0b17e`. A stage's coverage target is never
below its row here, and `task coverage` (foundation B4) makes that a gate.

| Package | Statement coverage |
|---|---|
| `cmd/doze-kafka` | 79.8% |
| `console` | 87.8% |
| `internal/broker` | 89.0% (27.5% under `-short`) |
| `internal/config` | 61.6% |
| `internal/protocol` | 71.9% |
| `internal/storage/log` | 80.5% |
| `internal/storage/meta` | 80.7% |
| `internal/telemetry` | 92.0% |
| `internal/traffic` | 96.7% |
| `schemaregistry` | 88.4% |
| `server` | 79.0% |

Also: 73 browser tests in 11 specs, 20 of 20 client-matrix cells passing, 7 fuzz
targets, no mutation baseline, no coverage floor, no lightness budget, and no
recording of a real Kafka broker (only of Confluent's registry).

## Order, and the numbers

Each row starts only when the row above has met its acceptance criteria, except
row 1 (see D1).

| # | Stage | Findings | P0 | P1 | P2 | P3 | Boards | Days |
|---|---|---|---|---|---|---|---|---|
| 0 | [Foundation](00-foundation.md) | 12 shell, 15 machinery, 2 names, 19 asks | | | | | shell, Sy-* | 12 |
| 1 | Durability first (the P0 and P1 items of [bad-days](stages/bad-days.md): BAD-1, 2, 3, 4, 13, 29) | 6 | 1 | 5 | | | none | 3 |
| 2 | [Traffic](stages/traffic.md) | 31 | 0 | 4 | 14 | 13 | 12 | 20 |
| 3 | [Topics](stages/topics.md) | 39 | 0 | 2 | 19 | 18 | 32 | 28 |
| 4 | [Groups](stages/groups.md) | 34 | 0 | 9 | 15 | 10 | 19 | 26 |
| 5 | [Schemas](stages/schemas.md) | 38 | 0 | 7 | 22 | 9 | 19 | 18 |
| 6 | [Trace](stages/trace.md) | 27 | 0 | 0 | 14 | 13 | 9 | 7 |
| 7 | [Access](stages/access.md) | 36 | 0 | 5 | 17 | 14 | 10 | 11 to 16 |
| 8 | [Cluster](stages/cluster.md) | 38 | 0 | 2 | 23 | 13 | 10 | 14 |
| 9 | [Connect](stages/connect.md) | 30 | 0 | 6 | 19 | 5 | 7 | 13 |
| 10 | [Bad days](stages/bad-days.md) (the rest) | 34 | 0 | 0 | 22 | 12 | 9 | 16 |
| 11 | [Map](stages/map.md) | 17 | 0 | 0 | 13 | 4 | 2 | 15 |
| 12 | [Integration](90-integration.md) | | | | | | | 7 |

**330 findings in the ten stage documents: 1 P0, 40 P1, 178 P2, 111 P3.** About
**190 focused days** of work in all (12 for the foundation, 3 for durability, 7
for integration and the rest in the stages; Access counted at 10.5, or 16 with
review, recordings and CI). That is not a promise: the
stage estimates were made by reading the code, and the first stage that
finishes re-estimates the rest from its actual time, as the doze-aws plan did.

Why this order:

- **Traffic is first** because three things the other stages need come out of
  it: the per-resource filter and cursor the wire dock reads, the inspector in
  its first real use, and the `Refusal` classification.
- **Topics before Groups, Schemas and Trace**: groups read topics, the registry
  decodes topic messages, and Trace needs the time index Topics hardens.
- **Access after the pages it denies** (Topics and Groups boards each have a
  Denied state that links to the checker).
- **Cluster after Topics, Groups and Access**: it reports their transactions,
  compaction and configuration.
- **Connect after Cluster and Schemas**: its snippets are filled from the
  listeners, SASL and registry facts those stages settle.
- **Map last of the surfaces**: it is entirely new backend and it needs the
  recorder, the coordinators and the registry to be trustworthy first.

## Cross-stage dependencies to keep in view

| Dependency | Between | Why |
|---|---|---|
| The wire dock (cursor and filter) | Traffic, then Topics, Groups, Schemas, Access | Built once in Traffic, used by four pages. |
| The `Refusal` type and its four kinds | Foundation A2, then every form | Registry errors and console-rule refusals are named by this stage's asks (E2). |
| The A5 graph | Foundation, Map, the neighbours sentence on every page | One type, defined in the foundation, filled in by Map; until then the sentence reads what each page can already know. |
| The time index and the bounded-scan helper | Topics, Trace, Schemas | One helper (E18). |
| The settings table (B7) | Foundation, Topics, Cluster, Schemas, Map, Groups | The console forms and `DescribeConfigs` read the same record. |
| The fault seam (B15) | Foundation, Bad days | The bad days' tests need it; the P0 fix in stage 1 needs only the existing sync hook. |

## Status

Updated when a stage meets its criteria, in the same commit as the work.

| Stage | Contract | Findings fixed | Proved | Console | Docs | Done | Actual days |
|---|---|---|---|---|---|---|---|
| Foundation | – | | | | – | | |
| Durability first | no new API; `KAFKA_STORAGE_ERROR` on 5 more write APIs, `INVALID_SESSION_TIMEOUT` on JoinGroup, `acks=0` closes on any refused partition | 6 of 6: BAD-1, 3, 4, 13, 29 fixed; BAD-2 decided and pinned (batch stays, as Kafka's `NOT_ENOUGH_REPLICAS_AFTER_APPEND`) | red test and hand mutation per finding; `task check`, coverage (broker 89.9, log 81.0, server 79.2, conformance 85.2), lightness, conformance replay green; 3 new recordings against `apache/kafka:4.3.1` (Q2, Q5, Q9) | – | `limitations.md`, `storage-and-durability.md`, `CHANGELOG.md`, claims | committed on branch, not pushed, CI not run; `internal/traffic` was already 95.9 against a floor of 96.7 on this base | one session, not measured in days (estimate 3) |
| Traffic | | | | | | | |
| Topics | | | | | | | |
| Groups | | | | | | | |
| Schemas | | | | | | | |
| Trace | | | | | | | |
| Access | | | | | | | |
| Cluster | | | | | | | |
| Connect | | | | | | | |
| Bad days | | | | | | | |
| Map | | | | | | | |
| Integration | – | | | – | – | | |

## Decisions

| # | Decision | State | Where it lands |
|---|---|---|---|
| D1 | A known data-loss bug does not wait five stages. **BAD-1** (a duplicate idempotent `acks=all` batch is acknowledged without an fsync) and the other P0 and P1 items of the Bad days stage are fixed straight after the foundation as their own stage, and the rest of Bad days stays where the order puts it. | **Proposed.** Say if you want BAD-1 fixed before even the foundation. | README order, stage 1 |
| D2 | Move to the latest `doze-names` in doze-kafka, doze-aws and doze, and delete the merged remote `zone-hardening` branch. | **Decided** (you said so). Not yet done: scheduled as foundation C1 and C2. | Foundation C |
| D3 | A recording of a real Apache Kafka 4.0 becomes the oracle, as Confluent's registry is for the schema work. Claims stay `memory` until recorded; a fix resting on `memory` is a skipped test. | **Proposed.** | Protocol, foundation B2 |
| D4 | The plan's definition of done includes coverage floors, mutation baselines and a lightness budget, not only passing tests. | **Proposed.** | Protocol step 4, foundation B3, B4, B9 |
| D5 | A board that cannot be built truthfully is cut or rewritten, with a sentence, rather than built as drawn. Several already say so in their stage document (invented sample values, the unverified `.doze` names). | **Proposed.** | Integration section 2 |
| D6 | `doze-kafka config` redacting credentials breaks round-tripping a valid config file. Redact by default and add `config --show-secrets`? | **Open** (raised by Connect). | [connect.md](stages/connect.md) |
| D7 | Offsets expiry (GRP-19) and groups that mix the two protocols (GRP-8) are counted at zero days. Build them, or document them as limits? | **Open** (raised by Groups). | [groups.md](stages/groups.md) |

## Rules that hold throughout

- **Never more forgiving than Kafka.** Accepting what Kafka refuses is a bug.
- **Weight is a feature.** Tables are built lazily; `task lightness` holds idle
  heap and binary size; no stage may move them without saying why.
- **Red first.** A finding becomes a failing test before it becomes a fix, and
  the fix is mutation-checked by reverting it.
- **Nothing on memory.** A claim about Kafka that is only remembered is queued
  for recording. Its test is written and skipped; the code is not changed.
- **One stage at a time.** Cross-stage behaviour is written on the stage's "owed
  to integration" list and left for the last stage, except where the table
  above moved it.
- **The design is the specification for the console, and the code is the
  specification for the design.** Where a board and the code disagree, the board
  is wrong until proven otherwise; each stage document lists the board
  sentences that describe a bug as it behaves today.
- **Commit to `main`** when a stage meets its criteria, CI green. Pushing is a
  separate decision.
