# Phase 2 report — Storage engine + topics

**Goal:** segment log (append, sparse index, roll), boot
recovery + torn-batch truncation, retention deletion, bbolt topic registry,
`CreateTopics`. Gate: create a topic, restart, recover to correct LEO; unit tests
for append→pread round-trip and torn-tail truncation.

## Implemented

- **bbolt metadata store** (`internal/storage/meta/store.go`): topics registry
  (name→record + reverse uuid→name), committed offsets, and a producer-id counter,
  in four buckets of one `meta.db`. Replaces the stub; adds `go.etcd.io/bbolt`.
  `CreateTopic` mints a random UUID and rejects duplicates (`ErrTopicExists`).
- **Log engine** (`internal/storage/log/`):
  - `batch.go` — header-only parse (`baseOffset`, `lastOffsetDelta`→count,
    `batchLength`→size) + `stampBaseOffset`. Records are never decoded.
  - `segment.go` — `WriteAt` append, `pread` reads, sparse-index maintenance,
    truncate. `index.go` — in-memory sparse index persisted to `.index`, binary-
    search lookup. `partition.go` — offset assignment, segment roll, offset→position
    read scan, log-start/end offsets, listing. `recovery.go` — tail scan +
    torn-batch truncation. `retention.go` — size-based delete-only `Enforce`
    (trigger wired in phase 5). `manager.go` — open/cache/close partitions.
- **`CreateTopics`** (`broker/topics.go`): registers each topic, materializes its
  partition directories, returns UUID + partition count; duplicates →
  `TOPIC_ALREADY_EXISTS (36)`. Handles `ValidateOnly`.
- **Metadata now serves the store** (`broker/metadata.go`): empty request → all
  topics; named topics resolved individually (unknown → `UNKNOWN_TOPIC_OR_PARTITION`);
  each partition led by self, replicas=isr=[self].
- **Profiles advertise `CreateTopics`** (majors 1–4).

## Acceptance

- **`go test -race ./...` PASS.**
- Log unit tests (`log_test.go`): `TestPartitionAppendReadRoundTrip` (append two
  batches, read back byte-for-byte with stamped offsets, mid-log read),
  `TestReopenRecoversLogEndOffset` (persist → reopen → LEO=5),
  `TestTornTailTruncatedOnRecovery` (partial trailing batch is truncated, LEO
  intact).
- Protocol gate (`phase2_test.go`): a franz-go client creates `orders` (3
  partitions); Metadata lists it; the broker is **restarted over the same data
  dir**; the topic and its partitions survive and the `orders-0` directory is on
  disk.

## Deviations / notes

- **Index is held in memory** (rebuilt from `.index` on open), not mmap'd — simpler,
  small footprint; the log itself stays out of the heap via `pread`. `storage.md`
  updated to match.
- **Retention** is size-based only and its periodic trigger lands in phase 5; the
  mechanism (`RetentionPolicy.Enforce`) exists now so the log-start offset is stable.
  Age-based deletion deferred.
- Produce/Fetch are **not** yet wired — the storage engine is exercised directly by
  unit tests. Phase 3 connects it to the wire (`Produce`/`Fetch`/`ListOffsets`).
- `DeleteTopics` remains a phase-5 stub.
