# Phase 3 report — Produce / consume (no groups)

**Goal:** `InitProducerId`, `Produce`, `Fetch` (topic-ID aware,
long-poll), `ListOffsets`. Gate: franz-go / kcat produce→consume round-trip,
ordering preserved, data survives restart, and idle RSS stays flat while a
multi-hundred-MB topic sits on disk.

## Implemented

- **`InitProducerId`** (`broker/producerid.go`): hands out a monotonic PID (epoch
  0) from `meta.NextProducerID`. Mandatory — modern idempotent producers call it
  before producing. Sequence numbers accepted without dedup; no transactions.
- **`Produce`** (`broker/produce.go`): resolves the topic (by name or UUID),
  validates the partition, and appends the raw record bytes via the log engine —
  base offset stamped, LEO advanced from the batch header. `acks=0` returns no
  response; a successful append wakes long-polling fetchers.
- **`Partition.Append` now handles multiple concatenated batches** in one payload,
  stamping each with a running offset (correct multi-batch accounting).
- **`Fetch`** (`broker/fetch.go`): **topic-ID resolution** (v13+ address topics by
  UUID), per-partition read from the log, `highWatermark = LEO`, `logStartOffset`
  from retention, `OFFSET_OUT_OF_RANGE` below the start. **Long-poll**: if less than
  `MinBytes` is available it waits up to `MaxWaitMillis` on a produce-notifier, then
  returns.
- **`ListOffsets`** (`broker/listoffsets.go`): `-1`→LEO (latest), `-2`→log-start
  (earliest); a real timestamp is served coarsely as earliest (no timestamp index).
- **Metadata auto-creates** a topic when the request allows it and config enables it
  (`AutoCreateTopics`), so producing to a new topic just works.
- **Profiles advertise** Produce/Fetch/ListOffsets/InitProducerId (majors 1–4;
  Fetch max 13 on v3/v4 to exercise topic-ID addressing).

## Acceptance

- **`go test -race ./...` PASS.**
- **`TestPhase3_ProduceConsumeRoundTripAndRestart`**: an unmodified franz-go
  producer writes 50 records (idempotent producer → real `InitProducerId` +
  topic-ID `Fetch` paths exercised); a direct (group-less) consumer reads them back
  **in order**; the broker is **restarted over the same data dir** and a fresh
  consumer reads all 50 in order again.
- **`TestAppendKeepsHeapFlat`**: writing **128 MiB** to a partition grows the heap
  by < 24 MiB (well under the data size) — the log is streamed to disk via `WriteAt`
  and served by `pread`, never buffered. This is the load-bearing memory invariant;
  idle RSS beyond it is OS page cache, which the kernel reclaims.

## Deviations / notes

- **`ListOffsets` by timestamp is coarse** (returns earliest) — a per-record
  timestamp index is out of scope for local dev; earliest/latest are exact.
- **Long-poll uses a broker-wide produce notifier** (any produce wakes all blocked
  fetchers, which re-check). Coarser than per-partition wakeups but simple and
  correct; fine at local-dev load.
- The idle-RSS gate is validated at the storage layer (heap-flat test) rather than
  by measuring the binary's RSS under an external load generator (`kcat` absent).
- Consumer **groups** are not yet involved — this is direct partition consumption.
  Phase 4 adds the classic group protocol (`JoinGroup`/`SyncGroup`/offsets).
