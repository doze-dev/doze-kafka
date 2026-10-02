# Phase 5 report — Polish (durability, retention, graceful shutdown, config)

**Goal:** coarse-timer fsync, retention config, optional idle
reaping, graceful shutdown (flush + fsync), config flags, optional tiny CLI. Gate:
a clean restart under load loses nothing acked; `kill -9` loses at most
in-flight-unsynced and recovers without corruption.

## Durability

Before this phase the log was **never fsynced** — appends used `WriteAt` and close
just closed the file. Data survived a process restart (page cache persists) but not
an OS crash / power loss. Phase 5 adds a real durability floor:

- **`Segment.sync()`** (`storage/log/segment.go`) fsyncs the log file and its index.
  Each segment tracks `lastModified` (for age-based retention).
- **Coarse fsync timer**: the `Manager` runs a background loop
  (`storage/log/manager.go`) that fsyncs every open partition's active segment every
  `FsyncInterval` (default 500 ms). Never per-append — throughput stays high; the
  window of loss on power failure is bounded by the interval.
- **Seal-on-roll**: when a segment rolls, the outgoing segment is fsynced before the
  new one opens, so a completed segment is never left unsynced (the periodic timer
  only touches the active one).
- **Flush on shutdown**: `Partition.close()` fsyncs the active segment before
  closing, and `Manager.Close()` stops the loop then flushes+closes every partition.
  A graceful shutdown is durable by construction.
- **Crash recovery** (already present, now load-bearing): `recoverSegment` truncates
  a torn final batch on open. New test `TestRecoverAfterCrashWithoutClose` simulates
  a `kill -9` — synced but never closed — and reopens: everything durably written
  recovers.

## Retention

`RetentionPolicy` existed with size-based `Enforce` but nothing called it and
age-based deletion was a stub. Now:

- **Size and age bounds** (`storage/log/retention.go`): `Enforce` deletes whole,
  oldest, non-active segments — first any sealed segment older than `MaxAge`, then
  the oldest while total size exceeds `MaxBytes` — advancing the log-start offset.
  The active segment is never deleted, so a partition always keeps its newest data.
- **Background sweeps**: the maintenance loop calls `EnforceRetention` every second
  (`retentionCheckInterval`); sizes/ages are tracked in memory, so an idle sweep
  touches no disk. Both `Sync` and `EnforceRetention` are exported so tests trigger
  them deterministically.
- Reads below the (advanced) log-start offset return `OFFSET_OUT_OF_RANGE`; the
  retained tail reads normally.

## Graceful shutdown

- **Connection drain** (`protocol/server.go`): the server tracks in-flight
  connections. On `ctx` cancellation it closes the listener **and** every live
  connection (unblocking any parked in a blocking read), then `Serve` waits for all
  connection goroutines before returning. `cmd/doze-kafka` then lets the deferred
  `Manager.Close` / `meta.Close` flush to disk — so SIGINT/SIGTERM is a clean,
  durable stop, verified end-to-end (boot → SIGTERM → exit 0 + "flushing logs").

## Config surface

- **New flags** wired in `cmd/doze-kafka`: `--index-interval-bytes`,
  `--retention-bytes`, `--retention-ms`, `--advertised-port` (the fsync/retention
  fields existed on `Config` but weren't reaching the log manager).
- `Config.Retention()` maps the config to a `log.RetentionPolicy`; `Validate` now
  rejects negative retention/fsync values (0 disables each). The manager receives
  the policy, cadence, and a logger through `ManagerOptions`.
- The broker test harness (`runBroker`) constructs `ManagerOptions` exactly as the
  binary does, so durability/retention under test matches production.

## Acceptance

- **`go test -race ./...` PASS** (the new background loop + connection drain are
  race-clean).
- **`TestPhase5_RetentionTrimsUnderLoadAndSurvivesRestart`** (broker gate): produce
  4000 records with a 16 KiB size bound and 4 KiB segments; retention trims to
  earliest offset ~3810/4000; the retained tail consumes **contiguously and
  correctly offset**; after a **graceful restart** the earliest/latest offsets and
  the tail are byte-for-byte unchanged.
- **Log-unit tests**: retention by bytes, retention by age, active-segment-never-
  deleted, crash-recovery-without-close, and the manager's `Sync`/`EnforceRetention`
  entry points.
- **CLI smoke**: the binary boots with the new flags and shuts down cleanly on
  SIGTERM (exit 0), logging the flush.
- Phase 4/5 tests are stable across `-count=3`.

## Deviations / notes

- **Optional idle reaping** (closing idle partition file handles) is **not**
  implemented — it's marked optional in the roadmap and outside the durability gate;
  idle RSS already stays flat because the log is served by `pread`, not buffered
  (Phase 3 heap-flat invariant). The `Manager` map holds one fd pair per touched
  partition; a reaper can be layered on later without interface changes.
- **`kill -9` is validated at the storage layer** (crash-without-close recovery +
  torn-tail truncation) rather than by actually SIGKILL-ing the running binary
  mid-load; the graceful path is validated end-to-end against the real process.
- **Retention check interval is a fixed 1 s** (internal constant), not a config
  knob — it's a soft bound and this keeps local-dev feedback snappy at no real cost.
- **No separate "tiny CLI"** — the flag-driven `cmd/doze-kafka` is the CLI; a
  subcommand surface wasn't needed for the gate.
- Auth (SASL/TLS) remains a separate, optional workstream — not part of
  Phase 5.
