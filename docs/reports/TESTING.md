# Testing report — coverage & hardening

The suite is integration-first: almost every test boots a broker on a real loopback
socket and drives it with a real Kafka client, exercising the true
`frame → codec → handler → storage` path. 61 tests; `go test -race` clean.

The highest-confidence test goes end-to-end against the **real compiled binary**
out-of-process (`TestBinary_RealProcessDurabilityAndSignals`: build → produce →
SIGTERM → restart → data survives — the actual artifact, flags, and signal
handling), answering "does it work when someone actually runs it."

## Harness

- `startBroker` / `runBroker(cfg)` (broker package) boot a real broker; tests speak
  to it with **franz-go** and **Sarama** — two independent client implementations —
  plus raw **kmsg** requests for low-level control.
- The storage engine also has white-box unit tests (`internal/storage/log`) for
  behaviour not observable through a client (fsync, segment rolls, torn-tail
  recovery). The wire codec has fuzz targets (`internal/protocol`).
- **Unit vs integration is a precise `-short` split.** Every integration test (a real
  broker driven over a socket) skips under `go test -short`, so `go tool task
  test:unit` (~1.5s) runs exactly the unit tests — the storage engine, the wire codec,
  and the white-box handler tests — wherever they live, and `go tool task
  test:integration` runs the socket-level suite. `go tool task test` runs both.
- Run with `GOWORK=off` (the module lives inside a workspace); the `go tool task`
  targets set it automatically.

## What's covered

- **Every one of the 23 client APIs** is advertised and exercised; an ApiVersions
  surface-guard test fails if a served API isn't advertised.
- **Both consumer-group protocols**: classic (franz + Sarama) and KIP-848 (franz,
  behind its beta opt-in — see PHASE_6_REPORT.md), each consume→commit→resume across
  a restart and split partitions across two members.
- **Durability**: five tests restart the broker over the same data dir; crash
  recovery (torn tail + synced-but-not-closed); retention under load; flat-memory
  invariant.
- **Auth**: SASL PLAIN/SCRAM-256/512 and TLS, with negative cases (wrong password,
  unauthenticated gating, TLS-rejects-plaintext).
- **Cross-client**: **Sarama** (a second, independent Go client) — produce/consume +
  classic group consume→commit→resume, exercising older non-flexible wire versions
  franz never sends.
- **Robustness**: an unhandled/unadvertised API closes the connection (fail fast)
  instead of hanging the client — `TestUnhandledAPI_ClosesConnectionNotHang`.
- **Codec fuzzing**: `FuzzParseAndDecodeRequest`, `FuzzSkipTaggedFields`,
  `FuzzReadFrame` — arbitrary bytes through the request decoder must never panic;
  ~350k execs/target found no crashes, and the seeds run in the normal suite.
- **Session-timeout eviction** (the ungraceful-death path every graceful-close group
  test skips): a member joins at the wire level and goes silent; the coordinator
  evicts it — covered for both classic and KIP-848 (the 848 session timeout is
  configurable via `--consumer-session-timeout` so the test needn't wait 45s).
- **Produce/fetch corners**: all four compression codecs (gzip/snappy/lz4/zstd)
  round-trip through verbatim storage; Fetch long-poll blocks and wakes on a produce
  (timing-asserted); acks=0 stores data with no response.

## Soak & chaos

A separate harness (behind the `soak` build tag) drives the **real compiled binary**
under sustained, correctness-checked load — idempotent `acks=all` producers writing a
per-producer sequence, a live consumer group, plus RSS/goroutine sampling via the
binary's `--pprof-addr`. Two modes:

- `go tool task soak` — **leak detection**: no faults, a single long-lived process, watching
  RSS and goroutine count for growth.
- `go tool task chaos` — **fault injection**: the broker is repeatedly `SIGKILL`/`SIGTERM`-
  restarted *under* the load; the harness then reads the whole log back and asserts
  no acknowledged record was ever lost and the log has no gaps.

Both are parameterized by `SOAK_DURATION` (e.g. `go tool task chaos SOAK_DURATION=1h`).

Observed (run longer for more confidence — the harness scales via `SOAK_DURATION`):

- **Chaos, 5 minutes, ~1.05M records, 4 restarts (2 hard `SIGKILL`)**: **no
  acknowledged record lost and no gaps** — the `acks=all` durability guarantee holds
  through hard crashes. The cross-restart duplicate rate (the documented
  in-memory-dedup-resets-on-restart window) measured at **~0.014%**, never as loss.
  RSS stayed flat at ~20 MB and goroutines constant at 24 across the whole run.
- **Leak, no restarts, 150k+ records in one process**: memory and goroutine count
  stayed **flat** and idempotence was **exact** (zero duplicates).

These are minutes, not multi-hour; a longer soak would raise the confidence further.

## Known gaps (honest)

- **No Java / kcat client** in CI (need a JVM / native install); franz + Sarama are
  the automated cross-check, the rest is manual smoke.
- The soak/chaos runs above are short (seconds/minutes), not multi-hour — the harness
  scales via `SOAK_DURATION`, but long-run results aren't recorded here yet.
- Many-connection churn and network-partition faults aren't injected.
- Idempotent-producer sequence dedup, static membership, and classic
  leader-dies-mid-sync are not directly tested.
