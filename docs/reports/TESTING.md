# Testing report — coverage & hardening

The suite is integration-first: almost every test boots a broker on a real loopback
socket and drives it with a real Kafka client, exercising the true
`frame → codec → handler → storage` path. About 370 Go test functions across the
packages; the gate (`go tool task check`) runs them all under `-race`, plus `gofmt`,
`go vet`, and a Windows cross-vet.

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
- **`-short` skips most of the socket-level tests, not all.** The broker package's and
  the console's tests that boot a broker skip under `go test -short`; the command's and
  the embed API's (`cmd/doze-kafka`, `server`) do not, because starting a broker is
  what they test. `go tool task test:unit` is therefore the quick pass (well under ten
  seconds), not a strict "no sockets" run. `go tool task test:integration` runs the
  broker package in full, and `go tool task test` everything.
- `go tool task check` is the one gate: format, vet (Linux and Windows), then the whole
  suite with the race detector. `go tool task hooks:install` runs it before each push
  (a `pre-push` hook, not `pre-commit`).

## What's covered

- **Every client API the broker serves** is advertised and exercised; an ApiVersions
  surface-guard test fails if a served API isn't advertised.
- **Both consumer-group protocols**: classic (franz + Sarama) and KIP-848 (franz,
  behind its beta opt-in — see history/PHASE_6_REPORT.md), each consume→commit→resume across
  a restart and split partitions across two members.
- **Durability**: five tests restart the broker over the same data dir; crash
  recovery (torn tail + synced-but-not-closed); retention under load; flat-memory
  invariant.
- **Auth**: SASL PLAIN/SCRAM-256/512 and TLS, with negative cases (wrong password,
  unauthenticated gating, TLS-rejects-plaintext).
- **Cross-client**: **Sarama** (a second, independent Go client) — produce/consume +
  classic group consume→commit→resume, exercising older non-flexible wire versions
  franz never sends.
- **Robustness**: a request for an API or a version the broker does not advertise
  closes the connection (fail fast) instead of hanging the client, and is never
  decoded or served: `TestUnhandledAPI_ClosesConnectionNotHang` and
  `TestOnlyAdvertisedAPIsAndVersionsAreServed`.
- **librdkafka**: `scripts/kcat-check.sh` runs kcat against the built binary (plain and
  SASL/PLAIN): metadata, produce with keys and headers, consume, a group that commits
  and resumes, 500 records in order, and a wrong password refused. CI runs it
  (`kcat.yml`); locally it is `go tool task test:kcat`. It found that a broker on
  127.0.0.1 advertised "localhost", which kcat could not follow on a machine where that
  name resolves to ::1 first.
- **The client-compatibility matrix**: `compat/` runs the Java client 4.0, Kafka Streams
  (exactly-once, restoring from compacted changelogs), Spring Kafka, confluent-kafka, kcat,
  KafkaJS, Kafka Connect, Confluent's and Apicurio's registries and the Kafbat and Redpanda
  UIs against the built binary, each with no authentication and over SASL/SCRAM and TLS
  (`compat.yml`, weekly and on every PR; locally `go tool task test:compat`). A toolchain
  that is missing is reported as skipped, not passed. It found that the registry refused
  Protobuf schemas sent as descriptors, and that librdkafka's zstd telemetry frames were
  refused. See [compat/README.md](../../compat/README.md).
- **The console in a browser**: 50 Playwright tests in `e2e/` drive a real Chromium
  against the real binary (a fresh data directory, a seeded broker): navigation and
  Back, the theme, the palette, the drawer, live updates and Pause, the composer, search,
  group offset changes, topic settings, the keyboard, and layout at phone width. They run
  in their own CI workflow (`e2e.yml`) and with `go tool task test:e2e`; they are not part
  of `task check`.
- **Codec fuzzing**: `FuzzParseAndDecodeRequest`, `FuzzSkipTaggedFields`,
  `FuzzReadFrame` — arbitrary bytes through the request decoder must never panic;
  the seeds (including a once-found crasher, a tagged-field loop in the upstream
  `kmsg` decoder, now fixed in a vendored patch under `internal/kmsg`) run in the
  normal suite, and a scheduled job fuzzes for longer and opens an issue when it fails.
- **Session-timeout eviction** (the ungraceful-death path every graceful-close group
  test skips): a member joins at the wire level and goes silent; the coordinator
  evicts it — covered for both classic and KIP-848 (the 848 session timeout is
  configurable via `--consumer-session-timeout` so the test needn't wait 45s).
- **Produce/fetch corners**: all four compression codecs (gzip/snappy/lz4/zstd)
  round-trip through verbatim storage; Fetch long-poll blocks and wakes on a produce
  (timing-asserted); acks=0 stores data with no response.

## Soak & chaos

A separate harness drives the **real compiled binary**
under sustained, correctness-checked load — idempotent `acks=all` producers writing a
per-producer sequence, a live consumer group, plus RSS/goroutine sampling via the
binary's `--pprof-addr`. Two modes:

- `go tool task soak` — **leak detection**: no faults, a single long-lived process, watching
  RSS and goroutine count for growth.
- `go tool task chaos` — **fault injection**: the broker is repeatedly `SIGKILL`/`SIGTERM`-
  restarted *under* the load; the harness then reads the whole log back and asserts
  no acknowledged record was ever lost and the log has no gaps.

Both are parameterized by `SOAK_DURATION` (e.g. `go tool task chaos SOAK_DURATION=1h`),
and `SOAK_RACE=1` builds the binary with the race detector and fails the run if it
reports a data race (the scheduled leak run sets it). The same chaos runs for six
seconds in the ordinary suite as `TestChaosSlice`, so a change that breaks crash
recovery fails the pull request rather than a run next week.

Alongside it, `TestStress_ProducersConsumersAndAdminAtOnce` runs producers, a group
whose members come and go, and admins creating, altering and deleting topics, all at
once in one process under `-race`, and asserts every acknowledged record is read.
`TestPersistence_EverythingTheBrokerKeepsSurvivesARestart` lists everything the broker
persists and checks each across a restart; a new persisted field belongs in it.
`TestVersionMatrix_*` sends a default request for every API at its lowest, middle and
highest version, for every Kafka major, and checks the answer decodes.

Observed (run longer for more confidence — the harness scales via `SOAK_DURATION`):

- **Chaos, 5 minutes, ~1.05M records, 4 restarts (2 hard `SIGKILL`)**: **no
  acknowledged record lost and no gaps** — the `acks=all` durability guarantee holds
  through hard crashes. The cross-restart duplicate rate (the documented
  in-memory-dedup-resets-on-restart window) measured at **~0.014%**, never as loss.
  RSS stayed flat at ~20 MB and goroutines constant at 24 across the whole run.
- **Leak, no restarts, 150k+ records in one process**: memory and goroutine count
  stayed **flat** and idempotence was **exact** (zero duplicates).

These are minutes, not multi-hour; a longer soak would raise the confidence further.

## Console

The web console is covered in Go by guards that read the source: every template class
is styled, every CSS variable is defined, the type scale is closed, inline styles are
on a ratchet, every text colour holds 4.5:1 in both themes, every GET route renders
(with data, with none, and with the broker down), and every POST is refused from a
foreign origin and fails with a 4xx rather than a 5xx. What needs a real browser (live
updates, history, focus, the keyboard, layout) is in the Playwright suite above.
Known limit: it runs Chromium only, and has no check that every console route is
reached from a page (doze-aws's route gate), so a form posting to the wrong path is
caught only if a spec exercises it.

## Known gaps (honest)

- **No Java, Spring, Streams or kafkajs client** in CI (a JVM or a Node install); franz,
  Sarama and librdkafka (through kcat) are the automated cross-check. The client matrix
  is Phase 13 of the plan.
- The soak/chaos runs above are minutes, not multi-hour; the scheduled workflow runs
  ten minutes a week, and long-run results are not recorded here.
- Network-partition and slow-disk faults are not injected. SIGKILL does not lose the OS's
  page cache, so the crash tests cannot show whether an fsync was skipped; the fsync
  discipline is checked by fault injection in the storage tests instead.
- Idempotent-producer dedup state is in memory and starts empty after a restart (Phase 11
  rebuilds it from the log).
- The Playwright suite is not part of `task check`: it needs `bun` and a browser.
