# Stage 0: the foundation

Work every stage depends on, done first and once. It has four parts: the shared
console shell (A), the evidence and test machinery that lets a later stage prove
itself (B), the move to the latest `doze-names` across the three repositories
(C), and the cross-cutting pages that wait for the stages that feed them (D).

Every item has a **Done when**, and every "done when" names the test or command
that shows it. Nothing here changes what a Kafka client sees; it changes what
the console is built from and how we know a stage is finished.

## Where the project stands (the starting point)

Facts, each measured on 2026-10-11 against `main` at `7b0b17e`.

| Fact | Value |
|---|---|
| Statement coverage, full suite | see [README.md](README.md#baseline) |
| Real-client matrix | 10 clients, plain and SASL/SCRAM over TLS: 20 of 20 cells pass (`compat/run.sh`) |
| Browser suite | 11 specs, 73 tests (`e2e/tests/`) |
| Fuzz targets | 7: request parse and decode, tagged-field skipping, frame reading, the record reader, registry register-and-compare, registry payload decode, the telemetry decoder (`task fuzz`) |
| Differential oracle | **Confluent SR only** (about 1,500 schema pairs and 25 request scripts recorded). **There is no recording of a real Apache Kafka broker.** Every claim about broker error codes and edge cases is `read` or `memory`. |
| Mutation testing | not set up |
| Coverage floor in CI | not set up |
| Idle-heap and binary-size budget | not set up |
| `doze-names` | all three repositories pin `fcf6a9e`; `main` is one commit ahead (`d5a7201`) |

The two gaps that matter most are the last-but-three and the oracle: without a
recording of a real broker, "the same error code as Kafka" is a belief, and
without a coverage floor a later change can remove a test and nothing notices.

---

## A. The console shell

The "One anatomy" page of the design, built once and used by every stage's
pages. Today's console is the earlier design: a list, a detail page, a drawer
over the content, and polling partials. The pieces below replace the drawer
and add what the design assumes.

### A1. Rail, list pane, inspector

The rail has three groups (Observe: Traffic and Trace; Data: Topics, Groups,
Schemas; Broker: Access and Cluster) and "Connect an app" at the foot. The
inspector is the right-hand panel; selecting a message, a request, a client, a
member, a subject or a transaction replaces its content in place.

**Done when.** One `inspector` partial contract exists (a title, a kind, a body,
a close control, and a URL so the selection survives a refresh and Back works),
all six selections render through it, and `#drawer` is gone. A Playwright test
selects each kind, presses Esc (focus returns to the row), reloads (the same
selection is open) and goes Back (the previous selection or none).

### A2. One vocabulary for "no"

The design uses four words and one colour each: **Refused** (grey-purple: real
Kafka would say no too), **Denied** (an ACL decided), **Not implemented** (this
broker does not do it), **Failed** (red: the broker is unhappy). Today errors
are plain text banners in `errors.html` and in toasts.

**Done when.** A `Refusal{Kind, Name, Message, Rule}` type and one partial render
every console error; `Name` is the Kafka error name taken from the same table
the protocol layer uses (so the console cannot show a name the wire does not
send); a test walks every handler that can fail and asserts a `Refusal`, not a
string. The four colours pass the contrast check in A11.

### A3. Teaching lines come from data

The design shows, under a field, its unit, range, default and whether it is
applied or only stored. That text is generated from the settings table (B7),
never typed into a template.

**Done when.** A test enumerates every form field in the console templates and
fails for one with no settings record.

### A4. The wire dock

Every topic and group page ends with the live requests that touched it. The
component is built here; the recorder filter it needs (by topic or group, newest
first, with a cursor) is built in the [traffic stage](stages/traffic.md).

**Done when.** The dock component renders from an interface with a fake, is
polled with the pause and disconnected states from A7, and a test shows it
shows only rows for its resource.

### A5. Neighbours

"Written by checkout-api, read by g-order-validator, schema subject …" as one
sentence of links. Its data is a small graph (clients write topics, groups read
topics, subjects describe topics) derived from what the recorder, the
coordinators and the registry already know.

**Done when.** The graph type is defined (nodes: client, topic, group, subject;
edges: writes, reads, describes) with its sources, a test builds it from fixtures,
and the sentence component renders from it. The [map stage](stages/map.md)
reuses the same graph.

### A6. Confirms that tell the truth

Every destructive action confirms, names the verb and the thing, says exactly
what goes with it, and focuses Cancel.

**Done when.** A table in code lists each destructive route with the sentence
its confirm shows and the effect it has; a test performs each action against a
real broker and asserts the effect matches the sentence (for example, "deletes
the committed offsets of 2 groups" is true because 2 groups lose their offsets).

### A7. The console when the broker is not there

One persistent "disconnected" state on the live pill, not a toast per poll; the
last good time; frozen content with a muted overlay; one retry. Every page
renders with the broker down (the surfaces test exists).

**Done when.** The Playwright suite stops the broker mid-poll and asserts the
pill, the frozen rows and the absence of a repeating toast, then restarts it and
asserts recovery without a reload.

### A8. No identity in the code

The design uses a fictional shop ("harbour") with `127.0.0.1:9092` everywhere.
The code must not.

**Done when.** The console and e2e suites run once with the listener on
`127.0.0.1:0`, the console under a prefix other than the default, a data
directory containing a space, and `DOZE_ZONE=off`, and once with the zone on.
(`--zone` does not exist as a flag today; C1 adds it, wired to `names.Disabled()`.)

### A9. Times say their zone

Absolute times show their zone on hover; relative times are computed in the
browser; the record table and the timeline agree.

**Done when.** One formatter is used by every template (a test greps the
templates for ad-hoc formatting), and a test renders a message at 23:59 UTC in a
non-UTC browser zone and asserts the displayed date.

### A10. Palette, keys, themes, sizes

The palette catalogue comes from `/api/palette` including actions; the key map
is declared once and shown in the help overlay; light, dark and the third
(high-contrast) theme exist as tokens; the pages render at 1440, 960 and 390
widths.

**Done when.** The help overlay is generated from the declared key map (a test
fails for a key bound and not listed); `cssguard_test.go` covers the new tokens;
`narrow.spec.ts` covers 960 and 390 for each section's home page.

### A11. Contrast is measured

**Done when.** A unit test computes the contrast ratio of every text and
background token pair in all three themes and fails below 4.5:1 (3:1 for large
text), and the ratios printed on the Themes board are the computed ones.

### A12. The accessibility sweep

**Done when.** A crawl (axe-core through Playwright, ported from
`doze-aws/e2e/audit/crawl.ts`) visits every route in B8 in light and dark and
reports zero serious or critical violations.

---

## B. Evidence, test machinery and shared code

### B1. A claims table per stage

A file per stage under `conformance/claims/` listing each behaviour claim with
its source (`spec`, `recorded`, `doc`, `memory`) and the scenario that would
record it. The per-stage "recording queue" sections seed it.

**Done when.** The format exists, the Topics stage's table is filled in as the
worked example, and a test prints the count of claims per source.

### B2. The real-broker oracle

The registry has a recorded oracle; the broker has none. Docker is available, so
this is cheap to build and it is the most valuable item in the plan.

A scenario is a short Go script of requests (raw `kmsg` or franz-go). A recorder
runs it against `apache/kafka:4.0.0` (and Confluent SR 7.7.1 for the registry),
normalises the nondeterministic parts (broker ids, topic ids, timestamps,
producer ids, member ids), and writes `conformance/recordings/<scenario>.json`.
A replay test runs the same script against doze-kafka and compares.

**Done when.** The harness exists with five worked scenarios recorded and
replayed in CI without Docker: CreateTopics validation errors, Metadata with
auto-create, ListOffsets (earliest, latest, timestamp, max timestamp),
OffsetCommit/OffsetFetch across generations, and FindCoordinator for group and
transaction. Each scenario lists any difference it found, and each difference is
either fixed (with the red test) or written in `docs/limitations.md`.

### B3. Mutation per package

A mutation tool (gremlins) added as a `go tool`; `task mutate:record -- ./pkg/...`
stores counts and surviving mutants as `file:line:type` in
`testdata/mutation.json`; `task mutate:check` fails on a survivor the baseline
does not have. Packages: `internal/protocol`, `internal/storage/log`,
`internal/storage/meta`, `internal/broker` (split by file group for time),
`internal/traffic`, `internal/telemetry`, `schemaregistry`, `console`.

**Done when.** The baseline for every package above is recorded, and `check`
passes against it. A package that takes more than ten minutes is split.

### B4. A coverage floor that cannot fall

`task coverage` runs the suite with `-coverprofile`, compares each package with
`testdata/coverage.txt`, and fails if any fell by more than a tenth of a point.
Each stage raises the floor for its packages to the target in its document.

**Done when.** The task exists, the baseline is committed, CI runs it, and a
deliberately deleted test (in a scratch branch, noted in the commit) turns CI
red.

### B5. Crash and restart helpers

`startBroker(t, ...)` gains `Restart()` and `Kill()` (SIGKILL of a subprocess
build) so a stage's restart and crash tests are three lines. The chaos harness
and the persistence-surfaces test move onto it.

**Done when.** The helper is used by the existing restart tests and a new
subprocess kill test for one acknowledged `acks=all` write.

### B6. Nothing absent in the API

The version-matrix test checks every advertised API at its minimum, middle and
maximum version. The inverse is missing: a list of every API in the Kafka 4.0
protocol with its tier.

**Done when.** `docs/api-support.md` has one row for every API key in the
4.0 specification (about 90), with F, C or S, generated and checked by a test
against the `kmsg` table; every S row has its reason.

### B7. Settings tables the console can read

Per setting: name, unit, range or enum, default, whether it is applied or only
stored, what enforces it, and its source. Topic configs, broker flags, the
registry's levels and modes. Built lazily (idle heap unchanged, held by B9).

**Done when.** One accessor returns the record, `DescribeConfigs`,
`IncrementalAlterConfigs`, `CreateTopics` validation and the console form all
read it, and a test fails for a topic config the broker accepts that has no
record.

### B8. The route table and the route gate

Every console route is listed once, with method, whether it mutates, and the
test that drives it. The browser suite fails for a GET never visited or a POST
never driven, unless `e2e/route-gaps.txt` says why.

**Done when.** The table is the single source `console.go` registers from, the
gate runs in `test:e2e`, and the gap file is empty or every line has a reason.

### B9. Lightness

Idle heap after start, binary size, and the module list are budgets in
`testdata/lightness.json`. `task lightness` fails on growth.

**Done when.** The task exists with today's numbers, runs in CI, and the
commit that adds `protowire`, `protocompile` and `hamba/avro` (already in) is
explained in the file.

### B10. The public surface

`server/` is the embedding API. `task api` writes its exported identifiers and
fails on a change that is not in the commit's CHANGELOG line.

**Done when.** The task exists and `docs/embedding.md` compiles as an example
test.

### B11. A second identity in CI

**Done when.** `check` and `test:e2e` each have a CI pass with the listener on
`127.0.0.1:0`, a console prefix, a data directory with a space, auth on, and TLS
on (the combination `compat` already uses for its secure cells).

### B12. Every parser of untrusted bytes has a fuzz target

**Done when.** `docs/architecture.md` lists the parsers (frame, request header
and body, record batch and record reader, decompression, OTLP, SASL messages,
ACL filters, config values, registry REST bodies, schema texts, trace headers,
the console's form inputs) and each has a target in `task fuzz`; the weekly job
runs them all for at least 60 seconds each.

### B13. The design's world as a fixture

The e2e seed builds the "harbour" world of the boards (7 topics, 4 groups, 5
clients, 5 subjects, ACLs, an open transaction, a trace across three topics).

**Done when.** `e2e/seed` produces it deterministically and idempotently, and a
Playwright test per section asserts the first screen shows the counts the board
shows.

### B14. Documents are checked

**Done when.** A test fails for a relative link in `docs/` or `README.md` that
points nowhere, and for a command in `docs/getting-started.md` that does not run
against a built binary.

### B15. A fault seam

Failure tests today either mock a sync hook or rely on luck. The bad-days stage
needs the log, the metadata store and the whole binary to fail on command: a
write error, a sync error, a full volume, a slow sync. A build tag `faultinject`
adds the seam (absent from release builds); a helper makes a small size-limited
volume (tmpfs on Linux, `hdiutil` and `newfs_hfs` on macOS, as the bad-days
audit did by hand).

**Done when.** The seam exists behind the tag, the existing `BeforeSync` hook
is one case of it, a test fills a 12 MiB volume and asserts the broker keeps
reading and answers writes `KAFKA_STORAGE_ERROR`, and `go build` without the
tag contains none of it (a test greps the symbol table).

---

## C. The move to the latest `doze-names`

`doze-names` `main` is `d5a7201`. All three repositories (doze-kafka, doze-aws,
doze) pin `fcf6a9e`, one commit behind; the remote branch `zone-hardening` is
already contained in both and can go.

### C1. One pin, three repositories

`d5a7201` changes `pool.go` and the daemon test, not the API that callers use,
but the series since the old pin (one network daemon on macOS, `Lease.Twin`,
`Registry.Listen` by address, `OnThisMachine`, a daemon serving the home it was
installed for) is what doze-kafka's `names.go` was last compiled against.

**Done when.**
- [ ] All three repositories pin the same commit, with `go mod tidy` clean.
- [ ] doze-kafka's `names_windows.go` stub and `//go:build !windows` tag are gone
      if `doze-names` builds for Windows (`GOOS=windows go vet ./...` on all
      three); otherwise the stub stays and the reason is written beside it.
- [ ] doze-kafka has a `--zone` flag (and TOML key `zone`) that maps to
      `names.Disabled()`, honours `DOZE_ZONE=off`, and uses `Reachable()` before
      printing a by-name URL. (Today there is no such flag; the earlier plan
      listed it and it was not built.)
- [ ] The by-name path is tested end to end on this Mac under the daemon: the
      console at `kafka.doze`, the registry at `schema.<name>.doze`, and the
      second claim routed like the console. Where the daemon cannot run in CI,
      the test is skipped there with a reason and run locally.
- [ ] `check` is green in each repository, and doze-aws's uncommitted work is
      not touched (its change is `go.mod` and `go.sum` only, committed alone).

### C2. Tidy

**Done when.** The remote `zone-hardening` branch in `doze-names` is deleted
(it is an ancestor of `main`), and the three repositories' CHANGELOGs say which
`doze-names` they were built against.

---

## D. Cross-cutting pages, scheduled after the stages that feed them

| Piece | Board | Built from | Stage |
|---|---|---|---|
| The map, and the map at scale | Map, Map-Large | The A5 graph with lag on the edges | [map](stages/map.md) |
| Connect an app | Cn-01 to Cn-07 | The broker's own configuration (listeners, SASL, TLS, registry) | [connect](stages/connect.md) |
| The bad days | Bad-* | Recovery, locks, errors the broker already produces | [bad-days](stages/bad-days.md) |
| First run, palette, keys, themes | Sy-05 to Sy-08 | A10 | built here |

---

---

## E. Asks from the stages

Each stage document ended with what it needs the foundation to provide. They are
folded in here so that nothing is built twice. The stage that raised each is in
brackets.

| # | Ask | Folds into |
|---|---|---|
| E1 | Inspector kinds `member`, `event`, `transaction` besides message, request, client and subject; the key for a request is its row id, not a sequence number [groups, cluster, traffic] | A1 |
| E2 | `Refusal` kinds beyond Kafka error names: registry errors named by their numeric `error_code`, console-rule refusals with no Kafka name ("Refused by the console"), and the Traffic classification expected, refused, denied, failed [schemas, groups, traffic] | A2 |
| E3 | A raw-`kmsg` request path so a Refusal can carry the broker's `ErrorMessage`, which franz-go's producer drops [topics] | A2 |
| E4 | The dock contract `Events(Filter{Topic or Group or Subject}, limit, after)` with a cursor, accepting a non-Kafka source (registry requests) [traffic, schemas] | A4 |
| E5 | A5 renders its sentence from `Neighbours(kind, name)` on the graph type; a rail item **Map** under Observe [map] | A5, A1 |
| E6 | A6's table includes computed effects: "groups that lose offsets" for delete, "groups behind the new start" for purge [topics] | A6 |
| E7 | The health pill's states (Healthy, Writes failing, Not reachable, Recovered) as data [bad-days] | A7 |
| E8 | B7 holds: the broker defaults that differ from Kafka, `--max-partitions` readable by the console, the 16 settings the Cluster page shows with a provenance column, the cleaner settings, the registry levels and modes (IMPORT marked not implemented), the map's windows and caps, the group fields [topics, cluster, schemas, map, groups] | B7 |
| E9 | B8 rules: no route spawns a process; the new route rows from each stage [cluster, schemas, map] | B8 |
| E10 | B9 budgets: the recorder with 2,000 maximal rows, the storage-stats cache, the map aggregator allocating nothing before first use [traffic, cluster, map] | B9 |
| E11 | B10: `Server.Traffic()` and `ClientMetrics()` either on the pinned surface or removed from it [traffic] | B10 |
| E12 | B12: `FuzzConsumerProtocolDecode`, `FuzzGroupSequence`, and three registry targets [groups, schemas] | B12 |
| E13 | B13 seeds: five subjects with a gap in ids, a soft-deleted subject, a READONLY subject, a reference, a descriptor-registered Protobuf, one record carrying an unregistered id [schemas] | B13 |
| E14 | B14: error strings quoted in templates and docs equal coded or recorded strings; a `docs:check` generator slot [schemas, connect] | B14 |
| E15 | B5 gains `Kill()` and `Pause()` [topics, bad-days] | B5 |
| E16 | **B15, a fault seam** for the log, the store and the binary behind a `faultinject` build tag, with a size-limited volume helper (tmpfs on Linux, `hdiutil` on macOS) [bad-days] | new |
| E17 | B2's oracle needs a restart step and a step that sets broker configs [groups] | B2 |
| E18 | Shared packages: a `Facts` type that Cluster, Connect and `env` import; a Host guard used by the console and the registry; one bounded-scan helper used by Trace and Schemas; a `ClusterSource` and a `LogSource` hook in the console `Options` beside `Traffic` and `ClientMetrics` [cluster, connect, schemas, topics] | new |
| E19 | A decision: does `doze-kafka config` redact credentials? The output is a valid config file, so redaction breaks round-tripping [connect] | decision |

These do not change any "done when" above; they enlarge what A1, A2, A4 to A7
and B2 to B14 must hold.

## Acceptance criteria for stage 0

- [ ] A1 to A12 each meet their "done when", with the test named in the commit.
- [ ] B1 to B15 each meet their "done when".
- [ ] Every ask in E is either built or has a sentence saying where it landed instead.
- [ ] C1 and C2 meet theirs, in all three repositories.
- [ ] `go tool task check`, `coverage`, `mutate:check`, `lightness`, `api`,
      `test:e2e` (route gate on), `test:compat` (all 20 cells) are green on
      `main`, with the results recorded in the last commit of the stage.
- [ ] The existing console still works for every section not yet rebuilt: the
      73 browser tests pass, with only the edits that A1, A2 and A6 change on
      purpose (a drawer becomes the inspector; an error becomes a refusal).
- [ ] `CHANGELOG.md` lists each user-visible change.
- [ ] The first Real-broker scenarios (B2) have either found nothing or each
      difference is closed or written down.

## Files

Console: `console/templates/layout.html`, `panes.html`, `errors.html`,
`console/static/{app.css,shell.js}`, `console/console.go`, new
`console/refusal.go`, `console/routes.go`, `console/settings.go`. Test
machinery: `Taskfile.yml`, `testdata/`, `conformance/`, `scripts/`,
`.github/workflows/`. Names: `cmd/doze-kafka/names*.go`, `go.mod` in the three
repositories.

## Prior art

`doze-aws/plan/00-foundation.md` (the shell, mutation baselines, the claims
tables, the second-identity pass), `doze-aws/e2e/audit/crawl.ts` (the accessibility
sweep), and the Schema Registry's own recorded corpus in this repository
(`schemaregistry/testdata/conformance/`) for the oracle's shape.
