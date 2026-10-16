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
| Real-client matrix | 10 clients, plain and SASL/SCRAM over TLS: 20 of 20 cells pass (`compat/run.sh`; at the time of this baseline. Two telemetry clients were added since: 12 clients, 24 cells, `compat/runner_test.go` holds the count to the directories) |
| Browser suite | 11 specs, 73 tests (`e2e/tests/`) |
| Fuzz targets | 7: request parse and decode, tagged-field skipping, frame reading, the record reader, registry register-and-compare, registry payload decode, the telemetry decoder (`task fuzz`) |
| Differential oracle | **Confluent SR only** (about 1,500 schema pairs and 25 request scripts recorded). **There is no recording of a real Apache Kafka broker.** Every claim about broker error codes and edge cases is `read` or `memory`. |
| Mutation testing | not set up |
| Coverage floor in CI | not set up |
| Idle-heap and binary-size budget | not set up |
| `doze-names` | all three repositories pin `fcf6a9e`; `main` was ahead (`d5a7201`, then `dc00aee`) |

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
runs it against `apache/kafka:4.3.1` (and Confluent SR 7.7.1 for the registry),
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

A mutation tool; `task mutate:record -- <unit>` stores counts and surviving mutants
as `file:line:type` in `testdata/mutation.json`; `task mutate:check` fails on a
survivor the baseline does not have. (Built first on gremlins as a `go tool`, which
counted every mutant whose test run failed to start as killed: `--test-cpu 2`
reached `go test` as one argument and every run failed in setup, so every baseline
it recorded said zero lived and none was true. It is `scripts/mutate` now, with a
self-test that plants that fault; see `plan/evidence/mutation-survivors.md` for the
honest numbers.) Packages: `internal/protocol`, `internal/storage/log`,
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
maximum version. The inverse is missing: a list of every API in the Kafka 4.x
protocol with its tier.

**Done when.** `docs/api-support.md` has one row for every API key in the
4.x specification as the protocol library has it (`internal/kmsg`, franz-go
`kmsg` v1.13.1 patched, older than Kafka 4.3: keys 0 to 92, about 90), with F, C
or S, generated and checked by a test against the `kmsg` table; every S row has
its reason.

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

When this was written `doze-names` `main` was `d5a7201` and all three repositories
pinned `fcf6a9e`. `main` is now `dc00aee` (two commits after `d5a7201`: the daemon
records the home it serves, and looks at the registry again whenever a connection
opens), and all three repositories pin `dc00aee` (checked 2026-10-16:
`v0.1.1-0.20261010121557-dc00aeec3013` in each go.mod). The remote branch
`zone-hardening` (`a18b816`) is an ancestor of `main` and can go.

### C1. One pin, three repositories

`d5a7201` and `dc00aee` change the daemon and its tests, not the API that callers use,
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

Where the asks that stage 0 did not build landed (each is also in the owning stage
document's own list, "Foundation asks this stage builds", so it cannot be lost):

- **E5**: `Neighbours(kind, name)` on the graph type is built (`console/graph.go`)
  and A5's sentence renders from it. The rail item **Map** is not: the only page it
  would open is the map stage's, and a rail item to nothing is a dead end. It lands
  in [map](stages/map.md).
- **E7**: the health pill's states as data are not built: the pill has one state
  string ("Not reachable", `console/static/shell.js`), and Healthy, Writes failing
  and Recovered are only meaningful once the bad-days stage makes the broker report
  writes failing. It lands in [bad-days](stages/bad-days.md).
- **E10**: the recorder's budget of 2,000 maximal rows is held (the Traffic stage's
  bounds). The storage-stats cache lands in [cluster](stages/cluster.md) (its only
  reader is the Cluster page) and the map aggregator that allocates nothing before
  first use in [map](stages/map.md).
- **E14**: built: `docs/errors_test.go` holds the error names, messages and log lines
  the documents quote to the source, and `task docs:check` runs every document check.
  The templates' quoted strings are not checked yet: they are owned by the console
  stages and land with the next one that edits `console/templates`
  ([cluster](stages/cluster.md)).
- **E18**: the Host guard is built as `internal/hostguard`, used by the console and
  the schema registry listener (DNS rebinding against an unauthenticated registry was
  a safety gap today, so it was not deferred). The cluster `Facts` type that Cluster,
  Connect and `env` import lands in [connect](stages/connect.md), the bounded-scan
  helper in [trace](stages/trace.md) (Schemas reuses it), and the `ClusterSource` and
  `LogSource` hooks in the console `Options` in [cluster](stages/cluster.md) and
  [topics](stages/topics.md).

## Additions to the design

What the console has that the boards do not draw. Each is built, tested in
`e2e/tests/design.spec.ts` or the Go suite, and written down here for the owner to
add to the boards. None of them contradicts a board.

| # | Addition | Where | Why it is there |
|---|---|---|---|
| AD1 | A **filter field** at the top of every list pane ("Filter topics", "Filter groups", "Filter subjects"), narrowing the list as you type; `/` focuses it; Esc clears it. Sy-06 lists `/` as "filter" but no board draws the field. | Sy-01 part 2, Sy-05, Sy-06 | `/` has to put focus somewhere, and a broker with 40 topics needs it. |
| AD2 | **`[` and `]`** step to the previous and next tab of the page (Lag, Members, Timeline, Offsets; Messages, Partitions, Readers, Config). | Sy-06, Gr-02 | Gr-02's key strip draws `1`-`4` for the tabs and `r` for reset; the console binds `[` and `]` instead (they work on every page with tabs, whatever its count) and does not bind the digits or `r`. The group's reset is in the palette. |
| AD3 | **`?tab=`** in the address of a group or topic selects its tab, so a tab survives reload, Back and a pasted link; an unknown value falls back to the first tab. | Gr-02..Gr-08, Tp-05, Tp-16, Tp-18, Tp-23 | The boards link between tabs as separate screens; in the console they are one page, so the address has to say which. |
| AD4 | The Traffic summary is the board's three items ("313 requests in the last minute · 3 errors"), counted by the broker's own counters over the last twelve whole five-second buckets, a minute to the millisecond (they were twelve with the one filling, 55 to 60 seconds: A7), folded repeats counted one by one; its tooltip says from when to when. The errors say on hover that they are counted over that minute and that normal flow is not one. A paused page's missed count says "at least" when the folded rows' ticks do not reach back. | Main, Traf-06, Traf-08 | The board's "3 errors" does not say what the errors are counted over; counted over the whole ring it was false. |
| AD5 | A long result name (`UNKNOWN_TOPIC_OR_PARTITION`) **wraps** in the Result column after an underscore and is never cut, so its row is as tall as its lines (41 px for two); Request, Topic and Detail carry their full text as a tooltip. | Main, Traf-03, Traf-12 | Time, Result and Took must be whole; the boards cut a result to "UNKNOWN_TOPIC" or "GROUP_AUTH…", which is not the name. |
| AD6 | A request's duration is the boards' whole milliseconds ("0 ms" under half a millisecond, "2 ms", "3.0 s"), rounded, not cut (22.8 ms was "22 ms"); the measured time, to the microsecond, is the cell's tooltip ("took 42 µs"). | Main, Traf-03, Traf-05, Traf-12 | The boards print "0 ms"; a request answered from memory in 40 µs is "0 ms" on them too, and the tooltip says what was measured. |
| AD7 | At 390 a Traffic request is a **card**: request and how long, what it is about and how it went, the detail, then the time and the client. The connected-clients summary is not drawn until a row is selected. | Sy-04 | Sy-04 draws only Topics; this is the same rule (cards, one screen at a time) applied to Traffic. |
| AD8 | **No keys strip** at 390 (Sy-04 draws none); whole at 960 and 1440. | Sy-01, Sy-04 | A phone has no keys; at 390 the strip was cut off mid-word. |
| AD9 | The create-topic form **says a name is taken** while it is typed ("Names that already exist" names the first and how many others), and its shell command is rebuilt on the server as the form changes (it knows how `7d` and `1MB` are written for Kafka). | Tp-02 | Tp-02 draws the sentence; the live check and the server-built command are how it is made true. |
| AD10 | A topic's **neighbours** carry a note saying where "Written by" comes from (the recent-requests ring, and how old its oldest row is) and what "Read by" counts (groups that have committed; a consumer that assigns partitions itself has no group). The Topics table's "Read by" column says **"no group has committed"**, not "nobody". | Tp-Home, Tp-05, Tc-01 | "Nobody" was false for an `assign()` consumer, and "written by" was a number of unknown reach. |
| AD11 | The **replication factor** row says what this broker does with a factor above 1: accepts it and makes the topic with its one replica (`docs/limitations.md`); a single Apache Kafka broker would answer `INVALID_REPLICATION_FACTOR`. | Tp-02 | Tp-02 says the broker refuses it; it does not, deliberately, so an app written for a cluster starts unchanged. |
| AD12 | A group's neighbours name its **members** (client ids from the coordinator) and the subjects of the topics it reads. | Gr-02 | Gr-02 draws three wells; the console had one. |
| AD13 | The palette's Actions also hold **Pause or resume live updates**, **Show the keyboard shortcuts** and, on a broker that records traffic, **Show errors**. | Sy-05, Sy-06 | Each has a key (`p`, `?`, `e`) and a person who opens the palette to "do something" looks for them there; Sy-05 lists none of them. The four theme rows it used to carry are gone: the theme is the top bar's control. |
| AD14 | The Traffic summary's "N clients connected" is a **link** to the Clients view, and the Clients view's tabs (Requests, Clients N) appear only there, as Traf-06 draws them. | Main, Traf-06 | Main has no tabs and Traf-06 has two: the way from one to the other has to be somewhere, and the summary is where the number is. |
| AD15 | A request's **result name is searchable** in the one filter field, and "N of M requests match" counts requests (a folded row's repeats included) among those the recorder keeps; the line under the table says what that is: "Filtering looks at the last 2,000 rows kept in memory, M requests in all". | Traf-02 | Traf-02 draws "87 of 412 requests match" beside "Filtering looks at the last 2,000 requests kept in memory": the ring keeps 2,000 rows, a folded row stands for many requests, and the page said both (A9). |
| AD16 | A group filter shows the rebalance **story** by default, with "Show them as a plain list" (`?flat=1`) to get the table back. | Traf-12 | The board is the Traffic page with a group in the filter; a plain list of the same rows has to stay reachable. |
| AD17 | Keys **e** (errors only, and back), **u** (unfold), **m** (all of a client's metrics) and **c** (copy the decoded fields) on Traffic, in the key map and the help overlay; the strip at the foot is each board's own for the state the page is in (a filter, a request, a folded row, a client, paused: shell.js draws it as the state changes). | Traf-02 to Traf-12, Sy-06 | The boards' key strips draw them; Sy-06's table does not list them. |
| AD18 | The **Unfold** list, and the all-metrics list, open inside the inspector, not on a page of their own. A folded row of more repeats than it keeps says "Unfold the last 64" (it keeps the last 64), not "Unfold these N". | Traf-05, Traf-07 | The boards name "Unfold these 42" and "m all 136 metrics" and draw no screen for them; "Unfold these 294" listed 64 (A9). |
| AD19 | A telemetry client's inspector says how many reports **came too fast to be read** and how many series were **not kept**. | Traf-07 | The store's bounds are real and a count that silently stops is the kind of thing that costs an afternoon. |
| AD20 | The wire dock has **Older** and **Newest** at its foot when there is more than it shows, and says "older requests that touched …" on an older page; a topic's or group's dock also links to the Traffic page filtered to it. | Sy-01 part 8 | The board draws three rows and no end. A4's contract has a cursor, which the endpoint never passed on (the paging half of `DockEvents` was dead); a subject's dock has no Traffic page to send anyone to, so the cursor is its only way back. |
| AD21 | A Traffic selection's head has a **close** control, out of flow in its top right corner (the head is the board's size). | Traf-04, Traf-05, Traf-07, Traf-08, Traf-11 | The boards draw none, and Esc closes; a pointer has no Esc. |
| AD22 | When more rows match than the table lists, its last row says so: "These are the newest 200 of N matching rows. Narrow the filter to see the others." | Main, Traf-02, Traf-03 | The boards have no case of a table cut short, and a silent cut would read as "that is everything". |
| AD23 | A client of more than one connection says how many: "· 3 connections" after its role in the connected-clients panel, and "Its 3 connections" (address, open for, last, requests) in its inspector. | Main, Traf-07 | The boards' clients have one connection each; one app often holds several (a group coordinator, a fetch and a produce connection), and the page counts apps. |

## Deviations from the boards

Where the console is not the board, and who owns closing the gap. Three verdicts:
**MATCHES**, **DEVIATES** (what differs, and the stage that owns it) and **CUT** (not
built, because the board shows something this broker does not know, or a board of
notes). For the boards the design gate compares (`e2e/design`: Sy-01 to Sy-08,
the six section homes and Tp-24, at 1440 in light and dark; Sy-03 at 960, Sy-04 at
390, and every home at both), the verdict is the gate's: every difference it measures
is on an allow-list (`e2e/design/shell/allow.ts`, `e2e/design/homes/allow.ts`) as
the board's value, the console's and the reason, an entry that stops matching fails,
and the rows below say those reasons in words. Where the boards disagree with one
another the console draws one of them, and the gate measures that the other board
does give that value (`per`). The other boards' verdicts come from the known gaps and
from what `design.spec.ts` holds; they have not been compared element by element:
that is the board review in [90-integration.md](90-integration.md), section 2.

Not built because the broker lacks the data: **a partition's size and segments**
(Tp-16, Tp-18, Tp-30; a topic's size on Tp-Home is its files in the data directory,
read from disk), the **registry wire dock** (the dock under every Schemas board),
**Copy as curl** (Tp-06, Sy-02, Sy-04) and the **tape lanes** (Tp-21). Owned by
later stages: **Ac-04, Ac-06, Ac-08 and Ac-09** to [access](stages/access.md) (Create
ACL and the checker, Ac-03 and Ac-05, are built), the **Cluster sub-pages** to
[cluster](stages/cluster.md), **Cn-02 to Cn-07** to [connect](stages/connect.md),
and the **Tp-02 extras** (cleanup policy and the "new" tags in Tp-03) to
[topics](stages/topics.md).

| Board | Verdict | Note |
|---|---|---|
| Sy-01 Anatomy | DEVIATES | `shell/anatomy.spec.ts`, light and dark, Sy-01's numbered markers taken out (annotations). Where Sy-01 and the topic page's own boards disagree the console draws Tp-05's and Tp-06's, and the gate measures each on that board: the page padded 16 20 0 with an 18 px title and an 8 px gap (Sy-01's specimen is 14 18 0, 16 px, 10), the list head "Topics · N · messages" with + New over a rule (and AD1's filter under it), list items with no underline, the count in the Partitions and Readers tabs, a record's value in ink, the wire's 24 px rows and its 103 px (its head and three rows). Beside an open inspector the wire is Tp-06's five columns (Tp-05's six need 634 px of 576). The records are a table, so a screen reader walks them: the board's row fill, rule and header type are on its cells (checked), and its 14 px gaps are each cell's padding; the offset track is 72 px (the Messages table row). Between the tabs and the records are Tp-05's tape and Tp-14's search and jumps, which Sy-01 does not draw. The wire is the page's own row under main (one grid for every page). The theme switch is in the top bar (Sy-07); the TP and TR chips' letters are black (below). |
| Sy-02 Dark | DEVIATES | `shell/anatomy.spec.ts`, dark, a traced record open; as Sy-01. Where Sy-02 and Sy-01 disagree (the keys strip, the list's foot note, the value's track) the console draws Sy-01's, checked on Sy-01. The inspector's head is a row holding Tp-06's older, newer and close; the line under its title is Tp-06's (the topic, in mono); the value's caption shares a row with Tp-06's Text, Hex and Base64 and reads "Value · 101 B · JSON" as Tp-06 does. "Matches schema" is drawn for a record written with a schema (the gate checks it on seed-events; the traced orders were not). "Copy as curl" CUT (Tp-06). |
| Sy-03 Tablet | DEVIATES | `shell/narrow.spec.ts` at 960, light and dark. The message is the inspector drawn as a card in its own row under main (the same panel at every width, with Sy-03's "esc" and its title in mono), its fields and its value sharing the card equally (the board's code block takes its own 26 px of padding over its share); the field names' column is 84 px at every width (Sy-02 and Tp-06; Sy-03 draws 70, Sy-04 64); the wire is Tp-05's six columns, which fit at 960 (Sy-03 draws four). Two of the board's sentences are notes on the design ("stacked here at this width"; "Try it (produce) is the first row of the next screen") and are not drawn. The back button's arrow and word are two parts (the word changes at 390), so its gap is none. |
| Sy-04 Phone | DEVIATES | `shell/narrow.spec.ts` at 390, light and dark. The sheet is the board's: 400 of 844, its border on every side, "Close", its title in mono, 14 px in, 8 px between its parts, over the board's 0.28 scrim with its 36 x 4 grab handle (both checked against the board: the scrim is the sheet's own shadow and the handle its `::before`). Its head and body carry the padding, so the record scrolls under a head that stays; its actions are Tp-06's (Send again, Edit a copy…, Download value) beside Follow this trace, two rows at the foot (Copy as curl CUT). A card is the table's row as a grid, 88 px as the board's, the open one ringed in ink; the tabs scroll in their own box; the jump button keeps its words for a screen reader, clipped; the broker's dot is inside the pill. No keys strip (AD8). The page above the sheet: the neighbours are one strip that scrolls sideways, the tape's marks are not drawn, and the search and the jumps come after the cards. Owner: topics. |
| Sy-05 Palette | DEVIATES | `shell/dialogs.spec.ts`, "ord" typed on a topic, light and dark. Centred in an overlay with the board's 70 px above it (the board places it absolutely). The palette has Topics, Groups, Subjects and Actions and no "Go to"; a subject says its versions and level ("Avro · v1-2 · BACKWARD") where the registry holds 25 or fewer; Actions add Pause or resume, the shortcuts and Show errors (AD13). The match in the chosen result is drawn with Plex Mono's 700 face, as the board's "bolder" (900) is: the face ends at 700. The strip while it is open is the board's ⌘K. |
| Sy-06 Keys | DEVIATES | `shell/dialogs.spec.ts` over Traffic, light and dark. The sheet starts 90 px down and is 900 wide in three columns, as the board's; its keys sit inline 6 px apart, as the board's `.k`. It is 536 px tall, not 372: On this page lists the keys the console binds that the board predates, each working on its page (strip.spec presses them): `r` (retry while the broker is lost, A7), `e`, `u`, `m` and `c` (copy the decoded fields) on Traffic (AD17), and `⌘↵` (send from the composer; create on a create form); and Create also names Access, an empty Traffic and Cluster. They are in the board's order, this help last. `[` and `]` are the board's own (Next tab, Previous tab). The foot is one sentence on what `p` and `g` do, where the board's is a note that the key layer is proposed. The strip while it is open is the board's `?` and `⌘K`. An earlier version of this row said the sheet added `[` and `]` and a foot the board lacks: the board has both. |
| Sy-07 Themes | DEVIATES | Not a page: `shell/themes.spec.ts` holds the switch to the board's, part by part; every token of the light, dark and high contrast sets to the board's; every ratio the board prints, recomputed from the console's tokens; the board's specimens drawn by the console's stylesheet in each theme; and the 2 px outline on every kind of selection in high contrast (a table row's too). The specimen sheet sets every border to `--line` (its own `.sp` rule); the console draws `con.css`'s, which every other board uses: the primary button's border in ink, the delete's in red, the ghost's and a coloured callout's transparent. High contrast draws its edges 1 px, as the board's set does. |
| Main Traffic | DEVIATES | Measured by the design gate (`e2e/design`, `go tool task design`) against the board's own render, element by element, light and dark; every difference is on its allow-list with one of these reasons. **Columns.** The board's own tracks (92 150 118 1fr 190 84 52) need 906 px and the card beside the 400 px inspector is 802 at 1440: its own render draws the topic 8 px wide. The page's tracks are Time 92, Request 130, Result 118 and Took 52, whole at every width; Client and Detail are the board's 150 and 190 where they fit and give way equally down to what is left once the topic has 96, and the topic takes the rest as the board's 1fr: 103, 96 and 103 px at 1440 beside the inspector, the board's 150 and 190 from 1600. Request is 130 because `FindCoordinator v4` is 130 px at the board's 12 px mono; Result is 118 because a name wraps only after an underscore and the longest run between two underscores of any Kafka error name is `DESERIALIZATION_`, 116 px (an e2e test measures every name, `e2e/errnames`); a row whose result wraps is as tall as its lines (AD5). **Colour.** A refusal Kafka would also make is purple, its row tinted --denybg, as Traf-03's own note says ("refusals that Kafka would also make are purple, and anything red is the broker unhappy"); the board tints its refused Produce red. **Selection.** The board draws its first row selected while its panel is the open connections; the page draws a row selected only when it is open. **The connected clients.** A client's first line is the header's three tracks (Client, Open for, Idle: 1fr 70 56, the row's 14 px gap): the board's row draws one value at the right, under Idle, which is its open-for; the page draws each under its heading. **Widths no board draws.** Below 1400 px the page's own summary beside the table (the open connections, who touched a topic, a group's timeline) waits for a selection, as at 390 (AD7), so the table keeps its columns; with a selection open beside it, Main draws no Detail below 1400 and no Client below 1300 (the inspector says both). The kinds control (all, errors, produce, fetch, groups, admin) is cut at 1440 beside the 400 px inspector in the board's own render (its flex shrink gives it 267.8 px for words that need 302.7: "admin" is clipped) and the page draws the same, because the gate holds it equal; below 1440, where no board is drawn, the control keeps its words and the field gives way. Below 1100 (Sy-03) the inspector is a card under the page and the table has the width. |
| Traf-02 Filtered | DEVIATES | Measured by the design gate. Main's columns and colours (above). The line under the table says what the filter looks at in rows and requests (AD15). A filter's value is cut with an ellipsis where it is longer than its pill can be (the board's sample fits; a topic name may be 249 characters), with the whole of it as the pill's tooltip. The pills, "N of M requests match", the "Who touched" panel with its five rows, "Open the topic" (drawn when the topic exists), and "esc clear filter", which clears it. Its Consumers are the clients that fetched (a client that only committed is not a consumer of the topic). |
| Traf-03 Errors | DEVIATES | Measured by the design gate. The board draws `UNKNOWN_TOPIC_OR_PARTITION` red beside its own note that "refusals that Kafka would also make are purple, and anything red is the broker unhappy"; the console follows the note and A2: refused, denied and expected are purple, not implemented amber, only failed red. A result name wraps (AD5). The heading is the board's words where the code says what ("Produce to a topic that does not exist", "FindCoordinator for a group the principal may not use") and the kind of no otherwise; "Nearest topic", "Create ..." and "See the N topics that exist" are drawn for a topic that does not exist. The line under the table says "rows kept" where the board says "requests kept" (AD15). |
| Traf-04 Request | DEVIATES | Measured by the design gate. Main's columns and colours. The decoded fields are the board's: the specification's names in snake case (`transactional_id`, `timeout_ms`, `topic_data`, `partition_data`, `index`). The head has a close control (AD21). "Follow in Topics" is drawn when the topic exists (a refused Produce named one that does not: A8). A request with no per-partition table (a Metadata, a FindCoordinator) lists its Topics and its Detail in the facts, which the board's Produce has in the table. Sizes are the bytes on the wire, length prefix included, as the client counts them. The partition table's base offset column is "Offset" for a fetch or a commit. |
| Traf-05 Folded | DEVIATES | Measured by the design gate. Main's columns and colours. A fold breaks on any change of the request, its generation included, and does not resume after a different answer (A4), so "Generation: generation 7 throughout" is the board's row and true; the strip is the last 64 repeats and says so, and "Unfold the last 64" where there were more (AD18). The head has a close control (AD21). |
| Traf-06 Clients | DEVIATES | Was CUT; built as `?view=clients` and measured by the design gate. The key strip's `g c` is not bound (it is Cluster in Sy-06). "Needs a look" is a rule: three errors in a row, the client's own requests in the order they came across its connections (A2), or three or more in five minutes making up at least half of what it asked; the callout's word is the class of the no ("denied" for an ACL's, A2 of this document; the board's sample says "refused" of GROUP_AUTHORIZATION_FAILED), and a client raised by the five-minute rule is told by its count. The foot sentence names the one client with errors that does not need a look, by count and its latest error, as the board's does. The board's "5 clients" are apps, and an app often holds several connections: the page counts clients by client id (the summary, the tab, the rail's "N open", the table), a client with more than one says so in its row's tooltip and its inspector lists each connection (AD23). The Clients view draws no Connected-clients panel until a client is opened, as the board draws none. When the broker is lost the table draws the same frozen line under its header as the others, and its rows are muted as the requests' are (see Traf-10). |
| Traf-07 Client | DEVIATES | Measured by the design gate. A label is a metric's own whole name in words, for the names the boards name ("Request latency, average" for `request.latency.avg`; `record.error.rate` is "Record error rate", not "Request error rate": A3); any other metric is its short name. A value carries the unit the client sent and none it did not. The bars are one a report, the last twelve, and the note says what they cover ("the last minute" when twelve reports five seconds apart do). "Worth a look first" is drawn when the client reports an error or a retry rate, at zero too. A client of several connections lists them (AD23); the head has a close control (AD21). |
| Traf-08 Paused | DEVIATES | Measured by the design gate. Main's columns and colours. The count comes from the broker, by time, from the newest row shown (with a typed filter too: A5), and is "at least N" when the folded rows' ticks do not reach back; a paused clients view counts from when it paused. A paused page asks the broker for that count every 2.5 s, and so notices when it has gone (B3). The head has a close control (AD21). |
| Traf-09 Empty | DEVIATES | Adds the time the broker began and that a restart empties the page (the board's two wordings, "no app yet" and "restarted", are one sentence the page can say truthfully). The SASL sentence names the listener's real mechanisms. The filter row, the kinds, the connected-clients panel and the "Select a row" note are not drawn when there is nothing to list (the board draws none of them). The keys strip is `c connect an app`, `p pause`, `⌘K`, as the board's: `c` is also "copy the decoded fields" (AD17), and the shell resolves it by context (an element marked `data-create` on the page, which only the empty state has, wins; with a request open it copies). Two extras remain: the time the broker began, and the sentence that a restart empties the page; while the console cannot reach the broker the card says so instead of "up and listening". |
| Traf-10 Disconnected | DEVIATES | The note's link "What to check when the broker is down" is not drawn: the Bad-Down page is the bad-days stage's. The retry is every five seconds (the shell backs off from 2.5). `r` is Retry now (key map, strip and the A-key tests), and the strip while lost is the board's `r`, `j k`, `⌘K`. The summary line and the connected-clients panel are not drawn while the broker is lost (what they count is not known); only the rows are marked as stale, not the header or the frozen line, as the board draws it; the frozen line is the row under the header, as the board draws it. The board marks the stale rows by dimming them to .55 opacity, and their text is then under WCAG's 4.5:1; the page mutes them instead: the rows stay at full opacity and their text takes the muted ink (`--mut`: rgb(86, 91, 99) in light, rgb(163, 169, 176) in dark), as an aborted transaction's rows do, so the board's opacity (.55 against 1) and its ink (the board's own text colours, faded, against the muted ink) differ, and the accessibility sweep leaves nothing out: it checks the lost page in light, dark and high contrast (`e2e/tests/disconnected.spec.ts`). The top bar's pill is the board's: red-tinted, `127.0.0.1:N not reachable`, with the time of the last good answer as its tooltip; Retry is the page header's ("Retry now"), and a page that has none (a topic's tape) gets a small Retry beside the pill, not inside it, so the Jump field keeps its 420. |
| Traf-11 Metrics-None | DEVIATES | "The broker asked for them and got none" is false (the client asks); the page says whether the client has asked and sent nothing or has not asked. "See a client that reports" appears only when one is connected. |
| Traf-12 Rebalance story | DEVIATES | Measured by the design gate. The Result track is Main's 118 (AD5), not the board's 80, which cuts names. A KIP-848 group's table is "Epochs" and its rebalances are one instant each. The group forming is a rebalance too. What a request did is said from what it and its answer say: "new member asks in, no member id yet" (a JoinGroup answered MEMBER_ID_REQUIRED, which is its result, not the board's "ok"), "existing member told to rejoin" (a heartbeat answered REBALANCE_IN_PROGRESS), "generation 7 accepted", "harbour.depot.dispatch p0–p2, generation 7"; the board's "answer: leader elected, 3 members" is not a row of its own, because it is no request (the JoinGroup's answer is that JoinGroup's row). The strip's `g group by rebalance` is not bound: the story is what a group filter shows, and "Show them as a plain list" is the way out; `esc` clears the filter as the board's. "Real Kafka does the same: any join in the classic protocol makes everyone rejoin" is said under a rebalance a join began, and not under one an expiry or a leave began (queue R8, recorded). The requests that bring a member in go under the earliest rebalance that begins within 5 s after them, a row is placed by when it was last seen, and rows inside a rebalance are in the order they happened. |
| Sy-01, Sy-02 and Sy-05 specimen values | DEVIATES | These three boards draw their specimen topic page with the inspector at 420, the page padded 14 18 0 and its h1 at 16 px; the page boards draw 16 20 0, and the console does too on every page (`design.spec.ts` holds each section's). The gap is each board's own: 10 on a page of parts (Main, Traf-02 to Traf-08 and Traf-10 to Traf-12, Gr-02, Sc-05, Ac-02), 12 on a home or an empty state (Tp-Home, Cl-Home, Tc-Home, Traf-09), 8 on a topic (Tp-05); the bottom padding is 0 as drawn. The inspector is 400 (420 for a message, as Tp-06 and Sy-01's specimen). A title is IBM Plex Sans 600 at 18 px: the boards' `.h1` follows `.m` in `con.css`, so `h1.h1.m` renders sans (the console drew mono); the page boards draw 18 (Tp-05, Tp-06, Gr-02, Sc-05), Tp-14 to Tp-24 17 and the three specimens 16, and the console follows the page boards. |
| Section chips TP and TR | DEVIATES | Fills are the boards' (#2f6bff, #c85f0a); the letters are black, not white, because white is 4.499:1 and 4.12:1 and the console holds text to 4.5:1. The other five chips keep white. |
| Sy-05 Follow a message by trace id | DEVIATES | The board hints `g r`, which is Traffic's chord in Sy-06; the row has no hint, and the "new" tag is a board annotation, not drawn. |
| Link buttons | DEVIATES | The boards draw a link as a button at `.btn`'s 500 (`.app button { font: inherit }` sets a `<button>` back to 400): the console draws the links the boards draw as links (Create topic, Connect an app, Create ACL, Register schema, Back to the list, Check first, the first run's three) at 500, and every other `.btn` at 400, so one row never mixes weights. |
| Tp-05 wire, and the docks of Gr and Tp boards | MATCHES | Six columns: time 92, client 120, request 100, detail 1fr (min 120), result 60, took 48. A result name longer than 60 px is cut with its full name on hover; the Traffic table wraps it. |
| Tp-Home | DEVIATES | `homes/homes.spec.ts` (and `data.spec.ts`: every count and size is the broker's). AD1's filter above the list; the list items have no underline (the boards disagree: Tp-Home and Tp-05 against Gr-Home, Sc-Home and Ac-Home; the console draws every list pane one way); the foot note names this broker's data directory; the caption counts the seed's topics. |
| Tp-01 None | MATCHES | Tp-01 reads "This broker has no topics." and says what the data directory holds; it does not say "This is a new data directory", which the console cannot know. |
| Tp-02 Create, Tp-03 Create settings | DEVIATES | Settings are always open, not behind "Set…"; no `cleanup.policy` field and no "new" tags; the replication row differs (AD11). Owner: topics. |
| Tp-04 Create refused | MATCHES | |
| Tp-05 Messages, Tp-26 Live paused, Tp-27 Empty topic | DEVIATES | The tape draws without lanes. Owner: topics, once the broker can say per-partition rate. |
| Tp-06 Message | DEVIATES | No "Copy as curl". CUT, not owned: a REST-shaped call has no meaning here. |
| Tp-07 Binary, Tp-08 Schema, Tp-09 Tombstone, Tp-10 to Tp-15 produce, resend, jumps | MATCHES | |
| Tp-16 Partitions | DEVIATES | Offsets and leaders; size and segments are CUT (broker lacks the data). Owner: topics. |
| Tp-17 Add partitions | DEVIATES | The form and the refusals; not the p0 p1 p2 → p3 diagram or the "who picks them up" prose. Owner: topics. |
| Tp-18 Config, Tp-19 Edit a key | DEVIATES | A form per setting; no before/after and no "what it affects" panel. Segment and size columns CUT. Owner: topics. |
| Tp-20 Refused, Tp-22 Txn, Tp-23 Readers, Tp-25 Denied, Tp-28 to Tp-30 | MATCHES | Tp-30 loses the segment line (CUT). |
| Tp-24 Confirms | DEVIATES | `homes/confirms.spec.ts`: the purge and the delete, each against its own specimen, and what each says held to what the broker holds. The board draws the two side by side over a scrim as specimens; the console opens one at a time, centred in an overlay with the board's 150 px above it. |
| Tp-21 Compacted | DEVIATES | Compact is a pill and a note; the lane drawing is CUT. |
| Gr-Home | DEVIATES | `homes/homes.spec.ts` and `data.spec.ts`. AD1's filter; the list items' underline as Tp-Home's; the first group's state and lag are the seed's (Empty, 39 behind) where the board's sample is Stable and 0. |
| Gr-01, Gr-02 Lag, Gr-03 Members, Gr-04 Timeline, Gr-05 Rebalancing, Gr-06 to Gr-11, Gr-15, Gr-16, Gr-18 | MATCHES | Gr-02 has Try it and the three neighbours (AD12); its key strip's `1`-`4` and `r` are not bound (AD2). |
| Gr-12 KIP-848, Gr-13 Classic, Gr-14 Read committed, Gr-17 Flapping | DEVIATES | The protocol is a pill and the timeline tells the story; the board's side-by-side explanation of the two protocols and of read-committed lag is not built. Owner: groups. |
| Sc-Home | DEVIATES | `homes/homes.spec.ts` and `data.spec.ts`. **The registry wire dock is CUT** (the registry's requests are not in the recorder). The ··· is not drawn on a home (Delete lives under ···, and a home has nothing to delete), so the title's column is wider; the strip binds `c` (Sy-06), not the `n` Sc-Home names; the board's first row is drawn selected as a sample and on a home nothing is; the registry's address is this broker's. |
| Sc-01 to Sc-04, Sc-05 to Sc-11, Sc-13 to Sc-18 | DEVIATES | Everything but the dock: **the registry wire dock is CUT**. Owner: schemas, with the E4 dock contract. |
| Sc-12 Confirms | MATCHES | |
| Tc-Home | DEVIATES | `homes/homes.spec.ts`. The recent records that carry a trace header are built (newest first). The board's rows carry no tracks (only its header sets them), so its own render stacks each row's cells in one column; the console gives the rows the header's. The board's page is taller than its frame and its column shrinks the cards to fit; the console's page scrolls. The board's first row is drawn selected as a sample. |
| Tc-01 to Tc-08 | MATCHES | |
| Ac-Home | DEVIATES | `homes/homes.spec.ts` and `data.spec.ts` (every ACL is the broker's). The wire is the page's own row under main (the board draws it inside main with negative margins); its rows are the ACL requests and refusals the broker keeps (the gate's run has none by the time it reads); Create ACL carries no "new" mark (an annotation); the seed has 6 ACLs to the board's 5. |
| Ac-01 None, Ac-02 ACLs, Ac-07 Principals | MATCHES | Type, operation and permission as the board writes them. |
| Ac-03 Create, Ac-05 Checker | MATCHES | Built (F6): the fields CreateAcls has, the command and what it will do as the form changes; the checker by the rules the broker enforces (a test asks 2 x 90 questions of it and of a real SASL broker, and they agree). Not compared element by element. |
| Ac-04 Create refused, Ac-06 Denied log, Ac-08 Default, Ac-09 Confirms | DEVIATES | Not built. Owner: access. |
| Cl-Home, Cl-01 to Cl-09 | DEVIATES | One Cluster page with its sections; the board's sub-pages (config, APIs, limits, storage, transactions, compaction, `.doze` names, listeners) are not separate pages. Owner: cluster. |
| Cn-01 Bootstrap | MATCHES | |
| Cn-02 to Cn-07 | DEVIATES | Not built. Owner: connect. |
| Bad-Auth, Bad-Disk, Bad-Down, Bad-Fenced, Bad-Lock, Bad-Port, Bad-Rebalance-Storm, Bad-Recovered, Bad-Slow | DEVIATES | The disconnected state (A7) is built; the nine bad-day pages are not. Owner: bad-days. |
| Map, Map-Large | DEVIATES | Not built. Owner: map. |
| Messages table (Sy-01, Sy-02, Sy-03, Sy-05, Tp-24 against Tp-05) | DEVIATES | The console draws Sy-01's table: P, Offset, Key, Value, Age in 28 px rows with 12 px 400 mono cells; it is a `<table>`, and the board's row fill, rule and header type are on its cells (`shell/anatomy.spec.ts` checks them). Tp-05, the topic's own board, draws another: Offset, P, Time, Key, Value, Hdr. The time of day is the Age's tooltip (and the inspector's Timestamp), the number of headers is the Value's tooltip (the inspector lists them), the offset track is 72 px where the board's 62 holds `4,789` and not `1,234,567`, and the age counts as the board's `3 s` and in days after two. Owner: topics, to choose between the two boards. |
| Sy-08 First run | DEVIATES | `shell/firstrun.spec.ts`, on an empty broker of its own, light and dark. Copy kept because the board's would be false for this build: the command's topic is `orders`, not the specimen's `harbour.checkout.orders`; step 2's button is "Settings for your client" / "the values, filled in" where the board has "Settings for your language" / "Java, librdkafka, franz-go" (the Connect page is a client.properties and an environment, not a page per language: Cn-02 to Cn-07 are the connect stage's); the foot reads "The connection settings are filled in for this broker" where the board has "The bootstrap guide walks through one topic, one producer and one group" (there is no such guide). The steps' numbers are in the theme's ink: the board's are its marker style, #17191c in any theme, 1.03:1 on the dark card. Step 2's facts are the board's two columns; an address longer than its column scrolls in its own box (focusable) and is never broken mid-word, and below 320 px the label goes above the value. Owner: connect. |
| Cl-Home | DEVIATES | `homes/homes.spec.ts` and `data.spec.ts`. The row above. Built from what the broker has: the sentence carries this broker's address, uptime and counts (on two lines, so what follows is lower); "Compacted partitions" (the partitions of the topics whose `cleanup.policy` says compact; the cleaner's own status is CLU-24) and "What this broker is not" (single node from DescribeCluster listing one broker, then what `docs/limitations.md` states; "All limits" is that document until Cl-03 exists). The strip adds `j` `k` (they move the cursor that `↵` opens). The board's second foot sentence says what Kafbat UI shows, a claim about another program this project has not recorded; the console keeps the first. **CUT: the wire dock** ("The wire, underneath: what this page asked the broker"): the recorder drops the console's own requests at the door (`isConsole` in `internal/traffic/traffic.go`), so no source exists; assigned to the cluster stage as CLU-32 (a per-request marker and a side ring). Owner: cluster. |
| Grounding | CUT | A board of notes about what is real, new and removed; there is no page to build. |

## Acceptance criteria for stage 0

- [ ] A1 to A12 each meet their "done when", with the test named in the commit.
- [ ] B1 to B15 each meet their "done when".
- [ ] Every ask in E is either built or has a sentence saying where it landed instead.
- [ ] C1 and C2 meet theirs, in all three repositories.
- [ ] `go tool task check`, `coverage`, `mutate:check`, `lightness`, `api`,
      `test:e2e` (route gate on), `test:compat` (all 24 cells) are green on
      `main`, with the results recorded in the last commit of the stage by
      `task gate-report -- --slow` (`scripts/gate-report.sh` writes
      `plan/evidence/gates-<date>.md`; the commit carries that file).
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
