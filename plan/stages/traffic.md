# Traffic

Stage 2 of the Observe group. ID prefix `TRF`. Size L. Follows
[../01-protocol.md](../01-protocol.md); the common acceptance criteria there
apply in full, and this page adds Traffic's own. Foundation items are cited as
`A4`, `B3` and so on ([../00-foundation.md](../00-foundation.md)).

## 1. Summary

Traffic is the page that answers "what did my app just do?" It is the one
surface that is not about Kafka data: it is the broker's own memory of the last
requests, the open connections, the group coordinators' decisions, and the
metrics clients push about themselves (KIP-714). Everything in it is derived at
runtime and kept in memory.

What exists today (`console/traffic.go`, `console/metrics.go`,
`console/templates/traffic.html`, `e2e/tests/traffic.spec.ts`):

- A recorder (`internal/traffic`): a ring of 2,000 rows plus 2,000 group
  events, fed by a `protocol.Observer` hook in the connection loop
  (`internal/protocol/conn.go`) and a `GroupEventSink` in the coordinators
  (`internal/broker/groupevents.go`). Idle repeats (a heartbeat that went well,
  a fetch that returned nothing) fold into one row with a count. The console's
  own client ids (prefix `doze-kafka-console`) are left out.
- A summariser per API (`internal/protocol/summary.go`, wired by the
  `Summary` field of the API table in `versions.go`) and an error-code finder
  that walks the response by reflection (`firstError`).
- A page with a filter form (client, request, topic, group, errors only), the
  connected clients, the request tail (200 rows), a drawer for one request, a
  polled region, and the Client metrics cards.
- KIP-714 (`internal/telemetry`, `internal/broker/telemetry.go`): APIs 71 and
  72, a hand-written OTLP decoder, a bounded store (200 instances, 400 series,
  120 samples), sparklines.

What the design adds (12 boards: Main and `Traf-02` to `Traf-12`): a header
with counters (clients, requests in the last minute, errors), filter chips by
kind, a per-resource filter, a "who touched this topic" panel, an errors view
with a plain-words explainer, an inspector with decoded fields, a folded-row
view with an unfold action, a client table with requests per minute and an
error rate, a per-client page with the metrics that matter, paused, empty and
disconnected states, a "why this client has no metrics" page, and a rebalance
story that interleaves a group's requests with its generations.

Honest size: the screens are the smaller half. The recorder keeps a row and a
count; most of the new boards need counters over a window, stable row
identity, a classification of error codes, richer per-API facts, a link from a
connection to its telemetry instance, and a per-group story. That is new
backend work (section 6 lists each piece), and it comes with a recorder that
must stay light. Reading the code also found four unbounded-input defects that
are fixed first (TRF-1 to TRF-4).

**Packages this stage owns:** `internal/protocol` (`observe.go`, `summary.go`,
the observer and routing wiring in `conn.go`, the `Summary` field in
`versions.go`), `internal/traffic`, `internal/telemetry`,
`internal/broker/telemetry.go` and `groupevents.go`, `console/traffic.go`,
`console/metrics.go`, `console/templates/traffic.html`,
`cmd/doze-kafka/traffic.go`, `internal/storage/log` (`Decompress`), and APIs 71
and 72.

**Peers.** The group dock on [groups](groups.md) and the topic dock on topics
use the recorder's per-resource filter (A4). The [map](map.md) reuses the
client to topic and group edges (A5). [Trace](trace.md) shares the live and
pause behaviour. [Access](access.md) shows principals that this stage records.
The empty state links to Connect (foundation D).

**Estimate:** L, about 20 focused days (40 half-days, section 11).
**Actual:** one session; not measured in days (see [the status of this pass](#status-of-this-pass)).

## 2. Contract

### Kafka APIs

Traffic has no APIs of its own. It observes all of them (the `Summary` field
exists for every row of the table), and it owns two:

| API | Key | Now | Target | Note |
|---|---|---|---|---|
| GetTelemetrySubscriptions | 71 | F | F | KIP-714. v0 only, Kafka 4 profile only, advertised only with `--client-telemetry` (default on). The subscription is fixed: every metric whose name starts `org.apache.kafka.`, every 5 s, cumulative. Real Kafka subscribes a client to nothing until an operator defines a `CLIENT_METRICS` config resource, which this broker does not have (C: the setting round trips nowhere). Say so in `docs/limitations.md`. See TRF-5, TRF-14. |
| PushTelemetry | 72 | F | F | Accepts OTLP metrics in the five codecs, up to 512 KiB compressed and 512 KiB inflated. Error codes today: `INVALID_REQUEST`, `UNKNOWN_SUBSCRIPTION_ID`, `UNSUPPORTED_COMPRESSION_TYPE`, `TELEMETRY_TOO_LARGE`. Not rate limited (TRF-5). Corrupt snappy panics (TRF-3). |
| ApiVersions | 18 | F | F | Carries the client's software name and version, which is where the Software column comes from. A newer-than-known version is answered `UNSUPPORTED_VERSION` and is deliberately not shown as an error (`observe.go:117`). Owned by [cluster](cluster.md); listed because Traffic reads it. |
| all others | | | | Observed only. The summariser per API is a Traffic contract: section 6 and TRF-17. |

Declined, stated so nobody builds them: a `CLIENT_METRICS` resource and its
`DescribeConfigs`/`AlterConfigs` support; exporting metrics anywhere; keeping
record bytes in the recorder; a request replay.

### Console routes

Read from `console/console.go:245-248`, plus the group page's timeline.

| Route | Method | Mutates | Now | Target | Note |
|---|---|---|---|---|---|
| `/traffic` | GET | no | F | F | Requests view. Gains `?view=clients`, `?kind=`, `?group=`, `?topic=`, `?errors=`. |
| `/traffic/live` | GET | no | F | F | Polled region, 2.5 s, 204 on unchanged hash. |
| `/traffic/request` | GET | no | F | F | The drawer partial; becomes the inspector body (A1) and takes a stable `id`, not `seq` (TRF-12). |
| `/traffic/unfold` | GET | no | absent | new | Ticks of a folded row (Traf-05). |
| `/traffic/clients` and `/traffic/clients/live` | GET | no | absent | new | Clients view (Traf-06). |
| `/traffic/client` | GET | no | absent | new | One client, inspector body (Traf-07, Traf-11). |
| `/traffic/client/metrics` | GET | no | absent | new | All metrics of one instance (key `m`). |
| `/traffic/dock` | GET | no | absent | new | The A4 contract: rows for one `topic=` or `group=`, newest first, `after=` cursor. |
| `/traffic/rebalance` | GET | no | absent | new | The story for one group (Traf-12). |
| `/groups/{id}` Timeline | GET | no | F | F | Owned by [groups](groups.md); fed by `GroupEvents`. |
| `/api/palette` entry "Traffic" | GET | no | F | F | Adds "Show errors", "Pause". |

Every route is a read. Traffic has no destructive action, so there is no
confirm to write. All routes disappear (404, rail entry gone) with no source,
as today (`TestWithoutATrafficSourceThereIsNoTrafficSection`).

## 3. Where it stands

**Tests that touch the surface**

| Layer | Tests |
|---|---|
| `internal/traffic` (8) | ring keeps newest, console ids left out, idle repeats fold, folding does not outlive the row, filters, topic ids resolved, clients are live connections, group events per group |
| `internal/protocol/observe_test.go` (5) | every API has a summariser, `firstError` finds the code, `Describe`, observer hears connections and requests, the ApiVersions downgrade is not an error |
| `internal/telemetry` (9, 1 fuzz) | encode/decode, attributes sorted, integer points, malformed refused, store bounds and TTLs, `FuzzDecode` |
| `internal/broker` | `telemetry_test.go` (7: advertised only where offered, subscribe, every compression, push errors, who/terminating, other broker's subscription, real librdkafka payload), `groupevents_test.go` (3), `authz_apis_test.go` (71/72 need no ACL) |
| `internal/storage/log/decompress_test.go` (3) | every codec, past-limit refused, a real librdkafka frame |
| `console/traffic_test.go` (16) | page, filters survive the poll, 204 on unchanged, drawer says when a row moved on, group timeline, absent without source, formatters, metrics cards, refresh, sparkline, rank |
| `cmd/doze-kafka/traffic_test.go` (1) | an app's requests and its group's timeline end to end |
| `server/server_test.go` | `TestOnlyABrokerWithAConsoleKeepsATrafficRecord` |
| `e2e/tests/traffic.spec.ts` (7) | rail, rows, filters survive refresh, drawer and Esc, live toggle aria-pressed (it never asserts that polling stops), group timeline, palette, metrics card |

No `compat/` cell covers telemetry or traffic. No test sends a Java client's
telemetry. No test covers the pause behaviour beyond the aria attribute.

**Coverage today** (full suite, 2026-10-11; function level from `go test -coverprofile`)

| Package | Statements | Functions under 100% that this stage owns |
|---|---|---|
| `internal/traffic` | 96.7% | `New` 66.7 (the capacity < 1 branch), `Request` 98.0, `Events` 92.9, `Clients` 93.3 |
| `internal/telemetry` | 92.0% | `eachField` 75.0, `decodeResourceMetrics` 81.8, `decodeHistogramPoint` 86.7, `decodeKeyValue` 83.3, `Push` 87.5, `Last` 66.7 |
| `internal/protocol` | 71.9% | `summary.go`: 31 of 34 summarisers under 70% (`sumAddPartitionsToTxn` 41.7, `sumApiVersions` 33.3, `sumOffsetFetch` 45.5, `sumListOffsets` 50, `sumMetadata` 58.3 ...), `firstError` 83.3, `ErrorName` 80, `ReplySized` 0, `serve` 85.7; `conn.go` accessors 0 in this package's own tests (covered from `internal/broker`) |
| `console` | 87.8% (35.7% under `-short`: the traffic tests are skipped there) | `lastSample` 66.7, `timelineKind` 40.0, `trafficRow` 91.7, `trafficData` 95.5, `metricCards` 96.8 |
| `cmd/doze-kafka` | 79.8% | `traffic.go`: `trafficFor` and `clientMetricsFor` 66.7 (the nil branch), `Instances` 63.6 |
| `internal/storage/log` | 80.5% | `inflateMax` 83.7, `readAllBounded` 83.3 |
| `internal/broker` | 89.0% (27.5% `-short`) | `telemetry.go` and `groupevents.go` are covered by `telemetry_test.go` and `groupevents_test.go`; the 2 Push branches for `Terminating` and a decode failure log are covered, the throttle does not exist |

## 4. Findings

How each was found: read the packages end to end; ran the built binary
(`dkbin`) with `kcat` as producer, as a group consumer, and with a client id
containing `&`, `=` and a space; wrote throwaway tests under the scratchpad and
ran them through `go test -overlay` so no file in the repo changed (a zstd bomb,
a short xerial header, a `firstError` sweep over every `kmsg` response type, a
900 KiB `JoinGroup` reason, a burst of 200 pushes). `run` below means that
reproduction; `read` means the code only. All sources are `read` + `spec` unless
a cell says otherwise; nothing here depends on a claim about real Kafka that is
not on the recording queue.

### Bounds on network input

| ID | Sev | Finding | Where | Ev. | Source | Test to write first |
|---|---|---|---|---|---|---|
| TRF-1 | P1 | The recorder keeps client-supplied text whole. A `JoinGroup` v9 with a 900 KiB `Reason` is stored as `Detail` (921,600 bytes kept, observed); the same for `ClientID` (up to 32 KiB), a `Metadata` topic list and the software string. The ring holds 2,000 rows, so an anonymous client can pin about 1.8 GiB, on the default CLI (console on), against the comment in `server.go:104` ("a few hundred KiB") and `traffic.go:8` ("a cost worth measuring"). The console then renders every byte of it. | `traffic.go:180` (`Event` built from `o.Detail`, `o.Topics`, `c.ClientID`), `summary.go:178-182` | run | spec (frame limits), memory (none) | Unit, `internal/traffic`: `TestRowFieldsAreClipped` (Detail <= 256 B, 20 topics then "and N more", ClientID <= 128 B, Software <= 128 B). Raw kmsg, `internal/broker`: JoinGroup v9 with a 900 KiB reason, then `Events()`, assert `len(Detail) <= 256`. |
| TRF-2 | P1 | `Decompress`'s limit does not bound zstd memory. The decoder is given `max(limit, 64 MiB)` and the size is checked after `DecodeAll`, so a 6,724-byte push that inflates to 60 MiB allocates 60 MiB (observed) before being refused, per connection, in parallel. The telemetry limit is 512 KiB. Same function guards record batches. | `records.go:42`, `records.go:154-166` | run | read | Unit, `internal/storage/log`: `TestDecompressZstdNeverAllocatesPastTheLimit` (compress 60 MiB of zeros, limit 512 KiB, `testing.AllocsPerRun`/`MemStats.TotalAlloc` delta < 4 MiB). Fix: stream through `LimitReader` with a small window cap, or `WithDecoderMaxWindow` plus a bounded reader. |
| TRF-3 | P1 | A xerial-framed snappy body of 8 to 15 bytes (magic and a short header) slices `body[16:]` and panics. The connection is dropped; real Kafka answers a bad payload with an error code. Reachable by PushTelemetry (codec 2) and by Produce records with snappy. | `records.go:124` | run | read | Unit, log: `TestDecompressShortXerialHeaderIsAnError`. Raw kmsg, broker: PushTelemetry codec 2 with 11 bytes returns `INVALID_REQUEST` and the same connection answers the next request. Fuzz: add `FuzzDecompress` over the five codecs to `task fuzz`. |
| TRF-4 | P1 | `Store.Push` merges `p.Resource` into the instance on every push and never trims it. 200 pushes in a burst, each with one new 1 KiB key and a 100 KiB value, left 20,200,400 bytes in one instance (observed); nothing stops this continuing. `Decode` also builds the same map per push with no count limit. | `store.go:147-149`, `otlp.go:135-153` | run | read | Unit, telemetry: `TestResourceIsBounded` (at most 32 keys, 128 B per key, 1 KiB per value, per instance; extra dropped). Raw kmsg: the 200-push burst, assert the store holds under 64 KiB for the instance. |
| TRF-5 | P2 | `PushTelemetry` is not rate limited: the advertised interval (5,000 ms) is advice. A client can push thousands of times a second, each decode and store hitting the lock the console polls under. What real Kafka does about an early push is unrecorded (queue R1). | `broker/telemetry.go:90-112` | run | memory | Raw kmsg: 100 pushes in 100 ms; today all return 0 (observed 200 of 200 accepted). After R1: assert the recorded code. |
| TRF-6 | P2 | The store's worst case is about 300 MB (200 instances x 400 series x 120 samples x 32 B), and `Instances()` deep-copies all of it on every poll (every 2.5 s per open tab), then `metricCards` draws a sparkline SVG for every series, including those folded under "N more", before the hash is even compared. Typical use is far smaller (136 series for a Java client), so the defect is the missing budget, not the typical cost. | `store.go:19-22`, `store.go:201-225`, `console/traffic.go:188-215` | read | read | Unit: `TestStoreWorstCaseIsUnderBudget` (fill to the caps, assert `unsafe`-free estimate under 32 MB: lower `maxSeriesEach` to 200 and `samplesKept` to 60 or drop to float32+int32 offsets). Console: the hash is computed from `Pushes` before cards are built (TRF-28). |

### The recorder and the error finder

| ID | Sev | Finding | Where | Ev. | Source | Test to write first |
|---|---|---|---|---|---|---|
| TRF-7 | P2 | `firstError` stops at depth 6, so errors nested deeper show as "ok": `OffsetFetch` v8+ `Groups[].Topics[].Partitions[].ErrorCode` (every current consumer's committed-offset fetch), `AddPartitionsToTxn` v4+, `WriteTxnMarkers`, `AssignReplicasToDirs`, `DescribeShareGroupOffsets`. Found by setting every `ErrorCode` field of every `kmsg` response in turn: 5 of all response types miss. | `summary.go:438` | run | spec | Unit, protocol: `TestFirstErrorFindsEveryErrorCodeOfEveryResponse` (the sweep; fails today for keys 9, 24, 27, 73, 90). Fix: walk without a depth cap but with a visited-count cap (the types are finite). |
| TRF-8 | P2 | Normal protocol flow is listed as an error. Every classic consumer's first `JoinGroup` (v4+) is answered `MEMBER_ID_REQUIRED` on purpose and is recorded with that name (observed with `kcat -G`), as are `REBALANCE_IN_PROGRESS` heartbeats. The code already exempts the `ApiVersions` downgrade for the same reason (`observe.go:117`). It makes "errors only", the header's error count and a per-client error rate lie, and it contradicts Traf-03 ("anything red is the broker unhappy, and there is none"). | `observe.go:101-125`, `traffic.go:189` | run | doc (KIP-394 for `MEMBER_ID_REQUIRED`; queue R6) | Unit, protocol: `TestExpectedFlowIsNotAnError` (a table of code, API, version -> class). Needs the classification of work item W4: `Class` in `{ok, expected, refused, denied, failed}`, one table next to `ErrorName` (foundation A2 uses it). |
| TRF-9 | P2 | The link from a connected client to "only this client's requests" never filters. The template writes the already percent-encoded query inside `href=".../traffic?{{.Filter}}"`; `html/template` escapes it again in a query context, giving `?client%3dcons%2520a%2526b%253Dc`, which the server reads as one key named `client=cons a&b=c`. The filter is ignored: 27 rows instead of 6 (observed). Even a plain name (`client%3dprod-a`) fails. `observability.md` says a filter link "shares it". | `console/traffic.go:87-114`, `206`, `traffic.html:41` | run | read | Console handler: `TestClientLinkFiltersToThatClient` (GET the href found in the clients table; rows all have that client). Playwright: click a client name, the table narrows. Fix: build the href with `url.Values` and mark it `template.URL`, or put `client` as a path/param pair. |
| TRF-10 | P2 | Transactional ids are recorded as consumer groups. `Summary.Group` is "the consumer group (or transactional id)": `Produce` with a transaction id, `InitProducerId`, `AddPartitionsToTxn`, `EndTxn`, `FindCoordinator` of type 1 and `DescribeTransactions` all set it. The recorder appends every non-empty `Group` to `Client.Groups`, and the clients table links each one to `/groups/<id>`, a page that does not exist for a transaction. The "Group" filter and the group dock would mix a topic-less group with a transaction of the same name. | `summary.go:70`, `264-266`, `276-279`, `303`, `173`; `traffic.go:162-167`; `traffic.html:48` | read | read | Unit, traffic: `TestTransactionalIdIsNotAGroup` (a Produce observation with a txn id leaves `Client.Groups` empty and sets `Event.TxnID`). Protocol: `Summary` gains `TxnID`; summarisers split. |
| TRF-11 | P2 | A request that ends the connection leaves no row: an API the broker does not handle, a handler panic, a bad header, a request before authentication, a failed write. Each returns from the loop before `observe`; the only trace is a log line. This is exactly "why does my app keep disconnecting". | `conn.go:237`, `253`, `264`, `274`, `286-294` | read | read | Protocol: `TestConnectionEndingRequestsAreObserved` (send an unadvertised API; the observer gets an `Observation` with `Class=failed`, a `Reason`, and `ConnClosed` follows). The row reads "closed the connection: ApiKey 99 is not served". |
| TRF-12 | P2 | A row has no stable identity. Folding rewrites `Seq` (`prev.Time, prev.Seq = o.Start, r.seq`), and the row id (`rq-<Seq>`) and the drawer URL (`?seq=`) are built from it, so for a live heartbeat or empty-fetch row the link goes stale at the next beat. The drawer then says "This row has changed or aged out" (`traffic.html`, the `Gone` branch), and A1's "the selection survives a refresh", Traf-08's "the selected one does not move" and Back cannot hold. | `traffic.go:193`, `console/traffic.go:188-199`, `247-267` | read | read | Unit, traffic: `TestAFoldedRowKeepsItsId` (id from the first Seq; `Seq` stays the ordering key). Console: `TestInspectorSurvivesTwoPolls` (open a folding row, run two ticks, GET again, 200 with the same row). Playwright in 7. |
| TRF-13 | P2 | A folded row lives and dies with its ring slot, which is assigned at creation. After 2,000 newer rows the heartbeat row (`x40000`) is overwritten and the next heartbeat starts at `x1`. So the count on a row is "since the row was last created", not a total, and nothing can add rows up to "412 requests in the last minute" (Main) or an error rate (Traf-06). | `traffic.go:196-211` | read | read | Unit, traffic: `TestBusyTrafficDoesNotResetAFoldedCount` (one heartbeat stream, 3,000 other rows in between; the heartbeat count keeps rising). Counters live beside the ring (work item W5). |
| TRF-14 | P2 | The recorder is console-only, but telemetry is not. `server.New` and `--console=false` leave `ClientTelemetry` true: APIs 71 and 72 are advertised, `kcat` subscribes (`Handled GetTelemetrySubscriptions ... PushScheduled`, observed with `--console=false`), and every client pushes every 5 s into a store nobody reads. `observability.md:3-6` says such a broker "keeps none of it and pays nothing". An embedded broker in a Go test also gets Java clients that push. | `server/server.go:153` (sets `Console=false` only), `broker/broker.go:102`, `observability.md:3-6` | run | read | Server: `TestEmbeddedBrokerDoesNotAdvertiseTelemetry` and CLI `TestNoConsoleMeansNoTelemetry` (ApiVersions has no 71/72). Decision to record in the stage: telemetry is on when the console is on and `--client-telemetry` is not false. |
| TRF-15 | P2 | The Requests heading counts rows shown, not rows matching. The page reads at most 200 rows (`trafficRows`) of the 2,000 kept and writes `Requests {{len .Rows}} matching`, so a filter that matches 1,500 says "200 matching". Traf-02 says "87 of 412 requests match". | `console/traffic.go:74`, `190`, `traffic.html:86` | read | read | Console: `TestRequestsHeadingSaysHowManyMatch` with 500 rows: "showing 200 of 500". Needs `Events` to return the total (W5). |
| TRF-16 | P2 | The Java client's telemetry has never been exercised. Only a recorded librdkafka frame (`decompress_test.go:106`) and hand-built payloads are tested. The code comment says a Java client's snappy is xerial-framed and zstd-compressed windows differ; nothing runs the Java client against the broker. | `compat/java`, `records.go:182-185` | read | read | Compat cell: Java client 4.0 (`enable.metrics.push` default) against the built broker; assert an instance with the client's `client.id` appears with at least one `org.apache.kafka.` series within 15 s. Add a librdkafka 2.5 cell (kcat) beside it. |
| TRF-17 | P2 | 31 of the 34 summarisers are under 70% covered, and `TestEveryAPIDescribesItself` only asserts that a row has a `Summary`, not that it reports the right topics and group. The summariser is what every column of the page is built from, and TRF-10 slipped through it. | `summary.go:75-410`, `observe_test.go:18` | run | read | Protocol: a table test `TestSummarisersReportWhatTheRequestNames`, one row per summariser with a request and the expected `Summary` (it also locks the txn/group split). |
| TRF-18 | P2 | The Group filter cannot find a group's `Fetch` rows. A `Fetch` carries no group on the wire, so `Event.Group` is empty; the group dock (A4) and Traf-12's story would omit every fetch of the group's members. The connection knows (`Client.Groups`), the row does not. | `summary.go:75-106`, `traffic.go:180-186`, `traffic.go:253` (`Filter.Group`) | read | read | Unit, traffic: `TestGroupFilterIncludesItsMembersFetches` (fetch on a connection that has joined group g appears under `Group: "g"`, marked "member of g"). |

### Smaller things

| ID | Sev | Finding | Where | Ev. | Source | Test to write first |
|---|---|---|---|---|---|---|
| TRF-19 | P3 | Group events share one 2,000-entry slice for all groups, and a full slice is shifted on every insert (`slices.Delete`, O(n)). A noisy group evicts a quiet group's rebalance story, which is what Traf-12 is built on. | `traffic.go:299-310` | read | read | Unit: `TestOneGroupCannotEvictAnothersStory` (per-group ring of 200, plus a global cap on groups). |
| TRF-20 | P3 | A row's time is when the request began; its order is when it ended (`Seq`). A 500 ms long-poll fetch or a 3 s `JoinGroup` therefore appears above rows that began after it, and the Time column of a list titled "newest first" is not monotonic. | `conn.go:278` (`began`), `traffic.go:180` | read | read | Unit: decide and test; either show end time or sort by start. Console wording follows. |
| TRF-21 | P3 | Hiding by prefix: any client id that begins `doze-kafka-console` (an app named `doze-kafka-console-demo`) is invisible in requests and in the clients list. | `traffic.go:26`, `126` | read | read | Unit: `TestOnlyTheConsolesOwnIdsAreLeftOut` (the three exact ids the console uses: `doze-kafka-console`, `-fetch`, `-trace`; the check moves next to where they are defined, `console/backend.go:76,600,670`). |
| TRF-22 | P3 | Three statements of one fact. `observability.md` says the chart covers "the last ten minutes"; the code draws the last 60 samples (5 min at 5 s; the store keeps 120); Traf-07 says "the last minute, one bar per report" and the code draws a polyline, not bars. | `metrics.go:187-198`, `store.go:13`, `observability.md` | read | doc | Console: `TestSparklineSpanMatchesTheStore`; fix the number in the copy and the board to what is drawn. |
| TRF-23 | P3 | The page and the docs do not say what is held about people: principal, peer address, client id, software, transactional ids and a free-text join reason are shown to anyone who can open the console. No record bytes are held (checked: `Detail` is sizes and names). Telemetry `Resource` holds whatever attributes the client chose. | `observability.md`, `traffic.html` honest note | read | read | Doc test: `observability.md` lists the fields held; console test that the Inspector never contains record bytes or header values (assert on a Produce with a distinctive value). |
| TRF-24 | P3 | The OTLP decoder swallows malformed data points: `decodeNumberPoint`, `decodeHistogramPoint` and `decodeKeyValue` discard `eachField`'s error, so a truncated point is stored as value 0 and the payload is accepted. | `otlp.go:221`, `246`, `277`, `282` | read | read | Unit: `TestTruncatedPointIsMalformed` (cut a valid payload at every byte; each is `ErrMalformed` or a prefix with no zero-value sample). Add to `FuzzDecode`. |
| TRF-25 | P3 | Only a timestamp more than a minute in the future is replaced; a point stamped 1970 or years ago is kept and series are appended in arrival order, not time order. | `store.go:166-168` | read | read | Unit: `TestPastTimestampsAreClamped`. |
| TRF-26 | P3 | New series beyond 400 per instance are dropped without a sign; the card shows "N metrics" as if complete. | `store.go:156-159` | read | read | Unit: store counts `Dropped`; the card says "N metrics, M not kept". |
| TRF-27 | P3 | Anyone can claim any instance id: an unknown non-zero id is adopted, 200 fresh ids evict real clients (`dropOldest`), and a push from another connection overwrites that instance's principal and host. | `store.go:102-125`, `broker/telemetry.go:63-69` | read | read | Unit: `TestInstanceIsBoundToItsConnection` (after W8 the instance remembers its first connection; a push from another connection or principal is `INVALID_REQUEST`). Queue R4. |
| TRF-28 | P3 | The poll builds all rows, sparklines and metric cards, then computes the hash, then compares it. An unchanged page costs the full render every 2.5 s; and the hash includes each client's `Pushes`, so metrics change the hash every 5 s and the whole region (200 rows, clients) is replaced although the request tail did not move. | `console/traffic.go:188-215` | read | read | Console benchmark `BenchmarkTrafficLiveUnchanged`; hash from cheap counters before rendering; metrics region polled on its own. |
| TRF-29 | P3 | `Server.Traffic()` and `Server.ClientMetrics()` return internal types from the public `server/` package: exported, unusable outside this module, part of the surface foundation B10 will pin. | `server/server.go:111`, `115` | read | read | `task api` baseline (B10) lists them; move behind an unexported hook or an interface defined in `console`. |
| TRF-30 | P3 | `ConnOpened` registers the connection as a client before it has sent a byte, so a port probe or a pre-authentication connection appears in "Connected clients" with only an address for up to 30 s. | `traffic.go:130-134`, `conn.go:188-193` | read | read | Unit: a connection with no request is not listed after 2 s, or listed as "connecting". |
| TRF-31 | P3 | A client metrics card with no client id is named by `Host`, which is Kafka's member-host form `/127.0.0.1` (leading slash, no port), so it reads as a path and two clients on one host cannot be told apart. | `broker/telemetry.go:57`, `console/metrics.go:115` | read | read | Console: card title falls back to `Software` + short id; after W8 the connection's address. |

**Count:** 31 findings: P0 0, P1 4, P2 14, P3 13.

**Where I looked and found nothing**

- `Recorder` locking: `Request`, `Events`, `Clients`, `GroupEvent` take one mutex and hold it for O(1) work except `Events` (copies the ring under the lock, sorts after); `go test -race` for `internal/traffic` and `internal/telemetry` is part of `task check` and passes. `resolve` (the topic id lookup) runs under the recorder's lock; it takes the broker's metadata read lock, and no broker path calls the recorder while holding that lock (checked: `emit` sites in `group.go` and `group848.go` hold only group locks). Written down as a rule in section 5.
- `ConnClosed` removes the connection and its fold map; `recent` entries are deleted when their row is evicted (tested by `TestFoldingDoesNotOutliveTheRow`). No goroutine or fd is created by the recorder or the store.
- Telemetry decode: depth is bounded by the schema (no recursion), `FuzzDecode` exists and is in `task fuzz`.
- Frame limits: a non-Produce request is capped at 1 MiB (`maxControlFrameBytes`), so the 512 KiB telemetry cap and the inflated cap sit under it as the comment says.
- `PushTelemetry` and `GetTelemetrySubscriptions` are not reachable before authentication (`preAuthAllowed`).
- Windows: nothing in these packages touches paths or signals.

## 5. What to check

Stage-specific, beyond section 4 and the common criteria.

- **The recorder is only fed by the console.** `NewFromConfig` builds one iff `cfg.Console`; embedded `server.New` forces `Console=false`. Check that a console-less broker constructs no `traffic.Recorder`, adds no observer (`protocol.Server.observer == nil`, so `serve` does one nil check per request), and, after TRF-14, no telemetry store.
- **The two bounds that define "light".** Rows 2,000 and group events (per group) 200 are the capacities; every client-controlled string is clipped on entry (TRF-1). A test fills the recorder with maximal input and asserts `runtime.MemStats` growth under 8 MiB. This is the number the lightness budget (B9) takes.
- **Folding rules, stated and tested.** A row folds iff the request is `Idle` and the answer carried no error; the key is (connection, API, topics, group/txn); the window is 10 minutes of silence. Not part of the key: version, partition, client id (it is per connection). `Fetch` is idle iff no bytes came back and no top-level error; a partition-level error makes it a row of its own (the `ev.Idle && ev.Error == ""` guard). Write the table in `observability.md` and have one test per line.
- **Console clients.** Excluded from rows and clients; not excluded from group events (a console consumer that joined a group would appear in a timeline). Check the console's consumers use no group, or exclude their events by member client id.
- **Error codes.** Names come from `kerr.TypedErrorForCode(code).Message`. Check every code this broker returns has a name in the vendored `kerr` (a unit test walks `internal/broker` for `kerr.X.Code` references and asserts `ErrorName` is not `ERROR_n`). Unknown code -> `ERROR_n` is covered at 80% only.
- **Principals and payloads.** A test drives Produce with a recognisable key, value and header and asserts none of them appears in any recorder field, any rendered page or the decoded-fields JSON.
- **Auth.** With SASL on, the principal column shows the authenticated name, `User:ANONYMOUS` before; both are checked. The console is a separate credential: say so on the page's honest note.
- **Both group protocols.** The story and the generations table (Traf-12) are built for classic groups and for KIP-848 (`epoch` events). One test per protocol, as `groupevents_test.go` already does for the sink.
- **Restart.** The recorder starts empty on restart by design; the page must say "since 14:02, the broker restarted" rather than look broken (the Empty board's wording differs between "no app yet" and "restarted").
- **The pause button is client-side** (`shell.js:727-753`): it stops polling for every region on the page. The server must not depend on it, and the paused count (Traf-08) comes from a cursor, not from state on the server.
- **Narrow widths.** The table at 960 and 390 px drops to the columns Client, Request, Result; the inspector becomes a sheet.

## 6. Console

Statuses: `real` (exists), `buildable` (data exists, screen or small query does not), `needs backend` (some data or behaviour is new). The inspector, dock and neighbours pieces come from the foundation (A1, A4, A5) and are referred to by name.

| Board | Template and handler | Status | Built from |
|---|---|---|---|
| Main (Traffic home) | `traffic_detail` / `trafficPage`, `trafficLive` | real | The tail, filter form, clients table and live toggle exist. New items on this board: the header counters ("5 clients connected, 412 requests in the last minute, 3 errors") need the window counters (W5); the kind chips (all, errors, produce, fetch, groups, admin) need `Kind` per API (W5); rich Detail ("p1, 1 batch, 412 B", "p0 from 4,812, 3 records", "epoch 4 -> 5", "generation 7, 3 members") needs response-time summaries (W6); the client sub-lines ("producer, tx checkout-1", "just rebalanced", "last 4 requests refused") need per-connection role and recent-class counters (W5, W8) |
| Traf-02-Filter | same, plus `/traffic/dock` | buildable | `Filter.Topic` exists; needs the filter chip row, "N of M match" (TRF-15), the cursor and the "Who touched" panel: producers and consumers by API role, Produce and Fetch counts weighted by `Count` ("46 (310 folded)"), errors; the "Open the topic" link is the topics stage. Text "Group requests that name this topic are included" is true today (OffsetCommit and OffsetFetch carry topics; Heartbeat does not) |
| Traf-03-Errors | `traffic_errors` partial / new `trafficErrors` | needs backend | The errors filter exists. New: `Class` per error code (TRF-8), the explainer text per code (a table of 40 codes: meaning, usual cause, what to do) generated from the same table as the name, "most frequent refusal" (group by code in the recorder), "Nearest topic" (edit distance over the topic list, backend call to the topics stage), actions "Create <topic>" and "See the N topics" (links into [topics](topics.md)). "Seen 1 time" needs counts per (client, code), not rows |
| Traf-04-Request | inspector body from `trafficRequest` | needs backend | Today: time, client, software, address, principal, version, topics, group, detail, result, took, size. New: correlation id (not on `Observation`: W7), Acks, transactional id, "result per partition" with base offsets, and "decoded fields" JSON. The recorder keeps no decoded request. Decision: keep a bounded `Facts` (2 KiB per row, record bytes replaced by "1 batch, 1 record, 412 B"), built at observe time, never record bytes or header values (TRF-23). "Follow in Topics" is a link |
| Traf-05-Folded | inspector body + `/traffic/unfold` | needs backend | A fold keeps `Count`, `First`, `Time` only. "Every 3 s" is derivable (span / count); the tick strip, "unfold these 42" and "generation epoch 12 throughout" need a bounded ring of the last 64 tick times, durations and result classes per folded row, and the generation from the response (W5, W6). The text "Idle repeats fold; any change breaks the fold" is true |
| Traf-06-Clients | `traffic_clients` / `/traffic/clients` | needs backend | Today: client, software, address, principal, open for, last request, requests (a total), groups. New: Req/min and Error rate (5 min window) need per-connection minute buckets (W5); "needs a look" is a rule on error rate and consecutive refusals; "Groups: none (producer), refused" needs role and class (W5). "g r / g c" keys switch views. "Error rate is refused requests over all requests" must use `Class` (TRF-8), not "any error" |
| Traf-07-Client | inspector body / `/traffic/client` | needs backend | Metrics exist (`Instances()`, `metricRank`, "N more metrics"); "Worth a look first" is the existing ordering. New: tie the instance to the connection (the store keys on instance id; the connection is not recorded), so "this client's metrics" cannot be looked up except by client id (W8); "Its requests" and "g-order-validator" are links; "136 metrics, last report 14:36:20" comes from the instance. Copy: "Sparklines cover the last minute, one bar per report" is false (TRF-22) |
| Traf-08-Paused | live toggle in `shell.js` + paused banner | buildable | Pause is client-side and works. New: "12 new requests since 14:36:21" is a count of rows with `Seq > cursor`, which needs the cursor parameter on `Events` and a cheap count endpoint; "the selected one does not move" needs TRF-12. Keys (Sy-06): `p` toggles, and on this page does not mean produce |
| Traf-09-Empty | `traffic_empty` partial | buildable | The condition (no rows, no clients) exists. The bootstrap line and the SASL sentence must come from the running config (A8), not `127.0.0.1:9092`; the three steps link to Connect (foundation D) and Create topic (`c`). Distinguish "nothing yet" from "restarted at 14:02" (section 5) |
| Traf-10-Disconnected | shell A7 | buildable | A7: the persistent pill, "last good 14:36:21", frozen rows with the line "Frozen at ...", retry every 5 s, "r" retry. The poll interval is 2.5 s today; the board says retry every 5 s, so the shell backs off. The "What to check when the broker is down" list is static text; each item checked against `doctor` output |
| Traf-11-Metrics-None | inspector body | needs backend | Needs to know whether this connection called `GetTelemetrySubscriptions` ("the broker asked for them and got none"): today it is unknown; one flag on the recorder's `Client` (W8). The two reasons and the settings to flip are static, with versions sourced (queue R9). "Other clients may not implement it" is true for franz-go as far as known (memory, queue R9) |
| Traf-12-Rebalance-Story | `/traffic/rebalance` | needs backend | The group events exist (joined, rebalance, generation, assigned, left, expired, dropped, epoch) and the group-filtered request rows exist. New: grouping requests under the rebalance they belong to (a span from the `rebalance` event to the `generation` event, W9), "6.0 s" from the event times, the generations table (generation, cause, members, time) from `generation` events (needs a member count in `Detail` as a field, not prose), the "Why it rebalanced" sentence from the `rebalance` reason, the fetch rows (TRF-18). The claim "Real Kafka does the same: any join in the classic protocol makes everyone rejoin" is queue R8 |
| Sy-05-Palette (live behaviour only) | `/api/palette` | buildable | Palette lists "Traffic" today; add "Pause live view", "Show errors" |
| Sy-06-Keys (live behaviour only) | `shell.js` | buildable | `j k Enter / p e g r g c u m Esc` on this page; `p` = pause here. Generated from the declared key map (A10) |

**Board count (12 boards of this stage):** real 1 (Main), buildable 4 (Traf-02, 08, 09, 10), needs backend 7 (Traf-03, 04, 05, 06, 07, 11, 12). Main is `real` for its table and chrome, with the items above that need backend.

**Teaching lines.** Traffic has no settings form. The text that explains things comes from two tables, both new, both in code (foundation B7 mechanism): the error table (`name`, `class`, `meaning`, `cause`, `fix`) beside `protocol.ErrorName`, and the API table (`Kind`, `Role`: produce, fetch, group, admin, txn, meta). A test fails for a code the broker can return that has no row. The "honest note" at the foot of the page is generated from the capacities (2,000; 200 per group) so the number in the copy cannot drift from the constant.

**New backend items (named here, sized in section 11).**

- W4: error `Class` and explainer table.
- W5: recorder redesign: stable id; `TxnID` separate from `Group`; counters (whole broker and per connection, one-minute buckets for 5 minutes; `Class`-aware); `Kind`; `After` cursor and totals; tick ring on folded rows; eviction by last activity; clipped fields.
- W6: summaries computed from the response where the request does not have the fact (partition, base offset, fetch offset, record count, producer epoch, generation, member count) and `Facts`.
- W7: connection-ending requests, correlation id.
- W8: connection to telemetry link; telemetry only with the console.
- W9: per-group event ring; span and generation model.

## 7. Tests to add

**Unit and store**

- `internal/traffic`: `TestRowFieldsAreClipped`, `TestAFoldedRowKeepsItsId`, `TestBusyTrafficDoesNotResetAFoldedCount`, `TestTransactionalIdIsNotAGroup`, `TestGroupFilterIncludesItsMembersFetches`, `TestOneGroupCannotEvictAnothersStory`, `TestOnlyTheConsolesOwnIdsAreLeftOut`, `TestCursorReturnsOnlyNewerRows`, `TestCountersPerConnectionAndWindow` (errors by class, 5 min window with an injected clock), `TestTickRingKeepsLast64`, `TestRecorderMemoryWithMaximalInput`, `TestNewClampsCapacity` (covers `New`'s branch).
- `internal/telemetry`: `TestResourceIsBounded`, `TestStoreWorstCaseIsUnderBudget`, `TestTruncatedPointIsMalformed`, `TestPastTimestampsAreClamped`, `TestDroppedSeriesAreCounted`, `TestInstanceIsBoundToItsConnection`, histogram and `Last` branches.
- `internal/storage/log`: `TestDecompressZstdNeverAllocatesPastTheLimit`, `TestDecompressShortXerialHeaderIsAnError`, `TestDecompressEveryCodecAtTheLimitBoundary` (limit-1, limit, limit+1), `FuzzDecompress` in `task fuzz`.
- `internal/protocol`: `TestFirstErrorFindsEveryErrorCodeOfEveryResponse` (the sweep), `TestExpectedFlowIsNotAnError`, `TestSummarisersReportWhatTheRequestNames` (34 rows), `TestConnectionEndingRequestsAreObserved`, `TestEveryErrorTheBrokerReturnsHasAName`, `TestErrorTableHasARowPerCode`, `TestCorrelationIdIsObserved`.
- `console`: `TestClientLinkFiltersToThatClient`, `TestRequestsHeadingSaysHowManyMatch`, `TestInspectorSurvivesTwoPolls`, `TestSparklineSpanMatchesTheStore`, `TestInspectorNeverContainsRecordBytes`, `TestErrorsViewGroupsByCodeAndClass`, `TestClientsViewRatesFromBuckets`, `TestUnfoldListsTicks`, `TestRebalanceStoryGroupsRequestsUnderTheirRebalance`, `TestDockShowsOnlyItsResource`, `TestEmptyStateUsesTheRunningConfig`, `TestTimelineKindCoversEveryKind` (covers `timelineKind`), `BenchmarkTrafficLiveUnchanged`.
- `cmd/doze-kafka`: `TestTrafficForNilIsNil`, `TestClientMetricsForNilIsNil`, `TestInstancesCopiesEveryField`.

**Raw kmsg (`internal/broker`)**

- JoinGroup v9 with a 900 KiB reason, then the recorder's row is clipped (TRF-1).
- PushTelemetry: codec 2 with an 11-byte xerial body -> `INVALID_REQUEST`, connection alive (TRF-3); the 200-push burst (TRF-4, TRF-5); a zstd bomb answered in under 5 MiB of allocation (TRF-2); a push from another connection with the same instance id (TRF-27).
- ApiVersions on `--console=false` and on an embedded broker has no 71/72 (TRF-14).
- Unadvertised API -> an observation with `Class=failed` and the connection closed (TRF-11).
- OffsetFetch v8 with a partition error, AddPartitionsToTxn v3: the recorded `Error` is that code (TRF-7).

**Client library (franz-go, Sarama)**

- A franz-go group consumer and producer: the recorded rows for join, sync, heartbeat, fetch, commit have the right topic, group, `Class` (`MEMBER_ID_REQUIRED` is `expected`) and generation in the story (both group protocols).
- A franz-go transactional producer: `Client.Groups` stays empty; `TxnID` is the transactional id.

**Compat cells** (`compat/`): `java-telemetry` (Java 4.0 pushes; the instance and a series appear within 15 s), `kcat-telemetry` (librdkafka 2.5; same), `kcat-traffic` (a produce and a consume appear in `/traffic/live` with the right client id and topic). Each plain and SCRAM over TLS where the client supports telemetry over that listener.

**Fuzz**: `FuzzDecompress` (all codecs, limit as input); `FuzzDescribe` (decode a request for a random API key from bytes, run `Describe` with a zero-value response: must not panic); the existing `FuzzDecode` extended with the truncation corpus.

**Differential against real Kafka** (foundation B2): scenarios R1 to R9 below. Zero "answered differently" required for the F rows (71, 72); the count still at `memory` or `doc` is written in section 10.

**E2E (browser, `e2e/tests/`)**: extend `traffic.spec.ts` and add `traffic-clients.spec.ts`, `traffic-rebalance.spec.ts`: (1) the kind chips narrow the tail and the count reads "N of M"; (2) errors view: `MEMBER_ID_REQUIRED` is not in it, a real refusal is, with the explainer; (3) inspector: open a folding row, wait two polls, still open; Esc returns focus; reload keeps it; Back; (4) unfold shows the ticks; (5) pause stops the table (assert the row count does not change while the seed produces) and shows "N new"; (6) client name link narrows to that client; (7) clients view columns and the "needs a look" row; (8) client with and without metrics (Traf-07, Traf-11); (9) rebalance story for a seeded group; (10) empty state shows the running bootstrap address; (11) broker stopped mid-poll: pill, frozen rows, one toast at most, recovery without reload (A7); (12) 960 and 390 px; (13) route gate on.

**Coverage targets**

| Package | Today | Target | Functions that must be covered to get there |
|---|---|---|---|
| `internal/traffic` | 96.7% | 98.0% | `New` clamp branch, `Request` txn/fold/evict branches, `Events` cursor and total, `Clients` role, the counters, the tick ring |
| `internal/telemetry` | 92.0% | 96.0% | `eachField` error arms, `decodeResourceMetrics`, `decodeHistogramPoint`, `decodeKeyValue`, `Push` caps, `Last` |
| `internal/protocol` | 71.9% | 74.0% package; `summary.go` >= 92%, `observe.go` 100%, the observer paths of `conn.go` 100% | all 34 summarisers, `firstError`, `ErrorName` unknown code, `ReplySized`, `Describe`, the new class table |
| `console` | 87.8% | 88.6% (traffic.go and metrics.go >= 95%) | `trafficRow`, `trafficData`, `metricCards`, `lastSample`, `timelineKind`, the new handlers |
| `cmd/doze-kafka` | 79.8% | 80.4% (`traffic.go` 100%) | `trafficFor`/`clientMetricsFor` nil, `Instances` |
| `internal/storage/log` | 80.5% | 81.5% (`Decompress`, `inflateMax`, `readAllBounded` >= 95%) | boundary and bomb tests |
| `internal/broker` | 89.0% | 89.3% (`telemetry.go`, `groupevents.go` 100%) | throttle/cap/bound branches |

## 8. Recording queue

Claims about real Kafka that are `doze-kafka` behaviour today or board text, and that nobody has recorded. Scenarios run against `apache/kafka:4.0.0` in Docker (and `confluentinc/cp-schema-registry:7.7.1` is not involved in this stage).

| # | Claim | Source now | Scenario that would record it |
|---|---|---|---|
| R1 | What the broker answers when a client pushes before its interval has passed (and the code for a push that arrives with an old subscription id after a subscription change). | memory | `telemetry-throttle`: raw kmsg against a broker with a `CLIENT_METRICS` resource (`kafka-configs.sh --entity-type client-metrics`), GetTelemetrySubscriptions, then PushTelemetry twice within 100 ms; record both codes. |
| R2 | `GetTelemetrySubscriptions`/`PushTelemetry` need no ACL (`authz_apis_test.go:28` says "Kafka requires no ACL for it"). | memory | `telemetry-acl`: authorizer on, a user with no ACLs, both calls; record codes. |
| R3 | Error codes for: zero instance id on push, unknown subscription id, unsupported compression, payload larger than `TelemetryMaxBytes`, inflated larger than the cap, corrupt compressed body. | doc (KIP-714) | `telemetry-errors`: raw kmsg, one case each. |
| R4 | A push with another client's instance id from a different connection or principal. | memory | `telemetry-instance-binding`. |
| R5 | What real Kafka returns with no `CLIENT_METRICS` configured (subscription id, push interval, requested metrics empty), to state the deliberate difference. | doc | `telemetry-empty-subscription`. |
| R6 | `MEMBER_ID_REQUIRED` on a first `JoinGroup` v4+ and `REBALANCE_IN_PROGRESS` on heartbeats, as the expected flow; the class table cites it. | doc (KIP-394) | `classic-join-flow`: three consumers join one after another; record every code of Join, Sync, Heartbeat, OffsetCommit. |
| R7 | The first error a real broker reports where a response has several (Produce with partition errors in a different order than ours). Only matters for "first error" display. | memory | `produce-mixed-partition-errors`. |
| R8 | "Any join in the classic protocol makes everyone rejoin" (Traf-12 copy) and the generation numbers across a join, a leave and an expiry. | doc | `classic-rebalance-generations`: record generation and member count per round. |
| R9 | The client versions that push by default (Java 3.7, librdkafka 2.5), the property `enable.metrics.push`, and that franz-go and Sarama do not. | doc / memory | Run each client against the recorded broker and observe whether `GetTelemetrySubscriptions` arrives (the compat cells TRF-16 make this a test). |
| R10 | The wording of the explainer for each error code's meaning. | spec (the `kerr` descriptions) | None; the table cites the description from `kerr`. |

## 9. Owed to integration

- **Wire dock (A4).** This stage builds the recorder filter by topic or group, newest first, with `after=`; the component and its placement on [topics](topics.md) and [groups](groups.md) belong to those stages. The group dock only works for fetches after TRF-18.
- **Neighbours graph (A5) and [map](map.md).** Edges "client writes topic", "client reads topic via group g" are recorded here (Produce topics, Fetch topics, `Client.Groups`); the map needs them with a time of last use and counts that survive the ring (W5 counters carry per (client, topic, direction) totals, bounded to 1,000 edges).
- **Create the missing topic** (Traf-03 action) is the topics stage's create form with the name prefilled; the "nearest topic" and "7 topics that exist" use its list.
- **Access.** Principals shown here link to the Access page when it has a user; "not authorised to read the group" rows link to the ACL that decided (access stage's checker).
- **Connect (foundation D).** The empty state's bootstrap line and SASL sentence come from its data.
- **Bad days.** The disconnected board is A7; the broker-down list is checked by the bad-days stage.
- **Trace.** `p`/`Pause` is shared (Sy-06); the pause component is common.
- **Cluster.** `ApiVersions` software strings are owned there.

**Asks of the foundation**

1. A4: state the dock contract as `Events(Filter{Topic|Group}, limit, after) (rows, total, newest)` so this stage and the dock agree on the cursor; this stage supplies the endpoint `/traffic/dock`.
2. A2: the `Refusal` type's `Kind` is taken from the error table in W4 (`expected`, `refused`, `denied`, `failed`), so Traffic and every form use one classification.
3. A1: the inspector needs a stable key per kind; for requests it is the row `id` (TRF-12), not a sequence number.
4. B9: a recorder memory entry in the lightness budget (idle heap with 2,000 maximal rows).
5. B10: `Server.Traffic()` and `ClientMetrics()` (TRF-29) on the pinned surface or removed from it.

## 10. Acceptance criteria

On top of the common list in [01-protocol.md](../01-protocol.md).

**Bounds and robustness**

- [ ] `TestRowFieldsAreClipped`, `TestDecompressZstdNeverAllocatesPastTheLimit`, `TestDecompressShortXerialHeaderIsAnError`, `TestResourceIsBounded` fail on the pre-fix code (reverted by hand) and pass after; the commit names the mutation check for each.
- [ ] `go test ./internal/storage/log -run Fuzz -fuzz FuzzDecompress -fuzztime 60s` and the existing telemetry fuzz run clean.
- [ ] A recorder filled with maximal input holds under 8 MiB (`TestRecorderMemoryWithMaximalInput`).
- [ ] The telemetry store's worst case is under the budget the test sets (32 MB), and `Instances()` copies only the instance asked for in the inspector.
- [ ] A broker with `--console=false` and an embedded broker advertise no APIs 71 and 72 (`TestEmbeddedBrokerDoesNotAdvertiseTelemetry`); `observability.md` and `api-support.md` say the same.
- [ ] `TestFirstErrorFindsEveryErrorCodeOfEveryResponse` passes for every response type in the `kmsg` table.
- [ ] `TestExpectedFlowIsNotAnError` and the class table: every error code the broker returns has a row; the errors-only filter and the error rate use the class.

**Contract and documents**

- [ ] `docs/api-support.md` rows 71 and 72 state the fixed subscription (C) and the codes; `docs/limitations.md` lists no `CLIENT_METRICS` resource and no export.
- [ ] `docs/observability.md` states the folding rules, capacities (2,000 rows, 200 per group), what is held about people, the chart range, and the telemetry-only-with-console rule; a test greps the capacities in the doc against the constants.
- [ ] `CHANGELOG.md` names each behaviour change: txn ids no longer listed as groups, `MEMBER_ID_REQUIRED` no longer an error, telemetry off without a console, bounded fields.

**Coverage and mutation**

- [ ] `go tool task coverage`: `internal/traffic` >= 98.0%, `internal/telemetry` >= 96.0%, `internal/protocol` >= 74.0% (with `summary.go` >= 92%), `console` >= 88.6%, `cmd/doze-kafka` >= 80.4%, `internal/storage/log` >= 81.5%, `internal/broker` >= 89.3%; the floor file is raised.
- [ ] `go tool task mutate:check -- ./internal/traffic/... ./internal/telemetry/... ./internal/protocol/...` (restricted to `observe.go`, `summary.go`) shows no new survivor; survivors left are listed here with a reason.

**Real clients**

- [ ] Compat cells `java-telemetry`, `kcat-telemetry`, `kcat-traffic` pass, plain and SCRAM over TLS where applicable; the existing 20 cells still pass.
- [ ] Recording scenarios R1 to R9 are recorded and replayed with zero "answered differently", or each difference is in `docs/limitations.md`; the count at `doc` or `memory` is written here: ____.

**Boards**

- [ ] Implemented: Main, Traf-02 to Traf-12 (12 boards); Sy-05 and Sy-06 behaviour for the live view only.
- [ ] Every `new` item on those boards works from the data named in section 6, or is cut from the board with a note in this file.
- [ ] No console text contradicts the code: the chart range (TRF-22), "N matching" (TRF-15), the filter link (TRF-9) have tests.
- [ ] The Inspector for a Produce never shows record bytes or header values (`TestInspectorNeverContainsRecordBytes`).
- [ ] E2E specs listed in section 7 pass with the route gate on, at 960 and 390 px, with the accessibility sweep clean on `/traffic`, `/traffic/clients`.
- [ ] The pages render with the broker down (pill, frozen rows, one retry) and with an empty recorder.
- [ ] `task lightness`: idle heap and binary size did not move by more than the budget; the commit gives the numbers.

## 11. Files and order of work

Half-day units (h). Total 40 h, about 20 focused days: **L**.

| Order | Work | h | Files |
|---|---|---|---|
| 1 | Red tests for TRF-1 to TRF-18 (the sweep, the bounds, the link, the txn split, the class) | 2 | `internal/*/*_test.go`, `console/traffic_test.go` |
| 2 | Bounds: zstd stream with a real cap, xerial guard, resource and series caps, push throttle (per R1), recorder clipping | 2 | `internal/storage/log/records.go`, `internal/telemetry/store.go`, `internal/broker/telemetry.go`, `internal/traffic/traffic.go` |
| 3 | `firstError` without a depth cap; the all-responses sweep | 1 | `internal/protocol/summary.go` |
| 4 (W4) | Error class and explainer table; `Observation.Class` | 2 | `internal/protocol/errors.go` (new), `observe.go` |
| 5 (W5) | Recorder redesign: stable id, `TxnID`, counters, `Kind`, cursor and total, tick ring, activity eviction | 4 | `internal/traffic/traffic.go`, `internal/protocol/versions.go` (`Kind`, `Role`) |
| 6 (W6) | Response-time summaries and bounded `Facts` | 3 | `internal/protocol/summary.go`, `observe.go` |
| 7 (W7) | Connection-ending requests; correlation id | 1 | `internal/protocol/conn.go` |
| 8 (W8) | Telemetry tied to its connection; telemetry only with a console | 2 | `internal/broker/telemetry.go`, `server/server.go`, `cmd/doze-kafka/traffic.go` |
| 9 (W9) | Per-group event ring; spans and generations | 2 | `internal/traffic/traffic.go`, `internal/broker/groupevents.go`, `group.go`, `group848.go` (event fields, not prose) |
| 10 | Console: Main, filter chips and totals, inspector, folded row, pause counter, link fix (Main, Traf-02, 04, 05, 08) | 4 | `console/traffic.go`, `templates/traffic.html`, `static/shell.js` |
| 11 | Console: errors view and explainers (Traf-03) | 2 | `console/traffic.go`, `templates/traffic.html` |
| 12 | Console: clients view, client inspector, no-metrics page (Traf-06, 07, 11) | 3 | `console/traffic.go`, `metrics.go`, `templates/traffic.html` |
| 13 | Console: empty and disconnected (Traf-09, 10) with A7 | 1 | `templates/traffic.html`, `shell.js` |
| 14 | Console: rebalance story (Traf-12) | 2 | `console/traffic.go`, `templates/traffic.html` |
| 15 | Dock endpoint (A4) and its contract test | 1 | `console/traffic.go`, `cmd/doze-kafka/traffic.go` |
| 16 | Compat cells and browser specs | 3 | `compat/java`, `compat/kcat`, `e2e/tests/traffic*.spec.ts` |
| 17 | Recording R1 to R9 and the replay | 2 | `conformance/` (foundation B2), claims file (B1) |
| 18 | Mutation baseline, coverage floor, summariser table test | 2 | `testdata/mutation.json`, `testdata/coverage.txt` |
| 19 | Ledger, `observability.md`, `console.md`, changelog, status table | 1 | `docs/`, `CHANGELOG.md`, `plan/README.md` |

Order matters in three places: 2 before 5 (the recorder redesign builds on clipped input), 4 before 10 and 11 (classes drive the errors view), 8 before 12 (the client page needs the connection link).

**Prior art.**

- Apache Kafka writes one line per request to a dedicated logger (`kafka.request.logger`, in the broker's `log4j.properties`) at DEBUG or TRACE, and keeps per-request-type metrics (`kafka.network:type=RequestMetrics`); it keeps no in-memory list of requests or clients to look at. This recorder is the small, bounded version of that log, with the folding that a log never needed. (doc)
- KIP-714 (client metrics and observability, Kafka 3.7) is the source for APIs 71 and 72, the client instance id and the push interval; real brokers hand a client a subscription only for a `CLIENT_METRICS` config resource and expect a plugin (`ClientTelemetryReceiver`) to take the metrics. Here the receiver is the console. (doc)
- KIP-394 explains `MEMBER_ID_REQUIRED` on a first join; the class table cites it. (doc)
- Redpanda Console and Kafbat UI show connected clients and group state from admin APIs; neither shows a request tail, which is why this page is built from the wire. (memory; not asserted anywhere in the product)
- The Schema Registry work in this repository (record, commit, replay) is the method R1 to R9 follow.

## Status of this pass

What was done, by finding, against what the document asked. "Red" means the test failed
on the code before the fix and was run again with the fix reverted by hand.

| ID | State | Where it is tested |
|---|---|---|
| TRF-1 | fixed, red | `TestRowFieldsAreClipped`, `TestRecorderMemoryWithMaximalInput` (under 8 MiB for 2,000 maximal rows from 100 clients; a connection costs about 2 KB more), `TestARecordedJoinReasonIsClipped` |
| TRF-2 | fixed, red | `TestDecompressZstdNeverAllocatesPastTheLimit`, `TestPushOfAZstdBombIsRefusedWithoutInflatingIt`. The decoder allocates the window a frame declares, so the window is capped at four times the limit (2 MiB for telemetry: a default-level compressor asks for exactly that) and a frame declaring more is refused. The "under 4 MiB" of the plan is 6 MiB at the 2 MiB window. |
| TRF-3 | already fixed by B12 | `TestDecompressShortXerialHeaderIsAnError`, and the on-the-wire test that the connection lives was added |
| TRF-4 | fixed, red | `TestResourceIsBounded`, `TestAResourceBurstDoesNotGrowTheStore` |
| TRF-5 | safeguard built; the answer is not changed | a token bucket (a burst of 20, then one per third of the interval) skips decoding and counts a push (`Instance.Skipped`), answering 0 as before. What real Kafka answers is `memory` (R1): `TestEarlyPushIsAnsweredAsRecorded` is written and skipped. |
| TRF-6 | fixed | caps 100 x 150 x 60, 12 bytes a sample, `TestStoreWorstCaseIsUnderBudget` (24.7 MB estimated, under 32 MiB measured); `Summaries`, `Instance`, `ForConn` copy only what is asked |
| TRF-7 | fixed, red | `TestFirstErrorFindsEveryErrorCodeOfEveryResponse` (failed for exactly the five keys listed) |
| TRF-8 | fixed, red | `TestExpectedFlowIsNotAnError`, the class table, the console's refusals read it |
| TRF-9 | fixed | the link is a `template.URL`; `TestClientRefusedInARowIsFlagged`, e2e "links a client name to only its requests" |
| TRF-10 | fixed, red | `TestTransactionalIdIsNotAGroup`, `TestSummarisersReportWhatTheRequestNames` |
| TRF-11 | fixed, red | `TestConnectionEndingRequestsAreObserved` (unserved API, panic, bad header, request before authentication). A failed write is observed too; an oversized frame is not (the loop returns before the key is known). |
| TRF-12, 13 | fixed, red | `TestAFoldedRowKeepsItsId`, `TestBusyTrafficDoesNotResetAFoldedCount`, `TestEvictionIsByLastActivity` |
| TRF-14 | fixed, red | `TestEmbeddedBrokerDoesNotAdvertiseTelemetry`, `TestNoConsoleMeansNoTelemetry` |
| TRF-15 | fixed | "N of M requests match" (requests among those kept); the table says when it is cut at 200 rows |
| TRF-16 | cells written and run | `compat/java-telemetry`, `compat/kcat-telemetry`: the client's card on the page. The cells now grep the `Traffic` page's client list; see the compat note below. |
| TRF-17 | fixed | `TestSummarisersReportWhatTheRequestNames` (22 rows) and the summariser coverage |
| TRF-18 | fixed, red | `TestGroupFilterIncludesItsMembersFetches` |
| TRF-19 | fixed, red | `TestGroupEventsAreKeptInOrderPerGroup` (a noisy group), `TestTheQuietestGroupIsTheOneForgotten` |
| TRF-20 | decided | a row's time is when it began, and the table is ordered by it; the row appears when the request ends |
| TRF-21 | fixed, red | `TestOnlyTheConsolesOwnIdsAreLeftOut`, `TestTheRecordersLeftOutIdsAreTheConsolesOwn` |
| TRF-22 | fixed | the client's chart copy says what it covers and "one point per report" (`TestClientInspectorShowsWhatItReportsAboutItself`) |
| TRF-23 | fixed | `TestFactsNeverHoldRecordBytesOrSecrets`, the fields held are listed in `observability.md` |
| TRF-24, 25, 26 | fixed, red | `TestTruncatedPointIsMalformed`, `TestPastTimestampsAreClamped`, `TestDroppedSeriesAreCounted` |
| TRF-27 | **narrowed** | an instance is a principal's: another principal is refused; another connection of the same principal is not, and the instance follows the connection that pushed. The plan's rule (refuse a push from another open connection) acted on `memory` (R4) and broke the Java client, which subscribes on its bootstrap connection and pushes on the one metadata gave it (found by the held-consumer browser spec). |
| TRF-28 | fixed | metrics are off the request region; the hash is from counters (`TestClientMetricsDoNotRefreshTheRequestRegion`) |
| TRF-29 | done by the foundation (E11) | `task api` |
| TRF-30 | fixed, red | a connection that has not sent a request is not a client |
| TRF-31 | fixed | a client with no id is named by its software and address |

**Not done, with the reason.**

- **R1 to R9 are not recorded.** The scenarios need the foundation's recorder run
  against `apache/kafka`; Docker is present, but the evidence standard asks for a
  scenario each and this pass built the console and the backend. Every claim of the pass
  that rests on them is `doc` or `memory` and is listed above (R1 the early push, R4 the
  other connection, R6 `MEMBER_ID_REQUIRED` as expected flow with KIP-394, R8 "any join
  makes everyone rejoin"). Count at `doc` or `memory`: 9 of 9.
- **The dock endpoint** is the existing `/dock`, which now pages by the recorder's row ids
  (the contract's cursor); no `/traffic/dock` was added.
- **Per (client, topic, direction) totals** for the map (W5) were not built; the map stage
  owns them.
- **Decoded fields use the library's names**, not Kafka's JSON spec names (see the
  deviations).
