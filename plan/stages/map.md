# Stage: the map (MAP)

Boards: `Map.dc.html`, `Map-Large.dc.html`, and the neighbours sentence on every
resource page (foundation [A5](../00-foundation.md#a5-neighbours)), which is the
same graph read one hop at a time. Follows [the protocol](../01-protocol.md).

## 1. Summary

The map answers "who writes to what, who reads it, how far behind are they, and
what is being refused". It is drawn from four kinds of node (client, topic,
group, schema subject) and three kinds of edge (writes, reads, describes), with
lag and error state on the edges.

**Almost all of this is new backend.** The console has no graph today: no node
list, no edge type, no layout, no route. What exists is the raw material:

| Material | Where | What it knows |
|---|---|---|
| Per-request rows | `internal/traffic/traffic.go` `Recorder.Request` | client id, software, principal, API, topics, group, error name, duration, bytes (a ring of 2,000 rows, `server/server.go:106`) |
| Live connections | `Recorder.Clients` | the groups a connection spoke for (at most 4) |
| Coordinator events | `Recorder.GroupEvent` | joined, left, expired, generation (not edges) |
| Committed offsets and lag | `console/backend_groups.go` `GroupOffsets`, `TopicConsumers` | group to topic, per partition, lag against the high watermark |
| Group members | `ListGroups`, `DescribeGroup` | state, protocol, member client ids and assignments |
| Subjects | `console/registry.go`, `handlers_schemas.go:277` | `<topic>-key` and `<topic>-value` by name only |

Two sources feed it, and the boards blur them (MAP-7). **Wire edges** exist
because a request was seen (client writes topic; client fetches topic; client is
refused by a group or topic). **State edges** exist because the broker holds
something (a group holds committed offsets on a topic; a subject is named after
a topic). A group that is empty and 4,816 behind (the board's `g-orders-audit`)
has a state edge and no wire edge. Both are drawn; they look different.

Size: **L, about 13 days** (section 11). The layout, the aggregator and the lag
batch are each a day or two on their own, and the page is three more.

Packages owned: `internal/traffic` (aggregator), `internal/protocol/summary.go`
(structured request summaries), `console` (graph, layout, handlers, templates),
`cmd/doze-kafka/traffic.go` (the adapter). Peers: [traffic](traffic.md) (the
recorder is its code; this stage adds to it and does not change its rows),
[groups](groups.md) (lag), [schemas](schemas.md) (subject to topic),
[access](access.md) (the ACL rule on a refused edge).

### Decisions, with reasons

**D1. Where the graph is computed: two halves, joined in the console.**

| Half | Computed where | Why there |
|---|---|---|
| Wire edges | Broker side, in `internal/traffic`, as a new aggregator beside the ring | Only the broker sees requests. The console is a Kafka client of the broker and must not import its internals; `cmd/doze-kafka/traffic.go` already adapts the recorder into console-owned types, and the aggregator gets one more method there (`Flow() console.FlowSnapshot`). |
| State edges, lag, subjects, node lists | Console side, from the Kafka API and the registry HTTP API | The console already does this for the Groups and Topics pages. It works with a console that has no recorder (D4). |
| Merge, layout, rendering | Console side | Pure functions of two inputs, testable without a broker. |

The aggregator is **not** a read of the ring. The ring has 2,000 rows, Produce
and non-empty Fetch never fold (`Idle` is set only for empty fetches and
heartbeats, `summary.go:98`, `:384`), so one producer at 12 requests a second
fills the ring in under three minutes and evicts every slower flow: a nightly
batch job would never appear (MAP-4). The aggregator keeps one small record per
edge, updated in `Request`, and the ring is untouched. The board text "the last
2,000, in memory" is wrong under this design and is rewritten (MAP-7).

**D2. Decay.** A wire edge is remembered for **60 minutes after its last
request** and drawn in three states by age: live (last request within 60 s),
recent (within 15 min), idle (older; dashed, captioned "last seen 41 min ago").
After 60 minutes it is dropped. State edges do not decay: they are present while
the state is. Nothing is persisted: a restart empties the wire edges, as it
empties the ring today, and the page says "no requests since this broker
started at 14:02" rather than drawing nothing silently. The 60-minute figure and
the 60 s and 15 min cut-offs are rows in the settings table (B7), not constants
in a template.

**D3. Bounds.**

| Bound | Value | Behaviour at the limit |
|---|---|---|
| Wire edges kept | 2,000 | evict the least recently seen; count evictions, show "N older edges not kept" |
| Distinct client keys | 200 | the rest fold into one node "other clients (N)" |
| Client id length | 128 bytes | truncated on a rune boundary, control characters replaced, before it is stored |
| Error names per edge | 8 | counted by name; a ninth is counted under "other" |
| Nodes drawn on the overview | 60 | the overview shows the 60 most relevant (anything refused or lagging first, then most recently active); the rest are one "N more" chip that opens Map-Large |
| Nodes drawn on Map-Large | 500 | beyond that, filters are required and the page says so |
| Groups whose lag is computed per refresh | 100 | the rest are drawn without lag and say "lag not computed" |
| Lag cache | 3 s, one flight at a time | many tabs share one computation |

Aggregator memory at the cap is about 2,000 edges x 300 B = 600 KB, allocated
lazily, so the idle heap with no traffic does not move (lightness gate B9).

**D4. Without the recorder.** The recorder exists only when the broker has a
console in the same process (`server/server.go:301`). `--console=false` has no
console and so no map; there is nothing to degrade. The two cases that matter:

- The **module console** (`module/serve.go:83`) and any console started apart
  from the broker pass no `Traffic` source. The map works from state alone:
  topics, groups (with members, committed offsets and lag), subjects. It shows no
  clients and no produce edges, and says so in a banner: "This console is not
  inside the broker, so it cannot see who produces. Groups, lag and schemas are
  shown." Today the Traffic section is *hidden* in this case
  (`TestWithoutATrafficSourceThereIsNoTrafficSection`); the map is *offered*,
  because two thirds of it does not need traffic.
- An embedder using `server.New` with `Console: true` gets the full map.

**D5. Auth off.** Every connection is `User:ANONYMOUS`
(`internal/protocol/conn.go:101`), so the principal cannot tell clients apart.
A client node's key is its **client id**; if that is empty, `software@host`
(host only, never the ephemeral port). The principal is an attribute shown in
the inspector, never part of the key, so one client id used by two users is one
node whose inspector lists both. With auth off there are no ACL refusals, so the
legend entry "refused by an ACL" is not shown; other errors
(`UNKNOWN_TOPIC_OR_PARTITION`, `COORDINATOR_NOT_AVAILABLE`) are drawn as
**Failed**, not **Refused**, per foundation A2. With auth on, the console's own
user needs Describe on every topic and group or the lag half goes blank; the
page says which call it was refused (a `Refusal`, A2), not an empty map.

**D6. Privacy.** The graph holds names and counters only: no keys, values,
headers, payload bytes or schema ids. The wire peer is reduced to a host (no
port). The principal is shown in the inspector only. The console's own clients
(`doze-kafka-console*`, documented in `docs/observability.md:31`) are excluded
as in Traffic. The exposure is the same as the Traffic page (anyone who can open
the console), and the map adds no new listener. It is in memory only, and
`/api/map` sits behind the same host guard as every other route.

**D7. Layout: computed in Go, on the server, as a pure function.** Reasons: it
is testable with `go test` property tests and a fuzz target, it needs no
JavaScript, it is the same for every viewer, and the boards' absolute-positioned
boxes can be emitted directly as `style="left:..;top:.."`. (The doze-aws
console once computed a layout in Go for a page that was later deleted, and kept
computing it unused; `client_flow.go` now carries no geometry. Here the layout
has exactly one consumer and is tested on its own.) The algorithm is in
section 6, under "The layout".

## 2. Contract

The map owns **no Kafka API and no broker behaviour a client can see**. It reads
these, all F today, and adds nothing to `docs/api-support.md` except a note on
the Console row. Tiers are the existing ones.

| Key | API | Used for | Now | Target | Note |
|---|---|---|---|---|---|
| 0 | Produce | wire edge client to topic | F | F | records and bytes need a structured summary (MAP-3) |
| 1 | Fetch | wire edge client to topic (reads) | F | F | no group on the request; the group is joined by connection |
| 2 | ListOffsets | high watermark for lag | F | F | one batched call for all topics, not one per topic |
| 3 | Metadata | node list | F | F | not an edge: admin tools call it for every topic |
| 8 | OffsetCommit | wire edge group to topic | F | F | the strongest evidence a group reads a topic |
| 9 | OffsetFetch | committed offsets, lag; edge | F | F | v8+ takes many groups in one request: confirm the broker answers it (check below) |
| 10 | FindCoordinator | none | F | F | its `Group` may be a transactional id (MAP-1) |
| 11-14 | JoinGroup, Heartbeat, LeaveGroup, SyncGroup | membership; refused edges | F | F | |
| 15, 16 | DescribeGroups, ListGroups | members, state | F | F | |
| 25, 28 | AddOffsetsToTxn, TxnOffsetCommit | group to topic for transactional consumers | F | F | |
| 68, 69 | ConsumerGroupHeartbeat, ConsumerGroupDescribe | group to topic for KIP-848 groups (`SubscribedTopicNames`) | F | F | the second group protocol: both must produce the same edges |

Console routes (`console/console.go`), all new, all GET, none mutate:

| Route | Purpose | Tier |
|---|---|---|
| `GET {p}/map` | the overview (Map) | new, F |
| `GET {p}/map?zoom=out` | all nodes (Map-Large), same handler | new, F |
| `GET {p}/map/graph` | HTMX partial, polled every 5 s while Live | new, F |
| `GET {p}/map/edge?kind=&from=&to=` | inspector body for an edge (A1) | new, F |
| `GET {p}/map/node?kind=&name=` | inspector body for a box | new, F |
| `GET {p}/neighbours?kind=&name=` | the A5 sentence, lazily loaded on resource pages | new, F |
| `GET {p}/api/map` | the graph as JSON, for tests and the palette | new, F |

Declined: persisting edges across a restart; reading payloads to learn which
schema a topic uses (MAP-9, owed to [schemas](schemas.md)); drawing transactional
ids as a node kind; drawing the console's own requests.

## 3. Where it stands

Nothing for the map itself. Tests that touch the material:

| Layer | What exists |
|---|---|
| `internal/traffic` (8 tests) | ring bounds, console exclusion, idle folding, folding window, filters, topic-id resolution, live connections, group events |
| `console/traffic_test.go` | Traffic page, filters, drawer, no-source case, client metrics |
| `e2e/tests/traffic.spec.ts` | 7 tests: rail, filters, drawer, live toggle, group timeline, palette, client metrics |
| `internal/protocol` | the API table test "a row for every API"; no test of what a `Summary` says per API |
| `compat/` | no cell reads the graph; every cell produces traffic the map would draw |

Coverage today (baseline, full suite): `internal/traffic` 96.7%, `console`
87.8%, `internal/protocol` 71.9%, `cmd/doze-kafka` 79.8%. Function level in
`internal/traffic`: `New` 66.7% (the `capacity < 1` branch), `Request` 98.0%,
`Events` 92.9%, `Clients` 93.3%. `summary.go` has no direct test: its functions
are covered only through the broker e2e tests, so the per-API `Summary` is
unchecked (MAP-1, MAP-2, MAP-3 have no test that could have caught them).

## 4. Findings

Evidence is `read` unless stated; no finding here needed a binary to show. Source
is `read` of code plus `spec` where the wire field is named.

| ID | Sev | What | Where | Ev | Src | Test to write first |
|---|---|---|---|---|---|---|
| MAP-1 | P2 | `Summary.Group` holds a consumer group **or a transactional id**: Produce with a transaction id, InitProducerID, AddPartitionsToTxn, EndTxn, DescribeTransactions and a transaction-type FindCoordinator all set it. A graph built on `Event.Group` would draw transactional ids as groups (and `Client.Groups` already lists them). | `internal/protocol/summary.go:70`, `:173`, `:266`, `:276-279`, `:303`, `:369` (`sumProduce`, `sumFindCoordinator`, `sumInitProducerID`, `sumAddPartitionsToTxn`, `sumEndTxn`, `sumDescribeTransactions`) | read | spec | Unit (protocol): table test over every API row, asserting `Group` is empty for a transactional Produce and `TxnID` carries the id. |
| MAP-2 | P2 | One error code per request: `firstError` walks the response and returns the first non-zero code. A Produce or Fetch naming two topics, one refused, attributes the error to **both** topics' edges. | `summary.go` `firstError` / `walkErrors`; `traffic.go:184` (`Error: protocol.ErrorName(o.ErrorCode)`) | read | spec | Unit (protocol): a two-topic Produce where the second topic answers `TOPIC_AUTHORIZATION_FAILED` yields a per-topic outcome with the error on topic 2 only. |
| MAP-3 | P2 | A `Summary` has no structured size: records, bytes, partitions and batches are folded into the free text `Detail` ("3 partitions, 4.8 KiB"). Rate, bytes per second, partition count and "1 batch" on the edge inspector cannot be computed, only parsed back from prose. Produce's size is `len(Records)` bytes with no record count; the count is in the v2 batch header (`baseRecordsCount`). | `summary.go:56-66`, `:98-103` | read | spec | Unit (protocol): a Produce with two batches of 3 and 4 records reports 7 records, 2 batches, and the bytes of the request, per topic. A truncated or corrupt batch reports 0 and does not panic (fuzz, section 7). |
| MAP-4 | P2 | The ring cannot be the source of a map. It holds 2,000 rows; Produce and data-bearing Fetch never fold (`Idle` only for empty fetch, heartbeat, steady telemetry push), so a busy client evicts the rest. The board's claim "the last 2,000, in memory" would make edges vanish under load. | `traffic.go:206-214`, `server/server.go:106`, `summary.go:98` | read | read | Unit (traffic): 1 slow produce then 3,000 busy fetches from another client; the aggregator still has the slow edge, `Events` does not. |
| MAP-5 | P3 | `Client.Groups` keeps at most four groups per connection and includes transactional ids, so "which groups does this client belong to" is wrong for a client in five groups. | `traffic.go:162-166` | read | read | Unit (traffic): five groups on one connection; all five are reported, none is a transactional id. |
| MAP-6 | P3 | `Filter.Client` is a substring match on client id, software **or remote address**, while `Topic` and `Group` match exactly. "Open in Traffic" from the edge `checkout-api -> orders` would also show `checkout-api-canary` and a client whose address contains the text. | `traffic.go:252-278` (`matches`) | read | read | Unit (traffic): `Filter{Client:"checkout-api", ExactClient:true}` excludes `checkout-api-canary`. Adds an exact-match field; the Traffic page keeps substring. |
| MAP-7 | P2 | Board copy contradicts the board. "drawn only from requests this broker has seen. No traffic, no edge." and "Edges come from requests in the traffic ring" are false for `g-orders-audit`, which is drawn with "lag 4,816 · empty" and a fetch edge though no group is connected. Its edge is a state edge from committed offsets. Also "Not drawn: Clients that never produced or fetched in the ring". | `Map.dc.html` (the header sentence, the edge note, the "Not drawn" line) | read | read | Console (handler test): the page text for a fixture with one wire edge and one state edge distinguishes them; the legend names both. E2e: the legend is present. |
| MAP-8 | P2 | Lag is defined three ways in the code that will feed the map. `TopicConsumers` sums `max(Latest-Committed, 0)` only over partitions the group **committed on**, so a group that committed on 1 of 3 partitions shows 1/3 of its lag; it uses the high watermark, not the last stable offset, for a `read_committed` group (the Gr-14 board); and `if err != nil { continue }` drops a group whose offsets could not be read, silently, so a refused or timing-out group disappears from the map instead of being marked. | `console/backend_groups.go:493-506` | read | read | Unit (console, fake backend): partial commit, committed beyond end (`max` clamp), and an OffsetFetch error; the last returns the group with `LagUnknown`, not nothing. |
| MAP-9 | P2 | A subject belongs to a topic only by `<topic>-key` / `<topic>-value`. `TopicRecordNameStrategy` (`<topic>-<record>`) and `RecordNameStrategy` subjects are never linked, so a topic whose producers use them shows no `describes` edge although every record carries a schema id. | `console/handlers_schemas.go:284-296` (`topicSchemas`), `:362`, `console/registry.go:316` | read | doc | Unit (console): subjects `orders-value`, `orders-com.acme.Order`, `com.acme.Order` against topic `orders`; the first is certain, the second is "by name, guessed", the third is not linked. Behaviour of real serializers goes on the recording queue (R2). |
| MAP-10 | P2 | A topic name can contain `-key` / `-value` and also be a prefix of another topic (`a`, `a-b`). Subject `a-b-value` is `a-b`'s value subject, and under the prefix rule in MAP-9 would also be a candidate for `a`. A naive prefix match draws two edges. | design (MAP-9 fix) | read | read | Unit (console): exact `-key`/`-value` wins and stops; the prefix rule runs only for subjects not claimed, and picks the longest existing topic name. |
| MAP-11 | P2 | Client identity is unreliable as a node key. Java consumers default to `consumer-<group>-1`, producers to `producer-1`, admin tools to `adminclient-1`; many pods share one id and a restarted pod keeps it, while some clients send none. A node per distinct id floods on a fleet; a node per connection floods more. | `traffic.go:122-129` (the id is per connection, last non-empty wins) | read | doc | Unit (traffic): 300 connections with 300 ids produce at most 200 nodes plus "other (100)"; 5 connections with one id are 1 node whose `Instances` is 5. |
| MAP-12 | P2 | `Recorder.Request` holds the one recorder mutex for the whole request, including `r.resolve` (which calls `Broker.TopicName`, which takes the metadata lock) and the clones. The aggregator would add work to the same critical section on every request of every connection. | `traffic.go:144-155`, `:158-165`, `broker.go:271` | read | read | Benchmark + `-race` stress (traffic): 64 goroutines x 100k requests; the aggregator has its own mutex and `Request` p99 does not grow by more than 20% against the baseline in `testdata/bench.txt`. A lock-order test: a coordinator emitting `GroupEvent` while a Fetch resolves a topic id does not deadlock. |
| MAP-13 | P3 | `New(0, nil)` and `capacity < 1` are the only uncovered statements in `New` (66.7%). | `traffic.go:116-120` | run | read | Unit (traffic): `New(0, nil)` holds one row. |
| MAP-14 | P2 | The Edge inspector on Map-Large says "The ACL rule is Read on the group for User:loyalty". The console has no way to name the rule that decided: the broker answers an error code and the Access page lists ACLs. Naming a rule needs an authorizer "explain" that does not exist. | `console/access.go` (list only); `internal/broker/authorizer.go` | read | read | Depends on [access](access.md) (the checker). Until then the inspector says "no ACL grants Read on group g-loyalty-etl to User:loyalty" from the ACL list, and says it was **derived** from the list, not reported by the broker. Handler test over a fixture. |
| MAP-15 | P2 | The Map board's captions rest on data nobody keeps: "retry · 3 in 5 m", "dlq · last 14:02", "23 records". Retry and DLQ are naming conventions (`.retry`, `.dlq`, `-dlt`), not a broker concept; "3 in 5 m" is a record count over a window, needing MAP-3's record count; "23 records" is end minus start offset (`ListOffsets`), real; "last 14:02" is the last produce edge time, real only while the edge is inside D2's window. | `Map.dc.html` | read | read | Unit (console): classification by suffix is a label only and never changes an edge; a topic named `x.dlq` with no matching suffix rules is drawn as a plain topic. |
| MAP-16 | P3 | `ListTopics`/`PartitionBounds` are per topic; computing lag for all topics with them is N+1 requests per refresh. | `console/backend.go:466` | read | read | Unit (console, counting fake): a refresh with 7 topics and 4 groups issues one Metadata, one ListGroups, one OffsetFetch (multi-group) and one ListOffsets. |
| MAP-17 | P2 | Client ids and topic names are attacker-chosen (any process that can connect). They reach HTML, the `style` attribute and URLs on this page for the first time as box labels and `?from=&to=` query values. | new code; pattern as in the existing "links built from unescaped names" class | read | read | Handler test: client id `"><script>`, a topic with `%` and `/`, a 5,000-byte client id; output is escaped, URL-escaped via `queryEscape`, truncated. E2e: a client with `<b>x</b>` as id renders as text. |

No P0 or P1 found. How: the map adds no write path and no Kafka API, so the
protocol-level classes (wrong error code, unbounded read, state not persisted)
do not apply to it; the existing code it reads was audited for what it hands
over, and the defects above are in that hand-over. Checked and **not** found:
data race on `Recorder` (every field access is under `r.mu`; `TestTheRing...`
runs under `-race` in `task check`); `Events` returns copies, so the console
cannot alias recorder rows.

## 5. What to check

- Edges come only from a **whitelist of APIs**: Produce, Fetch, OffsetCommit,
  OffsetFetch, JoinGroup, SyncGroup, Heartbeat, LeaveGroup, TxnOffsetCommit,
  AddOffsetsToTxn, ConsumerGroupHeartbeat. Metadata, ListOffsets,
  DescribeConfigs, DeleteTopics, CreateTopics and the rest name topics and are
  **not** edges. A test: `kafka-topics.sh --describe` (Metadata, DescribeConfigs)
  adds no node edge.
- Both group protocols produce the same reads edge: a classic consumer
  (OffsetCommit) and a KIP-848 consumer (`ConsumerGroupHeartbeat.SubscribedTopicNames`
  plus its OffsetCommit) subscribing to the same topic give the same graph
  except for the `protocol` attribute.
- A group with committed offsets on a **deleted topic** has no topic to draw to:
  it is listed under "Not drawn", with the topic name, not given a ghost node
  (Gr-11).
- A produce to a topic that does not exist and is refused
  (`UNKNOWN_TOPIC_OR_PARTITION`) draws a dashed Failed edge to a node marked
  "does not exist". It does not create the node as a real topic.
- A transactional producer is one client; its transactional id is not a node.
- Empty graph, one client, one topic, a cycle of apps (an app that consumes
  topic A and produces topic B and consumes B): the layout has no cycles because
  columns are by node kind, never by flow depth (section 6).
- `OffsetFetch` with the `Groups` array (v8+) answers many groups in one call on
  this broker; if not, the lag batch is N calls and MAP-16's budget changes.
  (Read `internal/broker/offsets.go`; test before relying on it.)
- Refresh while a group rebalances: lag is per-refresh and not sticky; a group
  with no commits between two refreshes does not flicker out of the graph.
- `-race` on the aggregator; lock order against `GroupEvent` and `TopicName`
  (MAP-12).
- Works with the console under a non-default prefix, a listener on `:0`, TLS and
  SCRAM on, a data dir with a space (A8).
- The page and `/api/map` answer when the broker is down (A7): last good graph,
  frozen, with the disconnected pill; never a 500.

## 6. Console

### Boards

| Board | Template + handler | Status | Built from |
|---|---|---|---|
| `Map.dc.html` (overview) | `templates/map.html`, `Console.mapPage`, partial `mapGraph` | needs backend | the aggregator (wire edges), `GroupOffsets` + batched `ListOffsets` (lag, state edges), `ListGroups`/`DescribeGroup` (members, state), registry subjects (describes), inspector partial (A1) |
| `Map-Large.dc.html` (zoomed out) | same template, `?zoom=out`; filters `only lagging`, `only errors`, `hide schema` are query flags, so a link keeps them | needs backend | the same graph; the filter dims, never removes, and the count line is computed ("3 of 4 groups lag; 4 boxes dimmed") |
| Neighbours sentence (A5) on every resource page | partial `neighbours`, `Console.neighbours`, `GET /neighbours` | needs backend (graph); the sentence component itself is foundation A5 | one hop of the graph around the node |

`new` items on the boards and what each is built from:

| `new` item | Built from | Status |
|---|---|---|
| Client boxes with "12 produce/s", "franz-go 1.18" | aggregator edge rate (records/s from MAP-3), client software from ApiVersions (`Summary.Software`) | needs backend |
| Topic boxes with partition count, retry/dlq captions, "23 records" | Metadata, `ListOffsets`, name suffix (MAP-15) | buildable (data exists) |
| Group boxes "lag 14 · 3 members", "empty" | `ListGroups`, `GroupOffsets`, lag rule (MAP-8) | buildable |
| Subject chips (overview) and column (large) | registry, naming rule (MAP-9, MAP-10) | buildable; partial coverage by design |
| Edge colours: traffic seen / lag to look at / refused | aggregator error names; lag threshold; D5 for Refused vs Failed | needs backend |
| Edge inspector: rate, last request, errors, schema, "Same topic, other edges" | aggregator edge record; the sentence is generated from the graph, not typed | needs backend |
| "Last request: Produce v9 · 14:36:21.402 / p1 · 1 batch · 412 B · 2 ms" | `Event` has API, version, time, duration, bytes; partition and batch count are not kept | needs backend (MAP-3); drop "1 batch" if batch walk is not built |
| Refused edge with the ACL rule | MAP-14 | needs backend ([access](access.md)) |
| "Not drawn" line | the aggregator's drop counters and the console's own exclusion | needs backend |
| Keys `<-` `->` follow an edge, Enter, `f`, `-`/`+` | boxes and edges as a focus order (below) | buildable |

Teaching lines (A3) come from the settings table (B7): the decay windows, the
lag-watch threshold, the caps in D3. Each is a row; the legend text "Amber: lag
worth a look" is generated as "Amber: lag of 100 records or more" from the same
row. A test fails for a threshold in code with no row.

Wording to correct on the boards when built (all MAP-7): the header sentence,
the edge note ("Edges come from requests in the traffic ring ..."), and "Not
drawn". Replacement: "Solid lines were seen on the wire in the last 60
minutes. Thin lines are state the broker holds: committed offsets and subject
names."

Focus and keyboard: boxes and edges form one list in layout order (column, then
row); Tab visits boxes, `<-`/`->` move along the edges of the focused box
(sorted by peer name), Enter opens the inspector (A1, with a URL so a refresh
and Back work), `f` focuses the filter, `-`/`+` switch overview and large. Every
edge also exists as a row in a visually hidden table for screen readers
(A12 sweep), because an SVG line is not a control.

### The layout

Pure function `layoutMap(g Graph, prev Layout, opt LayoutOpts) Layout` in
`console/maplayout.go`. No time, no randomness, no map iteration order in the
output.

1. **Columns by kind**, fixed: client, topic, group, then subject (Large only; on
   the overview a subject is a chip inside its topic box). Edges therefore all go
   left to right (writes: client to topic; reads: topic to group), and an app that
   both consumes and produces appears as a client node and a group node, so the
   graph is acyclic by construction and no cycle-breaking is needed. `describes`
   edges (subject to topic) are drawn in the gutter as thin dotted curves.
2. **Components.** Connected components (undirected) are laid out as separate
   horizontal bands, sorted by their smallest node key (`kind/name`), isolated
   nodes last in a final band. Band order does not depend on rates or times.
3. **Order within a column and band.** Initial order is `prev`'s order for nodes
   it knew, then new nodes inserted at their barycentre of already-placed
   neighbours (ties by `kind/name`). Then at most 4 down-and-up barycentre sweeps,
   accepted only if they reduce edge crossings by at least 1 (hysteresis, so a
   refresh that changes one edge weight reorders nothing). Weights are
   presence-only: **rate, lag and age never enter the ordering**, only whether an
   edge exists.
4. **Geometry.** Integer pixels on an 8 px grid. Box height by kind and content
   (fixed per kind, taller with a caption), 24 px gap, column x fixed, band gap
   48 px. Edge endpoints are the right-middle of the source box and left-middle of
   the target; routing is a cubic with control points at the column mid-gutter.
5. **Output.** `Layout{Width, Height, Boxes[key]{X,Y,W,H}, Bands}`; the template
   emits `left`/`top` styles. The previous layout is held in the `Console` (one
   per zoom level, replaced under a mutex, order only, no times), so two viewers
   and consecutive refreshes see the same positions. With `prev == nil` the result
   is the canonical layout of the graph.

The cap of 60 (overview) or 500 (large) nodes bounds the cost: the sweeps are
O(sweeps x edges x log n), well under a millisecond at the cap.

### Tests for the layout (section 7 lists them by layer)

- **Determinism:** the same graph given as 100 random permutations of its node
  and edge slices yields byte-identical `Layout`; and once more with `prev` set to
  its own output.
- **No overlap:** property test over random graphs (1 to 500 nodes, 0 to 2,000
  edges): no two boxes intersect (including the gutter margin), every box is
  inside `Width x Height`, every edge endpoint lies on its box's border, no box
  has a negative coordinate.
- **Stability:** (a) changing only rates, lag, ages or error counts moves zero
  boxes; (b) adding one isolated node moves no existing box; (c) adding one node
  with one edge moves at most the boxes in its own column and band; (d) removing a
  node moves at most the boxes below it in its column and band; (e) layout
  followed by the same layout with `prev` is a fixed point.
- **Bounds:** 500 nodes lay out in under 50 ms (benchmark) and the canvas is
  under 16,000 px in either dimension.
- **Fuzz:** `FuzzLayoutMap` over arbitrary node/edge lists (dangling endpoints,
  duplicate edges, self-edges, empty names) never panics and drops dangling edges
  with a count.

## 7. Tests to add

Every row below is red before the code it tests exists (protocol step 2).

| Layer | Test | Covers |
|---|---|---|
| Unit (protocol) | `TestSummaryPerAPI` table over every row of the API table: topics, group, transactional id, records, bytes, per-topic error | MAP-1, MAP-2, MAP-3 |
| Fuzz | `FuzzBatchCount` on the produce record-count walker: arbitrary bytes, claimed batch lengths larger than the buffer, zero length, nested | MAP-3; a new fuzz target (8th) |
| Unit (traffic) | `TestFlowKeepsSlowEdgesUnderLoad`, `TestFlowDecay` (fake clock: live, recent, idle, dropped), `TestFlowCapEvictsLeastRecent`, `TestFlowClientFold`, `TestFlowErrorNamesCapped`, `TestFlowTruncatesClientIDs`, `TestFlowSkipsConsoleClients`, `TestFlowWhitelistOfAPIs`, `TestClientGroupsNotCapped` | MAP-4, MAP-5, MAP-11, D2, D3 |
| Benchmark / race (traffic) | `BenchmarkRequestWithFlow`, `TestFlowRaceStress`, `TestRequestAndGroupEventLockOrder` | MAP-12 |
| Unit (traffic) | `TestFilterExactClient`, `TestNewZeroCapacity` | MAP-6, MAP-13 |
| Unit (console) | `TestBuildGraph` from fixtures: wire plus state edges merge; deleted-topic group is "not drawn"; unknown-topic failed edge; refused vs failed by auth mode; one edge per (client, topic) across both group protocols | D5, section 5 |
| Unit (console) | `TestLagRule`: partial commit, clamp, unknown on error, read_committed noted | MAP-8 |
| Unit (console) | `TestSubjectToTopic`: exact key/value, TopicRecordName guessed, longest-topic rule, no link | MAP-9, MAP-10 |
| Unit (console) | `TestLagBatchRequestCount` with a counting fake | MAP-16 |
| Unit (console) | `TestLayout*` (determinism, no overlap, stability, bounds), `FuzzLayoutMap` | section 6 |
| Handler (console) | `TestMapPageWithoutTrafficSource` (banner, state only), `TestMapPageEscapesNames`, `TestMapEdgeInspectorURLRoundTrip`, `TestNeighboursSentence` (written by / read by / schema subject; empty graph says nothing), `TestMapLegendNamesBothEdgeKinds` | D4, MAP-7, MAP-14, MAP-15, MAP-17 |
| Raw kmsg / broker | `TestBrokerFeedsFlow`: franz-go producer + classic consumer + KIP-848 consumer + a refused user (SCRAM, ACL) against `server.New`, then assert `Flow()` edges exactly | contract, both protocols |
| Client library | the same through Sarama (second client family: different client id and batching) | MAP-11 |
| Compat cell | no new cell; add an assertion step to two existing cells (Java, librdkafka) that after the run `/api/map` contains the cell's client id and topic | real clients |
| E2e (Playwright) | `map.spec.ts`: (1) overview shows a seeded producer, topic, group, lag; (2) click an edge opens inspector, URL survives reload, Back closes; (3) zoom out and filters dim and count; (4) refused edge shows `GROUP_AUTHORIZATION_FAILED` as Refused; (5) keyboard follow with arrows; (6) stop the broker mid-poll: frozen map + pill; (7) 960 and 390 widths; (8) empty data dir shows the empty state | boards |
| E2e (A12) | map and large added to the axe crawl | accessibility |
| Mutation | `task mutate:check -- ./internal/traffic/... ./console/...` | step 4 |
| Differential vs real Kafka | none applies: the map invents no broker behaviour. The one broker fact it relies on (OffsetFetch with several groups) goes to R1 | |

Coverage targets and what must be covered to reach them:

| Package | Baseline | Target | Functions that must be covered |
|---|---|---|---|
| `internal/traffic` | 96.7% | 97.5% | every new `Flow*` function; `New`'s zero-capacity branch; decay and eviction branches |
| `internal/protocol` | 71.9% | 73% | `summary.go` every `sum*` function at >= 90%, the batch walker 100% |
| `console` | 87.8% | 88.5%, new map files >= 92% | `buildGraph`, `layoutMap`, `lagFor`, `subjectTopic`, the handlers and the inspector partials |
| `cmd/doze-kafka` | 79.8% | 80% | the `Flow()` adapter |

## 8. Recording queue

| ID | Claim | Source now | Scenario that would record it |
|---|---|---|---|
| R1 | `OffsetFetch` v8+ with several groups in one request returns each group's committed offsets, and an authorization failure on one group does not fail the others | memory | Docker `apache/kafka:4.3.1`: commit offsets for three groups, one with a deny ACL for the console user; one OffsetFetch with all three; record per-group error codes. Needed before the lag batch (MAP-16) is promised. |
| R2 | Which subject names the Confluent serializers register under `TopicNameStrategy`, `RecordNameStrategy`, `TopicRecordNameStrategy` | doc (Confluent serializer docs) | Confluent SR 7.7.1 plus the Java Avro serializer in the compat Java cell, once per strategy; record the subjects created. Settles MAP-9's guessed rule. |
| R3 | `kafka-topics.sh --describe`, `kafka-consumer-groups.sh --describe` and `kcat -L` issue only the non-edge APIs (Metadata, DescribeConfigs, ListOffsets, OffsetFetch) and so add no edge | memory | run each tool against doze-kafka and record the API sequence from `/traffic` into the compat harness; note `kafka-consumer-groups.sh` does issue OffsetFetch, so a human looking up a group briefly looks like a reader (see section 9). |
| R4 | A KIP-848 member's `SubscribedTopicNames` is sent on join and on change only, not on every beat | memory | Docker `apache/kafka:4.3.1`, a franz-go and a Java 848 consumer, record the heartbeat request bodies. If it is sent only on change, the reads edge for 848 comes from OffsetCommit alone. |

## 9. Owed to integration

- **Access stage:** the authorizer "explain" that names the deciding rule
  (MAP-14), and a 'check' result the inspector can link.
- **Schemas stage:** subject to topic by evidence (the schema id on a produced
  record) rather than by name. Declined here for privacy and because compressed
  batches cannot be peeked without decompressing; the schemas stage decides.
- **Groups stage:** the lag rule (MAP-8) is one function used by the Groups
  page, the Topic consumers panel, the map and the neighbours sentence; groups
  owns its definition (partial commits, read_committed). Until it lands, the map
  uses `lagFor` and a note on the board.
- **Traffic stage:** the recorder gains the aggregator and `ExactClient`; the
  stage that owns `traffic.go` merges them. The two must not both restructure
  `Request` in one release.
- **Bad days:** a fenced producer or a rebalance storm should colour its edge;
  the aggregator exposes error names, [bad-days](bad-days.md) decides which are
  drawn.
- **Observation:** `kafka-consumer-groups.sh --describe` run by a person issues
  OffsetFetch and would draw a short-lived reads edge for the group it looked at
  (R3). Not a bug; the 60 s "live" state and the "idle" dashing make it visible.

Asks of the foundation:

- A rail item **Map** under Observe (the boards show it, `A1` lists only Traffic
  and Trace).
- A5 should say the sentence is rendered from `Neighbours(kind, name)` on the
  graph type, not from a second query, so the two cannot disagree.
- B7 rows for: edge live/recent/idle/drop windows, lag-watch threshold, the
  caps in D3.
- B8 route-table rows for the seven routes in section 2.
- B9: the aggregator must not allocate before the first request (idle heap).

## 10. Acceptance criteria

- [ ] `TestSummaryPerAPI` fails if any API row lacks a topics/group/txn/size/error
      description; `Summary.Group` is empty for every transactional-id case.
- [ ] `go tool task coverage`: `internal/traffic` >= 97.5%, `internal/protocol`
      >= 73%, `console` >= 88.5% with each new map file >= 92%, `cmd/doze-kafka` >= 80%.
- [ ] `go tool task mutate:check -- ./internal/traffic/... ./console/...` shows
      no new survivor; survivors listed in the PR with a reason.
- [ ] `go test -race ./internal/traffic -run 'Flow|LockOrder'` passes;
      `BenchmarkRequestWithFlow` is within 20% of `BenchmarkRequest` before the change.
- [ ] `go tool task fuzz` runs `FuzzBatchCount` and `FuzzLayoutMap` clean (nine
      targets in all, not seven).
- [ ] `TestLayoutDeterministic`, `TestLayoutNoOverlap`, `TestLayoutStable`
      pass; 500 nodes lay out in under 50 ms.
- [ ] `TestBrokerFeedsFlow` passes with a classic consumer and a KIP-848 consumer
      and gives the same reads edge; also passes with SCRAM and an ACL refusal.
- [ ] `TestWithoutATrafficSource...` is updated: Traffic is still hidden, Map is
      shown with the state-only banner.
- [ ] `task lightness`: idle heap and binary size unmoved, or the commit says why;
      no new module dependency (the layout is hand-written).
- [ ] Both boards implemented: `map.spec.ts` passes with the route gate on; the
      axe crawl is clean on `/map` and `/map?zoom=out` at 1440, 960 and 390 widths.
- [ ] Every `new` item in section 6 works from the data it names or is cut with
      a note; "retry/dlq" captions are labels only (MAP-15).
- [ ] The three pieces of board wording in MAP-7 are replaced; no console text
      says "no traffic, no edge" or "the last 2,000".
- [ ] Neighbours: a topic page shows "Written by ..., read by ..., schema subject
      ..." from `GET /neighbours`; empty graph shows no sentence, not "none".
- [ ] Compat Java and librdkafka cells assert their client and topic appear in
      `/api/map`; `go tool task test:compat` green plain and SCRAM over TLS.
- [ ] Settings rows exist for every threshold; a test fails for a threshold with
      none.
- [ ] `docs/console.md` has a Map section; `docs/observability.md` states what the
      map remembers (60 minutes, in memory, nothing persisted, names and counters
      only); `docs/limitations.md` lists: subjects linked by name only, no
      clients when the console is not in the broker, edges lost on restart.
      `CHANGELOG.md` updated.
- [ ] Findings MAP-1 to MAP-17 each closed or moved with a reason; R1 to R4
      recorded or still queued and listed.

## 11. Files and order of work

### Order (half-day units)

| # | Work | Half-days | Files |
|---|---|---|---|
| 1 | Structured `Summary`: `TxnID` apart from `Group`, records, bytes, partitions, per-topic outcome; batch walker with its fuzz target; red tests first | 3 | `internal/protocol/summary.go`, `observe.go`, tests |
| 2 | Flow aggregator: edge record, rate buckets, decay, caps, client folding, whitelist; benchmark, race and lock-order tests | 4 | `internal/traffic/flow.go`, `traffic.go` (one call in `Request`), tests |
| 3 | Adapter and console interface: `FlowSource`, `Flow()`; `ExactClient` filter | 1 | `cmd/doze-kafka/traffic.go`, `console/traffic.go` |
| 4 | State side: batched OffsetFetch and ListOffsets, lag rule with unknown state, 3 s single-flight cache, group and topic node lists, subject to topic | 4 | `console/mapstate.go`, `backend_groups.go`, `handlers_schemas.go` |
| 5 | Graph type, merge, `Neighbours(kind, name)`, JSON | 3 | `console/mapgraph.go` |
| 6 | Layout, its property tests and fuzz target | 4 | `console/maplayout.go` |
| 7 | Handlers, templates, partial poll, inspector bodies, filters, zoom, keyboard, hidden table, legend and settings rows | 6 | `console/handlers_map.go`, `templates/map.html`, `static/shell.js`, `console.go` |
| 8 | Neighbours sentence on topic, group, subject, client pages | 1.5 | `templates/*.html`, `console/neighbours.go` |
| 9 | E2e, compat assertions, docs, mutation baseline, lightness | 3.5 | `e2e/tests/map.spec.ts`, `compat/`, `docs/`, `testdata/` |
| | **Total** | **30 half-days** | |

**Estimate: L, about 15 days of elapsed work, 13 if steps 4 and 6 run in
parallel.** The risk is item 7, not item 6: a map that is correct and
unreadable when the shop has 40 topics. The caps (60 and 500) and the dimming
filter are the answer, and the Large board should be checked against a seeded
broker with 40 topics and 15 clients before the overview is declared done.

Dependencies: the traffic stage should land its row changes first or the two
conflict in `Request` (section 9). Foundation A1 (inspector) and A5 (sentence)
are needed for items 7 and 8.

### Prior art

- **doze-aws** `internal/console/client_flow.go`: a wiring graph of resource
  nodes and typed edges with `Neighbors(svc, name)` for the one-hop strip and
  connected components for "unwired". It is derived from **configuration** (what
  points at what), so it needs no decay and no traffic. It once carried per-node
  geometry for a page that was deleted and kept computing it; the lesson taken is
  to give the layout one consumer and test it alone. The one-hop `Neighbours`
  shape is reused. Its node/edge split into a type with a lookup index is the
  same as here; the Kafka graph differs in having two evidence sources and a clock.
- **Redpanda Console** and **Kafbat UI** (both in the compat matrix) show a
  group's lag per topic and topic consumers, and neither draws a graph from
  traffic; consumer lag, not topology, is what they offer. No recorded behaviour
  is relied on here.
- **Confluent Control Center** has a data-lineage view in the commercial
  product, derived from interceptors in the clients; doze-kafka cannot require
  that, which is why the broker's own view of the wire is the source.
- **Apache Kafka** itself has no such concept: topology comes from client
  tooling (Streams topology descriptions) and from `kafka-consumer-groups.sh`,
  which is the lag definition MAP-8 follows.
- Layered graph drawing (Sugiyama): columns by kind replace the layering step,
  and barycentre sweeps with a crossing-reduction acceptance test are the
  ordering step; no cycle removal, no dummy-node routing.

## Foundation asks this stage builds

From [00-foundation.md section E](../00-foundation.md#e-asks-from-the-stages): asks stage 0 did not build because this stage is their only consumer.

- E5: a rail item **Map** under Observe that opens this stage's page (`Neighbours(kind, name)` is already built in `console/graph.go`).
- E10: the map aggregator allocates nothing before the map is first opened, held by the lightness gate (`testdata/lightness.json`'s idle heap).
