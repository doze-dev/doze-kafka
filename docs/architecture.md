# Architecture

You don't need any of this to *use* doze-kafka — but if you're curious how a Kafka
broker fits in a few thousand lines of Go, or you want to contribute, here's the tour.

## The shape of it

```
        TCP (optionally TLS)
                │
     ┌──────────▼───────────┐
     │   Wire layer          │  frame ⇄ request/response, version negotiation
     │   (internal/protocol) │  connection loop, SASL gate
     └──────────┬───────────┘
                │ decoded request
     ┌──────────▼───────────┐
     │   Broker / handlers   │  one function per Kafka API
     │   (internal/broker)   │  produce, fetch, groups, admin, auth
     └───┬──────────────┬───┘
         │              │
   ┌─────▼─────┐  ┌─────▼──────┐
   │  Log       │  │  Metadata  │
   │  storage   │  │  store     │
   │ (log)      │  │ (meta)     │
   └────────────┘  └────────────┘
    segment files    bbolt: topics,
    + sparse index    offsets, PIDs
```

Everything is one process. There's no controller, no quorum, no second node.

## Wire layer — `internal/protocol`

Owns the socket. It reads length-prefixed frames, parses the request header far
enough to route and log, and dispatches to a handler by API key. Request and response
bodies are encoded and decoded with [franz-go's `kmsg`](https://github.com/twmb/franz-go),
which knows every Kafka message at every version — so doze-kafka gets correct
flexible/tagged-field encoding for free and only has to decide *what* to answer, not
*how* to frame it.

`ApiVersions` is answered from a **version profile** (`internal/protocol/versions.go`)
selected by `--kafka-version`. A profile is just the set of API version ranges to advertise
plus whether the KIP-848 group protocol is on. An API is only advertised once a
handler actually serves it.

The connection loop also enforces the SASL gate: when auth is enabled, every request
other than the handshake is rejected until the connection authenticates.

## Handlers — `internal/broker`

A thin function per API: decode the request, call into storage or the group
coordinators, encode the response. The interesting logic lives in a few places:

- **Produce/Fetch** stamp offsets and stream record batches to and from the log,
  verbatim. Fetch supports long-poll: if there's nothing new yet, it waits (up to the
  client's max wait) on a notifier that any produce wakes.
- **Consumer groups** have two coordinators. The classic one
  (`group.go`/`groups.go`) is a rendezvous point that relays a client leader's
  assignment. The KIP-848 one (`group848.go`, with its assignors in `group848_assign.go`) computes assignments itself and
  drives an incremental, safe rebalance keyed on member epochs. Both keep membership
  in memory and rebuild it when clients reconnect; only offsets are durable.
- **Auth** (`auth.go`, `scram.go`) implements the SASL handshakes and per-connection
  state.

## Log storage — `internal/storage/log`

The append-only heart. A partition is an ordered set of segment files with exactly
one active segment. Appends write raw bytes with `WriteAt`; reads use `pread`, so the
log is served from the OS page cache and never buffered into the heap (this is the
flat-memory property). A background manager runs the coarse fsync timer and the
retention sweep. On open, recovery truncates any torn tail so the partition resumes at
its last intact record.

## Metadata store — `internal/storage/meta`

A small [bbolt](https://github.com/etcd-io/bbolt) database for the things that must be
durable but aren't the message log: the topic registry (name ⇄ UUID, partition count,
per-topic config), committed consumer-group offsets, and the monotonic producer-id
counter.

## The console — `console/`

A server-rendered web UI (Go templates, htmx, Alpine) that is deliberately **a client
of the broker**: it talks Kafka wire protocol through franz-go like any application,
and imports nothing from `internal/`. That keeps it honest (it shows what clients
see) and lets it run in the same process (the CLI) or against a unix socket (the doze
module). Pages are plain HTML; live views poll a partial that answers `204` when
nothing changed. See [Console](console.md).

The one thing a Kafka client cannot see is the broker's own view of its traffic, so that
is handed to the console rather than reached into: the connection loop tells an
`Observer` about every request (decoded and described by a per-API summariser in the
one API table), the group coordinators tell a sink what they did, and `internal/traffic`
keeps both in a bounded in-memory ring. `cmd/doze-kafka` adapts that to an interface
the console defines, so `console/` still imports nothing from `internal/`. With no
console there is no observer, and the connection loop pays a nil check.
See [Observability](observability.md).

## Embedding and the CLI — `server/`, `cmd/doze-kafka`

`server` is the public, embeddable seam: `server.New(Options)` wires storage, broker,
router and listener exactly as the CLI does, so the two cannot drift
([Embedding](embedding.md)). `cmd/doze-kafka` resolves configuration (defaults, then
`doze-kafka.toml`, then flags — one `config.Config` for all of it), validates it, binds
the broker and the console, claims the instance name in the `.doze` zone (or, under `--listen`, serves that address
instead; the two are exclusive, see [Endpoints](endpoints.md)), and prints the start-up
summary on stdout. `names.go` is the zone, `dnsready.go` the first-run setup, `listen.go`
where the broker binds and what it advertises, `doctor.go`/`env.go` the two commands; on
Windows the zone builds but has no machine setup yet, so the default there is an address.

## Parsers of untrusted bytes

Anything read from a socket, a request body or a file somebody else wrote is
parsed by one of these, and each has a fuzz target: bytes in, and no panic, no
unbounded allocation, no hang. `task fuzz` runs them all (`testdata/fuzz-targets.txt`
is the list); the weekly workflow runs each for at least 60 seconds, and a test
(`docs/fuzz_test.go`) fails when a `Fuzz` function is missing from the list or from
this table.

The same test holds the code to the list. A function that reads bytes from outside
says which targets reach it in the last line of its doc comment
(`//fuzz:parser class=frame FuzzReadFrame`), and the test fails when a marker names
a target that does not exist, when a listed target is named by no marker, when one
of the classes of parser below has no marked function, and when a function in a
package that faces the network is named and shaped like a parser (`Parse…`,
`Decode…`, `Unmarshal…`, `Read…`, `Scan…` taking a `[]byte`, `string`, `io.Reader`
or `*http.Request`) and carries neither that marker nor
`//fuzz:not-a-parser <why>`. The net is by name: a parser written inline, or called
something else, is still its author's to mark.

| Parser | Where | Fuzz target |
|---|---|---|
| Frame (length prefix, size cap) | `internal/protocol` | `FuzzReadFrame` |
| Request header and body, including tagged fields | `internal/protocol` | `FuzzParseAndDecodeRequest`, `FuzzSkipTaggedFields` |
| What the recorder says about a decoded request | `internal/protocol` | `FuzzDescribe` |
| Record batch (shape, CRC, whole batches only) | `internal/storage/log` | `FuzzValidateBatches` |
| Record reader | `internal/storage/log` | `FuzzDecodeRecords` |
| Decompression (gzip, snappy block and xerial, lz4, zstd) | `internal/storage/log` | `FuzzDecompress` |
| The streamed look inside a produced batch (keys, inflated size) | `internal/storage/log` | `FuzzCheckRecords` |
| A Produce payload read whole before it is stored (shape, every record through its codec, timestamps) | `internal/storage/log` | `FuzzCheckProduce` |
| Segment scan that recovery truncates by | `internal/storage/log` | `FuzzScanLog` |
| OTLP metrics (client telemetry) | `internal/telemetry` | `FuzzDecode` |
| SASL/PLAIN and SCRAM messages | `internal/broker` | `FuzzSASLPlain`, `FuzzSCRAMClientFirst`, `FuzzSCRAMClientFinal`, `FuzzSCRAMUsernameEscaping` |
| ACL filters and the identity of a stored entry | `internal/broker`, `internal/storage/meta` | `FuzzACLFilterMatches`, `FuzzACLKey` |
| Topic config values | `internal/broker` | `FuzzTopicConfigValues` |
| The classic consumer protocol bytes (a subscription, an assignment) | `internal/broker` | `FuzzDecodeConsumerProtocol` |
| Consumer-group state machine (the KIP-848 coordinator parses nothing; the bytes are its schedule) | `internal/broker` | `FuzzGroupSequence` |
| Configuration file | `internal/config` | `FuzzConfigFile` |
| Schema texts (Avro, JSON Schema, Protobuf) and their compatibility | `schemaregistry` | `FuzzRegisterAndCompare` |
| Registry wire payloads | `schemaregistry` | `FuzzDecode` |
| Registry REST bodies, every route | `schemaregistry` | `FuzzAPIBody` |
| Protobuf descriptors | `schemaregistry` | `FuzzDescriptor` |
| Schema references (graph shapes) | `schemaregistry` | `FuzzRegisterReferences` |
| A JSON document to encode under a schema (the console's compose panel) | `schemaregistry` | `FuzzEncode` |
| The version in a registry path | `schemaregistry` | `FuzzVersionPath` |
| The console's form inputs and query strings | `console` (driven through its handler) | `FuzzConsoleForms` |
| Trace headers in a record (W3C `traceparent`, B3, the configured header) | `console` | `FuzzTraceHeaders` |
| The Host header (the DNS-rebinding guard of the console and of the schema registry) | `internal/hostguard` | `FuzzHostGuard` |

Not yet covered, with the reason: the console reads a classic group member's
assignment bytes inline in `describeGroupClassic` (`console/backend_groups.go`),
with the Kafka client library's decoder, behind a DescribeGroups call; a target
for it needs a group whose leader sends fuzzed bytes, and is owed to the Groups
stage. The console's schema forms (`/schemas/…`) need a registry behind the
console, which `FuzzConsoleForms` does not start.

## Why this stays small

Most of Kafka's size is distributed-systems machinery — replication, the controller
quorum, ISR, leader election. doze-kafka is a single node by design, so all of that
simply isn't there. What remains is the wire protocol, an append-only log, and a
couple of group state machines — and franz-go's `kmsg` carries the protocol's
encoding burden. The design goal throughout: **be indistinguishable from Kafka to a
client on the socket, and nothing more.**

For the blow-by-blow of how each piece was built and verified, see
[reports/history/](reports/history/).
