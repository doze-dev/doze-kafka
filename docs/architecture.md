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

## Why this stays small

Most of Kafka's size is distributed-systems machinery — replication, the controller
quorum, ISR, leader election. doze-kafka is a single node by design, so all of that
simply isn't there. What remains is the wire protocol, an append-only log, and a
couple of group state machines — and franz-go's `kmsg` carries the protocol's
encoding burden. The design goal throughout: **be indistinguishable from Kafka to a
client on the socket, and nothing more.**

For the blow-by-blow of how each piece was built and verified, see
[reports/history/](reports/history/).
