# Observability

What the console can tell you about your app's use of the broker, and how much of it
to trust. Everything here is **in memory, for the life of the broker**: nothing is
written to disk, the oldest rows are dropped when the record is full, and a restart
starts it empty. A broker run without the console (`--console=false`, or embedded in
a Go test) keeps none of it, advertises no client telemetry and pays nothing for it.

## Traffic: what did my app just do?

**Traffic** in the console's rail is a live tail of the requests clients have made,
newest first. The connected clients are on the right until you select something.

Each row is one request: when it began, who sent it (the client id, else the address),
which API and version, the topics, group or transactional id it was about, a short
detail ("p1 · 1 batch · 412 B", "p0 from 4,812 · 3 records", "generation 7 · 3
members"), how it went, and how long it took. A request answered with an error shows
the error's Kafka name. Click a row for the request in full: who sent it (software and
version from `ApiVersions`, address, principal), its sizes, its correlation id, how
each partition of it went, and its decoded fields. A request that ended its connection
(an API the broker does not serve, a request before authentication, a handler that
panicked) is a row too, and says why.

### What a refusal means

The four words of the console are the classes of an answer, and every error code the
broker returns has a row in one table (`internal/protocol/errors.go`) with its class, its
usual cause and what to do about it:

| Class | Colour | Means | Counted as an error |
|---|---|---|---|
| expected | purple, no tint | part of the protocol's normal flow: a classic consumer's first `JoinGroup` is answered `MEMBER_ID_REQUIRED` on purpose (KIP-394), a heartbeat during a rebalance `REBALANCE_IN_PROGRESS` | no |
| refused | purple | a no that real Kafka would also give: a topic that does not exist, a fenced producer, an offset out of range | yes |
| denied | purple | an ACL or a login decided | yes |
| not implemented | amber | a feature or version this broker does not do | yes |
| failed | red | the broker could not do it: storage, a bug | yes |

Red is only ever the broker unhappy. *Errors only* and the error count leave expected
flow out. A code the table has no row for is still named, and is classed by Kafka's own
retriable flag.

### Folding

A consumer sends a heartbeat every few seconds and a long-poll fetch every half
second; shown one per row they bury everything else. A request **folds** into the row
before it when all of these hold, and into nothing else:

- it moved nothing and went well (a heartbeat that was accepted, a fetch that returned no
  records, a telemetry push; a fetch with a partition error is its own row);
- it is the same connection, the same API, the same topics and the same group or
  transactional id (the version, the partition and the client id are not part of this:
  a client id is per connection);
- the row's last request was less than ten minutes ago.

A folded row keeps its count (`×40`) and moves to the front as it repeats, keeps the
start of its first and of its latest request, and keeps its last 64 repeats as ticks (when
each began, how long it took, how it went), which the inspector draws and *Unfold*
lists. A row stays while it is active: when the ring is full the row that was
active least recently is the one dropped, not the one made first, so a long-lived
heartbeat keeps its count however busy the broker is.

### Counts and windows

Every number on the page says what it is counted over. The **summary** (clients
connected, requests and errors in the last minute) and each client's **Req/min** and
**Error rate** (the last five minutes) come from counters kept in five-second buckets:
they count a folded row's every repeat, not its rows, and are exact to the nearest five
seconds. *N of M requests match* counts requests among those the recorder keeps.

### Filters, the clients view, pausing

- **Filters.** One field matches the client, its software or address, a topic, a group,
  the request name or the result name; the kind chips narrow to produce, fetch, groups
  or admin, or to errors. A topic or group filter is a pill with a way to remove it.
  A group's fetches are included in its rows (a fetch names no group on the wire; the
  connection that joined one is known). Filters are in the address, so a link shares
  them and the live refresh keeps them.
- **Clients** (`?view=clients`): who is connected, their software, principal and address,
  requests a minute, groups and error rate. A client is flagged when it was refused three
  times in a row, or when three or more of its requests in five minutes make up at least
  half of what it asked.
- **Pause** stops the table (`p`). The broker is not paused: the page counts the requests
  it has missed since the newest row it shows, and says so.
- **A group filter** shows the group's rebalances in order, with the requests that
  belong to each nested under it, and the generations table.
- **When the broker is lost** the rows stay, dimmed, under a line that says when they
  were last known; the console asks again every five seconds.
- **The console is left out.** Its own requests would otherwise fill the page that
  shows traffic. A client whose id is exactly `doze-kafka-console`,
  `doze-kafka-console-fetch` or `doze-kafka-console-trace` is not listed; an app that
  merely begins that way is.
- **Version negotiation is not an error.** A client newer than the broker is told
  `UNSUPPORTED_VERSION` by `ApiVersions` and asks again at a version it can use.

### What is kept, and what it holds about people

Everything is in memory and starts empty at a restart (the page says since when). The
bounds are constants, and the strings in a row are clipped on the way in because a client
chose them:

| What | Bound |
|---|---|
| request rows | 2,000 (the least recently active is dropped) |
| group events | 200 per group, 100 groups (the group quiet longest is dropped) |
| ticks on a folded row | 64 |
| a detail, or the reason a connection was closed | 256 bytes |
| a client id, software, principal, group, transactional id or member id | 128 bytes |
| a peer address | 64 bytes |
| topics named by one request | 20 names and 1,024 bytes; the rest are counted |
| decoded fields of a request | about 1 KiB, 16 partitions |

A row holds the client id, software name, peer address and principal (shown to anyone
who can open the console), the topics, group and transactional id, a join reason, sizes,
timings and error names. It holds **no record bytes, no header value and no secret**:
the decoded fields give byte strings as their length, a batch as "1 batch, 2 records,
412 B", and hide the value of a configuration whose name says it is a secret.

## Group timeline: why does my consumer keep rebalancing?

A **consumer group's page** has a **Timeline**: what the group's coordinator did, in
order, newest first.

| Event | Means |
|---|---|
| joined | a member arrived (with its client id) |
| rebalance | the group started to re-divide its partitions, and why |
| new generation | a rebalance finished: how many members, which is the leader |
| assigned | the leader handed out the assignments |
| left | a member said goodbye |
| session expired | a member sent no heartbeat within its session timeout |
| dropped | a member did not rejoin before the rebalance timeout |
| new epoch | the KIP-848 equivalent of a generation |

A consumer that rebalances every few seconds shows the pattern here: a member whose
session keeps expiring (a handler slower than `max.poll.interval.ms`, a paused JVM),
or one that joins and leaves in a loop (a crash-looping app). The same events apply to
both group protocols.

## Client metrics: how is each client doing?

Kafka clients can report on themselves ([KIP-714](https://cwiki.apache.org/confluence/display/KAFKA/KIP-714%3A+Client+metrics+and+observability)):
the Java client from 3.7 and librdkafka from 2.5 send their own metrics to the broker
they are connected to, every few seconds, **by default**, as soon as the broker says it
accepts them. doze-kafka does, and **a client's page in Traffic** shows them: select a
client (in the connected clients, or in the Clients view) and its inspector lists what
it reported, with the latest value and a small chart, and says when it last did.

These are **metrics, not traces**: numbers a client keeps about itself (a producer's
`request.latency.avg` and `record.error.rate`, a consumer's `poll.idle.ratio.avg`,
connection counts), not a record of what it did. The error and retry rates come first
under "Worth a look first", the rest follow, and the chart covers what the samples cover
(the page says how long), one point per report. A client that reports nothing says why:
too old, or `enable.metrics.push=false`.

- **Only with the console.** A broker run without the console (`--console=false`, or
  embedded in a Go test) does not advertise the two APIs and keeps nothing.
  `--client-telemetry=false` turns them off with the console on. A client can also
  refuse by itself: `enable.metrics.push=false`.
- **Always subscribed.** Real Kafka sends a client nothing until an operator defines a
  subscription. Here every client is asked for the standard set every five seconds,
  because the only reader is the console on your machine. The setting that would
  narrow it (a `CLIENT_METRICS` resource) does not exist here.
- **Memory only, and bounded.** At most 100 clients, 150 series each, 60 points each
  (five minutes at the five-second interval): under 32 MB at the worst. A client that
  has not reported for ten minutes (one that said it was closing, for one) is forgotten.
  Series past the cap are counted (`N not kept`). A resource has at most 32 attributes
  of 128 bytes and 1 KiB; a metric name and its attributes are cut at 256 bytes.
- **Rate.** A push before the interval (five seconds) has passed since the last one taken
  is answered `THROTTLING_QUOTA_EXCEEDED`, is not read and is counted, as Apache Kafka
  does (recorded: `conformance/recordings/telemetry-throttle.json`). The first push after
  a subscription and a client's last (terminating) push are never early.
- **An instance follows its connection.** A Java client subscribes on one connection and
  pushes on another; Kafka ties an instance to neither a connection nor a principal, and
  here it follows the connection that last spoke for it. At most 8 instances per
  connection are kept: one that makes more loses its own oldest.
- Payloads are OpenTelemetry metrics, in any of the four compressions (gzip, snappy,
  lz4, zstd), up to 1 MiB compressed and inflated (Kafka's `telemetry.max.bytes`); a zstd frame may declare a window of
  at most four times that (2 MiB, which is what a default-level compressor asks for), and
  the broker reads it through the limit, never inflating more than it. A cut-off payload
  is refused. Kafka 4 profile only; older profiles do not advertise the APIs, as the
  clients that use them are Kafka 3.7 and later.

## Message lineage: where did this message go?

Open a record in the topic page's drawer. If it carries a trace id in its headers it
offers **Follow this trace**, which lists every record, in every topic, that carries
the same id, in the order they were written: topic, partition and offset (linked to
the record), the key, the span id (and the parent span id where the convention has
one), and which consumer groups have **read** past it or **not yet**. A small time axis
shows the gaps: a request that took 400 ms to turn into an event, an event that sat
for a minute before its consumer wrote the next.

The trace id is read from, in this order:

- W3C **`traceparent`** (`00-<trace>-<span>-<flags>`),
- B3, either the single **`b3`** header or **`X-B3-TraceId`** with `X-B3-SpanId` and
  `X-B3-ParentSpanId` (a 64-bit B3 id matches the same trace's 128-bit form),
- and the header named by **`--trace-header`** (`trace-header` in the file), whose whole
  value is the id: a request id, a correlation id.

How it is found, and its limits, which the page states too:

- **By reading.** The broker keeps no index of headers. The console asks the broker,
  by timestamp, which records were written within a span either side of the message
  (1 minute, 5 minutes, 30 minutes or 3 hours; 5 by default), then reads those
  records with an ordinary consumer and keeps the ones that match. The page shows
  progress while it runs.
- **It has a budget**: 50,000 records or 8 seconds. When it stops at either, the page
  says *This is not everything*, says which budget, and lists the partitions it left
  unread. Choose a narrower span to read less.
- **It only sees what apps put in headers.** A message with no trace header is invisible
  to it, and so is a service that receives a trace and does not pass it on.
- It scans from the broker's current contents: records already deleted by retention or
  compaction are not there to be found.

## Not available

The console reaches the broker's traffic through the process it runs in. A console
running apart from its broker has nothing to show, and its rail has no Traffic entry
rather than an empty page.
