# Observability

What the console can tell you about your app's use of the broker, and how much of it
to trust. Everything here is **in memory, for the life of the broker**: nothing is
written to disk, the oldest rows are dropped when the record is full, and a restart
starts it empty. A broker run without the console (`--console=false`, or embedded in
a Go test) keeps none of it and pays nothing for it.

## Traffic: what did my app just do?

**Traffic** in the console's rail is a live tail of the requests clients have made,
newest first, with the clients that are connected now above it.

Each row is one request: when it began, who sent it (the client id, else the address),
which API and version, the topics or group it was about, a short detail ("2
partitions, 6.0 KiB"), how it went, and how long it took. A request the broker
answered with an error shows the error's Kafka name (`UNKNOWN_TOPIC_OR_PARTITION`,
`OFFSET_OUT_OF_RANGE`…) in red, which is usually the whole answer to "why is my
producer failing". Click a row for the request in full: the client's software and
version (from `ApiVersions`), its address and principal, sizes in and out.

- **Repeats fold.** A consumer sends a heartbeat every few seconds and a long-poll
  fetch every half second; shown one per row they bury everything else. Requests that
  moved nothing (a heartbeat that went well, a fetch that returned no records) fold
  into one row with a count (`×40`) and keep moving to the front as they repeat.
  Anything that failed, or moved data, is always its own row.
- **Filters.** By client (id, software or address), request type, topic, group, and
  *errors only*. The filter is in the address, so a link shares it, and the live
  refresh keeps it.
- **The console is left out.** Its own requests would otherwise fill the page that
  shows traffic. A client whose id begins `doze-kafka-console` is not listed.
- **Version negotiation is not an error.** A client newer than the broker is told
  `UNSUPPORTED_VERSION` by `ApiVersions` and asks again at a version it can use; that
  is the protocol working, and it is shown as such.
- **How much is kept.** The last 2,000 requests (and 2,000 group events). A busy
  producer can push a slow one out in seconds.
- Pause the tail with the **Live** toggle to read it without it moving.

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
accepts them. doze-kafka does, and the **Traffic** page shows them under **Client
metrics**, one card per client: who it is (client id, software, the id the broker gave
it), when it last reported, and its metrics with the latest value and a small chart of
the last ten minutes.

These are **metrics, not traces**: numbers a client keeps about itself (a producer's
`request.latency.avg` and `record.error.rate`, a consumer's `poll.idle.ratio.avg`,
connection counts), not a record of what it did. The page lists the ones that say
whether a client is healthy first (latency, send rate, queue time, an error or retry
rate above zero), then the rest by name, folded away. A Java producer reports well over a
hundred.

- **Always subscribed.** Real Kafka sends a client nothing until an operator defines a
  subscription. Here every client is asked for the standard set every five seconds,
  because the only reader is the console on your machine.
- **Memory only.** The last ten minutes of each client's series, for up to 200 clients; a
  client that has not reported for ten minutes (one that said it was closing, for one)
  is forgotten. Nothing is written to disk or exported.
- **Off with `--client-telemetry=false`.** The broker then stops advertising the two APIs
  (`GetTelemetrySubscriptions`, `PushTelemetry`), so clients stop sending, and the section
  disappears. A client can also refuse by itself: `enable.metrics.push=false`.
- Payloads are OpenTelemetry metrics, in any of the four compressions (gzip, snappy,
  lz4, zstd), up to 512 KiB. Kafka 4 profile only; older profiles do not advertise the
  APIs, as the clients that use them are Kafka 3.7 and later.

## Message lineage: where did this message go?

Open a record in the topic page's drawer. If it carries a trace id in its headers it
offers **Follow this message**, which lists every record, in every topic, that carries
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
