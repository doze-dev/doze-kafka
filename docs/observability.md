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

## Not available

The console reaches the broker's traffic through the process it runs in. A console
running apart from its broker has nothing to show, and its rail has no Traffic entry
rather than an empty page.
