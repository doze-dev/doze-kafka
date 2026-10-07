# API support

doze-kafka implements the Kafka APIs a client needs to discover the cluster, produce,
consume, use consumer groups, and do basic administration. It advertises them through
`ApiVersions`, so clients negotiate versions automatically and only ever call what's
supported.

The `--kafka-version` flag (1–4, default 4) selects which Kafka major to emulate, which
controls the version ranges advertised for each API and whether the KIP-848
consumer-group protocol is offered. Higher versions advertise wider ranges; **version
4 is the default and the most capable**, and is what you want unless you're
specifically testing an older client's paths.

## Implemented APIs

| API | Key | Purpose |
|---|---|---|
| ApiVersions | 18 | Version negotiation (the first thing every client does). |
| Metadata | 3 | Discover brokers, topics, and partitions. |
| CreateTopics | 19 | Create topics, with their own retention and segment size if given. |
| DeleteTopics | 20 | Delete topics and their data. |
| Produce | 0 | Append records to partitions. |
| Fetch | 1 | Read records (topic-ID aware; supports long-poll). |
| ListOffsets | 2 | Resolve earliest/latest offsets. |
| InitProducerId | 22 | Producer id for idempotent producers. |
| FindCoordinator | 10 | Locate the group coordinator (always this node). |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | 11 / 14 / 12 / 13 | Classic consumer-group protocol. |
| OffsetCommit / OffsetFetch | 8 / 9 | Commit and read consumer-group offsets. |
| ConsumerGroupHeartbeat | 68 | KIP-848 broker-driven group protocol (version 4 only). |
| ConsumerGroupDescribe | 69 | Describe KIP-848 groups (version 4 only). |
| ListGroups | 16 | List consumer groups. |
| DescribeGroups | 15 | Describe classic consumer groups. |
| DescribeConfigs | 32 | Read topic and broker configuration. |
| AlterConfigs | 33 | Replace a topic's own configuration. |
| IncrementalAlterConfigs | 44 | Set or remove individual topic configs (Kafka 2 and later). |
| CreatePartitions | 37 | Raise a topic's partition count (never lower it). |
| DeleteRecords | 21 | Move a partition's log-start offset up (-1 empties it), deleting what is below. |
| DeleteGroups | 42 | Delete empty consumer groups and their committed offsets. |
| OffsetDelete | 47 | Delete a group's committed offsets for a topic, including an offset left over from a topic that no longer exists. |
| DescribeCluster | 60 | Describe the cluster (this single node). |
| SaslHandshake / SaslAuthenticate | 17 / 36 | SASL, advertised only when auth is enabled. |

## Advertised version ranges

Each emulated major advertises the ranges below (min–max); a client uses the highest
version you both support. This table is the broker's own, generated from the code (a
test fails if the two differ).

The ranges are **ours**, not a copy of Apache Kafka's. Each profile is close to what a
real broker of that major offers, but not the same: several maxima are lower, because
the newer versions carry features doze-kafka does not have (Kafka 4.x brokers go well
past Produce v9 and Fetch v13, for example), and a few are a version higher than that
release shipped. The minima are raised: Produce starts at v3 and Fetch at v4, the
first versions that carry record batch format v2, the only format the log stores.
None of this needs configuring. A client reads the ranges from `ApiVersions` and uses
the highest version both sides support; a request outside them is not served.

<!-- api-table:begin (generated from internal/protocol/versions.go; do not edit by hand) -->
| API | Key | Kafka 1 | Kafka 2 | Kafka 3 | Kafka 4 |
|---|---|---|---|---|---|
| Produce | 0 | 3–5 | 3–7 | 3–9 | 3–9 |
| Fetch | 1 | 4–6 | 4–10 | 4–13 | 4–13 |
| ListOffsets | 2 | 0–2 | 0–5 | 0–7 | 0–7 |
| Metadata | 3 | 0–6 | 0–8 | 0–12 | 0–12 |
| OffsetCommit | 8 | 0–3 | 0–6 | 0–8 | 0–9 |
| OffsetFetch | 9 | 0–3 | 0–5 | 0–8 | 0–9 |
| FindCoordinator | 10 | 0–2 | 0–2 | 0–4 | 0–4 |
| JoinGroup | 11 | 0–2 | 0–4 | 0–7 | 0–9 |
| Heartbeat | 12 | 0–1 | 0–2 | 0–4 | 0–4 |
| LeaveGroup | 13 | 0–1 | 0–2 | 0–4 | 0–5 |
| SyncGroup | 14 | 0–1 | 0–3 | 0–4 | 0–5 |
| DescribeGroups | 15 | 0–1 | 0–3 | 0–5 | 0–5 |
| ListGroups | 16 | 0–2 | 0–2 | 0–4 | 0–5 |
| SASLHandshake | 17 | 1–1 | 1–1 | 1–1 | 1–1 | only when SASL is configured
| ApiVersions | 18 | 0–1 | 0–2 | 0–3 | 0–3 |
| CreateTopics | 19 | 0–2 | 0–3 | 0–7 | 0–7 |
| DeleteTopics | 20 | 0–1 | 0–3 | 0–5 | 0–6 |
| DeleteRecords | 21 | 0–0 | 0–1 | 0–2 | 0–2 |
| InitProducerID | 22 | 0–1 | 0–1 | 0–4 | 0–4 |
| DescribeConfigs | 32 | 0–1 | 0–2 | 0–4 | 0–4 |
| AlterConfigs | 33 | 0–0 | 0–2 | 0–2 | 0–2 |
| SASLAuthenticate | 36 | 0–2 | 0–2 | 0–2 | 0–2 | only when SASL is configured
| CreatePartitions | 37 | 0–0 | 0–1 | 0–3 | 0–3 |
| DeleteGroups | 42 | 0–1 | 0–2 | 0–2 | 0–2 |
| IncrementalAlterConfigs | 44 | — | 0–1 | 0–1 | 0–1 |
| OffsetDelete | 47 | — | 0–0 | 0–0 | 0–0 |
| DescribeCluster | 60 | — | — | 0–1 | 0–2 |
| ConsumerGroupHeartbeat | 68 | — | — | — | 0–1 |
| ConsumerGroupDescribe | 69 | — | — | — | 0–1 |
<!-- api-table:end -->

The hard floor is Kafka's record-batch format v2 (Kafka 0.11), so the oldest
emulated major is 1; the pre-0.11 message-set format is not supported.

## Not implemented (by design)

Transactions — idempotent producers are fully supported (`InitProducerId` plus
sequence-number deduplication), but there's no transactional produce/commit
(`AddPartitionsToTxn`/`EndTxn`). `OffsetForLeaderEpoch`, `DescribeLogDirs`, ACL and quota APIs, and
the client-metrics/telemetry APIs. The broker serves exactly what it advertises: a
request for an API it does not have, or at a version outside the advertised range,
gets no response and its connection is closed, rather than being left to hang (the one
exception is `ApiVersions` itself, where an unknown version is answered with the
supported range, as Kafka does). See [Limitations](limitations.md).
