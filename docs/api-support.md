# API support

doze-kafka implements the Kafka APIs a client needs to discover the cluster, produce,
consume, use consumer groups, and do basic administration. It advertises them through
`ApiVersions`, so clients negotiate versions automatically and only ever call what's
supported.

The `--version` flag (1–4, default 4) selects which Kafka major to emulate, which
controls the version ranges advertised for each API and whether the KIP-848
consumer-group protocol is offered. Higher versions advertise wider ranges; **version
4 is the default and the most capable**, and is what you want unless you're
specifically testing an older client's paths.

## Implemented APIs

| API | Key | Purpose |
|---|---|---|
| ApiVersions | 18 | Version negotiation (the first thing every client does). |
| Metadata | 3 | Discover brokers, topics, and partitions. |
| CreateTopics | 19 | Create topics (with per-topic config). |
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
| DescribeConfigs | 32 | Read topic and broker configuration (read-only). |
| DescribeCluster | 60 | Describe the cluster (this single node). |
| SaslHandshake / SaslAuthenticate | 17 / 36 | SASL, advertised only when auth is enabled. |

## Advertised version ranges

Each emulated major advertises the ranges below (min–max). A client uses the highest
version you both support. This mirrors what a real broker of that major offers, so
clients behave as they would against Apache Kafka.

| API | v1 | v2 | v3 | v4 |
|---|---|---|---|---|
| ApiVersions | 0–1 | 0–2 | 0–3 | 0–3 |
| Metadata | 0–6 | 0–8 | 0–12 | 0–12 |
| Produce | 0–5 | 0–7 | 0–9 | 0–9 |
| Fetch | 0–6 | 0–10 | 0–13 | 0–13 |
| ListOffsets | 0–2 | 0–5 | 0–7 | 0–7 |
| CreateTopics | 0–2 | 0–3 | 0–7 | 0–7 |
| DeleteTopics | 0–1 | 0–3 | 0–5 | 0–6 |
| InitProducerId | 0–1 | 0–1 | 0–4 | 0–4 |
| FindCoordinator | 0–2 | 0–2 | 0–4 | 0–4 |
| JoinGroup | 0–2 | 0–4 | 0–7 | 0–9 |
| SyncGroup | 0–1 | 0–3 | 0–4 | 0–5 |
| Heartbeat | 0–1 | 0–2 | 0–4 | 0–4 |
| LeaveGroup | 0–1 | 0–2 | 0–4 | 0–5 |
| OffsetCommit | 0–3 | 0–6 | 0–8 | 0–9 |
| OffsetFetch | 0–3 | 0–5 | 0–8 | 0–9 |
| ListGroups | 0–2 | 0–2 | 0–4 | 0–5 |
| DescribeGroups | 0–1 | 0–3 | 0–5 | 0–5 |
| DescribeConfigs | 0–1 | 0–2 | 0–4 | 0–4 |
| DescribeCluster | — | — | 0–1 | 0–2 |
| ConsumerGroupHeartbeat | — | — | — | 0–1 |
| ConsumerGroupDescribe | — | — | — | 0–1 |

The hard floor is Kafka's record-batch format v2 (Kafka 0.11), so the oldest
emulated major is 1; the pre-0.11 message-set format is not supported.

## Not implemented (by design)

Transactions — idempotent producers are fully supported (`InitProducerId` plus
sequence-number deduplication), but there's no transactional produce/commit
(`AddPartitionsToTxn`/`EndTxn`). `AlterConfigs` (config is read-only — the size/time
knobs are broker-global here, so a per-topic write wouldn't take effect),
`CreatePartitions`, `OffsetForLeaderEpoch`, `DescribeLogDirs`, ACL and quota APIs, and
the client-metrics/telemetry APIs. A client that calls an unadvertised API has its
connection closed rather than left hanging. See [Limitations](limitations.md).
