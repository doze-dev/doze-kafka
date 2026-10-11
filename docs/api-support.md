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
| ListOffsets | 2 | Resolve earliest, latest and max-timestamp offsets, and the first offset at or after a timestamp. |
| InitProducerId | 22 | Producer id for idempotent producers, and for a `transactional.id` (see [Transactions](transactions.md)). |
| AddPartitionsToTxn / AddOffsetsToTxn / EndTxn / TxnOffsetCommit | 24 / 25 / 26 / 28 | Transactional producers: what a transaction touches, and its commit or abort. |
| DescribeTransactions / ListTransactions / DescribeProducers | 65 / 66 / 61 | Inspect open transactions and a partition's producers. |
| FindCoordinator | 10 | Locate the group coordinator (always this node). |
| JoinGroup / SyncGroup / Heartbeat / LeaveGroup | 11 / 14 / 12 / 13 | Classic consumer-group protocol. |
| OffsetCommit / OffsetFetch | 8 / 9 | Commit and read consumer-group offsets. |
| ConsumerGroupHeartbeat | 68 | KIP-848 broker-driven group protocol (version 4 only). |
| ConsumerGroupDescribe | 69 | Describe KIP-848 groups (version 4 only). |
| GetTelemetrySubscriptions / PushTelemetry | 71 / 72 | Client telemetry (KIP-714, version 4 only): a client asks what to report and pushes its own metrics, which the console shows ([Observability](observability.md)). Offered only with the console on and not `--client-telemetry=false`. |
| ListGroups | 16 | List consumer groups. |
| DescribeGroups | 15 | Describe classic consumer groups. |
| DescribeConfigs | 32 | Read topic and broker configuration. |
| AlterConfigs | 33 | Replace a topic's own configuration. |
| IncrementalAlterConfigs | 44 | Set or remove individual topic configs (Kafka 2 and later). |
| CreatePartitions | 37 | Raise a topic's partition count (never lower it). |
| DescribeACLs / CreateACLs / DeleteACLs | 29 / 30 / 31 | Create, read and delete ACLs. They are enforced ([Authorization](authorization.md)). |
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
| AddPartitionsToTxn | 24 | 0–0 | 0–1 | 0–3 | 0–3 |
| AddOffsetsToTxn | 25 | 0–0 | 0–1 | 0–3 | 0–3 |
| EndTxn | 26 | 0–0 | 0–1 | 0–3 | 0–3 |
| TxnOffsetCommit | 28 | 0–0 | 0–2 | 0–3 | 0–3 |
| DescribeACLs | 29 | 0–0 | 0–1 | 0–3 | 0–3 |
| CreateACLs | 30 | 0–0 | 0–1 | 0–3 | 0–3 |
| DeleteACLs | 31 | 0–0 | 0–1 | 0–3 | 0–3 |
| DescribeConfigs | 32 | 0–1 | 0–2 | 0–4 | 0–4 |
| AlterConfigs | 33 | 0–0 | 0–2 | 0–2 | 0–2 |
| SASLAuthenticate | 36 | 0–2 | 0–2 | 0–2 | 0–2 | only when SASL is configured
| CreatePartitions | 37 | 0–0 | 0–1 | 0–3 | 0–3 |
| DeleteGroups | 42 | 0–1 | 0–2 | 0–2 | 0–2 |
| IncrementalAlterConfigs | 44 | — | 0–1 | 0–1 | 0–1 |
| OffsetDelete | 47 | — | 0–0 | 0–0 | 0–0 |
| DescribeCluster | 60 | — | — | 0–1 | 0–2 |
| DescribeProducers | 61 | — | — | 0–0 | 0–0 |
| DescribeTransactions | 65 | — | — | 0–0 | 0–0 |
| ListTransactions | 66 | — | — | 0–0 | 0–0 |
| ConsumerGroupHeartbeat | 68 | — | — | — | 0–1 |
| ConsumerGroupDescribe | 69 | — | — | — | 0–1 |
| GetTelemetrySubscriptions | 71 | — | — | — | 0–0 |
| PushTelemetry | 72 | — | — | — | 0–0 |
<!-- api-table:end -->

The hard floor is Kafka's record-batch format v2 (Kafka 0.11), so the oldest
emulated major is 1; the pre-0.11 message-set format is not supported.

## Every Kafka API, by tier

One row for every API key in the Kafka 4.x protocol specification (the protocol
library's key table, which a test walks). Nothing is absent from this table.

- **F, functional.** It works locally and its effect is observable.
- **C, cosmetic.** Accepted and round-tripped with no local effect.
- **S, stub.** Not served: it is not advertised, and a request for it closes the
  connection. Every S row says why.

An API row is F when the broker advertises it (see the version table above) and S
when it does not; the notes are written by hand and a test fails for an S row
without a reason. Settings that are accepted and stored without acting (most topic
configs) are listed in [Limitations](limitations.md), not here.

<!-- api-tiers:begin (generated from the protocol library's key table and internal/protocol/versions.go; do not edit by hand) -->
| API | Key | Tier | Note |
|---|---|---|---|
| Produce | 0 | F | `acks=all` fsyncs before the reply. |
| Fetch | 1 | F | Long-poll capped at 30 s; no fetch sessions. |
| ListOffsets | 2 | F |  |
| Metadata | 3 | F |  |
| LeaderAndISR | 4 | S | Broker-to-broker traffic; there is one node and no peer sends it or waits for it. |
| StopReplica | 5 | S | Broker-to-broker traffic; there is one node and no peer sends it or waits for it. |
| UpdateMetadata | 6 | S | Broker-to-broker traffic; there is one node and no peer sends it or waits for it. |
| ControlledShutdown | 7 | S | The controller's shutdown handshake; there is no controller. |
| OffsetCommit | 8 | F |  |
| OffsetFetch | 9 | F |  |
| FindCoordinator | 10 | F | Always this node. |
| JoinGroup | 11 | F |  |
| Heartbeat | 12 | F |  |
| LeaveGroup | 13 | F |  |
| SyncGroup | 14 | F |  |
| DescribeGroups | 15 | F |  |
| ListGroups | 16 | F |  |
| SASLHandshake | 17 | F | Only when SASL is configured. |
| ApiVersions | 18 | F |  |
| CreateTopics | 19 | F | A replication factor above 1 is not honoured (one node). |
| DeleteTopics | 20 | F |  |
| DeleteRecords | 21 | F |  |
| InitProducerID | 22 | F |  |
| OffsetForLeaderEpoch | 23 | S | Replica truncation after a leader change; there are no leader changes. A client falls back to ListOffsets. |
| AddPartitionsToTxn | 24 | F |  |
| AddOffsetsToTxn | 25 | F |  |
| EndTxn | 26 | F |  |
| WriteTxnMarkers | 27 | S | A transaction coordinator writing markers to other brokers' partitions; the coordinator here writes its own log directly. |
| TxnOffsetCommit | 28 | F |  |
| DescribeACLs | 29 | F |  |
| CreateACLs | 30 | F |  |
| DeleteACLs | 31 | F |  |
| DescribeConfigs | 32 | F | Topic configs; broker configs read-only. |
| AlterConfigs | 33 | F | Five topic settings act, the rest are stored (see [Limitations](limitations.md)). |
| AlterReplicaLogDirs | 34 | S | Moves a replica between log directories; there is one data directory. |
| DescribeLogDirs | 35 | S | Not implemented yet: per-partition sizes are planned (the topics stage). A UI that probes it gets the connection closed. |
| SASLAuthenticate | 36 | F | Only when SASL is configured. |
| CreatePartitions | 37 | F |  |
| CreateDelegationToken | 38 | S | Delegation tokens are not implemented; authentication is SASL/PLAIN and SASL/SCRAM. |
| RenewDelegationToken | 39 | S | Delegation tokens are not implemented; authentication is SASL/PLAIN and SASL/SCRAM. |
| ExpireDelegationToken | 40 | S | Delegation tokens are not implemented; authentication is SASL/PLAIN and SASL/SCRAM. |
| DescribeDelegationToken | 41 | S | Delegation tokens are not implemented; authentication is SASL/PLAIN and SASL/SCRAM. |
| DeleteGroups | 42 | F |  |
| ElectLeaders | 43 | S | There is one replica and it is always the leader. |
| IncrementalAlterConfigs | 44 | F | As AlterConfigs. |
| AlterPartitionAssignments | 45 | S | Replica placement and reassignment; every partition has one replica on this node. |
| ListPartitionReassignments | 46 | S | Replica placement and reassignment; every partition has one replica on this node. |
| OffsetDelete | 47 | F |  |
| DescribeClientQuotas | 48 | S | Quotas are not implemented: nothing is throttled, so there is nothing to describe. |
| AlterClientQuotas | 49 | S | Quotas are not implemented: nothing is throttled. |
| DescribeUserSCRAMCredentials | 50 | S | SCRAM users come from the configuration file and flags; they cannot be managed over the wire. |
| AlterUserSCRAMCredentials | 51 | S | SCRAM users come from the configuration file and flags; they cannot be managed over the wire. |
| Vote | 52 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| BeginQuorumEpoch | 53 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| EndQuorumEpoch | 54 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| DescribeQuorum | 55 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| AlterPartition | 56 | S | ISR changes sent to the controller; there is no ISR to change. |
| UpdateFeatures | 57 | S | Cluster feature levels are not tracked; the broker emulates one Kafka major (`--kafka-version`). |
| Envelope | 58 | S | Forwarding a request from a broker to the controller; there is no controller to forward to. |
| FetchSnapshot | 59 | S | KRaft metadata snapshots; there is no metadata log to snapshot. |
| DescribeCluster | 60 | F | This single node. |
| DescribeProducers | 61 | F |  |
| BrokerRegistration | 62 | S | Broker registration with a controller; there is no controller to register with. |
| BrokerHeartbeat | 63 | S | Broker registration with a controller; there is no controller to register with. |
| UnregisterBroker | 64 | S | Broker registration with a controller; there is no controller to register with. |
| DescribeTransactions | 65 | F |  |
| ListTransactions | 66 | F |  |
| AllocateProducerIDs | 67 | S | A broker asking the controller for a block of producer ids; this node allocates its own. |
| ConsumerGroupHeartbeat | 68 | F | Kafka 4 profile only. |
| ConsumerGroupDescribe | 69 | F | Kafka 4 profile only. |
| ControllerRegistration | 70 | S | Broker registration with a controller; there is no controller to register with. |
| GetTelemetrySubscriptions | 71 | F | Kafka 4 profile only; offered with the console on and not `--client-telemetry=false`. Answers a fixed subscription (every `org.apache.kafka.` metric, every 5 s, cumulative: C, there is no `CLIENT_METRICS` resource); `INVALID_REQUEST` when one connection already holds 8 instances. |
| PushTelemetry | 72 | F | Kafka 4 profile only; same condition. Codes: `INVALID_REQUEST` (no instance id, an undecodable or cut-off payload, another principal's instance), `UNKNOWN_SUBSCRIPTION_ID`, `UNSUPPORTED_COMPRESSION_TYPE`, `TELEMETRY_TOO_LARGE`. A push faster than the interval allows is answered 0 and counted, not read. |
| AssignReplicasToDirs | 73 | S | Replica-to-directory assignment between a broker and the controller; one directory. |
| ListConfigResources | 74 | S | Not implemented; Metadata and DescribeConfigs cover what a client needs. |
| DescribeTopicPartitions | 75 | S | Not implemented; Metadata serves the same answer, and clients fall back to it. |
| ShareGroupHeartbeat | 76 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| ShareGroupDescribe | 77 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| ShareFetch | 78 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| ShareAcknowledge | 79 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| AddRaftVoter | 80 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| RemoveRaftVoter | 81 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| UpdateRaftVoter | 82 | S | KRaft controller-quorum management; there is no controller quorum, the one node is its own metadata store. |
| InitializeShareGroupState | 83 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| ReadShareGroupState | 84 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| WriteShareGroupState | 85 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| DeleteShareGroupState | 86 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| ReadShareGroupStateSummary | 87 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| StreamsGroupHeartbeat | 88 | S | Streams groups (KIP-1071) are not implemented; Kafka Streams uses the classic group protocol. |
| StreamsGroupDescribe | 89 | S | Streams groups (KIP-1071) are not implemented; Kafka Streams uses the classic group protocol. |
| DescribeShareGroupOffsets | 90 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| AlterShareGroupOffsets | 91 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
| DeleteShareGroupOffsets | 92 | S | Share groups (KIP-932) are not implemented; the consumer group protocols are classic and KIP-848. |
<!-- api-tiers:end -->

## Not implemented (by design)

`OffsetForLeaderEpoch`, `DescribeLogDirs`, quota APIs, and
the client-metrics/telemetry APIs. The broker serves exactly what it advertises: a
request for an API it does not have, or at a version outside the advertised range,
gets no response and its connection is closed, rather than being left to hang (the one
exception is `ApiVersions` itself, where an unknown version is answered with the
supported range, as Kafka does). See [Limitations](limitations.md).
