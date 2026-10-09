# Limitations

doze-kafka is a development tool. It is deliberately a **single node with no
clustering**, and a fair amount of Kafka is intentionally absent. This page is the
honest boundary — worth reading so nothing surprises you.

The guiding rule: if a client on the socket can't observe it, doze-kafka doesn't
implement it.

## Not a cluster, not production

- **Single node.** No replication, no in-sync replicas, no `min.insync.replicas`,
  no leader election, no controller quorum, no KRaft or ZooKeeper. Replication factor
  is effectively always 1. There is no high availability and no horizontal scale —
  that's not what it's for.
- **Not durable against disk loss.** Your data is on your local disk with no copies.
  With `acks=all` an acknowledged write is fsynced before the ack, so it survives a
  process crash — and a power loss too, as far as your disk honors the fsync (a real
  caveat on volatile-cache hardware); with `acks=1` a power loss can lose the last
  sub-second (see [storage & durability](storage-and-durability.md)). But a lost
  *disk* is a lost broker either way — there is no replica.

## Deliberately out of scope

- **Transactions: the classic protocol only.** Transactions work ([Transactions](transactions.md)),
  but the newer "transaction v2" of KIP-890 (Produce v12+) is not offered, so clients use
  the classic one. Idempotence survives a restart: each producer's recent sequence numbers
  are read back from the last 16 MiB of the log, so a batch resent after a crash is
  recognised. (A producer idle for longer than that window loses the protection, as one idle
  past 15 minutes does in memory.)
- **Log compaction runs in the background, on open partitions.** See
  [Storage & durability](storage-and-durability.md#compaction). A partition nothing has
  opened since the broker started is cleaned when it is first used. Offsets of removed
  records stay as gaps, so a count of offsets can exceed the number of records. A compacted
  topic refuses a record with no key. (Committed offsets are stored in the metadata
  database, not in a compacted `__consumer_offsets` topic.)
- **Topic configuration: five settings act, the rest are kept.** A topic's own
  `retention.ms`, `retention.bytes` and `segment.bytes`, given at `CreateTopics` or
  changed later with `AlterConfigs` / `IncrementalAlterConfigs`, are in force for that
  topic and override the broker's. `max.message.bytes` refuses a larger record batch
  (`MESSAGE_TOO_LARGE`), and `message.timestamp.type=LogAppendTime` stamps records
  with the broker's clock; these two apply only where a topic sets them, and there is
  no broker-wide batch limit below the 100 MiB request cap. Every other Kafka topic
  config is validated, stored and reported by `DescribeConfigs`, and changes nothing.
  `segment.bytes` must be at least 1024. The broker's own configuration cannot be altered over the wire: it comes
  from flags and the config file, and a request to change it is refused.
- **ACLs: users only; no quotas.** ACLs are enforced, with Kafka's rules (see
  [Authorization](authorization.md)), but principals are `User:` names only and hosts
  are matched as written. No throttling or quotas.
- **Schema registry: the core API only.** Subjects, versions, ids, references, modes
  `READWRITE` and `READONLY`, and compatibility for Avro, JSON Schema and Protobuf, all
  checked against a real Confluent registry. Not offered: `IMPORT` mode, contexts, data
  contracts (rules, metadata, tags), schema linking and exporters. The few places it
  answers differently on purpose are listed in [Schema Registry](schema-registry.md).
- **Some admin APIs.** `OffsetForLeaderEpoch`, `DescribeLogDirs`,
  and the client-metrics/telemetry APIs are not implemented. A Kafka UI that probes
  these will get a clean connection close on that call, not a hang — and generally
  keeps working for everything else.

## Behavioral simplifications

- **Timestamp lookups read batch headers.** `ListOffsets` by timestamp (`offsetsForTimes`,
  start-from-timestamp, "replay from ten minutes ago") and the max-timestamp query are
  exact, but there is no time index on disk: the first lookup on a partition reads its
  batch headers and keeps a summary in memory, and the active segment's is extended as
  it grows. That is milliseconds for a development-sized log and proportional to its
  size for a very large one.
- **On-disk format is doze-kafka's own.** Record batches are stored verbatim (so
  compression round-trips), but the segment/index file layout is not byte-compatible
  with Apache Kafka's. Only doze-kafka reads its own files.
- **Consumer-group assignor** is a plain, even round-robin (`uniform`) or `range`; it
  isn't the sticky, cross-topic-balanced reference assignor. Static membership
  (`group.instance.id`) is accepted on the wire but not acted on.
- **SASL details.** No Kerberos/GSSAPI, no OAUTHBEARER, no delegation tokens, and no
  `SASLprep` normalization (use ASCII credentials).

## If you hit a wall

Most of the above is intentional. But if a mainstream client can't do a normal
produce/consume/group/admin operation against doze-kafka, that's a bug, not a
limitation — it's meant to be indistinguishable from Kafka for exactly those things.
See [Troubleshooting](troubleshooting.md) first, then file it.
