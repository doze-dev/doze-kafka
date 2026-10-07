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

- **Transactions.** Idempotent producers are fully supported — sequence numbers are
  deduplicated, so a retried batch won't duplicate — but there's no *transactional*
  produce/commit (`AddPartitionsToTxn`, `EndTxn`, …), so there's no exactly-once
  *processing* across a read-process-write cycle. One caveat on the idempotence: the
  dedup state is in memory, so a batch that was in flight exactly across a broker
  restart could still duplicate (rare, and `acks=all` makes it rarer still, since an
  acked batch was fsynced and the producer won't resend it).
- **Log compaction.** Retention is delete-only (by size and age). `cleanup.policy=compact`
  is accepted and reported, because Kafka Connect and Schema Registry check for it on
  their own topics, but nothing is compacted: every record is kept until retention
  removes it. A consumer reading a "compacted" topic from the start sees every value a
  key ever had, not only the latest. (Committed offsets are stored in the metadata
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
- **ACLs and quotas.** Authentication exists (SASL/TLS), but there is no
  authorization: an authenticated client can do anything. No throttling either.
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
