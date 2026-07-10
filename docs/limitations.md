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
  is not honored; compacted topics like a real `__consumer_offsets` aren't emulated
  (offsets are stored in the metadata database instead).
- **Config writes.** `DescribeConfigs`/`DescribeCluster` report the effective
  configuration, but there's no `AlterConfigs`. Most knobs (retention, segment size)
  are broker-global here, so a per-topic config change would be stored without taking
  effect — better to be read-only and honest than to accept a write that lies.
- **ACLs and quotas.** Authentication exists (SASL/TLS), but there is no
  authorization: an authenticated client can do anything. No throttling either.
- **Some admin APIs.** `CreatePartitions`, `OffsetForLeaderEpoch`, `DescribeLogDirs`,
  and the client-metrics/telemetry APIs are not implemented. A Kafka UI that probes
  these will get a clean connection close on that call, not a hang — and generally
  keeps working for everything else.

## Behavioral simplifications

- **Timestamp lookups are coarse.** `ListOffsets` by timestamp returns the earliest
  offset; there's no per-record timestamp index. Earliest/latest are exact.
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
