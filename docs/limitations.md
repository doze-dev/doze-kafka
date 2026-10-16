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
- **A failed disk refuses writes; it does not take the broker down.** When the data
  directory's disk is full or failing, every request that writes answers
  `KAFKA_STORAGE_ERROR` (retriable), as Apache Kafka does (recorded against
  `apache/kafka:4.3.1`: `conformance/recordings/full-disk.json`), at `acks=1` and at
  `acks=all`; at `acks=0`, which has no response to carry it, the connection is closed.
  Reads, Metadata and group traffic keep working, and freeing space lets writes resume
  without a restart. Kafka goes further: with its only log directory failed the broker
  shuts down (the recording's last two steps are a closed connection); this one stays
  up, which is kinder to a development machine and not what a production broker does.
- **After a failed `fsync` the partition refuses writes until the broker restarts.**
  On Linux a failed `fsync` can drop the unflushed pages and clear the error, so a
  retry that "succeeds" proves nothing about the bytes that were lost. Kafka does not
  retry, it takes the log directory offline; doze-kafka does the same for the one
  partition, answering `KAFKA_STORAGE_ERROR` to every write (and every `acks=all`
  acknowledgement) until a restart, when recovery decides from what is really on disk.
- **Every produced batch is read whole before it is stored, as Kafka reads it, and
  three kinds of batch Kafka stores are refused here.** Each partition's records must
  be exactly one version 2 batch whose header, checksum, record count and every record
  (through whatever codec it is compressed with) check out; what Kafka refuses is
  refused with Kafka's code on every kind of topic and every Produce version, and
  nothing refused is stored (recorded against `apache/kafka:4.3.1`:
  `conformance/recordings/produce-validation.json`). A batch is held to
  `max.message.bytes`, or the broker's `message.max.bytes` (`--message-max-bytes`,
  Kafka's default of 1048588) when the topic sets none, as it arrived, and as the topic
  stores it when the topic's `compression.type` is not the batch's. The read is one
  streaming pass that holds a decompressor's window and a buffer, never the payload.
  Stricter than Kafka, because this broker's readers (and every consumer) could not
  read what Kafka keeps, or would have to allocate without bound to try:
  a compressed record whose length disagrees with its own fields is INVALID_RECORD
  (Kafka stores it on a topic that keeps the producer's codec, and the read-back in the
  recording shows it cannot be consumed); a batch that inflates past 64 MiB is
  MESSAGE_TOO_LARGE (Kafka accepted 65 MiB, 70 MiB and a single 70 MiB record on every
  topic whose limit the compressed size is under:
  `conformance/recordings/produce-inflated-size.json`); and a zstd frame declaring a
  window past 8 MiB is MESSAGE_TOO_LARGE, because a decoder allocates the window it is
  told before reading a byte (Kafka takes 16 MiB and 128 MiB, and refuses 256 MiB with
  UNKNOWN_SERVER_ERROR as this does). A batch stored by an earlier version that cannot
  be read is kept, and skipped by the readers that look inside (timestamp lookups and
  the cleaner use its header), so such a log still opens and serves.
- **A consumer-protocol member is asked to heartbeat every second**, where Kafka's
  `group.consumer.heartbeat.interval.ms` is 5 seconds: rebalances settle in a couple of
  seconds on a development machine instead of ten.
- **A consumer group's target assignment is computed before the request that causes it
  is answered.** Kafka answers a join first and runs the assignor after, so a describe
  taken in between shows the group `Assigning`, the old targets, and a classic member
  that has just joined is told the epoch before its own join as its generation. Here the
  new targets are there when the join is answered, the group is never `Assigning`, and
  that generation is one higher. Which request is answered with which code is the same
  (recorded: `mixed-protocol-groups.json`, `mixed-protocol-rolling.json`).
- **An empty group with no committed offsets is forgotten at once.** Kafka keeps it
  listed, `Empty`, until its offsets retention has passed; here a group that holds
  nothing is not worth listing, and a console that showed every consumer that ever
  came and went would be a long page.
- **A batch whose `acks=all` fsync failed stays in the log.** The producer is told
  `KAFKA_STORAGE_ERROR` with no offset, but the batch was already appended: a consumer
  can read it, and a non-idempotent retry stores it again (an idempotent retry is
  deduplicated). This is the same shape as Kafka's `NOT_ENOUGH_REPLICAS_AFTER_APPEND`,
  and it is pinned by `TestFsyncFailedBatchIsEitherRolledBackOrDocumented`.
- **Session timeouts are bounded as Kafka's defaults bound them.** A classic group
  member asking for a `session.timeout.ms` below 6000 or above 1800000 is refused with
  `INVALID_SESSION_TIMEOUT` on every JoinGroup version, as a stock broker's
  `group.min.session.timeout.ms` and `group.max.session.timeout.ms` do (recorded:
  `conformance/recordings/joingroup-session-timeout.json`). The bounds are not
  settable here; a broker run with different bounds would differ.

## Client telemetry and the Traffic page

- **No `CLIENT_METRICS` resource.** Real Kafka subscribes a client (KIP-714) to nothing
  until an operator defines one and runs a plugin to take the metrics; the resource can be
  set with `IncrementalAlterConfigs` and read back. Here the resource is not kept: the
  setting round-trips nowhere and nothing is exported.
- **A subscription is the same for every client: the standard metric set, every five seconds, cumulative.**
  Kafka with nothing configured answers a client's subscription request with no metrics, an
  interval of five minutes and delta temporality, so that a client pushes nothing (recorded:
  `conformance/recordings/telemetry-empty-subscription.json`). Here every client that speaks
  KIP-714 is subscribed to the `org.apache.kafka.` metrics every five seconds, the only reader
  being the console, and the interval it was told is the interval a push is throttled by.
- **Telemetry is on with the console.** A broker without a console (`--console=false`, or
  embedded) does not advertise `GetTelemetrySubscriptions` and `PushTelemetry`. With one,
  they are advertised by default, which Apache Kafka 4.3.1 does not do: it lists them in
  `ApiVersions` only when a broker plugin that implements `ClientTelemetry` is configured,
  and until then a client never subscribes (recorded:
  `conformance/recordings/client-telemetry.json`). The point of the page is that clients do.
- **Answered as Kafka does, recorded.** A push before its interval has passed, a second
  subscription request within it, the push after a terminating one, a push for an
  instance the broker has not seen (it is made, and a push that holds the subscription id
  it would have given is taken, which is what a client finds after a restart), a push from
  another connection or principal, an unsupported compression type, a payload over
  1 MiB, a body that does not inflate, and a body that is not OpenTelemetry metrics: each is
  answered as `conformance/recordings/telemetry-*.json` show Apache Kafka 4.3.1 doing.
  Two bounds are this broker's own: at most 8 instances per connection (a connection that
  makes more loses its own oldest, never another client's) and 100 instances in all.
- **Memory only.** The Traffic page keeps the last 2,000 requests and 200 events per
  group, and a restart empties it: it is the last few minutes of a laptop's traffic, not
  an audit log.

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
- **Some admin APIs.** `OffsetForLeaderEpoch` and `DescribeLogDirs` are not implemented. A Kafka UI that probes
  these will get a clean connection close on that call, not a hang — and generally
  keeps working for everything else.

## Behavioral simplifications

- **Timestamp lookups read batch headers.** `ListOffsets` by timestamp (`offsetsForTimes`,
  start-from-timestamp, "replay from ten minutes ago") and the max-timestamp query are
  exact, but there is no time index on disk: the first lookup on a partition reads its
  batch headers and keeps a summary in memory, and the active segment's is extended as
  it grows. That is milliseconds for a development-sized log and proportional to its
  size for a very large one.
- **A replication factor above 1 is accepted.** `CreateTopics` with a factor of 2 or 3
  creates the topic with its one replica, where a single Apache Kafka broker answers
  `INVALID_REPLICATION_FACTOR`: an application written for a cluster should start
  unchanged. A factor of zero, or below -1, is refused, as in Kafka.
- **Topic names that differ only in letter case are refused.** Kafka lets `Orders` and
  `orders` live side by side; here the second is `INVALID_TOPIC_EXCEPTION`, because
  partition directories are named after topics and the macOS and Windows file systems
  do not tell the two apart, and that holds for two such names in one request as well.
  (Kafka's own rule, that `a.b` and `a_b` collide, applies too, and as Kafka applies it:
  a name is held against the topics that existed before the request, so `a.b` and `a_b`
  sent in one `CreateTopics` request are both created. Recorded:
  `conformance/recordings/createtopics-collisions.json`.)
- **Validation messages are doze-kafka's own words.** For a refused topic name, partition
  count, topic config value and a broker config change, the error code is Kafka's and the
  `error_message` text is this broker's (it names what to do about it). A Produce batch
  that is too large or corrupt is refused with Kafka's code and, here, a text where Kafka
  sends none. Match on the code.
- **A partition named twice in one ListOffsets request is answered twice.** Kafka answers
  such an entry once, with `INVALID_REQUEST`. The console asks for a partition's earliest
  and latest offset as two entries of one request, so this stays until it asks differently.
- **Group membership is not kept across a restart.** Committed offsets are; the members,
  their generation and their assignments are not. After a restart every member must
  rejoin, and a commit that carries the old generation is `ILLEGAL_GENERATION`, where
  Kafka (which reloads its groups) accepts it until the session times out.
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
