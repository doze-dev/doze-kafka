# Claims: Topics stage

Worked example of the claims format (see `conformance/claims_test.go`). One file per
stage, one table row per claim about how Apache Kafka behaves. The rows are the
stage document's recording queue (R1 to R17, `plan/stages/topics.md` section 8)
and the claims in its findings that rest on the wire specification or on
documentation.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| R1 | CreateTopics with replication factor 3, 0 and -2 on a one-broker cluster is INVALID_REPLICATION_FACTOR, with a given message (TOP-1) | memory | | Create three topics with those factors; record code and message. |
| R2 | validateOnly on an existing topic is TOPIC_ALREADY_EXISTS; `Case` and `case` coexist; `a.b` then `a_b` is INVALID_TOPIC_EXCEPTION (TOP-2a, TOP-5) | memory | | Sequence of creates and validate-only creates. |
| R3 | The exact ErrorMessage for an invalid name, a 250-character name, an unknown config and a bad value (`abc`; 512 for segment.bytes; `2` for the dirty ratio); and the segment.bytes minimum in 4.0 (TOP-8) | memory | | One CreateTopics per case; record ErrorMessage. |
| R4 | CreatePartitions with fewer and with the same count: code and text (TOP-8) | memory | | Two calls on a 3-partition topic. |
| R5 | DescribeConfigs key list, defaults, IsDefault and source for a fresh topic, with and without overrides, and after an alter (TOP-2) | memory | | Describe after create with and without overrides; alter then describe. |
| R6 | Age retention uses the largest record timestamp of a segment; the check interval default; a retention.ms change deletes at the next check (TOP-3, TOP-18) | memory | | Small segment.bytes, backdated timestamps, log.retention.check.interval.ms=1000; watch log start. |
| R7 | A batch with a valid CRC and a garbage body is refused CORRUPT_MESSAGE (TOP-4) | memory | | Hand-built batch through a raw Produce. |
| R8 | Empty-key partitioning equals murmur2 of zero bytes; the generated kafka-console-producer command reproduces key, value, headers and null exactly (TOP-12) | memory | | Produce the same record with the generated command and a client; fetch both and compare bytes. |
| R9 | ListOffsets for a time before the first record, after the last (-1), the max timestamp on a compacted partition, and read_committed latest equal to the LSO; a Fetch with one broken and one live partition does or does not wait (TOP-31) | memory | | Scripted per case. |
| R10 | DeleteRecords: an offset above the high watermark, offset -1, an offset inside an open transaction; a fetch below the start; the read_committed LSO afterwards (TOP-34) | memory | | Scripted. |
| R11 | Compaction on the wire: offsets after cleaning, tombstone retention, the newest segment untouched, min.compaction.lag.ms honoured (TOP-7) | doc | Kafka documentation, "Log compaction" (design section) and the topic configs min.compaction.lag.ms, delete.retention.ms | Small segments, keyed writes, wait for the cleaner, fetch from 0. |
| R12 | acks=0 with a refused batch closes the connection (TOP-16) | memory | | Raw produce with acks=0 to a topic with a limit. |
| R13 | INVALID_RECORD text and per-record errors for a keyless record on a compacted topic | memory | | One produce. |
| R14 | MESSAGE_TOO_LARGE text; the limit is on the batch; the default max.message.bytes is 1048588 (TOP-2, TOP-11) | doc | Kafka documentation, topic configs: max.message.bytes (default 1048588) | Produce just under and over, batched and single. |
| R15 | LogAppendTime: the timestamp a consumer sees for each record of a multi-record batch | memory | | Produce 3 records with old timestamps to a LogAppendTime topic; fetch. |
| R16 | DeleteTopics removes the committed offsets of groups on the topic (TOP-21) | memory | | Commit, delete, recreate, OffsetFetch. |
| R17 | Metadata auto-create by a Java producer, and the produce refusal when allow.auto.create is false | memory | | Two producers, one flag each. |
| S1 | A Produce response carries a per-partition ErrorMessage and per-record errors (RecordErrors) from v8 (TOP-10) | spec | kmsg ProduceResponse.Topics.Partitions.ErrorMessage, RecordErrors (v8+) | Raw kmsg Produce answered with a refused batch; the message is read from the response. |
| S2 | DescribeLogDirs is API key 35, versions 0 to 4, and reports the size of each partition (TOP-27) | spec | kmsg DescribeLogDirsRequest, DescribeLogDirsResponse (key 35) | Raw kmsg DescribeLogDirs against a topic with known data. |
| S3 | DescribeTopicPartitions is API key 75, version 0 (TOP-26) | spec | kmsg DescribeTopicPartitionsRequest (key 75) | Not advertised: the claim is that the boards' wire samples name a key this broker declines. |
| S4 | Fetch and ListOffsets exist at versions above those this broker advertises (Fetch to 17, ListOffsets to 11), so a sample naming v16 or v8 is a valid request for the specification and an unadvertised one here (TOP-26) | spec | kmsg FetchRequest, ListOffsetsRequest MaxVersion | A version-matrix replay at the advertised maxima and one above. |
| D1 | A real broker checks retention every log.retention.check.interval.ms, 300000 (five minutes) by default, so "about once a minute" (a board) is not Kafka's default (TOP-18) | doc | Kafka documentation, broker configs: log.retention.check.interval.ms (default 300000) | Same scenario as R6. |
| D2 | A segment is rolled by segment.bytes or segment.ms; compaction cleans sealed segments only (TOP-6) | doc | Kafka documentation, topic configs: segment.bytes, segment.ms; "Log compaction" | Compact topic with segment.bytes=1024, 200 keyed records over 5 keys, wait. |
| D3 | A non-idempotent producer that retries after a failed ack can duplicate a record (TOP-32) | doc | Kafka documentation, "Message delivery semantics" | Produce with retries and an injected fsync failure on doze-kafka; a real broker cannot be made to fail an ack on demand, so this one stays a doze-kafka test, not a recording. |
