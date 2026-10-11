# Claims: Bad days (stage 1, "Durability first")

The recording queue of `plan/stages/bad-days.md` section 8, as far as stage 1 got.
Q2, Q5 and Q9 (acks=0) are recorded against `apache/kafka:4.3.1`; the rest stay at `memory` or
`doc` until a stage that needs them records them.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| Q2a | A full data disk answers Produce with KAFKA_STORAGE_ERROR (56), at acks=all and at acks=1, with no ErrorMessage (BAD-4) | recorded | conformance/recordings/full-disk.json | full-disk (a 24 MiB tmpfs, 700 KiB batches until refused) |
| Q2b | After the disk filled, the single-log-directory Kafka broker went away: the next requests got a closed connection, not an answer. doze-kafka stays up and refuses writes only (docs/limitations.md) | recorded | conformance/recordings/full-disk.json | full-disk, steps list-earliest-after and create-after |
| Q5a | JoinGroup with session.timeout.ms below 6000 or above 1800000 is INVALID_SESSION_TIMEOUT (26), on v0, v7 and v9, and the bounds are inclusive: 6000 and 1800000 are accepted (BAD-29) | recorded | conformance/recordings/joingroup-session-timeout.json | joingroup-session-timeout |
| Q5b | The session timeout is checked before the member id is handed out: 1000 ms is 26, not MEMBER_ID_REQUIRED (79); a negative value is 26 | recorded | conformance/recordings/joingroup-session-timeout.json | joingroup-session-timeout, steps session-1000, session-negative |
| Q5c | The rebalance timeout has no bounds of its own: 1 ms and 2000000000 ms are accepted | recorded | conformance/recordings/joingroup-session-timeout.json | joingroup-session-timeout, steps rebalance-1, rebalance-huge |
| Q1 | A failed fsync is not retried by Kafka; the log directory goes offline | doc | Kafka KIP-112 (handle disk failure for JBOD); the broker's LogDirFailureChannel | Not reproducible by a client; the observable result is Q2b. The doze-side behaviour is a design choice (BAD-3). |
| Q9 | An acks=0 Produce that carries any partition error (unknown topic, unknown partition, message too large, corrupt batch) makes the broker close the connection; an accepted one does not | recorded | conformance/recordings/produce-acks-zero.json | produce-acks-zero (each acks=0 request followed by a Metadata request on the same connection) |
