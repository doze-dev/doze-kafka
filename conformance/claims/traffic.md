# Claims: Traffic stage

The stage's recording queue (R1 to R9, `plan/stages/traffic.md` section 8) and the
claims the console's explainers make about Kafka. A claim rests on `doc` or `memory`
until a scenario is recorded; a fix that rests on `memory` is a skipped test.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| R1 | What the broker answers to a PushTelemetry that arrives before its interval has passed, and to one holding an old subscription id after a subscription changed (TRF-5) | memory | | `telemetry-throttle`: raw kmsg against a broker with a `CLIENT_METRICS` resource; two pushes within 100 ms; record both codes. |
| R2 | GetTelemetrySubscriptions and PushTelemetry need no ACL | memory | | `telemetry-acl`: authorizer on, a user with no ACLs, both calls. |
| R3 | The error codes for a zero instance id, an unknown subscription id, an unsupported compression, a payload over TelemetryMaxBytes, an inflated payload over the cap, a corrupt compressed body | doc | KIP-714 | `telemetry-errors`: raw kmsg, one case each. |
| R4 | A push for another client's instance id, from another connection or principal (TRF-27) | memory | | `telemetry-instance-binding`. |
| R5 | What real Kafka subscribes a client to with no `CLIENT_METRICS` configured | doc | KIP-714 | `telemetry-empty-subscription`. |
| R6 | A first JoinGroup (v4+) of a classic consumer is answered `MEMBER_ID_REQUIRED`, and a heartbeat during a rebalance `REBALANCE_IN_PROGRESS`, as normal flow: the class table calls both expected | doc | KIP-394 (member id required) | `classic-join-flow`: three consumers join one after another; record every code of Join, Sync, Heartbeat, OffsetCommit. |
| R7 | Which error is first when a Produce response has several | memory | | `produce-mixed-partition-errors`. |
| R8 | Any join in the classic protocol makes every member rejoin; the generation numbers across a join, a leave and an expiry | doc | Kafka documentation, consumer group rebalance protocol | `classic-rebalance-generations`: record generation and member count per round. |
| R9 | The client versions that push by default (Java 3.7, librdkafka 2.5) and that a franz-go client does too (it did, in the browser suite: its report carried `consumer.connection.creation.rate`) | doc | KIP-714; the browser suite's held consumer saw a franz-go client push | the compat cells `java-telemetry`, `kcat-telemetry` run it against the broker, not against real Kafka. |
| C1 | A Java client subscribes on one connection and pushes on another (its bootstrap connection and the one metadata gave it), so a broker must not tie an instance to the connection that subscribed | memory | | `telemetry-two-connections`: a Java client against the recorded broker, record the connection of each. |
