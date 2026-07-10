# Consumer groups

Consumer groups are how Kafka lets several consumers share the work of a topic and
remember where they left off. doze-kafka implements them fully, including both the
classic protocol and the newer KIP-848 one, so from your app's side there is nothing
special to do — set a group id and consume.

## Using a group

Set a `group.id` (the exact option name varies by client) and subscribe. The broker
assigns partitions across the group's members, tracks each member's health, and
persists committed offsets so a restarted consumer resumes where it stopped.

```sh
# kcat: consume topic "demo" as part of group "workers"
kcat -b localhost:9092 -G workers demo
```

```go
// franz-go
cl, _ := kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.ConsumerGroup("workers"),
    kgo.ConsumeTopics("demo"),
)
```

Start a second consumer in the same group and the partitions rebalance across the
two. Stop one and its partitions move to the survivor.

## Committing and resuming offsets

Committed offsets are the one piece of group state that is **durable** — they're
written to `--meta-db` and survive a broker restart. Membership and assignments are
in-memory and simply rebuilt when clients reconnect.

Most clients auto-commit periodically; you can also commit manually. Either way, when
a consumer in a group restarts (or the broker does), it fetches its committed offset
and resumes from there rather than replaying from the beginning. If a group has never
committed for a partition, the client falls back to its `auto.offset.reset` setting
(earliest/latest).

## Two protocols, chosen automatically

Kafka has two consumer-group protocols, and doze-kafka speaks both. You don't pick;
the client and the broker negotiate it.

### Classic

The original protocol (`JoinGroup`/`SyncGroup`/`Heartbeat`). The group's elected
*leader* — a client — computes the partition assignment; the broker is the rendezvous
point that relays it. Used by kcat/librdkafka, Sarama, kafkajs, and Kafka clients
before 4.0 (or any client pinned with `group.protocol=classic`). The client requests
its own session timeout.

### KIP-848 (broker-driven)

The newer protocol (`ConsumerGroupHeartbeat`), where the **broker** computes the
assignment and drives an incremental rebalance. doze-kafka advertises it only when
emulating **Kafka 4** (`--version 4`, the default), so a modern client at its default
`group.protocol=consumer` uses it with no override. The broker's `uniform` (round
robin) and `range` assignors are both supported. The session timeout is the broker's,
set with `--consumer-session-timeout` (default 45s).

A practical note on franz-go: as of v1.21 it keeps its KIP-848 support behind a beta
opt-in, so franz uses the classic protocol against doze-kafka unless you opt in. Most
other clients (including the Kafka 4 Java client) use KIP-848 by default. Both paths
are fully supported by the broker.

## Rebalancing and member health

When membership changes — a consumer joins, leaves, or dies — the group rebalances
and partitions are redistributed. A consumer that leaves cleanly (a graceful
shutdown) hands its partitions back immediately. A consumer that just vanishes is
detected by its **session timeout**: once it stops heartbeating for long enough, the
broker evicts it and rebalances to the rest.

Under the **KIP-848** protocol the broker computes assignments, and the handoff is
safe by construction: doze-kafka never hands a partition to a new owner while the
previous owner still reports holding it — a partition is only reassigned once its old
owner has released it. Under the **classic** protocol the broker is only a relay, so
avoiding overlap is the client's job (its rebalance protocol), exactly as it is with
Apache Kafka.

## Inspecting groups

The admin APIs `kafka-consumer-groups` uses are implemented:

```sh
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group workers
```

`ListGroups` reports classic and KIP-848 groups (with their type); `DescribeGroups`
covers classic groups and `ConsumerGroupDescribe` covers KIP-848 ones — your tooling
picks the right one automatically.

## Limits

Single node means every group is coordinated by the one broker, which keeps this
simple. Not implemented: static membership (`group.instance.id` is accepted but not
acted on) and the sticky cross-topic balancing of the reference `uniform` assignor —
doze-kafka's assignor is a straightforward, even round-robin. Neither matters for
local development.
