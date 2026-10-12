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
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -G workers demo
```

```go
// franz-go
cl, _ := kgo.NewClient(
    kgo.SeedBrokers("kafka.myproject.doze:9092"),
    kgo.ConsumerGroup("workers"),
    kgo.ConsumeTopics("demo"),
)
```

Start a second consumer in the same group and the partitions rebalance across the
two. Stop one and its partitions move to the survivor.

## Committing and resuming offsets

Committed offsets are the one piece of group state that is **durable** — they're
written to the metadata database (`<data-dir>/meta.db`) and survive a broker restart. Membership and assignments are
in-memory and simply rebuilt when clients reconnect.

Most clients auto-commit periodically; you can also commit manually. Either way, when
a consumer in a group restarts (or the broker does), it fetches its committed offset
and resumes from there rather than replaying from the beginning. If a group has never
committed for a partition, the client falls back to its `auto.offset.reset` setting
(earliest/latest).

### Both at once: a rolling upgrade

A group id can have members of both protocols, as on Kafka 4 with its default migration
policy: one instance of an application moves to `group.protocol=consumer` while the
others are still classic, and nothing is stopped. What the broker does is what Apache
Kafka does, recorded in `conformance/recordings/mixed-protocol-groups.json` and
`mixed-protocol-rolling.json`:

- A consumer-protocol member joining a classic group (protocol type `consumer`) turns it
  into a consumer group. The classic members stay in it, holding the partitions they
  held, until they rejoin; the new member waits for what they give up.
- A classic client joining a consumer group becomes one of its members. Nobody relays
  its assignment: its `JoinGroup` is answered with no leader and no member list, and its
  `SyncGroup` carries the partitions it may have now.
- A classic member is told to rejoin the way the protocol always did, by
  `REBALANCE_IN_PROGRESS` on a heartbeat, when its partitions are to change. Its rejoin
  gives up everything it held, which is what lets another member take them.
- When the last consumer-protocol member leaves, the group is a classic group again and
  its members rejoin (`ILLEGAL_GENERATION` on their next heartbeat).
- A classic group whose protocol type is not `consumer` (a Connect worker) is not
  upgraded: the consumer-protocol join is refused with `GROUP_ID_NOT_FOUND`.
- A group with no members moves from one protocol to the other whole, and keeps its
  committed offsets.

The differences left are in [limitations](limitations.md): Kafka computes the target
assignment after it has answered a join, so epochs read one apart for a moment and the
group is shown `Assigning`; this broker computes it first.

### When committed offsets expire

Offsets are not kept for ever: as on Kafka (`offsets.retention.minutes`), they expire
after `--offsets-retention` (default 7 days; `0` keeps them for ever). Which clock
applies is Kafka's, recorded against `apache/kafka:4.3.1`
(`conformance/recordings/offsets-retention.json`, `offsets-retention-restart.json`):

- A **classic group that had members and has none now** keeps its offsets for the
  retention from the moment it became empty, however late its last commit was. A commit
  made into an empty group does not extend it.
- A group **nobody ever joined** (a standalone consumer committing by hand) keeps each
  offset for the retention from its own commit. So does a **consumer-protocol group
  whose members have gone**: Kafka keeps no moment of emptying for it.
- A group **with members** never loses the offsets of the topics it is subscribed to;
  those of a topic it is not subscribed to expire from their own commit. A group in the
  middle of a rebalance is not looked at.
- A group whose offsets are all gone is gone: it is no longer listed.

**Across a restart** a real broker has its groups' members again when it is back. A
member that does not return runs out of session, and the group is without members from
then: not from the restart, and not from its commits. doze-kafka does not keep members
([limitations](limitations.md)), so it keeps what that rule needs. The metadata
database records, for each group, that it has members and the longest session among
them, written before the first member's join is answered, or when a classic group lost
its last one. After a restart, a clean one or a crash, a group recorded as having
members is treated as having them until that session has passed with nobody joining,
and as having lost them at that moment. An application that is back within its session
timeout finds its offsets, however old they are. One that stays away gets the retention
from there if it is a classic group, and for a consumer-protocol group offsets as old
as their commits, as on Kafka. Until the session has passed nothing of such a group
expires, not even the offsets of a topic it was not subscribed to.

Three things follow from keeping that record:

- A join that cannot be recorded (the metadata database cannot be written) is refused
  with `COORDINATOR_NOT_AVAILABLE` and tried again by the client; the member is not
  seated in a group the store would later take for one nobody joined.
- The record is kept with `--offsets-retention 0` too, so turning the retention on later
  does not expire the offsets of groups that were live all along.
- A data directory written before the record existed is adopted once: every group that
  has offsets and no record is treated as having had members with a 45-second session.
  Which of them really had is not known, and an offset is kept when that is in doubt.

An offset stored by a version that did not record commit times is given the time of the
first sweep that sees it.

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
emulating **Kafka 4** (`--kafka-version 4`, the default). Clients choose it themselves:
the Java client uses it when configured with `group.protocol=consumer` (its default is
still `classic`, as of Kafka 4.1), and a client that has opted in needs no other change. The broker's `uniform` (round
robin) and `range` assignors are both supported. The session timeout is the broker's,
set with `--consumer-session-timeout` (default 45s).

A practical note on franz-go: as of v1.21 it keeps its KIP-848 support behind a beta
opt-in, so franz uses the classic protocol against doze-kafka unless you opt in. Most
clients default to the classic protocol and need an explicit opt-in. Both paths are
supported by the broker.

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
kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --list
kafka-consumer-groups.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --describe --group workers
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
