# doze-kafka documentation

doze-kafka is a single Go binary that speaks the Kafka protocol, stores data on
disk, and runs on your laptop to aid development. No JVM, no ZooKeeper, no cluster.

These docs are for developers **running** it — pointing an app, a CLI, or a Kafka UI
at a local broker. If you just want to get going, read
[Getting started](getting-started.md); the rest is reference.

## Guides

1. **[Getting started](getting-started.md)** — build it, run it, produce and consume
   your first message, confirm it survives a restart.
2. **[Configuration](configuration.md)** — every command-line flag, its default and
   meaning, and copy-paste examples (data location, retention, partitions, Kafka
   version to emulate).
3. **[Clients](clients.md)** — how to connect from franz-go, Sarama, kafkajs,
   librdkafka (kcat, confluent-kafka-python, node-rdkafka), and the Java client;
   version negotiation and the common connection gotchas.
4. **[Consumer groups](consumer-groups.md)** — using consumer groups, the classic
   and KIP-848 protocols, committing and resuming offsets, and rebalancing.
5. **[Auth & TLS](auth-and-tls.md)** — enabling SASL/PLAIN, SASL/SCRAM, and TLS, with
   matching client configuration.
6. **[Storage & durability](storage-and-durability.md)** — how messages are stored,
   how retention works, what a restart or crash keeps, and the flat-memory property.
7. **[API support](api-support.md)** — the exact set of Kafka APIs and versions the
   broker advertises, per emulated Kafka major.
8. **[Architecture](architecture.md)** — a tour of the internals for the curious and
   for contributors.
9. **[Limitations](limitations.md)** — the deliberate boundary: what doze-kafka does
   not do, and why.
10. **[Troubleshooting](troubleshooting.md)** — fixes for the usual "my client won't
    connect" situations.

## Development history

The [reports/](reports/) folder holds the phase-by-phase build notes and acceptance
evidence, plus the [testing strategy](reports/TESTING.md). They're a record of how
the broker was built and verified — useful if you're contributing, skippable if
you're just using it.
