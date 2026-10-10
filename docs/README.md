# doze-kafka documentation

doze-kafka is a single Go binary that speaks the Kafka protocol, stores data on
disk, and runs on your laptop to aid development. No JVM, no ZooKeeper, no cluster.

These docs are for developers **running** it — pointing an app, a CLI, or a Kafka UI
at a local broker. If you just want to get going, read
[Getting started](getting-started.md); the rest is reference.

## Guides

1. **[Getting started](getting-started.md)** — build it, run it, produce and consume
   your first message, confirm it survives a restart.
2. **[Console](console.md)** — the web console: topics, messages, consumer groups,
   cluster, and what each page shows.
3. **[Configuration](configuration.md)** — every command-line flag, its default and
   meaning, and copy-paste examples (data location, retention, partitions, Kafka
   version to emulate).
4. **[Endpoints](endpoints.md)** — every port and file it opens, the `.doze` names,
   reaching it from a container.
5. **[Clients](clients.md)** — how to connect from franz-go, Sarama, kafkajs,
   librdkafka (kcat, confluent-kafka-python, node-rdkafka), and the Java client;
   version negotiation and the common connection gotchas.
6. **[Consumer groups](consumer-groups.md)** — using consumer groups, the classic
   and KIP-848 protocols, committing and resuming offsets, and rebalancing.
7. **[Auth & TLS](auth-and-tls.md)** — enabling SASL/PLAIN, SASL/SCRAM, and TLS, with
   matching client configuration. **[Authorization](authorization.md)** — ACLs, enforced
   with Kafka's rules.
   **[Schema Registry](schema-registry.md)** — the built-in, Confluent-compatible registry
   (Avro, JSON Schema, Protobuf), where it listens, and where it differs.
8. **[Storage & durability](storage-and-durability.md)** — how messages are stored,
   how retention works, what a restart or crash keeps, and the flat-memory property.
9. **[API support](api-support.md)** — the exact set of Kafka APIs and versions the
   broker advertises, per emulated Kafka major.
10. **[Architecture](architecture.md)** — a tour of the internals for the curious and
   for contributors.
11. **[Limitations](limitations.md)** — the deliberate boundary: what doze-kafka does
   not do, and why.
12. **[Troubleshooting](troubleshooting.md)** — fixes for the usual "my client won't
    connect" situations.
13. **[Embedding](embedding.md)** — start a broker from Go code, for tests.
14. **[Observability](observability.md)** — the console's traffic tail and group
    timeline: what your app asked the broker, and why a group rebalances.

## Development history

The [testing strategy](reports/TESTING.md) is current. The phase-by-phase build notes
and acceptance evidence are in [reports/history/](reports/history/), a record of how
the broker was built and verified — useful if you're contributing, skippable if
you're just using it.
