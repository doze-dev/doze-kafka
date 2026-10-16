# Frameworks and tools

What teams run beside Kafka, pointed at doze-kafka. Each of these is run in CI by the
[compatibility matrix](../compat/README.md), with no authentication and over SASL/SCRAM and
TLS, so what is written here has been done.

Bootstrap with `eval "$(doze-kafka env)"` for the address. From a container, run the broker
with `--listen 0.0.0.0:9092 --advertised-host host.docker.internal` (see
[Endpoints](endpoints.md)): a client in a container is told to reconnect to the address the
broker advertises, and `127.0.0.1` there is the container.

## Kafka Streams

Nothing special: `bootstrap.servers`, and everything Streams needs is there.

- `processing.guarantee=exactly_once_v2` works (transactions, [Transactions](transactions.md)).
- The **repartition** and **changelog** topics Streams creates are real topics; the
  changelogs are `cleanup.policy=compact`, and a fresh instance with an empty state
  directory restores its state from them and carries on. The cleaner runs in the
  background ([Storage & durability](storage-and-durability.md#compaction)).
- `application.reset` / `kafka-streams-application-reset` uses `DeleteRecords`, which works.

## Spring Kafka

```properties
spring.kafka.bootstrap-servers=${KAFKA_BOOTSTRAP_SERVERS}
spring.kafka.producer.transaction-id-prefix=tx-          # transactions, if you use them
spring.kafka.consumer.isolation-level=read_committed
```

- `KafkaAdmin` creates every `NewTopic` bean at start-up and **raises the partition count**
  when a declaration grows (`CreatePartitions`); it never shrinks one.
- A `@KafkaListener` inside the container's transaction: what it sends commits with the
  offsets it read, a thrown exception aborts both, and the record is redelivered.

## Kafka Connect

Distributed mode works. The worker creates its three internal topics and checks that they
are **compacted**, which they are; its connectors come back from the config topic after a
restart and a source resumes from its stored offset. File source and sink connectors were
run; any connector that needs only the Kafka protocol should work.

```properties
bootstrap.servers=kafka.myproject.doze:9092
group.id=connect-cluster
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
offset.storage.topic=connect-offsets
config.storage.topic=connect-configs
status.storage.topic=connect-status
offset.storage.replication.factor=1
config.storage.replication.factor=1
status.storage.replication.factor=1
```

These are the worker settings `compat/connect` runs with (it adds a per-run suffix to the
group and topic names); `group.id` is required in distributed mode.

## Schema Registry

doze-kafka has its own, in the binary: [Schema Registry](schema-registry.md). Point
`schema.registry.url` at it (`SCHEMA_REGISTRY_URL` from `doze-kafka env`). Confluent's Java
serializers (Avro, Protobuf, JSON Schema) and confluent-kafka for Python work with nothing
else changed, and the compatibility rules are Confluent's, checked against recordings of
the real thing.

If you would rather run another registry, both of these start against doze-kafka and keep
their state in a Kafka topic here:

- **Confluent Schema Registry** (7.7): `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` at the
  broker; it creates its `_schemas` topic and replays it after a restart.
- **Apicurio Registry** with `APICURIO_STORAGE_KIND=kafkasql` and
  `APICURIO_KAFKASQL_BOOTSTRAP_SERVERS` at the broker.

## UIs

Both connect and list topics and consumer groups, with no API they need missing:

- **Kafbat UI** (`ghcr.io/kafbat/kafka-ui`): `KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS` at the broker.
  It shows the cluster's controller as ZooKeeper and the version as unknown: doze-kafka has
  neither.
- **Redpanda Console** (`redpandadata/console`): `KAFKA_BROKERS` at the broker.

For SASL/SCRAM and TLS, give them the same credentials and CA as any client; the matrix's
scripts (`compat/kafbat-ui/run.sh`, `compat/redpanda-console/run.sh`) are working examples.
