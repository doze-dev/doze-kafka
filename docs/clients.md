# Clients

doze-kafka speaks the real Kafka wire protocol, so you connect to it exactly like
you'd connect to Apache Kafka: give your client its bootstrap address and go. With no flags that is
`kafka.<project>.doze:9092` (the name defaults to the directory you started it in);
`eval "$(doze-kafka env)"` sets `KAFKA_BOOTSTRAP_SERVERS` (and `KAFKA_BROKERS`) to the
right address for this instance, and the shell examples below use it. In Go and
Node snippets, `kafka.myproject.doze:9092` stands for your own instance name. Under
`--listen` it is the address you gave it. There's no special SDK and no doze-specific configuration.

This page covers how a few common clients connect, and the handful of things worth
knowing.

## How version negotiation works

Every client starts by asking the broker which API versions it supports (an
`ApiVersions` request). doze-kafka answers with the set for the emulated Kafka major
(`--kafka-version`, default 4 — see [API support](api-support.md)). The client then uses
the highest version you both support for each request. You don't configure any of
this; it's automatic. If a client needs an API the broker doesn't advertise, it
simply won't use it.

## Tested in CI

Two suites run on every change.

**The Go suite** runs [franz-go](https://github.com/twmb/franz-go) (the primary reference:
topic IDs, modern versions, both group protocols) and [Sarama](https://github.com/IBM/sarama)
(an independent implementation that speaks older, non-flexible request versions).

**The compatibility matrix** (`compat/`, see its [README](../compat/README.md)) runs real
clients and frameworks against the built binary, each **twice: with no authentication, and
over SASL/SCRAM-SHA-512 and TLS**. The versions are the ones the cells pin (`compat/java/pom.xml`,
`compat/spring/pom.xml`, `compat/kafkajs/package.json`, `compat/versions.env`):

| Client | Version tested | What it does against the broker |
|---|---|---|
| Apache Kafka **Java** client | 4.3.1 | admin (create with config, describe, alter configs, add partitions, delete records, list, delete topics and groups), `offsetsForTimes`, idempotent and transactional producers, `read_committed` and `read_uncommitted`, consume-transform-produce with offsets in the transaction, classic groups and `group.protocol=consumer` |
| **Kafka Streams** | 4.3.1 | a word count with `exactly_once_v2`; its repartition and compacted changelog topics; a new instance restoring its state from the changelog |
| **Spring Kafka** | Spring Boot 3.4.4 | `KafkaAdmin` creating `NewTopic` beans and growing one; a transactional `@KafkaListener` whose aborted delivery is redelivered with exactly-once output |
| **confluent-kafka** (Python, librdkafka) | 2.16.0 | admin, idempotent producer, groups that commit and resume, `group.protocol=consumer`, transactions |
| **kcat** (librdkafka) | as installed (the run prints it; kcat 1.7.0 with librdkafka 2.15.1 here, apt's in CI) | metadata, keys and headers, a group that resumes |
| **KafkaJS** | 2.2.4 | admin, idempotent producer, group consume and commit |

Confluent's Java Avro, Protobuf and JSON Schema serializers (8.3.2) and confluent-kafka's Avro,
Protobuf and JSON Schema serializers also run, against the [built-in registry](schema-registry.md).
See [Frameworks and tools](frameworks.md) for Streams, Spring, Connect, schema registries
and the UIs.

Both the Java client and librdkafka also push **client metrics** (KIP-714) to the broker by
default; the console shows them ([Observability](observability.md)).

## Expected to work, not run here

These use the same wire protocol and the same APIs: **confluent-kafka-go**,
**node-rdkafka**, **librdkafka** from C/C++, the Java client at other versions, and the
Kafka command-line tools. If you hit a compatibility problem with any of them, it's a bug
worth reporting.

## Connection snippets

**kcat / kafkacat** (librdkafka):

```sh
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -P          # produce (type lines, Ctrl-D)
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -C -o beginning -e   # consume from start
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -L                  # list metadata
```

**franz-go** (Go):

```go
cl, _ := kgo.NewClient(kgo.SeedBrokers("kafka.myproject.doze:9092"),
	kgo.AllowAutoTopicCreation()) // franz-go does not ask for a new topic to be created otherwise
cl.ProduceSync(ctx, &kgo.Record{Topic: "demo", Value: []byte("hi")})
```

**Sarama** (Go):

```go
cfg := sarama.NewConfig()
cfg.Producer.Return.Successes = true // a SyncProducer refuses to start without it
p, _ := sarama.NewSyncProducer([]string{"kafka.myproject.doze:9092"}, cfg)
p.SendMessage(&sarama.ProducerMessage{Topic: "demo", Value: sarama.StringEncoder("hi")})
```

**kafkajs** (Node):

```js
const kafka = new Kafka({ brokers: ["kafka.myproject.doze:9092"] })
const producer = kafka.producer()
await producer.connect()
await producer.send({ topic: "demo", messages: [{ value: "hi" }] })
```

**Apache Kafka Java tools:**

```sh
kafka-console-producer.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic demo
kafka-console-consumer.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic demo --from-beginning
```

## Things worth knowing

- **Reconnect address.** Clients bootstrap to the address you give them, then
  reconnect to the address the broker *advertises*. On one machine that's the
  instance's name (`kafka.<name>.doze`), or `127.0.0.1` under a loopback `--listen`,
  and everything just works. Across container/host boundaries, set
  `--advertised-host` — see [Configuration](configuration.md).
- **Idempotent producers just work.** Modern producers default to idempotent writes
  (`InitProducerId` + a per-batch sequence number, and `acks=all`). doze-kafka
  tracks those sequences and **deduplicates retries** — a batch resent after a
  timeout returns its original offset instead of appending a second copy. And because
  an idempotent producer uses `acks=all`, its acknowledged writes are **fsynced to
  disk before the ack** (see [storage & durability](storage-and-durability.md)). The
  transactions (exactly-once *processing*) work too: see [Transactions](transactions.md).
- **Consumer group protocol** is negotiated for you. Older clients use the classic
  protocol; a client that opts in to KIP-848 (the Java client with
  `group.protocol=consumer`, which is **not** its default even in Kafka 4.x, where the
  default is still `classic`) uses the new protocol, which doze-kafka also serves. See [Consumer groups](consumer-groups.md).
- **Auth is off by default.** If you start the broker with SASL or TLS, every client
  must be configured to match, or it won't get past the handshake. See
  [Auth & TLS](auth-and-tls.md).
- **Unknown APIs fail fast.** If a client (or a UI) calls an API doze-kafka doesn't
  implement, the broker closes that connection rather than leaving the client
  hanging. Conformant clients never do this — they only call advertised APIs.
