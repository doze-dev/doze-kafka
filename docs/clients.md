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

## Verified in CI

These are exercised end-to-end on every test run:

- **[franz-go](https://github.com/twmb/franz-go)** (Go) — the primary reference. Uses
  topic IDs and modern versions aggressively, and supports both group protocols.
- **[Sarama](https://github.com/IBM/sarama)** (Go) — an independent implementation
  that speaks older, non-flexible request versions.

## Confirmed by hand, not in CI

- **[kafkajs](https://kafka.js.org/)** (Node) — produce/consume via a consumer group
  was run against the real binary and works; it just isn't part of the automated
  suite (to keep Node out of the Go test dependencies).

## Expected to work

These use the same standard wire protocol and the APIs doze-kafka advertises. They
haven't been run here, but nothing about them is special:

- **librdkafka** and everything built on it — **kcat**, **confluent-kafka-python**,
  **confluent-kafka-go**, **node-rdkafka**, **librdkafka** C/C++.
- The **Apache Kafka Java client** and tools (`kafka-console-*`,
  `kafka-consumer-groups`), and frameworks on top (Spring Kafka, Alpakka).

If you hit a compatibility problem with any of these, it's a bug worth reporting.

## Connection snippets

**kcat / kafkacat** (librdkafka):

```sh
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -P          # produce (type lines, Ctrl-D)
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -C -o beginning -e   # consume from start
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -L                  # list metadata
```

**franz-go** (Go):

```go
cl, _ := kgo.NewClient(kgo.SeedBrokers("kafka.myproject.doze:9092"))
cl.ProduceSync(ctx, &kgo.Record{Topic: "demo", Value: []byte("hi")})
```

**Sarama** (Go):

```go
p, _ := sarama.NewSyncProducer([]string{"kafka.myproject.doze:9092"}, sarama.NewConfig())
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
  reconnect to the address the broker *advertises*. On one machine that's
  `localhost` and everything just works. Across container/host boundaries, set
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
