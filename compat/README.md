# Client-compatibility matrix

Real clients and frameworks against the real binary, to answer the question that
matters: can a team finish local development against it?

```sh
go tool task test:compat                  # everything installed
go tool task test:compat -- java python   # only these
compat/run.sh -b ./doze-kafka kcat        # a binary you built
```

Each directory is one client: a self-contained project whose `run.sh` reads its
configuration from the environment and exits **0** (passed), **77** (the toolchain it
needs is not installed: **skipped**, never counted as a pass) or anything else (failed).
`run.sh` at this level boots the binary on free ports over fresh data directories, runs
every client once with no authentication and once over **SASL/SCRAM-SHA-512 and TLS**, and
prints a matrix.

The run is green only when every cell (each client, plain and scram-tls: 12 clients,
24 cells) PASSED: a skipped cell fails it, and so does a cell with no result. On a
machine that lacks a toolchain on purpose, name the clients that may skip:
`ALLOW_SKIP="spring connect" compat/run.sh`. That is refused when `CI` is set: in CI
every cell runs. `compat/runner_test.go` is the runner's own test (fake clients that
pass, skip and fail).

| Client | Toolchain | Covers |
|---|---|---|
| `java` | Docker | Java client 4.3: admin (create with config, describe, alter configs, grow, list, delete, delete records, groups), `offsetsForTimes`, idempotent and transactional producers, `read_committed`, consume-transform-produce, classic groups and `group.protocol=consumer` (KIP-848); **Kafka Streams** word count with `exactly_once_v2`, its compacted changelog, and a fresh instance restoring from it |
| `spring` | Docker | Spring Boot 3.4 / Spring Kafka: `KafkaAdmin` creating `NewTopic` beans and growing one on the next start; a transactional `@KafkaListener` whose aborted delivery is redelivered with exactly-once output |
| `python` | `uv` | `confluent-kafka` (librdkafka): admin, idempotent producer, groups that commit and resume, KIP-848, transactions with `read_committed` and `read_uncommitted` |
| `kcat` | `kcat` | librdkafka's command line: metadata, keys and headers, a group that resumes |
| `kcat-telemetry` | `kcat` (librdkafka 2.5+) | KIP-714: a producer left connected pushes its metrics and its client id appears on the Traffic page with them |
| `java-telemetry` | Docker | KIP-714 with the Java client: Kafka's own console producer, same assertion |
| `kafkajs` | `bun` or `node` | KafkaJS: admin, idempotent producer, group consume and commit |
| `connect` | Docker | Kafka Connect (distributed): the worker creates and verifies its three compacted internal topics; a file source into a topic and a file sink out of it; after a restart the connectors and the source's offset come back from those topics |
| `confluent-sr` | Docker | Confluent's Schema Registry 8.3 against the broker: it keeps its state in a `_schemas` topic, completes an Avro round trip and replays after a restart |
| `apicurio` | Docker | Apicurio Registry (Kafka SQL storage): the same |
| `kafbat-ui`, `redpanda-console` | Docker | the UIs connect and list topics, consumer groups and brokers |

The `java` and `python` clients also run Confluent's serializers (Avro, Protobuf, JSON Schema)
against the built-in schema registry.

Go clients (franz-go, Sarama) are covered by the Go test suite.

Clients in a container cannot reach `127.0.0.1` on the host, and the broker tells every
client to reconnect to the address it advertises, so those (marked by a `docker` file)
get a broker of their own that listens on all interfaces and advertises
`host.docker.internal`.

The runner puts its scratch directory under `$HOME`, not `/tmp`: Docker (Colima, Docker
Desktop) shares the home directory with its VM and not the system temp directory.

Adding a client: make `compat/<name>/run.sh`, read `BOOTSTRAP`, `SECURITY` (`plain` or
`scram-tls`) and, for the latter, `SASL_USER`, `SASL_PASS`, `SASL_MECHANISM` and
`CA_CERT`. Print `ok   what it proved` for each thing and exit non-zero on the first
that is wrong.
