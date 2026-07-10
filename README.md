# doze-kafka

[![CI](https://github.com/doze-dev/doze-kafka/actions/workflows/ci.yml/badge.svg)](https://github.com/doze-dev/doze-kafka/actions/workflows/ci.yml)

**A JVM-free Kafka for local development — one Go binary, single node, disk-backed.**

Running Kafka on your laptop usually means a JVM (a gigabyte of heap, a slow cold
start) inside a container, plus a controller quorum you never asked for, just to
publish a few messages to a topic. doze-kafka takes the other road: a **native
process** that boots in milliseconds, sits quiet until a client connects, keeps your
data on disk, and shuts down cleanly. No JVM. No ZooKeeper. No KRaft quorum. No
Docker.

It is not a re-implementation of Apache Kafka's internals. It is a broker that
**behaves like Kafka to any client on the socket** — franz-go, Sarama, kafkajs,
librdkafka (kcat, confluent-kafka), the Java client. If your app can't tell the
difference through `bootstrap.servers`, it has done its job.

doze-kafka is part of the [doze](https://github.com/doze-dev) family, but it needs
nothing else: **it's a single standalone binary you run.**

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
# or
wget -qO- https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
```

This grabs the prebuilt binary for your OS and architecture, verifies its SHA-256
checksum, and installs it to `~/.doze-kafka/bin`. The script is plain POSIX `sh` (no
bash required). Tune it with environment variables:

| Variable | Effect |
| --- | --- |
| `DOZE_KAFKA_VERSION` | Pin a version, e.g. `v1.0.0` (default: latest release). You can also put the tag in the URL: `.../doze-dev/doze-kafka/v1.0.0/install.sh`. |
| `DOZE_KAFKA_DIR` | Install prefix (default `~/.doze-kafka`; the binary lands in `bin/`). |
| `DOZE_KAFKA_NO_MODIFY_PATH` | Set to `1` to leave your shell profile untouched. |

Linux and macOS, amd64 and arm64. No Homebrew, no package manager.

Prefer to build it yourself? See below.

## Quickstart

```sh
# zero config: listens on :9092, stores logs in ./data and metadata in ./meta.db
doze-kafka
```

Or build from source (the module lives inside a go workspace, hence `GOWORK=off`):

```sh
GOWORK=off go build -o doze-kafka ./cmd/doze-kafka && ./doze-kafka
```

Point any Kafka client at `localhost:9092`:

```sh
# with kcat (librdkafka)
echo "hello" | kcat -b localhost:9092 -t demo -P
kcat -b localhost:9092 -t demo -C -o beginning -e
```

Press Ctrl-C to stop — it flushes to disk and exits cleanly. Start it again over the
same `--data-dir` and your topics, messages, and committed offsets are all still
there.

Full walkthrough: **[docs/getting-started.md](docs/getting-started.md)**.

## What you get

- **Produce & consume**, with correct per-partition ordering and monotonic offsets.
- **Consumer groups** — both the classic protocol and the new KIP-848
  broker-driven protocol, chosen automatically by the client.
- **Admin** — create/delete topics, list/describe groups, describe configs and
  cluster. Enough for the CLI tools and most Kafka UIs.
- **Durable** — messages and committed offsets survive a restart; with `acks=all` an
  acknowledged write is fsynced to disk before the ack (a full device flush on
  macOS), not merely held in memory — as durable as `fsync` gets on your hardware.
- **Idempotent producers** — retried batches are deduplicated by sequence number, so
  a producer retry won't write a record twice.
- **Retention** — bound each partition by size or age; old segments are deleted.
- **Auth, when you want it** — SASL/PLAIN, SASL/SCRAM-256/512, and TLS.
- **Flat memory** — idle RSS stays in the tens of MB no matter how much data sits on
  disk; the log is served straight off the filesystem, never buffered into the heap.

## What it isn't

Anything a local client can't observe through the socket is out of scope on purpose:
replication and ISR, the controller quorum, KRaft/ZooKeeper, transactions,
byte-for-byte on-disk segment compatibility, tiered storage, and quotas. It is a
**single node** — there is no clustering, and that's the point: it exists to aid
development, not to run production. See
**[docs/limitations.md](docs/limitations.md)** for the full, honest boundary.

## Documentation

Start here → **[docs/](docs/README.md)**

| Guide | What's in it |
|---|---|
| [Getting started](docs/getting-started.md) | Build, run, and round-trip your first message. |
| [Configuration](docs/configuration.md) | Every flag and TOML config-file key, defaults, and worked examples. |
| [Clients](docs/clients.md) | Connecting franz-go, Sarama, kafkajs, librdkafka/kcat, Java. |
| [Consumer groups](docs/consumer-groups.md) | Classic vs KIP-848, offsets, rebalancing. |
| [Auth & TLS](docs/auth-and-tls.md) | Turning on SASL and TLS, with client snippets. |
| [Storage & durability](docs/storage-and-durability.md) | Segments, retention, fsync, restart. |
| [API support](docs/api-support.md) | The exact Kafka API/version matrix. |
| [Architecture](docs/architecture.md) | How it works inside, for the curious. |
| [Limitations](docs/limitations.md) | What's out of scope, and why. |
| [Troubleshooting](docs/troubleshooting.md) | When a client won't connect. |

## Versioning

doze-kafka has its **own** version (starting at **1.0.0**), separate from the Kafka
protocol. **One binary speaks Kafka 1–4** — you pick which at runtime with
`--version` (default 4). So `doze-kafka 1.0.0` is *this tool's* version, not a Kafka
version; `doze-kafka version` prints it and spells out the range. Support for a future
Kafka 5 would be a normal minor release of doze-kafka, not "doze-kafka 5".

## Status

Feature-complete for local development. The automated suite verifies it end-to-end
against **franz-go** and **Sarama** (two independent client implementations) plus the
real compiled binary; kafkajs, librdkafka (kcat), and the Java client speak the same
protocol and work, but aren't in CI. Not production software and not trying to be.
Development history and acceptance notes
live in [docs/reports/](docs/reports/).

## License

[Apache License 2.0](LICENSE).
