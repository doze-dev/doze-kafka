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
**is built to behave like Kafka to any client on the socket**: franz-go and Sarama
are tested in CI, and kafkajs, librdkafka (kcat, confluent-kafka) and the Java client
speak the same protocol. If your app can't tell the difference through
`bootstrap.servers`, it has done its job.

doze-kafka is part of the [doze](https://github.com/doze-dev) family, but it needs
nothing else: **it's a single standalone binary you run.**

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
```

or

```sh
wget -qO- https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
```

This grabs the prebuilt binary for your OS and architecture, verifies its SHA-256
checksum, and installs it to `~/.doze-kafka/bin`. The script is plain POSIX `sh` (no
bash required). Linux and macOS, amd64 and arm64. No Homebrew, no package manager.

**Install a specific version** by passing `--version` through the pipe:

```sh
curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh -s -- --version v1.0.0
```

Run the script with `--help` for the full list. Each option also has an environment
variable — the flag wins if both are set:

| Option | Environment variable | Effect |
| --- | --- | --- |
| `--version <tag>` | `DOZE_KAFKA_VERSION` | Install a specific release, e.g. `v1.0.0` (default: latest). You can also put the tag in the URL: `.../doze-dev/doze-kafka/v1.0.0/install.sh`. |
| `--dir <path>` | `DOZE_KAFKA_DIR` | Install prefix (default `~/.doze-kafka`; the binary lands in `bin/`). |
| `--no-modify-path` | `DOZE_KAFKA_NO_MODIFY_PATH` | Leave your shell profile untouched. |

**To uninstall**, run the same script with `--uninstall` — it removes the binary and
the PATH entry it added, and leaves your data alone:

```sh
curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh -s -- --uninstall
```

(If you installed to a custom `--dir`, pass the same `--dir` when uninstalling.)

Prefer to build it yourself? See below.

## Quickstart

```sh
# zero config: answers on kafka.<this-directory>.doze:9092, console at http://kafka.<this-directory>.doze,
# data (logs and metadata) in ./data
doze-kafka
```

Or build from source:

```sh
go build -o doze-kafka ./cmd/doze-kafka && ./doze-kafka
```

Point any Kafka client at the address it prints (`eval "$(doze-kafka env)"` exports it):

```sh
# with kcat (librdkafka)
echo "hello" | kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -P
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -C -o beginning -e
```

Open the console URL it prints (**http://kafka.&lt;name&gt;.doze**) for the web console: browse topics and messages,
send a record, watch a consumer group's lag. ([Console](docs/console.md))

Press Ctrl-C to stop — it flushes to disk and exits cleanly. Start it again over the
same `--data-dir` and your topics, messages, and committed offsets are all still
there.

The instance is its name, on its own loopback address, so nothing on your network can
reach it and two projects can both be on port 9092. `--listen host:port` serves an address
INSTEAD of the name (no name is claimed); `doze-kafka doctor` says what is set up. To reach
it from a container or another machine, see [Endpoints](docs/endpoints.md).

Full walkthrough: **[docs/getting-started.md](docs/getting-started.md)**.

## What you get

- **Produce & consume**, with correct per-partition ordering and monotonic offsets.
- **Consumer groups** — both the classic protocol and the new KIP-848
  broker-driven protocol, chosen automatically by the client.
- **Admin** — create/delete topics, list/describe/delete groups, describe configs and
  cluster. Enough for the CLI tools and most Kafka UIs.
- **A web console** — topics, messages (binary-safe), consumer-group lag, a command
  palette. On by default, off with `--console=false`.
- **Embeddable** — `server.New(...)` starts a broker inside a Go test, on a free
  port, with nothing to clean up. ([Embedding](docs/embedding.md))
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
| [Installer](install.sh) | Install/uninstall, version pinning, and options — its header, or `install.sh --help`. |
| [Getting started](docs/getting-started.md) | Build, run, and round-trip your first message. |
| [Console](docs/console.md) | The web console: what each page shows and does. |
| [Configuration](docs/configuration.md) | Every flag and TOML config-file key, defaults, and worked examples. |
| [Endpoints](docs/endpoints.md) | Every port and file it opens, the `.doze` names, containers. |
| [Embedding](docs/embedding.md) | Starting a broker from Go code, for tests. |
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
`--kafka-version` (default 4). So `doze-kafka 1.0.0` is *this tool's* version, not a
Kafka version; `doze-kafka --version` prints it and spells out the range. Support for a future
Kafka 5 would be a normal minor release of doze-kafka, not "doze-kafka 5".

## Status

Built for local development. The automated suite verifies it end-to-end against
**franz-go** and **Sarama** (two independent client implementations) plus the real
compiled binary. kafkajs, librdkafka (kcat) and the Java client speak the same wire
protocol and are expected to work, but are not yet exercised in CI; if one doesn't,
that is a bug worth reporting. Not production software and not trying to be: see
[Limitations](docs/limitations.md) for what is deliberately out of scope.
Development history and acceptance notes live in [docs/reports/](docs/reports/history/).

## License

[Apache License 2.0](LICENSE).
