# Configuration

doze-kafka is configured with command-line flags **or** a TOML config file — every
setting is available either way, under the same name. Every setting has a sensible
default, so `./doze-kafka` with no arguments is a complete, working broker. Run
`./doze-kafka -h` to see the flags at any time.

## Config file

Drop a `doze-kafka.toml` in your working directory and doze-kafka loads it
automatically; point elsewhere with `--config path/to/file.toml`. The keys match the
flags one-to-one:

```toml
listen = ":9092"
data-dir = "./data"
retention-ms = 604800000   # 7 days
fsync-interval = "500ms"

sasl-mechanisms = ["SCRAM-SHA-256"]

[credentials]
alice = "s3cret"

[tls]
cert = "./certs/server.pem"
key  = "./certs/server-key.pem"
```

Copy [`doze-kafka.example.toml`](../doze-kafka.example.toml) for a fully commented
starting point.

**Precedence is flags > file > defaults.** The file overlays only the keys it names
— everything else stays at its default — and any flag still wins for a one-off
override, so you can commit a `doze-kafka.toml` for your project and still run
`./doze-kafka --version 3` for a quick experiment. A mistyped key is rejected rather
than silently ignored.

Two things a file does better than flags: it keeps a **list of SASL users** readable
(and keeps passwords out of your shell history and `ps` output), and it's
**committable** — check `doze-kafka.toml` into your repo and the whole team gets the
same broker.

`doze-kafka config print` writes your *effective* configuration — defaults, then the
file, then any flags — as TOML, so you can see exactly what the broker will run with,
or generate a starter file:

```sh
doze-kafka config print > doze-kafka.toml
```

## All flags

Every flag below has the same name as its config-file key (durations like
`--fsync-interval` are written as strings, e.g. `fsync-interval = "500ms"`).

### Network

| Flag | Default | Meaning |
|---|---|---|
| `--listen` | `:9092` | `host:port` to accept connections on. Use `:0` for an ephemeral port. |
| `--advertised-host` | `localhost` | Host returned to clients in Metadata. **This is the address clients actually reconnect to** — see the note below. |
| `--advertised-port` | `0` | Port advertised to clients. `0` derives it from `--listen`. |

### Storage

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | `./data` | Directory holding per-partition log segments. |
| `--meta-db` | `./meta.db` | File holding the topic registry, committed offsets, and producer-id counter. |
| `--segment-bytes` | `134217728` (128 MiB) | Roll to a new log segment once the active one exceeds this. |
| `--index-interval-bytes` | `4096` | Add one sparse index entry per this many bytes of log. |

### Retention (both default to unbounded)

| Flag | Default | Meaning |
|---|---|---|
| `--retention-bytes` | `0` | Delete oldest segments once a partition exceeds this size. `0` = keep everything. |
| `--retention-ms` | `0` | Delete segments older than this many milliseconds. `0` = keep everything. |

### Durability & groups

| Flag | Default | Meaning |
|---|---|---|
| `--fsync-interval` | `500ms` | How often active segments are flushed to disk. `0` disables the timer (the OS still flushes eventually; a clean shutdown always syncs). |
| `--consumer-session-timeout` | `45s` | KIP-848 group session timeout: evict a member after this long without a heartbeat. (Classic groups use the timeout the client requests.) |

### Topics

| Flag | Default | Meaning |
|---|---|---|
| `--auto-create-topics` | `true` | Create a topic the first time a client references it. |
| `--default-partitions` | `1` | Partition count for auto-created topics. |

### Protocol version

| Flag | Default | Meaning |
|---|---|---|
| `--version` | `4` | Which Kafka major (1–4) to emulate. Sets the API versions advertised and whether the new KIP-848 group protocol is offered. See [API support](api-support.md). |

### Authentication (all optional; see [Auth & TLS](auth-and-tls.md))

| Flag | Default | Meaning |
|---|---|---|
| `--sasl-mechanisms` | *(none)* | Comma-separated SASL mechanisms to require: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Enabling any turns SASL on. |
| `--sasl-user` | *(none)* | A `user:password` credential. Repeatable. |
| `--tls-cert` | *(none)* | PEM certificate file. With `--tls-key`, terminates TLS on the listener. |
| `--tls-key` | *(none)* | PEM private key file. |

## Worked examples

**Persistent broker in a fixed location:**

```sh
./doze-kafka --data-dir /var/tmp/kafka/data --meta-db /var/tmp/kafka/meta.db
```

**Bounded disk use** — keep at most ~1 GiB per partition and nothing older than a
day:

```sh
./doze-kafka --retention-bytes 1073741824 --retention-ms 86400000
```

**Multi-partition topics by default:**

```sh
./doze-kafka --default-partitions 6
```

**Emulate an older Kafka** for a client that expects it (e.g. to exercise classic
consumer groups only):

```sh
./doze-kafka --version 3
```

**Throwaway broker on a random port** (handy for scripts and CI):

```sh
./doze-kafka --listen 127.0.0.1:0 --data-dir "$(mktemp -d)"
# the chosen port is in the "listening" log line
```

**Locked down** with SCRAM + TLS:

```sh
./doze-kafka \
  --sasl-mechanisms SCRAM-SHA-256 --sasl-user alice:s3cret \
  --tls-cert cert.pem --tls-key key.pem
```

## A note on `--advertised-host`

Kafka clients connect to a bootstrap address, ask the broker for Metadata, and then
**reconnect to whatever host and port the broker advertises**. By default that's
`localhost:<listen-port>`, which is right for a client on the same machine.

If a client connects from elsewhere — another container, another host, a VM — it must
be able to reach the *advertised* address, not just the bootstrap one. Set
`--advertised-host` (and `--advertised-port` if it's remapped) to an address that
client can actually reach. This is the single most common reason a client connects
but then hangs or fails on the next request. See
[Troubleshooting](troubleshooting.md).
