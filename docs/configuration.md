# Configuration

doze-kafka is configured entirely with command-line flags — there's no config file
to manage. Every flag has a sensible default, so `./doze-kafka` with no arguments is
a complete, working broker. Run `./doze-kafka -h` to see the list at any time.

## All flags

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
