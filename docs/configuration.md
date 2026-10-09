# Configuration

doze-kafka is configured with command-line flags **or** a TOML config file — every
setting is available either way, under the same name (with two exceptions, the
`[credentials]` and `[tls]` tables, shown below). Every setting has a sensible
default, so `./doze-kafka` with no arguments is a complete, working broker. Run
`./doze-kafka --help` to see the flags, grouped, at any time.

## Config file

Drop a `doze-kafka.toml` in your working directory and doze-kafka loads it
automatically; point elsewhere with `--config path/to/file.toml`, or run without any
file with `--config ""`. A key is its flag's name without the dashes in front. The two
that differ are tables: SASL users (`--sasl-user alice:s3cret`) are a `[credentials]`
table, and `--tls-cert` / `--tls-key` are `cert` and `key` under `[tls]`. Put tables
last: in TOML, every key after a `[table]` line belongs to that table.

```toml
name = "harbour"            # answers on kafka.harbour.doze (default: this directory's name)
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
`./doze-kafka --kafka-version 3` for a quick experiment. A mistyped key is rejected
rather than silently ignored.

Two things a file does better than flags: it keeps a **list of SASL users** readable
(and keeps passwords out of your shell history and `ps` output), and it's
**committable** — check `doze-kafka.toml` into your repo and the whole team gets the
same broker.

`doze-kafka config print` writes your *effective* configuration — defaults, then the
file, then any flags — as TOML, so you can see exactly what the broker will run with,
or generate a starter file. It validates first, so what it prints is something the
broker would accept:

```sh
doze-kafka config print > doze-kafka.toml
```

A bad setting is reported once, before anything starts, as a single line, with exit
code 2 (a broker that fails *while running* exits 1). That covers a mistyped flag, a
value out of range, and a TLS certificate that is missing or does not match its key:

```console
$ doze-kafka --listen nonsense
doze-kafka: listen address "nonsense" is not host:port (for example 127.0.0.1:9092 or :9092)
$ doze-kafka --listn :9092
doze-kafka: unknown flag --listn — did you mean --listen?
```

## All flags

Every flag below has the same name as its config-file key, apart from the two tables
above (durations like `--fsync-interval` are written as strings, e.g.
`fsync-interval = "500ms"`). A file written for an earlier version may say `version`
where this one says `kafka-version`; that spelling is still read.

### Network

| Flag | Default | Meaning |
|---|---|---|
| `--name` | *this directory's name* | This instance's name: it answers on `kafka.<name>.doze:9092`, on a loopback address of its own, so two projects can both be on port 9092 and nothing on your network can reach it. |
| `--listen` | *(unset)* | Serve this `host:port` **instead of** the name: no name is claimed, no DNS check runs, nothing is written to the registry. For a case a name cannot serve, such as a sibling container. `:9092` accepts other machines and containers (read the note on `--advertised-host` below); `127.0.0.1:9092` restores the old loopback default; `127.0.0.1:0` is a free port. On Windows, which has no names, the default is `127.0.0.1:9092`. |
| `--advertised-host` | *derived* | Host returned to clients in Metadata: a host name or an IP address, without a port. **This is the address clients actually reconnect to** — see the note below. Unset, it is the instance name (or its address where the name does not resolve on this machine); under `--listen` it follows the address: a specific one is advertised as itself, and a wildcard (`:9092`) as `localhost`. |
| `--advertised-port` | `0` | Port advertised to clients. `0` means the port actually bound, which is right even with `:0`. |

### Storage

| Flag | Default | Meaning |
|---|---|---|
| `--data-dir` | `./data` | Directory holding per-partition log segments. |
| `--meta-db` | `<data-dir>/meta.db` | File holding the topic registry, committed offsets, and producer-id counter. It lives *inside* the data directory by default, so the directory is self-contained: move it or delete it and the topics and offsets go with it. Earlier versions kept it at `./meta.db` in the working directory. That file is still used (with a notice) when the data directory has partition data but no database of its own, however the data directory is spelled; run from a different working directory and it is not found, so move it into the data directory when you can. |
| `--segment-bytes` | `134217728` (128 MiB) | Roll to a new log segment once the active one exceeds this. |
| `--index-interval-bytes` | `4096` | Add one sparse index entry per this many bytes of log. |

### Retention (both default to unbounded)

| Flag | Default | Meaning |
|---|---|---|
| `--retention-bytes` | `0` | Delete oldest segments once a partition exceeds this size. `0` = keep everything. |
| `--retention-ms` | `0` | Delete segments older than this many milliseconds. `0` = keep everything. (The most is about 292 years; for "for ever", use 0.) |

### Durability

| Flag | Default | Meaning |
|---|---|---|
| `--fsync-interval` | `500ms` | How often active segments are flushed to disk, at least `1ms`. `0` disables the timer (the OS still flushes eventually; a clean shutdown always syncs). `acks=all` always fsyncs before it is acknowledged. |

### Topics and groups

| Flag | Default | Meaning |
|---|---|---|
| `--kafka-version` | `4` | Which Kafka major (1–4) to emulate. Sets the API versions advertised and whether the new KIP-848 group protocol is offered. See [API support](api-support.md). (This was `--version`; `--version` now prints doze-kafka's own version. `--version 3` is still understood, with a notice.) |
| `--auto-create-topics` | `true` | Create a topic the first time a client references it. |
| `--default-partitions` | `1` | Partition count for auto-created topics. |
| `--max-partitions` | `1000` | Most partitions one topic may have. Each is a directory and open files, so a typo like `--default-partitions 100000` is refused rather than taking the machine down. |
| `--consumer-session-timeout` | `45s` | KIP-848 group session timeout: evict a member after this long without a heartbeat. (Classic groups use the timeout the client requests.) |

### Authentication (all optional; see [Auth & TLS](auth-and-tls.md))

| Flag | Default | Meaning |
|---|---|---|
| `--sasl-mechanisms` | *(none)* | Comma-separated SASL mechanisms to require: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512`. Enabling any turns SASL on. A flag replaces the file's list. |
| `--sasl-user` | *(none)* | A `user:password` credential. Repeatable. Needs `--sasl-mechanisms`: users without a mechanism are an error, not silently ignored. |
| `--tls-cert` | *(none)* | PEM certificate file. With `--tls-key`, terminates TLS on the listener. |
| `--tls-key` | *(none)* | PEM private key file. |
| `--acl-default` | `allow` | What a resource with no ACL allows: `allow`, or `deny` (Kafka's own default). ACLs you create are enforced either way. See [Authorization](authorization.md). |
| `--acl-super-user` | *(none)* | A principal ACLs never refuse, as `User:admin`. Repeatable. |

### Console (see [Console](console.md))

| Flag | Default | Meaning |
|---|---|---|
| `--console` | `true` | Serve the web console. `--console=false` to turn it off. |
| `--console-addr` | *beside the broker* | `host:port` for the console's HTTP listener. Unset, it is port 9093 on the name's own address (and `http://kafka.<name>.doze`, port-less, through the shared front door); under `--listen`, `127.0.0.1:9093`. A taken port is an error naming this flag. |
| `--console-allow-host` | *(none)* | Extra Host names the console answers to, besides `localhost`, IP addresses and `*.doze`. Comma-separated or repeated. A guard against DNS rebinding. |

### Schema registry (see [Schema Registry](schema-registry.md))

| Flag | Default | Meaning |
|---|---|---|
| `--schema-registry` | `true` | Serve the built-in Confluent-compatible schema registry. `--schema-registry=false` to turn it off. |
| `--schema-registry-addr` | *beside the broker* | `host:port` for the registry's HTTP listener. Unset, it is port 8081 on the name's own address (and `http://schema.<name>.doze`, port-less, through the shared front door); under `--listen`, `127.0.0.1:8081`. A taken port is an error naming this flag. |

### Process

| Flag | Default | Meaning |
|---|---|---|
| `--config` | `./doze-kafka.toml` if present | Path to a TOML config file. `--config ""` uses none, ignoring a `doze-kafka.toml` in the working directory. |
| `--log-level` | `info` | `debug`, `info`, `warn` or `error`. `debug` logs every request (client id, API, version). |
| `--log-format` | `text` | `text` or `json`. |
| `--pprof-addr` | *(none)* | If set, serve `net/http/pprof` there (for soak and debugging). |

### Commands

| Command | Does |
|---|---|
| `doze-kafka version` / `--version` / `-v` | Print doze-kafka's version and the Kafka range it speaks. |
| `doze-kafka env` | Print the shell block that points a client at this instance (`eval "$(doze-kafka env)"`): `KAFKA_BOOTSTRAP_SERVERS` and `KAFKA_BROKERS`. |
| `doze-kafka doctor` | Report what is set up for `.doze` names and what is not, and which process holds which name. Needs no privilege. |
| `doze-kafka config print [flags]` | Print the effective configuration as TOML. |
| `doze-kafka dns-setup` | One-time setup for the shared `.doze` names. See [Endpoints](endpoints.md). |
| `doze-kafka help` | The grouped help. |

A word that is not a flag or a command is an error (`did you mean --listen?`), not
ignored — `doze-kafka listen :9092` used to start the broker on the default port.

## Worked examples

**Persistent broker in a fixed location:**

```sh
./doze-kafka --data-dir /var/tmp/kafka/data     # meta.db is in there too
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
./doze-kafka --kafka-version 3
```

**Throwaway broker on a random port** (handy for scripts and CI):

```sh
./doze-kafka --listen 127.0.0.1:0 --console-addr 127.0.0.1:0 --data-dir "$(mktemp -d)"
# the summary printed at start-up has the bootstrap address and the console URL
```

**Reachable from a container on this machine:**

```sh
./doze-kafka --listen :9092 --advertised-host host.docker.internal
```

**Locked down** with SCRAM + TLS:

```sh
./doze-kafka \
  --sasl-mechanisms SCRAM-SHA-256 --sasl-user alice:s3cret \
  --tls-cert cert.pem --tls-key key.pem
```

## What start-up prints

Once everything is bound — and only then — doze-kafka prints where it is:

```text
  doze-kafka is up.

    kafka     kafka.project.doze:9092
    console   http://kafka.project.doze   topics, messages and consumer groups, live
    data      /home/you/project/data
    serving   Kafka 4 protocol · no auth · plaintext

    eval "$(doze-kafka env)" point this shell at it
    doze-kafka doctor        when something looks wrong
```

The summary goes to **stdout** and the log lines to stderr, so `doze-kafka 2>/dev/null`
shows just the summary. The kafka line is the name, or the address where the name does not
resolve on this machine. With `--listen :9092` it gives the advertised address
(`localhost:9092`) and notes that the broker is listening on every interface, since
`[::]:9092` is not something to paste into a client. Ctrl-C, `SIGTERM` and closing the terminal (`SIGHUP`) all stop the
broker cleanly: it stops accepting, flushes the logs and exits 0. With
`--log-format json` it is a single `ready` record instead, and with
`--log-level warn` or `error` it is not printed. The first run on a machine without `.doze`
set up asks once (root installs without asking; a terminal gets a Y/n prompt; with neither,
it prints the command and exits).

## A note on `--advertised-host`

Kafka clients connect to a bootstrap address, ask the broker for Metadata, and then
**reconnect to whatever host and port the broker advertises**. By default that is
the instance name (or its address where the name does not resolve here), which is right
for a client on the same machine. Under `--listen` it follows the address: `127.0.0.1:<bound-port>`
for a loopback listener, and `localhost:<port>` when listening on every interface. (It is the address and not the name `localhost` for a loopback listener
because that name can resolve to `::1` first, where nothing is listening, and not every
client goes on to try the other address: kcat stops at the first refusal.)

If a client connects from elsewhere — another container, another host, a VM — it must
be able to reach the *advertised* address, not just the bootstrap one. Listen beyond
loopback (`--listen :9092`), then set `--advertised-host` (and `--advertised-port` if
it's remapped) to an address that client can actually reach. This is the single most
common reason a client connects but then hangs or fails on the next request. See
[Troubleshooting](troubleshooting.md).
