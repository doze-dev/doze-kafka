---
title: "Getting started"
description: "Install doze-kafka, run a broker, round-trip a message through it and confirm it survives a restart."
---

This walks you from nothing to a running broker with a message round-tripped
through it, and confirms your data survives a restart. It takes a couple of minutes.

You need a Kafka client to talk to it. Any will do; the examples use
[`kcat`](https://github.com/edenhill/kcat) because it's a single command, but see
[Clients](clients.md) for franz-go, Sarama, kafkajs, the Java client, and more.

## Install

```sh
curl -fsSL https://raw.githubusercontent.com/doze-dev/doze-kafka/main/install.sh | sh
exec $SHELL
```

The installer picks the prebuilt binary for your OS and architecture (Linux and
macOS, amd64 and arm64), checks its SHA-256 against the release's checksums, and puts
it in `~/.doze-kafka/bin`. It adds that directory to your PATH in `.zshrc` (zsh) or
`.bashrc` or `.bash_profile` (bash); `exec $SHELL` starts a shell that has it. With another shell it
prints the `export` line for you to add. `--uninstall` takes it away again and
leaves your data alone; the [README](../README.md#install) lists every option. To
build it yourself instead, see [Build from source](#build-from-source).

## Run

With no flags at all, it is `kafka.<project>.doze:9092`, named for the current
directory and reachable from this machine only, and stores
everything under the current directory in `./data`: the partition logs, and
`meta.db`, the topic and offset registry:

```sh
doze-kafka
```

You'll see it come up, with everything you need to connect (the `time=` and `level=`
lines are the log, on stderr; the block between them is the summary, on stdout):

<!-- output: banner -->
```text
time=2026-10-16T16:12:14.687+01:00 level=INFO msg=doze-kafka version=dev kafka_compat=4

  doze-kafka is up.

    kafka     kafka.project.doze:9092
    console   http://kafka.project.doze   topics, messages and consumer groups, live
    registry  http://schema.project.doze   Confluent-compatible schema registry
    data      /home/you/project/data
    serving   Kafka 4 protocol · no auth · plaintext

    eval "$(doze-kafka env)" point this shell at it
    doze-kafka doctor        when something looks wrong

time=2026-10-16T16:12:14.721+01:00 level=INFO msg=listening addr=127.0.0.2:9092
```

The first time, if `.doze` names are not set up on the machine, it asks once
(`Set it up now? [Y/n]`, one `sudo`, per machine rather than per project). It asks
only when both its input and its error output are a terminal, and only Enter, `y` or
`yes` runs `sudo`. Started any other way (a service manager, CI, `nohup`,
`</dev/null`, a pipe) it prints the command to run and exits 1 rather than waiting
on a password; it never runs `sudo` there. `doze-kafka --listen 127.0.0.1:9092` skips names entirely.

That's it — there's a Kafka broker at that name, and a web console at the URL on the
next line ([Console](console.mdx)). Run the `eval "$(doze-kafka env ...)"` line the
summary ends with, in the client terminal, so the commands below find it. By name it is
`eval "$(doze-kafka env)"`, run in the project directory; under `--listen` the summary
spells out the flags that give this broker's addresses (`env` reads only its own flags
and `doze-kafka.toml`, not a running broker), for example
`eval "$(doze-kafka env --listen 127.0.0.1:9092 --console-addr 127.0.0.1:9093 --schema-registry-addr 127.0.0.1:8081 --config '')"`. Leave it running in this terminal;
open another for the client.

## Produce and consume

With **kcat**:

```sh
# produce three messages (type lines, then Ctrl-D)
printf 'one\ntwo\nthree\n' | kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -P

# consume them back from the beginning, then exit
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -C -o beginning -e
```

<!-- output: consume -->
```
one
two
three
```

The topic `demo` was created automatically on first use (auto-create is on by
default; turn it off with `--auto-create-topics=false`).

Prefer the official tools? They work the same way — point `--bootstrap-server` at
`$KAFKA_BOOTSTRAP_SERVERS`:

```sh
kafka-console-producer.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic demo
kafka-console-consumer.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --topic demo --from-beginning
```

## Confirm it's durable

This is the part a container-in-memory setup can't give you. Stop the broker with
**Ctrl-C** — it logs a clean shutdown:

<!-- output: shutdown -->
```
level=INFO msg="shutting down: flushing logs to disk"
```

Start it again with the same command, and read the topic once more:

```sh
doze-kafka &
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -t demo -C -o beginning -e
```

<!-- output: consume -->
```
one
two
three
```

Your messages — and any committed consumer-group offsets — are still there, because
they were written to `./data` (the logs and `meta.db`), not held in memory.

## Build from source

doze-kafka is a normal Go program. **Go 1.27+** is the only build dependency:

```sh
git clone https://github.com/doze-dev/doze-kafka
cd doze-kafka
go build -o doze-kafka ./cmd/doze-kafka
```

You now have the same self-contained binary in the current directory. Run it as
`./doze-kafka` where this page says `doze-kafka`, or move it onto your PATH.

## Where to next

- Look at your topics, messages and consumer groups in a browser →
  [Console](console.mdx).
- Change ports, storage location, retention, or the emulated Kafka version →
  [Configuration](configuration.md). Reach the broker from a container or another
  machine → [Endpoints](endpoints.md).
- Start a broker inside a Go test → [Embedding](embedding.md).
- Wire it into your app's Kafka client → [Clients](clients.md).
- Use consumer groups → [Consumer groups](consumer-groups.md).
- Turn on authentication → [Auth & TLS](auth-and-tls.md).
- Something not connecting → [Troubleshooting](troubleshooting.md).
