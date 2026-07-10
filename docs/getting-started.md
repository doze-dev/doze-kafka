# Getting started

This walks you from nothing to a running broker with a message round-tripped
through it, and confirms your data survives a restart. It takes a couple of minutes.

## Prerequisites

- **Go 1.26+** to build the binary. That's the only build dependency.
- A Kafka client to talk to it. Any will do; the examples use
  [`kcat`](https://github.com/edenhill/kcat) because it's a single command, but see
  [Clients](clients.md) for franz-go, Sarama, kafkajs, the Java client, and more.

## Build

doze-kafka is a normal Go program with one wrinkle: the repository lives inside a Go
workspace, so builds and tests need `GOWORK=off` to treat it as a standalone module.

```sh
git clone https://github.com/doze-dev/doze-kafka
cd doze-kafka
GOWORK=off go build -o doze-kafka ./cmd/doze-kafka
```

You now have a single self-contained binary named `doze-kafka`.

## Run

With no flags at all, it listens on `:9092` and stores everything under the current
directory (`./data` for the logs, `./meta.db` for the topic/offset registry):

```sh
./doze-kafka
```

You'll see it come up:

```
level=INFO msg=listening addr=[::]:9092
```

That's it — there's a Kafka broker on `localhost:9092`. Leave it running in this
terminal; open another for the client.

## Produce and consume

With **kcat**:

```sh
# produce three messages (type lines, then Ctrl-D)
printf 'one\ntwo\nthree\n' | kcat -b localhost:9092 -t demo -P

# consume them back from the beginning, then exit
kcat -b localhost:9092 -t demo -C -o beginning -e
```

```
one
two
three
```

The topic `demo` was created automatically on first use (auto-create is on by
default; turn it off with `--auto-create-topics=false`).

Prefer the official tools? They work the same way — point `--bootstrap-server` at
`localhost:9092`:

```sh
kafka-console-producer.sh --bootstrap-server localhost:9092 --topic demo
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic demo --from-beginning
```

## Confirm it's durable

This is the part a container-in-memory setup can't give you. Stop the broker with
**Ctrl-C** — it logs a clean shutdown:

```
level=INFO msg="shutting down: flushing logs to disk"
```

Start it again with the same command, and read the topic once more:

```sh
./doze-kafka &
kcat -b localhost:9092 -t demo -C -o beginning -e
```

```
one
two
three
```

Your messages — and any committed consumer-group offsets — are still there, because
they were written to `./data` and `./meta.db`, not held in memory.

## Where to next

- Change ports, storage location, retention, or the emulated Kafka version →
  [Configuration](configuration.md).
- Wire it into your app's Kafka client → [Clients](clients.md).
- Use consumer groups → [Consumer groups](consumer-groups.md).
- Turn on authentication → [Auth & TLS](auth-and-tls.md).
- Something not connecting → [Troubleshooting](troubleshooting.md).
