# Troubleshooting

Most problems connecting to doze-kafka are the same ones you'd hit with any Kafka
broker. Here are the common ones and their fixes.

## The name does not resolve

`kafka.<project>.doze` is only an address once `.doze` is set up on the machine. If a
client says the host is unknown, or connects to nothing, run:

```sh
doze-kafka doctor            # what is set up and what is not, no sudo
doze-kafka dns-setup         # one sudo, once per machine
doze-kafka dns-setup --print # the script, to read or run yourself
```

`doctor` also lists which process holds which name: "my name stopped working" is usually
another instance with the same name (start this one with `--name <other>`). When the name
does not resolve on the machine the broker is on, it advertises its address instead, so
clients still reach it. With no setup and no terminal (its input or its error output is
something else: a pipe, a file, `/dev/null`), the broker prints the command and
exits 1; it never runs `sudo` and never waits on a password. At a terminal it asks, and
input that ends without an answer (Ctrl-D) changes nothing. `--listen host:port` serves an address and needs none
of this.

## The client connects, then hangs or fails on the next request

Almost always an **advertised address** problem. A client bootstraps to the address
you gave it, asks for Metadata, then reconnects to the host/port the broker
*advertises*. By default that's the instance name (or its address where the name does not resolve),
and under `--listen` the listen address itself (`localhost` for a wildcard) — perfect for a
client on the same machine, wrong for one somewhere else.

If your client runs in another container, VM, or host, set `--advertised-host` (and
`--advertised-port` if it's remapped) to an address that client can actually reach:

```sh
./doze-kafka --listen :9092 --advertised-host 192.168.1.50
# or, from inside Docker talking to a broker on the host:
./doze-kafka --listen :9092 --advertised-host host.docker.internal
```

Check what's being advertised with `kcat -b <bootstrap> -L` — the broker list it
prints is what clients will reconnect to.

## "Broker is too old" / an API isn't supported

The client wants a feature at a version doze-kafka doesn't advertise for the emulated
Kafka major. Run with the default `--kafka-version 4` (the most capable) if you've lowered
it. If it still happens with a mainstream client doing an ordinary operation, that's
worth reporting — see [API support](api-support.md) for what's advertised.

## Authentication fails / connection dropped immediately

If you started the broker with `--sasl-mechanisms` or TLS, **every** client must be
configured to match:

- SASL on, client sends nothing → the broker drops the connection. Configure the
  client's `security.protocol` (`SASL_PLAINTEXT` or `SASL_SSL`), `sasl.mechanism`, and
  credentials.
- Mechanism mismatch → make sure the client's mechanism is one you enabled.
- TLS on, client connects in the clear → it can't complete the handshake. Set the
  client to use `SSL`/`SASL_SSL` and trust your certificate (or skip verification).

See [Auth & TLS](auth-and-tls.md) for matching client snippets. Usernames and
passwords must be ASCII (no `SASLprep`).

## A topic doesn't exist / "unknown topic"

Auto-create is on by default, so producing to a new topic creates it. If you ran with
`--auto-create-topics=false`, create it explicitly first:

```sh
kafka-topics.sh --bootstrap-server "$KAFKA_BOOTSTRAP_SERVERS" --create --topic demo --partitions 1
```

## My data disappeared after a restart

doze-kafka only persists what's under `--data-dir` (the logs, and `meta.db` inside
it). If you didn't set it, it defaults to `./data` **relative to the current working
directory** — start the broker from a different directory and it looks like a fresh
broker. The start-up summary prints the absolute `data` path it is using. Pin an
absolute path to avoid this:

```sh
./doze-kafka --data-dir /var/tmp/kafka/data
```

Also make sure you stopped it cleanly (Ctrl-C / `SIGTERM`); a hard `kill -9` can lose
the last sub-second of unsynced writes (see
[Storage & durability](storage-and-durability.md)).

## A Kafka UI shows errors but produce/consume works

Some UIs probe admin APIs doze-kafka doesn't implement (e.g. `DescribeConfigs` and
`AlterConfigs` are supported, but `DescribeLogDirs` isn't). The
broker closes that individual call cleanly; the UI may show a warning but the core
topic/group/message views keep working. See [Limitations](limitations.md).

## Consumer group isn't rebalancing when a member dies

A member that crashes (rather than leaving cleanly) is removed only after its
**session timeout** elapses. For KIP-848 groups that's `--consumer-session-timeout`
(default 45s); for classic groups it's the timeout the client requested. Until then,
its partitions stay assigned. This matches Kafka's behavior.

## Still stuck?

Turn up the broker's logs — `--log-level debug` logs every request, including the API,
version and client id, which usually makes it obvious what the client tried and where it
stopped. If a mainstream client can't do an ordinary operation, please file an issue.
