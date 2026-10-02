# Endpoints, ports and the `.doze` zone

Everything doze-kafka opens on your machine, and what each is for. The start-up
summary prints the first two for the run you are in.

## What it listens on

| What | Default | Flag | Notes |
|---|---|---|---|
| Kafka wire protocol | `127.0.0.1:9092` | `--listen` | Loopback only. `--listen :9092` accepts other machines and containers. |
| Web console and `/healthz` | `127.0.0.1:9093` | `--console-addr`, `--console=false` | HTTP. Kafka's protocol cannot share a port with HTTP, so this is separate. |
| Profiling (off) | — | `--pprof-addr` | `net/http/pprof`, only if you set it. |

Both defaults are loopback: a development broker has no auth by default, and nothing
on the network you happen to be plugged into should be able to reach it. Listening
beyond this machine is a choice. When you make it, set `--advertised-host` to an
address your clients can reach (see [Configuration](configuration.md)), and turn on
[SASL and TLS](auth-and-tls.md) if the network is not yours alone.

`GET /healthz` on the console port answers `200 ok` while the process is up. Use it
as a compose or CI readiness check:

```sh
until curl -fs http://127.0.0.1:9093/healthz >/dev/null; do sleep 0.2; done
```

## Files

| Path | What |
|---|---|
| `--data-dir` (default `./data`) | Partition logs, and `meta.db` (topics, committed offsets, producer ids) inside it. |
| `$DOZE_HOME` (default `~/.doze`) | The shared registry the `.doze` zone uses. Written only if the zone is on (below). |

## The `.doze` zone

doze-kafka also joins the **shared `.doze` name zone**, the one every doze tool
uses (the doze CLI, doze-aws and doze-kafka). With it, the broker is reachable as
`kafka.doze:9092` and the console as `http://kafka.doze`, the same way doze-aws is
`http://aws.doze`.

Names are **additive**. The address you configured always works and never moves;
the zone only adds a prettier one, and every failure in it is a log line rather than
a refusal to start. The start-up summary lists a by-name address only if it answers.

What joining the zone does, concretely:

- **Claims `kafka.doze`**, which resolves to `127.0.0.3`. If another process already
  holds the name, the log says which, and yours keeps using its configured address.
- **Listens on `127.0.0.3:<your port>`** as well as your listen address when that is
  loopback, so `kafka.doze:9092` and `127.0.0.1:9092` reach the same broker.
- **Serves the zone's DNS** on port 5323 (`127.0.0.1` on macOS, `127.0.0.54` on
  Linux) if no other doze process is already doing so, answering for every doze tool's names, not only its own.
- **Binds `:80`** (the shared front door) if free, to give console names their
  port-less URLs. It only forwards requests whose `Host` is a registered `.doze`
  name.
- **Writes a registry entry** under `$DOZE_HOME`, removed on exit.

The names resolve only after the one-time machine setup:

```sh
doze-kafka dns-setup --check     # what is set up, no sudo
doze-kafka dns-setup --print     # the privileged script, to read first
doze-kafka dns-setup             # runs it (asks for sudo)
doze-kafka dns-setup --uninstall # takes it all back out
```

On macOS this adds loopback aliases and a `/etc/resolver/doze` entry; on Linux it
edits `/etc/hosts` and one sysctl. Without it everything still works through the
configured addresses.

**Opting out.** `--zone=false` (or `zone = false` in the file) skips all of the
above: no registry entry, no `:80`, no DNS, no second listener.

**Windows.** The zone is not available there; the broker, the console and every flag
work as everywhere else, minus the names. `dns-setup` says so and exits non-zero.

## Containers

A container reaching the broker on your machine needs the broker listening beyond
loopback and an advertised host the container can resolve:

```sh
doze-kafka --listen :9092 --advertised-host host.docker.internal
```

(`host.docker.internal` is Docker Desktop's name for the host; on Linux add
`--add-host=host.docker.internal:host-gateway` to the container.)
