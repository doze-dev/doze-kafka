# Endpoints, ports and the `.doze` name

Everything doze-kafka opens on your machine, and what each is for. The start-up
summary prints the first two for the run you are in.

## The name is the instance

With no flags, an instance is **`kafka.<name>.doze`**, where the name is the project
directory (`--name` changes it). Each instance gets a loopback address of its own and
binds the same port on it, so `kafka.harbour.doze:9092` and `kafka.atlas.doze:9092` are
two brokers that never contend. This is the arrangement doze-aws has (`aws.<name>.doze`),
on purpose: one experience across the doze tools.

| What | Default | Flag | Notes |
|---|---|---|---|
| Kafka wire protocol | `kafka.<name>.doze:9092` | `--name`, `--listen` | On the name's own loopback address: nothing on your network can reach it. |
| Web console and `/healthz` | `http://kafka.<name>.doze` (port-less, via the front door); the listener is port 9093 on the name's address | `--console-addr`, `--console=false` | HTTP. Kafka's protocol cannot share a port with HTTP, so this is separate. |
| Schema registry | `http://schema.<name>.doze` (port-less, via the front door); the listener is port 8081 on the name's address | `--schema-registry-addr`, `--schema-registry=false`, `--schema-registry-tls` | HTTP, Confluent's REST API; HTTPS at `https://kafka.<name>.doze:8081` when the broker has a TLS certificate. See [Schema Registry](schema-registry.md). |
| Profiling (off) | — | `--pprof-addr` | `net/http/pprof`, only if you set it. Without `/debug/pprof/cmdline`: the command line is where `--sasl-user` is. |

`kafka.doze`, the machine-wide apex, is deliberately **not** claimed: an address under it
would mean whichever instance started first. If you want a short name, name the instance
something short (`--name`).

`GET /healthz` on the console answers `200 ok` while the process is up. Use it as a
compose or CI readiness check:

```sh
until curl -fs http://kafka.myproject.doze/healthz >/dev/null; do sleep 0.2; done
```

## `--listen` replaces the name

`--listen host:port` serves that address and **nothing else**: no name is claimed, no DNS
check runs, nothing is written to the registry, and the console defaults to
`127.0.0.1:9093`. An instance answering on both would hand clients two different
addresses depending on how they bootstrapped. It is for the case a name cannot serve, such
as a sibling container reaching this one. `--listen 127.0.0.1:9092` restores the previous
loopback default exactly.

Listening beyond this machine is a choice. When you make it, set `--advertised-host` to an
address your clients can reach (see [Configuration](configuration.md)), and turn on
[SASL and TLS](auth-and-tls.md) if the network is not yours alone.

## Files

| Path | What |
|---|---|
| `--data-dir` (default `./data`) | Partition logs, and `meta.db` (topics, committed offsets, producer ids) inside it. |
| `$DOZE_HOME` (default `~/.doze`) | The shared registry `.doze` names use. Written only when the instance is its name (not under `--listen`). |

## How the name works

doze-kafka joins the **shared `.doze` zone**, the one every doze tool uses. Concretely:

- **Claims `kafka.<name>.doze`**, which resolves to a loopback address hashed from the
  name. If another live process already holds it, startup says which (pid and owner) and
  suggests `--name <other>` or `--listen`.
- **Serves the zone's DNS** (port 5323: `127.0.0.1` on macOS; `192.0.2.53` on a `doze0`
  interface on Linux) if no other doze process is, answering for every doze tool's names.
- **Binds `:80`** (the shared front door) if free, which routes the console by `Host`
  header so its URL is port-less. It only forwards registered `.doze` names.
- **Writes a registry entry** under `$DOZE_HOME`, removed on exit.

The name only resolves after the one-time machine setup, and doze-kafka handles the first
run itself: as root (a container) it installs; at a terminal it asks `Set it up now?
[Y/n]` (one `sudo`, per machine, not per project); with neither (CI, a background job) it
prints the command and exits rather than waiting on a password.

```sh
doze-kafka doctor                # what is set up and who holds which name, no sudo
doze-kafka dns-setup --check     # the same for the machine setup alone
doze-kafka dns-setup --print     # the privileged script, to read first
doze-kafka dns-setup             # runs it (asks for sudo)
doze-kafka dns-setup --uninstall # takes it all back out
```

On macOS this adds loopback aliases and a `/etc/resolver/doze` entry. On Linux it installs
a small systemd unit that creates the `doze0` interface and points the machine's resolver at
the zone (systemd-resolved, or a dnsmasq drop-in), and a sysctl that lets an unprivileged
process bind port 80. `--print` shows exactly what will run on your machine.

**What is advertised.** The name is advertised only if it resolves *here* to this
instance's address; otherwise the address itself is, so a container inheriting its host's
DNS (which can answer for a name with an address that means something else inside) still
connects. `eval "$(doze-kafka env)"` makes the same choice.

**Windows.** There are no names there (doze-names does not build): the default listen
address is `127.0.0.1:9092`, the console is on `127.0.0.1:9093`, and `dns-setup` says so
and exits non-zero.

## Containers

A container reaching the broker on your machine cannot resolve your instance name, so use
an address: the broker listening beyond loopback and an advertised host the container can
resolve:

```sh
doze-kafka --listen :9092 --advertised-host host.docker.internal
```

(`host.docker.internal` is Docker Desktop's name for the host; on Linux add
`--add-host=host.docker.internal:host-gateway` to the container.) A container *running*
doze-kafka as root sets `.doze` up for itself on first start.
