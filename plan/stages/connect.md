# Stage: Connect an app (CON)

The console page that shows copy-ready client settings, filled in from how this
broker is running. Boards `Cn-01` to `Cn-07`. Its only value is that the text is
true: a snippet that does not work is worse than no snippet. So this stage is
less about a page than about one rule: **a snippet the console shows is produced
by the code the compatibility matrix runs.** Read [01-protocol.md](../01-protocol.md)
for the evidence standard and the shape of this document; foundation items
(`A1`, `A2`, `A3`, `B3` ...) are in [00-foundation.md](../00-foundation.md).

## 1. Summary

**Surface.** One new section, "Connect an app", at the foot of the rail
(foundation A1). Seven boards: Bootstrap and settings (`Cn-01`), Java (`Cn-02`),
Python (`Cn-03`), kcat (`Cn-04`), Spring Boot (`Cn-05`), Docker (`Cn-06`), SASL and
TLS (`Cn-07`). No such page exists today: `console/console.go` has no `connect`
route, and the only trace of the idea is the endpoint chip in
`console/templates/layout.html:55`.

**Size.** The page itself is small (7 templates, one handler file). The work is
under it: a `Facts` value built from the running configuration, a snippet
package that both the console and `compat/` consume, and new matrix cells so
that every variant the page can show has been run. About 13 days, size L.

**What this stage owns.**

| Path | Owned content |
|---|---|
| `compat/` | every client's `run.sh`, `compat/run.sh`, `compat/lib.sh`, the new `compat/snippets/` package, the new cells |
| `docs/clients.md`, `docs/frameworks.md`, `docs/getting-started.md` | the prose and snippets; they become generated from the same templates (see 9) |
| `docs/configuration.md` (the network, security and registry rows) | the settings the page reads |
| `cmd/doze-kafka/env.go`, `cmd/doze-kafka/listen.go` (`instanceAddress`, `registryURL`) | the facts the page needs from the running instance |
| `console/` | the `connect` handler and templates (new files `handlers_connect.go`, `templates/connect.html`) |

**Peers.** [Access](access.md) (users, ACL default; Cn-07 links to it),
[Cluster](cluster.md) (listeners, TLS; the same facts), [Traffic](traffic.md)
(the "wire, underneath" strip on every board is a filtered traffic view),
[Schemas](schemas.md) (registry URL and its basic-auth rule),
[Groups](groups.md) (Cn-02 "Group g-order-validator is on it today").

**The one design decision.** Today every client snippet exists three times: in
the docs (`docs/clients.md`, `docs/frameworks.md`, `docs/auth-and-tls.md`), in
the boards, and in the matrix's scripts. Only the third is executed, and the
three already differ (section 4, `CON-3` to `CON-12`). The proposal (section 6, "One source for snippets"):
the snippet is a template file under `compat/snippets/`; the console renders it
with `Facts`; the matrix renders the same file with real credentials and runs the
result. A page and a cell that disagree become one failing test instead of a
silent drift.

## 2. Contract

The page makes no Kafka request of its own beyond what the console backend
already does. It owns no API; it owns the facts it presents.

### Kafka APIs a snippet exercises

All are already advertised and tiered in `docs/api-support.md`; this stage adds no
tier, it adds proof that the connection settings reach them.

| Key | API | Tier now | Target | Note |
|---|---|---|---|---|
| 18 | ApiVersions | F | F | first request of every client in the strip |
| 3 | Metadata | F | F | the advertised address is read from its response; Cn-06 is about this |
| 17 | SaslHandshake | F | F | only advertised with auth on |
| 36 | SaslAuthenticate | F | F | PLAIN, SCRAM-SHA-256, SCRAM-SHA-512; matrix runs 512 only (CON-3) |
| 22 | InitProducerId | F | F | `transactional.id` rows on Cn-02, Cn-05 |
| 37 | CreatePartitions | F | F | Cn-05 `INVALID_PARTITIONS` text (CON-14) |
| 50/51 | Describe/AlterUserScramCredentials | not advertised | S (stays) | users cannot be created from the console; see CON-17 |

### Console routes (new)

| Route | Method | Mutates | Tier | Note |
|---|---|---|---|---|
| `/connect` | GET | no | F | board Cn-01, the default tab |
| `/connect/{client}` | GET | no | F | `java`, `python`, `kcat`, `spring`, `docker`, `sasl-tls` (and `go`, `node` if the owner accepts the extra tabs, section 6) |
| `/connect/{client}/snippet/{id}` | GET | no | F | `text/plain` raw block, for "Copy" and for curl; same renderer |
| `/api/connect` | GET | no | F | the `Facts` as JSON, passwords never included; consumed by the page, `doze-kafka env --json`-style tools and the matrix's drift test |

Declined: creating or changing a SASL user from the page (restart-to-apply is the
truth; `CreateUserScramCredentials` is not advertised), and showing any password.

### The facts the page needs (`Facts`)

| Field | Source today | Gap |
|---|---|---|
| `Bootstrap` | `instanceAddress(cfg)` (`cmd/doze-kafka/listen.go`) | the console is handed `Options.DisplayAddr`, not the full set below |
| `Listen`, `AdvertisedHost`, `AdvertisedPort` | `config.Config`, `ResolveAdvertisedHost` (`internal/config/config.go:239`) | no "is this reachable from a container" verdict |
| `SASLMechanisms` | `cfg.SASLMechanisms` | `env` prints only `[0]` (`env.go:43`); the page needs the list |
| `Users` (names only) | `cfg.Credentials` keys, plus the reserved `__console` | no mechanism or "used by" |
| `TLS` (on/off, cert path, self-signed?, SANs) | `cfg.TLSCertFile` | the CA path to give clients, and whether the SANs name the advertised host, are not computed anywhere |
| `RegistryURL`, `RegistryAuth` | `registryURL(cfg)` | the page must say the registry takes the broker's SASL credentials as HTTP basic (`schemaregistry/api.go:42`) |
| `ConsoleURL` | `instanceAddress` second result | board shows `http://kafka.doze`, which is not claimed (CON-1) |
| `KafkaVersion` | `cfg.Version` | decides whether the KIP-848 lines show |
| `Platform` | none | `darwin`, `linux`, `windows`, `wsl` (CON-19) |
| `ClientTelemetry` | `cfg.ClientTelemetry` | Cn-03 telemetry line |

`Facts` is built once in `cmd/doze-kafka` (which already knows everything above),
passed to the console as `Options.Connect func() Facts`, and also used by
`doze-kafka env`, so `env` and the page cannot disagree (the same trick `env.go`'s
header describes for the banner).

## 3. Where it stands

### Tests that touch the surface

| Layer | What exists | Covers |
|---|---|---|
| `cmd/doze-kafka/env_test.go` | `TestEnvPointsAtTheInstance`, `TestEnvUnderListenUsesTheAddress`, `TestEnvHonoursTheAdvertisedHost` | `env` output, no SASL, no TLS case |
| `internal/config/advertised_test.go` | advertised-host derivation | loopback, wildcard, specific address |
| `cmd/doze-kafka/names_test.go`, `listen` tests | name vs address, `resolvesHere` | bootstrap in name mode |
| Go clients | `internal/broker/sarama_test.go` (3 tests: round trip, group commit/resume, transactional producer); franz-go throughout `internal/broker` | **plain only**: no Sarama or franz-go test uses SASL or TLS (grep for `sasl`/`tls` in `sarama_test.go`: none) |
| `compat/` | 10 clients x (plain, SCRAM-SHA-512 over TLS) = 20 cells; `.github/workflows/compat.yml` runs on push, PR and weekly | see the table below |
| `e2e/tests/` | 11 specs | none for a Connect page |

### What the matrix actually runs, per client

This is the evidence every board snippet is compared to (section 6 does the comparison).

| Cell | Toolchain pin | Security keys it uses (scram-tls) | Notes |
|---|---|---|---|
| `java` | `kafka-clients` 4.0.0 (`pom.xml`; `KAFKA_VERSION` env overrides, no cell sets it); Confluent serializers 8.0.0; JDK 21 image | `security.protocol=SASL_SSL`, `sasl.mechanism`, `sasl.jaas.config` (one line), `ssl.truststore.type=PEM`, `ssl.truststore.location` (`Check.java:26-33`) | registry: `basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info` (`SerdeCheck.java:45-46`) |
| `spring` | Boot 3.4.4 (`pom.xml`) | `spring.kafka.security.protocol`, `spring.kafka.properties.sasl.mechanism`, `...sasl.jaas.config`, `spring.kafka.ssl.trust-store-type=PEM`, `...trust-store-location=file:` (`Check.java:57-65`) | passed as command-line args, not a yaml file, not `${ENV}` placeholders |
| `python` | `confluent-kafka` **unpinned** (`uv run --with confluent-kafka`) | `security.protocol=SASL_SSL`, `sasl.mechanism` (singular), `sasl.username`, `sasl.password`, `ssl.ca.location` (`check.py:15-18`) | registry `basic.auth.user.info`; Protobuf from a dynamically built descriptor (`descriptor_pb2`), not generated classes |
| `kcat` | whatever is on `PATH` | `-X security.protocol=SASL_SSL -X sasl.mechanism ... -X ssl.ca.location` (`run.sh:8`) | never `-F`, never `sasl.mechanisms` |
| `kafkajs` | 2.2.4 (`package.json`) | `ssl: {ca}`, `sasl: {mechanism: 'scram-sha-512'}` | no board |
| `connect` | Kafka 4.0.0 image | `security.protocol`, `sasl.*`, `ssl.*` repeated with `producer.`, `consumer.`, `admin.` prefixes (`connect/run.sh:11-17`) | no board; `docs/frameworks.md` shows none of it |
| `confluent-sr`, `apicurio`, `kafbat-ui`, `redpanda-console` | pinned images | each has its own SASL/TLS spelling | no board |

Harness shape (`compat/run.sh`): two modes only, `plain` (no auth, no TLS) and
`scram-tls` (SCRAM-SHA-512 + TLS with a self-signed RSA cert for `127.0.0.1`,
`localhost`, `host.docker.internal`). Non-Docker cells talk to a broker on
`127.0.0.1`; Docker cells to a broker on `0.0.0.0` advertising
`host.docker.internal` with `--add-host host.docker.internal:host-gateway`. The
default named mode (`kafka.<name>.doze:9092`) is never run.

### Coverage today (full suite)

| Package | Now | Relevant uncovered |
|---|---|---|
| `cmd/doze-kafka` | 79.8% | `env.go` is covered for no-auth only; `instanceAddress` registry branches |
| `internal/config` | 61.6% | `ResolveAdvertisedHost`, `TLSConfig`, `WriteTOML` credential branch; the package is the weakest of the eleven |
| `console` | 87.8% | no connect handler yet |

## 4. Findings

Evidence: `run` (reproduced or held by a running test) or `read`. Source:
`spec`/`recorded`/`doc`/`memory`. IDs are stable.

| ID | Sev | Finding | Where | Ev | Src | Test to write first |
|---|---|---|---|---|---|---|
| CON-1 | P2 | Boards show the broker at `127.0.0.1:9092` and the console at `http://kafka.doze`. The default is `kafka.<name>.doze:9092`, and `docs/endpoints.md` says the apex `kafka.doze` "is deliberately **not** claimed". The console URL on Cn-01 would not resolve. | Cn-01; `docs/endpoints.md` "The name is the instance" | read | doc | console test: render with a name-mode `Facts`; assert the console URL equals `instanceAddress`'s second result and never `kafka.doze` |
| CON-2 | P3 | `layout.html` hard-codes `127.0.0.1:9092` when `.Endpoint` is empty. | `console/templates/layout.html:55` | read | memory | template test: render with empty endpoint, assert the literal is gone (A8) |
| CON-3 | P1 | Six of seven boards show `SASL_PLAINTEXT` + SCRAM-SHA-512 as the primary example. **No real client has ever run that combination.** The matrix runs only `plain` and `SASL_SSL`. SCRAM-SHA-256, PLAIN, and TLS without SASL (`SSL`) are likewise never run by a real client. A normal configuration (auth on, TLS off) is the common local case. | `compat/run.sh:97-103` | read | spec | compat cells: new security modes `sasl-plain` (SASL_PLAINTEXT SCRAM-512), `sasl256-tls`, `plain-mech-tls`, `tls-only` for kcat, python, kafkajs, java (clients section); expected to pass, written first |
| CON-4 | P1 | Cn-07 "client" block: `ssl.truststore.location=/path/to/ca.jks # Java` and `ssl.ca.location=./certs/ca.pem # librdkafka, kcat`. In a Java `.properties` file and in a librdkafka config file a `#` after a value is part of the value, so the pasted setting points at `"/path/to/ca.jks # Java"`. The Java line also tells users to make a JKS, while the matrix runs `ssl.truststore.type=PEM` with the PEM file the broker was given. | Cn-07 | read | doc | unit: render every snippet; parse it with a Java-properties and a librdkafka-conf parser (Go test, two small parsers); assert no value contains ` #`. Compat: the java cell consumes the rendered file |
| CON-5 | P1 | With SASL on, the registry requires the broker's credentials as HTTP basic auth (`schemaregistry/api.go:42`; `cmd/doze-kafka/main.go:485` "reaches the registry as the same user"; the matrix sets `REGISTRY_USER`/`REGISTRY_PASS`). No board says so: `schema.registry.url` alone (Cn-01), `SchemaRegistryClient({"url": ...})` (Cn-03), and `SCHEMA_REGISTRY_URL` in the compose file (Cn-06) all get 401 on a SASL broker. | Cn-01, Cn-03, Cn-06 | read | doc | compat: the snippet for the registry in SASL mode (`basic.auth.credentials.source=USER_INFO`, `basic.auth.user.info=user:<password>`; python `basic.auth.user.info`) rendered and consumed by `java` and `python` serdes sections; unit: no-auth registry snippet has no auth lines |
| CON-6 | P2 | Python key spelling: board `"sasl.mechanisms"`, matrix `"sasl.mechanism"` (`check.py:15`). kcat file: board `sasl.mechanisms`, matrix `-X sasl.mechanism`. librdkafka documents both, but only the matrix spelling has run. | Cn-03, Cn-04, Cn-01 | read | memory | follows from the single-source rule: the board spelling becomes whatever the cell used. Queue: confirm both spellings in the librdkafka docs (section 8) |
| CON-7 | P2 | Spring: board puts the protocol under `spring.kafka.properties.security.protocol`; matrix uses `spring.kafka.security.protocol` and a `spring.kafka.ssl.trust-store-type: PEM` / `trust-store-location` pair that no board shows. The board uses a YAML `${KAFKA_PASSWORD}` placeholder in a folded scalar that the matrix never loads (it passes literal args). | Cn-05 | read | doc | compat: spring cell reads a rendered `application.yaml` (template) with `KAFKA_PASSWORD` in the environment, for plain and SASL_SSL and SASL_PLAINTEXT |
| CON-8 | P1 | Board copy-all (Cn-01) and each client's "Copy" button copy `<password>` literally. A pasted `sasl.jaas.config=... password="<password>";` parses and fails at SCRAM with `SASL_AUTHENTICATION_FAILED`, which is correct, but the console then shows an error row on its own wire strip for a user who copied exactly what the page said. | Cn-01 to Cn-05 | read | memory | e2e: Copy on a SASL page copies text that still contains the placeholder, and the placeholder is the same token (`<password>`) on every board; a unit test fails if any template emits a real secret (property test over `Facts` with a canary password in config) |
| CON-9 | P2 | `doze-kafka config` prints the effective configuration including `[credentials]` in clear (`internal/config/file.go:249-251`, called from `cmd/doze-kafka/main.go:174`). The page's "Copy all" or a future "Download as properties" must never be built from `WriteTOML`. | `file.go:249` | read | memory | unit: build `Facts` from a config with password `canary-pw`; JSON and every rendered snippet contain no `canary-pw` and no `--sasl-user x:y` pair; cmd test: `config` output redacts credentials or warns (decision for the owner, section 9) **Closed:** D6: `config print` redacts, `--show-secrets` writes them; `TestNoSecretReachesAnyOutput`. |
| CON-10 | P2 | Cn-01 says the password is "set in doze-kafka.toml under [credentials]". If the user passed `--sasl-user` the statement is false and the password is in their shell history. The config does not remember where a credential came from. | Cn-01; `internal/config` | read | memory | unit: `config.Load` records `CredentialSource` (`file`/`flag`); page text switches; test both |
| CON-11 | P2 | Version claims on boards are not what the matrix pins. Python "tested at 2.16" and kcat "1.7 and 2.15": `compat/python/run.sh` uses an unpinned `confluent-kafka`, `compat/kcat/run.sh` uses `command -v kcat`. Java "4.0.0" is pinned but only one version is ever run (the `KAFKA_VERSION` knob has no cell). The weekly cron can quietly move "tested at 2.16" to 2.17. | Cn-02, Cn-03, Cn-04 | read | doc | compat: each run prints `version: <client> <x.y.z>` and writes `compat/versions.json`; console test: board versions come from that file; a CI step fails when a board version differs |
| CON-12 | P2 | Cn-03 says "The same keys work in any librdkafka binding". Only `confluent-kafka` (Python) and kcat run. confluent-kafka-go, node-rdkafka, rdkafka-ruby are listed in `docs/clients.md` as "expected to work, not run here". | Cn-03 | read | doc | cut the sentence, or add a Go librdkafka cell (section 7: `confluent-kafka-go`, optional) |
| CON-13 | P2 | Cn-03 Python "Both work against this broker" (classic and `group.protocol=consumer`) and Cn-02 "The broker serves it because --kafka-version is 4": the Python tab does not condition on `--kafka-version`; with `--kafka-version 3` the new protocol is not offered (`docs/configuration.md:110`) and the pasted `group.protocol` line fails or falls back. | Cn-03 | read | doc | console test: `Facts.KafkaVersion=3` hides the line on every board; compat: python cell against `--kafka-version 3` consumes the classic variant |
| CON-14 | P3 | Cn-05 "Refused INVALID_PARTITIONS: Topic currently has 6 partitions, which is higher than the requested 3." The broker sends "the topic has %d partitions and %d is fewer: partitions cannot be removed" (`internal/broker/createpartitions.go:69`). Either the board imitates Kafka's text or it is wrong for this broker. | Cn-05; `createpartitions.go:69` | read | recorded? no (`memory`) | queue the exact Kafka text (section 8); until recorded the console shows the broker's own message |
| CON-15 | P2 | Cn-02 claims `application.reset` "works too, it uses DeleteRecords", and a second producer "fences the first". The matrix (`java/Check.java`) has no `application.reset` step and no fencing step; fencing is covered in Go (`internal/broker/txn_api_test.go`), reset not at all (grep of `compat/`, `internal/`: only `docs/frameworks.md` says it). | Cn-02; `docs/frameworks.md:Kafka Streams` | read | memory | compat java: a Streams reset step (run `kafka-streams-application-reset` from the Kafka image or call `Admin.deleteRecords` with the same arguments) and a two-producer fencing step |
| CON-16 | P1 | Cn-06 compose file uses `KAFKA_SECURITY_PROTOCOL`, `KAFKA_SASL_MECHANISM`, `KAFKA_USERNAME`, `KAFKA_PASSWORD`. `doze-kafka env` prints `KAFKA_SASL_MECHANISM`, `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_BROKERS`, `SCHEMA_REGISTRY_URL`, and no protocol, no username, no CA (`env.go:36-47`). The names an app reads are the user's; the board must say they are placeholders, and `env` should print the variables the board uses. | Cn-06, Cn-01; `env.go` | read | doc | cmd test: `env` with SASL+TLS prints `KAFKA_SECURITY_PROTOCOL=SASL_SSL` and `KAFKA_SSL_CA_LOCATION=<path>`; board and `env` read the same name list from one table |
| CON-17 | P2 | Cn-07 user table: "Used by" (checkout-api...) needs principal -> client. `traffic.Conn` carries `Principal` and `ClientID` (`internal/traffic/traffic.go:44`), so it is buildable, but the Mechanism column is not stored: credentials are a `map[string]string` and every user is available under every enabled mechanism. The `__console` row shows an internal user; the console may show it, but "super user" must come from `cfg.ACLSuperUsers`/the provisioning code, not a literal. | Cn-07 | read | memory | unit: users list from a config with two users and three mechanisms; the `__console` user is listed once and labelled from code; "Used by" from a traffic fixture |
| CON-18 | P2 | Cn-07 certificate sentence: "The CA file is the one that signed server.pem, or server.pem itself if it is self-signed. The certificate must name the host clients connect to". Nothing computes either. `docs/auth-and-tls.md:77` tells users to make a cert for `DNS:localhost,IP:127.0.0.1` only, and the page's own Docker board then asks them to connect to `host.docker.internal` and the default name `kafka.<name>.doze` would not match either. | Cn-07, Cn-06; `docs/auth-and-tls.md:77` | read | doc | unit: `Facts.TLS` parses the cert, sets `SelfSigned` and `Names`; a mismatch with `AdvertisedHost` adds a warning; the doc's openssl command is generated with the advertised host as a SAN; compat already covers a cert with the Docker name |
| CON-19 | P2 | Windows, WSL, Docker-on-Mac are not distinguished. `docs/endpoints.md:88`: no names on Windows, default listen `127.0.0.1:9092`, console `127.0.0.1:9093`. WSL2 is a Linux binary: names may "resolve" inside WSL while the Windows browser and Windows apps cannot, and Docker Desktop containers cannot reach a loopback alias at all. The board shows one answer. The matrix runs only on Linux CI and macOS (Colima) developer machines; Windows and WSL are not exercised anywhere. | Cn-01, Cn-06 | read | doc | unit: `Facts.Platform` from a fake `/proc/version`; page picks the `--listen` form on `windows` and `wsl`; manual QA list in section 7 for the three platforms because CI has none |
| CON-20 | P1 | Cn-06 gives `doze-kafka --listen :9092 --advertised-host host.docker.internal --schema-registry-addr :8081` and "Your SASL settings stay as they are". That is only true when the settings are in a file. Restarting from this command with flag-given SASL or TLS drops them: the broker comes back on all interfaces with **no auth**. The board also never warns that `--listen :9092` is reachable by the network (`docs/endpoints.md` "Listening beyond this machine is a choice"). | Cn-06 | read | doc | unit: the restart command is built from the effective config (flags that differ from defaults, passwords replaced by `--config` file reference); with auth off the page shows the warning; cmd test round-trips the generated command through `loadConfig` and compares the effective config except the listen and advertised fields |
| CON-21 | P3 | Cn-06 "Console ... answers to localhost and *.doze only". `console/hostguard.go:22-43` also allows IP literals and `*.localhost`; `--console-allow-host` adds more. | Cn-06; `hostguard.go` | read | memory | wording from the guard's table; test that the sentence lists what `hostAllowed` accepts |
| CON-22 | P2 | Cn-06 verdict "not reachable from a container as it is running now" has no data: nothing computes "advertised host is loopback / bound to loopback". | Cn-06 | read | memory | unit: `Facts.ReachableFromContainer(cfg)` over a table (name mode, `--listen 127.0.0.1`, `--listen :9092` no advertised host -> `localhost`, with `host.docker.internal`) |
| CON-23 | P2 | Docker engines differ: `host.docker.internal` is built in on Docker Desktop and recent Colima and OrbStack, needs `host-gateway` on Linux Engine (the matrix passes it), and is `host.containers.internal` on Podman. A host firewall on Linux can block the docker0 bridge to a `0.0.0.0` listener. Board covers only the first two. | Cn-06 | read | doc | compat: the Docker cells already run on Linux with `host-gateway`; add a Podman cell if podman exists (skip 77 otherwise); text says what is and is not tested |
| CON-24 | P1 | Default (named) mode is never run by the matrix: every non-Docker cell talks to `127.0.0.1:<port>` (`compat/run.sh:start`). The page's headline address is `kafka.<name>.doze:9092`. Whether a real client bootstraps to a name that resolves to a hashed loopback alias and then follows Metadata back to the same name is covered only by Go tests (`names_test.go`), not by kcat/Java/librdkafka. | `compat/run.sh:51-59` | read | spec | compat: a `named` mode (skips with 77 when `.doze` does not resolve, so it runs on a set-up developer machine and on a CI job that runs `dns-setup`); kcat, python, java cells |
| CON-25 | P2 | Teaching/claim drift risk with no gate: `docs/clients.md` "Version tested" table, `docs/compat/README.md` table, the three boards and the matrix are four copies of the same facts. | `docs/clients.md`, `compat/README.md` | read | doc | generated tables from `compat/versions.json` and a CI diff check (see CON-11) |
| CON-26 | P2 | Cn-04 kcat `-F ~/.config/kcat.conf` and `-f '%t p%p @%o key=%k %s\n'` never run; the matrix uses `-X` flags and `-f '%k=%s %h\n'`. The `-F` file with `ssl.ca.location` is missing from the board for TLS. | Cn-04 | read | doc | compat: the kcat cell writes the rendered config file and uses `-F`; one consume uses the board's `-f` format string and asserts the line shape |
| CON-27 | P3 | Cn-03 and Cn-05 pin sample code to `harbour.checkout.orders`, `g-order-validator`, user `checkout`: the board is a fictional shop (Grounding). The page must fill topic, group and user from this broker, or use neutral names; hard-coded names would turn into a wrong example on any other machine. | all | read | memory | e2e: with a broker holding topics `a`, `b`, the Python snippet names `a`; with none, it says `my-topic` (a placeholder token, same on every board) |
| CON-28 | P2 | Boards show no variant for TLS on (Cn-02 to Cn-05 are drawn in the no-TLS state; only Cn-07 shows TLS lines, and only as a static block). The page must render 4 states (open, SASL_PLAINTEXT, SASL_SSL, SSL) for each of 5 clients = 20 renders, each of which should have been run. | Cn-02..05 | read | spec | golden test per (client, state); the matrix cell list is derived from the same table (section 6) |
| CON-29 | P2 | Docs link snippets (Go, Node) that no board or cell covers with SASL/TLS: `docs/clients.md` franz-go/Sarama/kafkajs examples are plain only; `docs/auth-and-tls.md` has franz-go SCRAM and kafkajs with `scram-sha-256` and `ssl` snippets not run (kafkajs cell uses 512). | `docs/clients.md`, `docs/auth-and-tls.md:55-70` | read | doc | compat: Go cell (franz-go + Sarama) with the doc's snippet shapes under both modes |
| CON-30 | P3 | `docs/getting-started.md` shows `kafka.project.doze` in the banner and `kcat -b "$KAFKA_BOOTSTRAP_SERVERS"`; it does not tell a SASL user that `eval "$(doze-kafka env)"` does not export credentials. It also says Go 1.26+ and "build from source" while `install.sh` exists. | `docs/getting-started.md` | read | doc | doc test: every fenced `sh` block in the three docs is either run by `scripts/docs-check.sh` or marked `no-run` with a reason |

Hunt log (so a clean result is credible): read all 7 boards' text and compared
each snippet line to `compat/*/run.sh`, `Check.java`, `check.py`, `serdes.py`,
`check.mjs`, `spring/Check.java`, `connect/run.sh`; read `env.go`, `listen.go`,
`config.go:239-257`, `schemaregistry/api.go`, `hostguard.go`, `main.go:418-495`
(console credential provisioning); grepped `internal/` for the SASL error text
and the partitions error text; checked that the clipboard fallback exists
(`console/static/shell.js:197-210`), so copying over `http://*.doze` works
(not a finding; covered by an e2e test below). Not found: a place where the
console prints a real password (the console does not hold user passwords, only
its own `__console` one, `main.go:425`).

## 5. What to check

Beyond the common criteria:

- Every snippet id has a producer template under `compat/snippets/` and a matrix
  cell that consumed its rendered output in the last green run (the "consumed"
  manifest, section 7).
- `Facts` is identical from `doze-kafka env`, `/api/connect` and the banner for the
  same configuration, in name mode, `--listen 127.0.0.1:0`, `--listen :9092`,
  `--advertised-host`, SASL on, TLS on.
- No rendered text, log line, JSON or toast contains a configured password
  (canary test).
- Every board wording about behaviour has a ledger or matrix row: "fsynced before
  the ack" (`docs/storage-and-durability.md`), "15 minutes" (`internal/broker/txn.go:52`),
  "fences the first" (`txn_api_test.go`), "Real Kafka answers the same"
  (recording queue).
- The page works with the broker down (A7: facts come from the config, not from
  a Kafka call, so it still renders; the wire strip says disconnected) and
  with an empty data directory.
- The clipboard path works over `http://kafka.<name>.doze` (non-secure origin):
  `shell.js` falls back to `execCommand`; e2e under a `.doze` Host header.
- `doze-kafka --kafka-version 1..4` shows only what that version serves.

## 6. Console

### Boards

Status key: `real` exists, `buildable` data exists, screen does not, `needs backend`.

| Board | Template, handler | Status | Built from |
|---|---|---|---|
| `Cn-01-Bootstrap` | `connect.html` (home tab), `connect` handler | needs backend | `Facts` (new, 2); settings table `Setting/Value/Copy` from the snippet package; shell block from `env`'s table (CON-16); the strip: traffic filter on "first requests of a new connection" ([traffic](traffic.md)) |
| `Cn-02-Java` | `connect.html` `java` partial | buildable | `compat/snippets/java/*` rendered with `Facts`; "group is on it today" from `DescribeGroups` protocol field via the groups backend (see [groups](groups.md)); the strip from a traffic filter on client software |
| `Cn-03-Python` | `python` partial | buildable | same; telemetry line from `ClientTelemetry`; version from `versions.json` |
| `Cn-04-Kcat` | `kcat` partial | buildable | same; `kcat -F` file written to the cell |
| `Cn-05-Spring` | `spring` partial | buildable | same; the `INVALID_PARTITIONS` block from the broker's message (CON-14) |
| `Cn-06-Docker` | `docker` partial | needs backend | `Facts.ReachableFromContainer`, the generated restart command (CON-20), compose template run by a Docker cell |
| `Cn-07-Sasl-Tls` | `sasl-tls` partial | needs backend | users from `cfg.Credentials` + traffic principals; `Facts.TLS` with SANs and self-signed (CON-18); refusal text from `auth.go` (CON-14 style test); Access link |

Counts: real 0, buildable 4, needs backend 3. The "wire, underneath" strip is the
foundation's `dock` (A4), filtered; it is not redefined here. Inspector, rail and
palette entries (`Connect an app` command) come from A1 and A10.

### `new` items

Items tagged new on the boards and what they are built from:

- "Copy all settings" -> concatenation of the rendered snippets in tab order;
  never includes secrets; ends with a comment line naming the matrix cell and
  version it was last run against.
- "Group g-order-validator is on it today" -> the group's protocol (classic or
  consumer) from the group coordinator; if the group has no members, omit the
  sentence.
- "librdkafka 2.5+ pushes its own metrics ... The console shows them for
  order-validator" -> the client-metrics store (`ClientMetrics`); link only when
  the store has an entry.
- "This broker is not reachable from a container" -> CON-22.

### One source for snippets

The proposal, to make `CON-4` to `CON-12` impossible.

1. **Templates live in `compat/snippets/`** as ordinary files in the client's own
   format, one per (client, snippet id): `java/client.properties.tmpl`,
   `python/conf.py.tmpl`, `kcat/kcat.conf.tmpl`, `spring/application.yaml.tmpl`,
   `docker/compose.yaml.tmpl`, `docker/restart.sh.tmpl`, `tls/broker.sh.tmpl`.
   Go `text/template` with a small, closed set of functions.
2. **Executable examples are real source files with markers**, not strings:
   `compat/python/snippets.py` contains `# snippet: producer` ... `# end`, and the
   cell runs the file. Producer/consumer code on Cn-02, Cn-03, Cn-05 is a marked
   region of a file the matrix executes. The renderer extracts regions.
3. **One Go package, `compat/snippets`** (`package snippets`, `//go:embed` of the
   template and snippet files; `compat/` contains no Go today and the module
   root is the repo root, so it is importable). API:
   `Render(client, id string, f Facts, secrets Secrets) (string, error)` and
   `All() []ID`. The console passes a `Secrets` that renders `<password>`
   (and `${KAFKA_PASSWORD}` for Spring and compose); the matrix passes the real
   password.
4. **The matrix executes the templates.** `compat/run.sh` builds one tiny helper
   (`go run ./compat/snippets/cmd/render -client java -facts facts.json -secrets
   env`) and each cell asks it for its configuration instead of building
   strings in shell/Java/Python. `facts.json` is `Facts` written by the runner
   from the exact flags it started the broker with. The cell prints
   `snippet java/client.properties consumed` for each template it used.
5. **The drift gate.** A Go test enumerates `snippets.All()`, renders every
   (client, state) with fixture facts and compares to golden files; a second test
   (`compat/snippets/consumed_test.go`, run in the `compat` workflow after the
   matrix) reads the matrix's `consumed` log and fails if any snippet id has no
   passing cell for **each state the console can render it in**. A snippet
   added without a cell cannot reach the page.
6. **Docs are outputs too.** `docs/clients.md`, `docs/frameworks.md` and
   `docs/getting-started.md` fenced blocks are marked
   `<!-- snippet: java/client.properties state=sasl_ssl -->` and regenerated by
   `go run ./compat/snippets/cmd/docs`; `task docs:check` fails on a diff.
   Hand-written code in those files that no cell runs is marked `no-run` with a reason.

Binary weight: templates embed as a few tens of kB of text; it counts against the
lightness budget (B9) and is stated in the commit. Alternative if the owner does
not want `compat/` imported by the binary: a `go generate` step copies the
rendered goldens into `console/snippets/`. The unit-level guarantee is the same;
the `consumed` gate still needs the runner.

### The state table (what the page renders, what the matrix runs)

| State | `security.protocol` | Rendered for | Cell (new unless noted) |
|---|---|---|---|
| open | `PLAINTEXT` | all five | existing `plain` |
| SASL, no TLS | `SASL_PLAINTEXT` | all five, mechanism picked from `Facts.SASLMechanisms` | **new** `sasl-plain` |
| SASL + TLS | `SASL_SSL` | all five | existing `scram-tls` |
| TLS only | `SSL` | all five | **new** `tls-only` |

Mechanism variants (SCRAM-SHA-256, PLAIN) run on kcat, python, kafkajs and java.
Not every client needs every state: the heavy JVM cells (`spring`, `java`
streams sections, `connect`) run `sasl-plain` for the `clients` section only.

### Board text compared with the matrix

| Setting on the board | Matrix | Action |
|---|---|---|
| Java `sasl.jaas.config=\` continued over 2 lines | single line | render one line; add test that the continuation form also parses (it does in Java properties; keep a unit test with a parser) |
| Java truststore `.jks` (Cn-07) | `ssl.truststore.type=PEM` + PEM file | board changes to PEM (Java 4.0 supports it, run) |
| Python `sasl.mechanisms` | `sasl.mechanism` | follow the matrix |
| kcat `sasl.mechanisms` in file; `-F` | `-X sasl.mechanism`, no `-F` | cell uses the file |
| Spring `spring.kafka.properties.security.protocol` | `spring.kafka.security.protocol` | follow the matrix; add SSL keys |
| Spring `${KAFKA_PASSWORD}` | literal arg | cell loads a rendered yaml with the env var |
| Docker host `host.docker.internal` + `extra_hosts` | `--add-host ...:host-gateway` | same; Docker cell runs the rendered compose (see below) |
| Docker `--listen :9092` | `--listen 0.0.0.0:<port>` | equivalent; test both forms bind the same |
| Registry URL only | basic auth on SASL | add the auth lines |
| Streams SASL via `sasl.*` | Streams section uses client props | ok (run) |
| `isolation.level=read_committed` on classic Java consumer | run | ok |
| `transaction.timeout.ms=60000` | default, not set explicitly | cell sets it |
| Kafka CLI tools tab (Cn-02) | `lib.sh:kafka_tool` runs the Kafka image tools with `--command-config`, only for connect/UI cells | add a `tools` cell: console-producer/consumer and `kafka-topics.sh` from the rendered properties |
| "Kafka Streams ... application.reset" | not run | CON-15 |

### Clients the boards name that the matrix does not run

| Named on a board | Verdict |
|---|---|
| "any librdkafka binding" (Cn-03) | cut the sentence, or add `confluent-kafka-go` cell (optional) |
| "any Java tool" / Kafka tools tab (Cn-01, Cn-02) | add the `tools` cell above |
| Kafka Streams `application.reset` (Cn-02) | add the step |
| Protobuf serializer (Cn-03 "exercised") | true for Python via dynamic descriptors; add a generated-classes cell (below) |
| Telemetry (Cn-03) | exists in `e2e`/broker tests (KIP-714), not a connect matter; the line stays, it links to Observability |
| Podman / Colima / Linux Docker (Cn-06) | Linux Docker run; Colima by the maintainer; Podman, OrbStack, WSL not run: say so on the page |

Not on any board, but in the matrix or the docs and worth an optional tab each
(an ask to the designer, not an invention): **Go** (franz-go, Sarama; `docs/clients.md`
leads with them), **Node** (KafkaJS), **Kafka Connect** (needs the `producer.`,
`consumer.`, `admin.` repeats that `docs/frameworks.md` omits). If the tabs are
rejected, those three stay in the docs only, generated from the same templates.

### Teaching lines

"Real Kafka settings, filled in from how this broker is running" and the
per-field lines (unit, default, applied/stored) come from the settings table (B7).
The page's settings are all client-side Kafka properties, not broker settings; the
table rows needed are: `bootstrap.servers`, `security.protocol`, `sasl.mechanism`,
`sasl.jaas.config`, `ssl.truststore.*`, `ssl.ca.location`, `group.protocol`,
`transactional.id`, `transaction.timeout.ms` (max 15 minutes, `txn.go:52`),
`enable.idempotence`/`acks`. A3's test (every form field has a record) does not
apply to a read-only page; instead a test asserts every setting name a template
emits has a settings-table record with a source (`spec`/`doc`).

### Credentials, Windows/WSL/Docker, the rest of the page

- Never a password. The page shows `<password>` (and the env-var form
  `${KAFKA_PASSWORD}` for Spring/compose), a link to Access for who the users
  are, and the one place the password lives ("the file you wrote, or your flags").
- How users are created: only by restarting with `--sasl-mechanisms` and
  `--sasl-user` or `[credentials]` in the config (no
  `CreateUserScramCredentials`). The page generates the command from the
  effective config (CON-20), never from a template with literal values.
- The restart command with flags shown uses `--sasl-user x:<password>` only if the
  user is using flags; otherwise `--config doze-kafka.toml`.
- Platforms: macOS name mode (default) -> name works for the host, not for
  containers; Linux with `dns-setup`; Windows -> `127.0.0.1:9092`, console
  `127.0.0.1:9093`, no names (`docs/endpoints.md:88`); WSL -> Windows-side
  apps need `--listen 127.0.0.1:9092` in WSL and rely on localhost forwarding
  (a claim to test manually, no automation); Docker on Mac: `host.docker.internal` plus
  `--listen :9092`.

## 7. Tests to add

### Unit and store

- `compat/snippets`: golden render for every (client, snippet, state) of the table
  above (about 60 files); parser checks for properties/conf/YAML/TOML/compose (`yaml.v3`,
  `go-toml` already in `go.mod`; a tiny properties and conf parser written for the test);
  `TestNoSecretInAnySnippet` with a canary password; `TestHashInValue`
  for CON-4; `TestPlaceholderTokenIsOneToken`.
- `cmd/doze-kafka`: `TestFactsMatchEnvAndBanner` (table of 8 configurations),
  `TestFactsCanaryPassword`, `TestRestartCommandRoundTrip` (CON-20),
  `TestReachableFromContainer` (CON-22), `TestEnvPrintsProtocolAndCA` (CON-16),
  `TestPlatformWSL` with a fake `/proc/version` (CON-19).
- `internal/config`: `TestCredentialSource` (CON-10), `TestTLSFactsSelfSignedAndNames`
  (CON-18), `TestResolveAdvertisedHostTable` (every branch of lines 239-257),
  `TestWriteTOMLCredentials` (the redaction decision, CON-9).
- `console`: `TestConnectPageEveryTab`, `TestConnectPageBrokerDown`,
  `TestConnectPageKafkaVersion3` (CON-13), `TestConnectNoHardcodedAddress` (CON-1,
  CON-2).

### Raw kmsg

- `TestSASLFailureMessage`: wrong password, unknown user, mechanism not offered:
  the three `ErrorMessage` strings the Cn-07 refusal panel quotes are what the
  broker sends, on every `SaslAuthenticate` version (CON-14 family).

### Client library

- Sarama and franz-go: SCRAM-SHA-256/512 over TLS and SASL_PLAINTEXT, round trip,
  group commit and resume (extends `sarama_test.go`).

### Compat cells to add

| Cell | What | Toolchain | Modes |
|---|---|---|---|
| security modes | `sasl-plain`, `tls-only`, `sasl256-tls`, `plain-mech-tls` | runner change | kcat, python, kafkajs, java `clients` first |
| `java-4x` | the Java client at a **second version**. Candidates: Kafka clients 3.9.x (the last 3.x, "older client" case) and the latest 4.x at the time (set by `KAFKA_VERSION`); pick versions by running, not by memory | Docker | both modes; `clients` and `streams` sections |
| `python-protobuf-generated` | Protobuf from `protoc`-generated classes (`_pb2.py`) with the Confluent `ProtobufSerializer`, register, produce, consume, deserialize; the existing cell builds descriptors dynamically | `uv` + `grpcio-tools` | both modes, with registry basic auth |
| `sarama` | Sarama (`IBM/sarama`) as a compat cell: SCRAM-512, TLS with a custom `RootCAs`, `sarama.V2_8_0_0` and a newer version, idempotent producer, group | Go (no Docker) | both modes + `sasl-plain` |
| `franz-go` | franz-go: `kgo.SASL(scram...)`, `kgo.DialTLSConfig`, both group protocols (`kgo.ConsumerProtocol`? choose by test, not memory), transactions | Go | both modes + `sasl-plain` |
| `tools` | `kafka-topics.sh`, `kafka-console-producer.sh`, `kafka-console-consumer.sh`, `kafka-consumer-groups.sh` with `--command-config` from the rendered properties | Docker | both modes |
| `docker-compose` | run the rendered compose snippet (`docker compose up --abort-on-container-exit`) with a one-file test app, against a broker started by the *generated* restart command | Docker | both modes |
| `named` | CON-24 | local | kcat, python, java |
| `connect` | already exists; render its worker properties from the Connect template (adds the four prefixes) | Docker | existing |
| `podman` (optional) | `host.containers.internal` | podman | 77 if absent |

The Go cells are separate from the Go unit tests: they run the built binary
like every other cell, so they test the same artefact users download.

### Fuzz

- `FuzzRenderSnippet`: arbitrary `Facts` strings (host names with `:`, `"`, `\n`,
  `$`, `#`, unicode, a user name with `=`) into every template; assert the output
  parses under the target format's parser and the injected value is quoted or
  rejected (the page must not let a hostile `--advertised-host` or user name
  break out of a snippet; `validAdvertisedHost` at `config.go:428` is the first
  line of defence). Seed from the golden files.

### e2e

- `connect.spec.ts`: each tab loads in one click; Copy copies the block (clipboard
  and `execCommand` paths, under a `.doze` Host header); `c` copies the selected
  block, `1`-`4` / Tab switch blocks (Cn-02 keys); the page at 960 and 390 px;
  broker down (A7); no password text on the page with the canary config;
  a `--listen :9092` broker shows the reachable verdict flip.

### Manual QA list (no CI)

macOS Docker Desktop + Colima, Windows 11 native, WSL2 default NAT and mirrored,
Linux Docker Engine with `ufw` on. Each runs the Docker board's three steps and
records the result in this document's section 9.

### Differential

None: this stage does not claim broker behaviour beyond what the matrix proves.
The SASL error strings and the `INVALID_PARTITIONS` text are in the recording
queue.

### Coverage targets

| Package | Baseline | Target | Functions that must be covered |
|---|---|---|---|
| `cmd/doze-kafka` | 79.8% | 83% | `envTo` (SASL, TLS, registry branches), `instanceAddress` (both arms, the unresolved-name arm), `registryURL`, the new `buildFacts`, `restartCommand`, `reachableFromContainer`, `platform` |
| `internal/config` | 61.6% | 66% | `ResolveAdvertisedHost` (all five branches), `TLSConfig`, `WriteTOML` credentials branch, `validAdvertisedHost`, the new credential-source and cert-facts functions |
| `console` | 87.8% | 88.5% | the new connect handlers, template funcs |
| `compat/snippets` (new) | n/a | 90% | `Render` for every client/state, `All`, region extraction |

## 8. Recording queue

None of these is acted on until `doc` or `recorded`.

| Claim | Scenario to record | Source now |
|---|---|---|
| librdkafka accepts `sasl.mechanisms` and `sasl.mechanism` as the same property, in `-X`, `-F` and the Python dict | not Kafka: the client library's own docs/config dump; run `kcat -X dump` with both spellings (Docker not needed) | memory |
| Real Kafka's wording for a failed SCRAM login (Cn-07: "Authentication failed during authentication due to invalid credentials with SASL mechanism SCRAM-SHA-512") | `apache/kafka:4.0.0` with `KAFKA_SASL_ENABLED_MECHANISMS=SCRAM-SHA-512`, create a user via `kafka-configs.sh --alter --add-config 'SCRAM-SHA-512=[password=...]'`, send a wrong password with the Java client and with a raw `kmsg` `SaslAuthenticate`; record the `ErrorMessage` for v0..v2 | **recorded** (`conformance/recordings/sasl-scram.json`, steps wrong-password-512-client-final and unknown-user-512-client-first; `conformance/claims/auth.md` A12): the wording is as quoted, at SaslAuthenticate v0, v1 and v2, and doze-kafka now sends it |
| Real Kafka's `INVALID_PARTITIONS` text on decrease ("Topic currently has 6 partitions, which is higher than the requested 3.") | `apache/kafka:4.0.0`, `CreatePartitions` 6 -> 3 and 6 -> 6 | memory |
| `group.protocol=consumer` is "not the default even in Kafka 4.x" | Kafka 4.0.0 Java client default, read from `ConsumerConfig` and a run against `apache/kafka:4.0.0` | doc (clients.md) |
| Max transaction timeout is 15 minutes | the real broker's `transaction.max.timeout.ms` default and the error code on 15 min + 1 ms | doc (matches `txn.go:52`) |
| Java client keeps `acks=all` with idempotence by default | client source / run | memory |
| `PLAIN` over plaintext sends the password in the clear; SCRAM never does | wire capture of both, from a raw `kmsg` exchange | doc |
| Docker Desktop, Colima, OrbStack and Podman host-name behaviour | manual, section 7 QA list | memory |

## 9. Owed to integration

- [Access](access.md): the Users table on Cn-07 links to Access; user creation
  stays a restart. If Access gains "create user" it needs
  `CreateUserScramCredentials` (not advertised, stub S) and a restart-free
  credential store; the Connect page then drops the "restart to apply" line.
- [Traffic](traffic.md): the "wire, underneath" strip needs a recorder filter by
  client software and by "first requests of a connection", and the request
  summaries "epoch 4 -> 5", "p1 - 1 batch - 412 B" come from decoded bodies; if
  the recorder keeps only headers, that stays blank and the strip shows request
  names only. Also: no `SaslAuthenticate` body in the trace may carry a secret
  (check there).
- [Cluster](cluster.md): `Listeners` (Cl-09) and `TLS` (Cl-08 zone) and this page
  read the same `Facts`; Cluster owns the cert-expiry display, this stage the
  SAN/CA part.
- [Schemas](schemas.md): registry basic-auth line (CON-5).
- Foundation asks: **B7** needs rows for the client-side properties above (a
  new `client` kind of setting, not only broker settings). **B8** route table:
  four new routes. **A8**: the e2e identity sweep covers this page.

Asks of the foundation (not numbered there): a shared `Facts` type that Cluster,
Connect and `env` import; a `docs:check` generator slot (CON-25); a decision on
whether `doze-kafka config` redacts credentials (CON-9) because the output is a
valid config file and redaction breaks round-tripping (option: print
`password = "<redacted>"` and add `config --show-secrets`).

## 10. Acceptance criteria

On top of the common criteria in [01-protocol.md](../01-protocol.md):

- [ ] `go test ./compat/snippets/...` passes with coverage >= 90% and golden files for every (client, snippet, state).
- [ ] `go test ./cmd/doze-kafka -run 'Facts|Env|RestartCommand|Reachable|Platform'` passes; `go tool task coverage` shows `cmd/doze-kafka` >= 83%, `internal/config` >= 66%, `console` >= 88.5%.
- [ ] `go tool task mutate:check -- ./compat/snippets/... ./cmd/doze-kafka/...` shows no new surviving mutant in `Render`, `buildFacts`, `restartCommand`, `reachableFromContainer`, `envTo`.
- [ ] `TestNoSecretInAnySnippet`, `TestFactsCanaryPassword`, `FuzzRenderSnippet` (clean for the standard fuzz time) pass.
- [ ] `go tool task test:compat` reports PASS (not SKIP) for: `kcat`, `python`, `kafkajs` in modes `plain`, `sasl-plain`, `scram-tls`, `tls-only`, `sasl256-tls`; `java`, `spring`, `connect`, `tools`, `docker-compose` in the modes the state table gives; new cells `sarama`, `franz-go`, `java-4x` (two kafka-clients versions), `python-protobuf-generated`.
- [ ] `compat/snippets/consumed_test.go` passes against the matrix's log: every snippet id has a passing cell for each state it can render in.
- [ ] `compat/versions.json` is written by the matrix; `console` test `TestBoardVersionsMatchMatrix` passes; `docs/clients.md` version table regenerates with an empty diff (`task docs:check`).
- [ ] The Docker cell runs the rendered compose snippet against a broker started by the generated restart command, and passes with auth off and on.
- [ ] Boards implemented, each with a Playwright test: Cn-01, Cn-02, Cn-03, Cn-04, Cn-05, Cn-06, Cn-07. `e2e/tests/connect.spec.ts` passes at 1440, 960 and 390 px, with the route gate on, and with the broker down.
- [ ] `grep -rn '127.0.0.1:9092' console/templates` is empty (CON-2).
- [ ] No board text names a client without a cell, or the sentence is cut: `CON-12` closed.
- [ ] Findings CON-1 to CON-30 each have a failing test before the fix (named in the table), or are listed as `owed` / `queue` with a reason in the commit.
- [ ] `docs/api-support.md` unchanged in tiers (no new API); `docs/clients.md`, `docs/frameworks.md`, `docs/getting-started.md`, `docs/configuration.md`, `docs/auth-and-tls.md`, `CHANGELOG.md` updated; the manual QA list results recorded (Windows, WSL, Docker Desktop, Colima).
- [ ] CI green on `ci`, `e2e`, `kcat`, `compat`, `fuzz`; the weekly `compat` cron still files its issue on failure (`.github/workflows/compat.yml`).

## 11. Files and order of work

Half-day units (h.d.). Total about 26 units, 13 days, size **L**.

| # | Work | Files | Units |
|---|---|---|---|
| 1 | `Facts` type, built from config; `/api/connect`; canary test (CON-9, 10, 16, 22) | `cmd/doze-kafka/facts.go`, `env.go`, `internal/config` | 3 |
| 2 | `compat/snippets` package, templates for kcat + java + python, golden tests, parser checks, fuzz target (CON-4, 6, 8) | `compat/snippets/**` | 4 |
| 3 | Runner: the render helper, `facts.json`, `consumed` log, new modes `sasl-plain`, `tls-only`, `sasl256-tls`; move kcat, python, kafkajs cells onto templates (CON-3, 26) | `compat/run.sh`, `lib.sh`, three `run.sh` | 3 |
| 4 | Java cell on templates; `java-4x`; `tools` cell; Streams reset and fencing steps (CON-15) | `compat/java/**`, `compat/tools/` | 3 |
| 5 | Spring cell on a rendered yaml; registry basic-auth lines (CON-5, 7) | `compat/spring/**` | 1 |
| 6 | `sarama`, `franz-go` cells; `python-protobuf-generated` (CON-29, plus the brief's list) | `compat/{sarama,franz-go,python}` | 3 |
| 7 | Docker: reachability verdict, restart command from effective config, compose cell (CON-20, 22, 23) | `facts.go`, `compat/docker-compose/` | 2 |
| 8 | TLS facts: self-signed, SANs, CA path; doc's openssl command from the facts (CON-18) | `internal/config`, `docs/auth-and-tls.md` | 1 |
| 9 | Console: `connect.html`, handler, 7 boards, strips (needs A1, A4) (CON-1, 2, 13, 17, 27, 28) | `console/` | 4 |
| 10 | `named` mode (CON-24); platform detection and the WSL/Windows text (CON-19) | `compat/run.sh`, `facts.go` | 1 |
| 11 | `versions.json`, generated docs tables, `docs:check` (CON-11, 25, 30) | `compat/`, `docs/`, `Taskfile.yml` | 1 |
| 12 | e2e spec, a11y sweep, 960/390 px; manual QA run (Windows, WSL, Docker Desktop, Colima) | `e2e/tests/connect.spec.ts` | 1 |

Order: 1 -> 2 -> 3 (the rule exists) -> 4, 5, 6 in parallel -> 7, 8 -> 9 (the page,
once the pieces are real) -> 10, 11, 12. Items 4 to 6 add cells and must be red
first (new modes may reveal broker bugs, which are P0/P1 and fixed in the stage
that owns the package; log them in this table).

### Prior art

- **Redpanda Console** has a "Quick start" / connect page showing generic
  `rpk` and client snippets with the broker address filled in; the snippets are
  static text, not executed in CI (as far as the docs show; not verified here).
- **Confluent Cloud** "Clients" page generates per-language config with API
  key placeholders; it generates but does not run on a local broker in CI.
  The shared-source idea above is stricter than either.
- **Testcontainers** modules (`KafkaContainer`) hand the test a
  `getBootstrapServers()` and have the same advertised-listener problem Cn-06
  solves; their docs describe `host.testcontainers.internal` for container -> host.
- **LocalStack** and the doze-aws plan solve the same "tell me how to point my SDK
  at it" with an `env`-style output; this stage reuses the `env` command as the
  CLI twin of the page, so the two stay equal.
- Apache Kafka's own `docker-compose` examples document `KAFKA_ADVERTISED_LISTENERS`
  per audience (host vs container); the page's single "advertised host" is the
  doze equivalent, and the reason `host.docker.internal` is the common answer.
