# Auth & TLS

By default doze-kafka listens in the clear with no authentication — which is usually
what you want on `localhost`. When you need to test an authenticated or encrypted
setup, it supports SASL (PLAIN and SCRAM) and TLS. They're independent: use either,
both, or neither.

The credential store is your config — usernames and passwords you pass as flags or
list in the config file (the file keeps them out of your shell history). There's no Kerberos, no KDC, no external identity provider. The point is to
exercise the *handshake* your client and app do, not to be an auth server.

## SASL

Enable SASL by naming one or more mechanisms and at least one credential:

```sh
./doze-kafka \
  --sasl-mechanisms PLAIN \
  --sasl-user alice:s3cret --sasl-user bob:hunter2
```

Once SASL is on, **every** connection must authenticate before doing anything else —
a client that skips the handshake is disconnected. `--sasl-user` is repeatable; each
is a `user:password` pair.

### Mechanisms

- `PLAIN` — the username and password are sent as they are, unencrypted. Pair it with
  TLS if the wire matters.
- `SCRAM-SHA-256` and `SCRAM-SHA-512` — challenge-response; the password never crosses
  the wire. (The broker itself still has it: credentials are configured in plain text,
  in the file or on the command line.)

Enable several at once and let the client pick:

```sh
./doze-kafka --sasl-mechanisms PLAIN,SCRAM-SHA-256,SCRAM-SHA-512 --sasl-user alice:s3cret
```

### Client configuration

The client must present a matching mechanism and credential. For example:

**kcat:**

```sh
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=SCRAM-SHA-256 \
  -X sasl.username=alice -X sasl.password=s3cret \
  -t demo -P
```

**franz-go:**

```go
kgo.NewClient(
    kgo.SeedBrokers("kafka.myproject.doze:9092"),
    kgo.SASL(scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism()),
)
```

**kafkajs:**

```js
new Kafka({ brokers: ["kafka.myproject.doze:9092"], sasl: { mechanism: "scram-sha-256", username: "alice", password: "s3cret" } })
```

## TLS

Point the broker at a PEM certificate and key to terminate TLS on the listener:

```sh
./doze-kafka --tls-cert cert.pem --tls-key key.pem
```

For local use you'll typically want a self-signed certificate valid for the address
clients connect to:

```sh
openssl ecparam -name prime256v1 -genkey -noout -out key.pem
openssl req -x509 -new -key key.pem -out cert.pem -days 365 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

(Two steps on purpose. The one-line `openssl req -newkey ec …` form writes a key the
broker cannot read when `openssl` is LibreSSL, which is what macOS ships as
`/usr/bin/openssl`: it spells out the curve's parameters instead of naming it. The
commands above work with both. A certificate or key that cannot be loaded is reported
at start-up, before anything else happens.)

Clients then connect over TLS. They must either trust your certificate or skip
verification:

```sh
# kcat, trusting the cert
kcat -b "$KAFKA_BOOTSTRAP_SERVERS" -X security.protocol=SSL -X ssl.ca.location=cert.pem -t demo -P
```

```go
// franz-go, trusting the cert (build a RootCAs pool from cert.pem)
kgo.NewClient(kgo.SeedBrokers("kafka.myproject.doze:9092"), kgo.DialTLSConfig(&tls.Config{RootCAs: pool}))
```

## SASL over TLS

Combine them for an encrypted, authenticated listener — the closest local
approximation of a locked-down cluster:

```sh
./doze-kafka \
  --tls-cert cert.pem --tls-key key.pem \
  --sasl-mechanisms SCRAM-SHA-256 --sasl-user alice:s3cret
```

Clients use `SASL_SSL` as their security protocol.

## What SASL and TLS cover

doze-kafka has up to three listeners, and the two settings do not reach them alike:

| Listener | With `--tls-cert` and `--tls-key` | With `--sasl-mechanisms` |
|---|---|---|
| Kafka | TLS | Every connection logs in. |
| Schema registry | HTTPS, with the same certificate. `--schema-registry-tls=false` keeps it plain HTTP. | HTTP basic authentication, with the same users and passwords. |
| Web console, and `/healthz` on its port | Plain HTTP. | No login. |

**The schema registry** follows the broker. Given a certificate it serves
`https://` and nothing else on its port (a plain-HTTP request is answered
`400 Bad Request`, with the body `Client sent an HTTP request to an HTTPS server.`), and the start-up summary and
`doze-kafka env` print the `https://` URL. A client trusts the certificate the way it
does for the broker:

```sh
curl --cacert cert.pem -u alice:s3cret https://127.0.0.1:8081/subjects
```

The certificate has to name the host in that URL, as it has to name the host clients
bootstrap on. Under a `.doze` name the TLS registry is at
`https://kafka.<name>.doze:8081`, the broker's own host, and not at
`http://schema.<name>.doze`: the shared front door that makes that name port-less
speaks plain HTTP. `--schema-registry-tls=false` is for a proxy that terminates TLS
itself; with it, a registry on an address other machines can reach is reported as a
warning at start-up, because with SASL its requests carry the passwords in the clear.

**The console** has no login of its own and is always plain HTTP. With SASL on it logs
in to the broker as a user of its own, `__console`, with a random password made at
start-up, and that user is a super user: no ACL refuses it. So anyone who can open the
console can read every topic and delete anything, whatever SASL and ACLs say. What
stands in front of it is where it listens and two checks:

- It listens on loopback: the name's own loopback address by default, `127.0.0.1:9093`
  under `--listen`.
- It answers only requests whose `Host` is `localhost`, an IP address, a `*.doze`
  name or a name given with `--console-allow-host`, and refuses a state-changing
  request from another origin (both `403`). These stop a web page in your browser from
  reaching it; they are not authentication, and a client on the network can send any
  `Host` it likes.

With SASL or TLS on, a `--console-addr` that is not loopback (`:9093`, `0.0.0.0:9093`,
a LAN address, a host name other than `localhost`) is **refused at start-up**, exit
code 2. It is refused rather than warned about because turning either on says who may
reach the broker and how, and an open console on the network undoes it without anyone
having asked for that. Before serving it elsewhere, do one of:

1. Keep it on loopback and put a reverse proxy that authenticates (and terminates TLS)
   in front of it; `--console-prefix` mounts it under a path and
   `--console-allow-host` names the host the proxy forwards.
2. Reach it through an SSH tunnel (`ssh -L 9093:127.0.0.1:9093 host`).
3. Turn it off: `--console=false`.
4. Accept it: `--console-unprotected`. This is the answer inside a container, where the
   process must listen on `0.0.0.0` for a published port to reach it; publish that port
   on the host's loopback only (`-p 127.0.0.1:9093:9093`). The broker logs a warning
   saying what is open every time it starts this way.

Without SASL and TLS nothing is refused: the Kafka port is as open as the console is.

## What's not here

No Kerberos/GSSAPI, no OAUTHBEARER, and no delegation tokens. Authorization is by ACL,
enforced; see [Authorization](authorization.md). And
`SASLprep` normalization isn't applied, so stick to ASCII usernames and passwords.
