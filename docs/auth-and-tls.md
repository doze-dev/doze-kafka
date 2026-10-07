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

## What's not here

No Kerberos/GSSAPI, no OAUTHBEARER, and no delegation tokens. Authorization is by ACL,
enforced; see [Authorization](authorization.md). And
`SASLprep` normalization isn't applied, so stick to ASCII usernames and passwords.
