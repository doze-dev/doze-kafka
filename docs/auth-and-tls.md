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

- `PLAIN` — username and password sent (base64) after the handshake. Pair it with TLS
  if the wire matters.
- `SCRAM-SHA-256` and `SCRAM-SHA-512` — challenge-response; the password never crosses
  the wire. doze-kafka stores only a derived verifier per user, not the plaintext.

Enable several at once and let the client pick:

```sh
./doze-kafka --sasl-mechanisms PLAIN,SCRAM-SHA-256,SCRAM-SHA-512 --sasl-user alice:s3cret
```

### Client configuration

The client must present a matching mechanism and credential. For example:

**kcat:**

```sh
kcat -b localhost:9092 \
  -X security.protocol=SASL_PLAINTEXT \
  -X sasl.mechanism=SCRAM-SHA-256 \
  -X sasl.username=alice -X sasl.password=s3cret \
  -t demo -P
```

**franz-go:**

```go
kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.SASL(scram.Auth{User: "alice", Pass: "s3cret"}.AsSha256Mechanism()),
)
```

**kafkajs:**

```js
new Kafka({ brokers: ["localhost:9092"], sasl: { mechanism: "scram-sha-256", username: "alice", password: "s3cret" } })
```

## TLS

Point the broker at a PEM certificate and key to terminate TLS on the listener:

```sh
./doze-kafka --tls-cert cert.pem --tls-key key.pem
```

For local use you'll typically want a self-signed certificate valid for the address
clients connect to:

```sh
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes \
  -keyout key.pem -out cert.pem -days 365 \
  -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1"
```

Clients then connect over TLS. They must either trust your certificate or skip
verification:

```sh
# kcat, trusting the cert
kcat -b localhost:9092 -X security.protocol=SSL -X ssl.ca.location=cert.pem -t demo -P
```

```go
// franz-go, trusting the cert (build a RootCAs pool from cert.pem)
kgo.NewClient(kgo.SeedBrokers("localhost:9092"), kgo.DialTLSConfig(&tls.Config{RootCAs: pool}))
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

No Kerberos/GSSAPI, no OAUTHBEARER, no delegation tokens, and **no authorization** —
authentication only. An authenticated user can do anything; there are no ACLs. And
`SASLprep` normalization isn't applied, so stick to ASCII usernames and passwords.
