# Auth report — SASL (PLAIN + SCRAM) & TLS

**Goal (optional auth):** faithful SASL handshakes
(`SaslHandshake`/`SaslAuthenticate`) for PLAIN and SCRAM-SHA-256/512, plus TLS on
the listener. The identity backend is a config map — the *handshake* must be
correct on the wire; the store behind it can be trivial. Fully client-verified
against franz-go. Not a roadmap phase — a separate, optional workstream that
composes with Phases 0–5.

## What ships

- **Two new APIs**: `SaslHandshake` (17, advertised 0–1) and `SaslAuthenticate`
  (36, advertised 0–2), registered and advertised **only when auth is configured**.
  franz-go keys off the presence of `SaslAuthenticate` (36) in `ApiVersions` to use
  the request-wrapped handshake flow, so both appear together.
- **Mechanisms**: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` — any subset enabled by
  config, advertised in `SaslHandshake`'s supported-mechanisms list.
- **TLS**: the listener is wrapped in `tls.NewListener` when a cert/key pair is
  configured. SASL and TLS are independent and compose (SASL-over-plaintext, TLS
  alone, or both).

## How it's wired

- **Connection gate** (`protocol/conn.go`, `protocol/server.go`): a SASL-required
  server marks each connection `authRequired`. The serve loop rejects (drops) any
  request that isn't `ApiVersions`/`SaslHandshake`/`SaslAuthenticate` until the
  connection is authenticated. The per-connection SASL state — negotiated
  mechanism, opaque in-flight conversation, authenticated flag — lives on `Conn`
  behind exported accessors; the SASL handlers (in the broker package) drive it via
  `req.Conn`.
- **SaslHandshake handler** (`broker/auth.go`): returns the enabled mechanisms and,
  for an enabled request, records the mechanism on the connection;
  `UNSUPPORTED_SASL_MECHANISM` otherwise.
- **SaslAuthenticate handler**: runs one token exchange through the authenticator.
  PLAIN completes in one step; SCRAM in two (client-first → server-first,
  client-final → server-final). Failure returns
  `SASL_AUTHENTICATION_FAILED` + message and leaves the connection gated; the final
  successful step sets `authenticated`.
- **PLAIN** (`broker/auth.go`): parses `[authzid] NUL authcid NUL password`,
  constant-time compares against the config credential.
- **SCRAM** (`broker/scram.go`): full RFC 5802 server side. At startup each user
  gets a random salt and a derived verifier — `SaltedPassword = PBKDF2(password,
  salt, 4096)`, `StoredKey = H(HMAC(salted,"Client Key"))`, `ServerKey =
  HMAC(salted,"Server Key")` — so no plaintext password is retained and no PBKDF2
  runs per connection. Verification recovers `ClientKey = ClientProof XOR
  HMAC(StoredKey, AuthMessage)` and checks `H(ClientKey) == StoredKey`; the server
  proves itself back with `ServerSignature`. PBKDF2 is stdlib `crypto/pbkdf2`
  (Go 1.24+), so no new dependency on the broker side.
- **Config** (`config/config.go`): `SASLMechanisms`, `Credentials` (user→password),
  `TLSCertFile`/`TLSKeyFile`; helpers `AuthEnabled()`, `TLSEnabled()`,
  `TLSConfig()`. `Validate` rejects unknown mechanisms, SASL-without-credentials,
  and a half-configured TLS pair.
- **Flags** (`cmd/doze-kafka`): `--sasl-mechanisms PLAIN,SCRAM-SHA-256,...`,
  repeatable `--sasl-user user:password`, `--tls-cert`, `--tls-key`.

## Acceptance (all franz-go, over a real socket)

- **`go test -race ./...` PASS.**
- `TestAuth_PlainRoundTrip`, `TestAuth_ScramSHA256RoundTrip`,
  `TestAuth_ScramSHA512RoundTrip`: a franz-go client authenticates with the
  mechanism (`kgo.SASL(plain/scram …)`) and does a real create→produce→consume.
- `TestAuth_WrongPasswordRejected`: a bad SCRAM password fails the request.
- `TestAuth_UnauthenticatedRejected`: a client with no SASL configured is dropped on
  its first non-SASL request (the gate).
- `TestAuth_TLSWithPlain`: a self-signed cert (SAN `127.0.0.1`/`localhost`) with
  `kgo.DialTLSConfig` verifying against the cert pool — TLS **and** SASL together,
  real round trip.
- `TestAuth_TLSRejectsPlaintextClient`: a plaintext client fails against the TLS
  listener.
- `TestAuth_DisabledDoesNotAdvertiseSASL`: no auth → SASL APIs absent from
  `ApiVersions` and normal traffic works (regression guard).
- **CLI smoke**: the binary boots with `--tls-cert/--tls-key` and all three
  mechanisms (`SASL required`, `TLS enabled` logged), shuts down cleanly, and
  `--sasl-mechanisms` without `--sasl-user` is rejected by validation.

## Deviations / notes

- **SASLprep** (RFC 4013) is not applied to usernames/passwords — only the SCRAM
  `=2C`/`=3D` username unescaping. ASCII credentials (the local-dev norm) are
  unaffected; a non-normalized Unicode password would need to match byte-for-byte.
- **No ACLs / authorization** — authentication only, by design. An
  authenticated principal may do anything; `authzid` in PLAIN is ignored.
- **Legacy SASL v0 bare-token framing** is not implemented; we advertise
  `SaslHandshake` from v0 but only the request-wrapped flow (what every modern
  client, franz-go included, uses) is served. Channel binding (`-PLUS`) is rejected.
- **Credentials are plaintext in config/flags** — appropriate for a local-dev
  fixture; there's no credential store, rotation, or hashing-at-rest for PLAIN.
- **Delegation tokens, OAUTHBEARER, GSSAPI/Kerberos** are out of scope.
- Client validation is franz-go only (as with the other phases); kcat/Java are the
  manual smoke clients.
