# Phase 1 report — Handshake: ApiVersions + Metadata

**Goal:** advertise the version matrix, handle the v0
`UNSUPPORTED_VERSION` downgrade, and return single-broker metadata. Gate: a
franz-go client's metadata request succeeds (and `kcat -L` lists the broker).

## Implemented

- **kmsg codec layer** (`internal/protocol/codec.go`): `DecodeRequest` (typed
  request decode, skips flexible request-header tagged fields) and
  `EncodeResponse` (correlation id → response-header tagged fields for flexible
  responses → body), with the **ApiVersions exception** — its response header is
  always v0. Dependency added: `github.com/twmb/franz-go/pkg/kmsg`.
- **Version profiles populated** (`internal/protocol/versions.go`): `ApiVersions`
  and `Metadata` ranges for majors 1–4 (v1:{AV≤1,MD≤6} … v4:{AV≤3,MD≤12}). An API
  is advertised only once a handler serves it. Added `Profile.RangeFor`.
- **`handleAPIVersions`**: builds the advertised set from the active profile;
  implements the downgrade — a version above our max returns a v0 response with
  `UNSUPPORTED_VERSION (35)` and our ranges so the client retries.
- **`handleMetadata`**: single broker (self), `controllerId=self`, cluster id;
  requested topics resolve to `UNKNOWN_TOPIC_OR_PARTITION` (no topics exist until
  phase 2's store). Broker helpers: `nodeID`, `clusterID`, `advertisedPort`.
- **`Server.Bind`**: split bind from accept so callers/tests learn the bound port
  under an ephemeral `:0`. `main` still calls `Serve` (binds implicitly).

## Acceptance

- **`TestPhase1_MetadataViaFranzGo`** (`go test ./...`, PASS): an unmodified
  franz-go client completes the ApiVersions handshake and a Metadata request
  against a fresh v4 broker, seeing exactly one broker at `localhost:<port>` with
  `controllerId=self` and zero topics. `TestPhase1_ClassicProfileBoots` covers the
  v1 profile.
- **Downgrade dance** (manual raw probe): `ApiVersions` at v99 → v0 response,
  `error_code=35`, correlation id echoed, our api-key ranges included.
- `go build`, `go vet`, `gofmt` clean.

## Deviations / notes

- `kcat` is not installed in this environment; the franz-go (kgo) test is the
  authoritative client gate. `kcat -L` remains a manual check where available.
- `advertisedPort` derives from the listen address when unset; an ephemeral `:0`
  listen with no `--advertised-host`/port override would advertise port 0 — fine
  for tests (which use a fixed free port), documented for real runs.
- Metadata does not yet consult the metadata store (stubbed until phase 2); it
  returns an empty topic set by design.
