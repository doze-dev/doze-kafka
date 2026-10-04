# Contributing

doze-kafka is a Kafka-compatible broker for local development. The test for a change
is: *can a client on the socket tell the difference?* If not, it probably does not
belong.

## The inner loop

```sh
go tool task check        # the gate: gofmt, go vet, Windows cross-vet, the whole suite with -race
go tool task test:unit    # ~seconds: storage, codec, handlers; no sockets (-short)
go test ./internal/broker -run TestThing -count=1     # one test
go tool task dev          # run the broker with the console read live from disk
go tool task hooks:install  # run the gate before every push
```

`task` is a Go tool dependency, so there is nothing to install beyond Go.

Every change lands with a test that **fails without it**. For a bug, write that test
first and watch it fail.

## Layout

| Path | What |
|---|---|
| `cmd/doze-kafka` | The CLI: flags, config, start-up, the `.doze` zone (`names.go`, a stub on Windows). |
| `server/` | The public embed API; the CLI is built on it. See [docs/embedding.md](docs/embedding.md). |
| `internal/protocol` | Framing, request/response codec, the version profiles, the connection loop. |
| `internal/broker` | One handler per Kafka API, the two group coordinators, auth. |
| `internal/storage/log` | The append-only log: segments, index, recovery, retention. |
| `internal/storage/meta` | bbolt: topic registry, committed offsets, producer ids. |
| `internal/config` | `Config`, defaults, validation, the TOML file layer. |
| `console/` | The web console. A *client* of the broker: imports nothing from `internal/`. |
| `internal/kmsg` | A patched copy of franz-go's `kmsg`, used by the broker's own code; see its `PATCHES.md`. |
| `docs/` | User documentation. `docs/reports/history/` is the build history. |

[docs/architecture.md](docs/architecture.md) explains how the pieces fit.

## Tests

- Almost every broker test boots a real broker on a loopback socket and drives it with
  a real client (`internal/broker/harness_test.go`: `boot`). Prefer that to mocking.
- Tests that need a broker skip under `-short`; the gate runs them all.
- Name test files after behaviour (`group_classic_test.go`), not after the phase that
  added them.
- Fuzz targets live next to the codec; a crasher goes in `testdata/fuzz/` so it stays
  fixed. `go tool task fuzz` runs them; `soak` and `chaos` run the long harnesses.

## Adding a Kafka API

An API is advertised only once something serves it. Today that is four places, and a
test fails if they disagree:

1. **`internal/protocol/versions.go`**: add its key and `[min,max]` to each profile
   that should offer it. (Check franz-go's `kversion` for the real ranges.)
2. **`internal/broker/<api>.go`**: write the handler; decode with `kmsg`, answer with
   `kmsg`.
3. **`internal/broker/broker.go`**: register it in `Register`.
4. **`docs/api-support.md`**: add its row (the version table is printed from the
   profiles; regenerate it rather than editing by hand).

Then test it with a raw `kmsg` request at the minimum, a middle and the maximum
version, and with a real client if one exercises it.

## The console

`DOZE_KAFKA_CONSOLE_DEV=./console` (what `task dev` sets) makes the console read
`templates/` and `static/` from disk on every request. There is no build step: the
front-end libraries are vendored (`console/static/VERSIONS`).

Three guards keep it consistent, and they fail with instructions: every template class
must be styled, every `var(--x)` defined, font sizes come from the `--fs-*` tokens, and
inline styles are on a ratchet (`console/cssguard_test.go`). Every GET route must render
(also with the broker down) and every POST must refuse a foreign origin and fail with a
4xx (`console/surfaces_test.go`); both read the route table from `console.go`, so a new
route is covered automatically.

## Commits

One logical change per commit, `area: what changed`, and say what you measured or
verified in the message. Do not weaken a test to make it pass; if a test is wrong, say
why in the commit.
