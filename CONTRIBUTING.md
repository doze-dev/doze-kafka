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
| `cmd/doze-kafka` | The CLI: flags, config, start-up, the `.doze` instance name (`names.go`, `listen.go`, `dnsready.go`; no setup on Windows yet). |
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
- Most tests that need a broker skip under `-short` (those of the command and the
  embed API do not); the gate runs them all.
- Name test files after behaviour (`group_classic_test.go`), not after the phase that
  added them.
- Fuzz targets live next to the codec; a crasher goes in `testdata/fuzz/` so it stays
  fixed. `go tool task fuzz` runs every target in `testdata/fuzz-targets.txt` for 30s
  each (`FUZZ_TIME=5m` for longer; `task fuzz -- Scram` for some); a new parser of
  untrusted bytes gets a target there and a row in `docs/architecture.md` (a test
  fails otherwise). `soak` and `chaos` run the long harnesses.
- Tests notice when the code is wrong, not only when it runs: `go tool task
  mutate:check -- ./internal/protocol` fails on a surviving mutant that
  `testdata/mutation.json` does not list (`mutate:record` after killing or
  understanding one; `mutate:list` shows the units, the broker is split by file
  group). It takes minutes to hours, so it runs weekly in CI, not in `task check`.
- The embedding API (`server/`) is pinned: `go tool task api` fails when its exported
  surface changed without a `CHANGELOG.md` line containing "server API"
  (`task api:record` writes the new surface).
- The console in a real browser is checked by the Playwright suite in `e2e/`
  (`go tool task test:e2e`; needs `bun`). It builds the binary, boots it on its own
  ports over a fresh data directory, and seeds it (`e2e/seed`). It runs in CI as its
  own workflow, not in `task check`. A console change that the Go tests cannot see
  (focus, the keyboard, live updates, layout) gets a spec there.

## Adding a Kafka API

There is one table, `protocol.APIs` in `internal/protocol/versions.go`. `ApiVersions`
answers from it, the router refuses anything not in it, and tests hold the rest to it.

1. **`internal/protocol/versions.go`**: add a row: the key, and the `[min,max]` it is
   offered at for each Kafka major (`none` where that major does not have it). Check
   franz-go's `kversion` for the real ranges.
2. **`internal/broker/<api>.go`**: write the handler against its own types,
   `func (b *Broker) handleX(ctx, req, q *kmsg.XRequest, resp *kmsg.XResponse) error`.
   Decoding, making the response at the right version and framing it are done for you
   (`typed` in `broker.go`); fill in `resp` and return nil.
3. **`internal/broker/broker.go`**: add it to `handlers()`. A row with no handler
   stops the broker starting and fails `TestEveryAPIInTheTableHasAHandlerAndTheReverse`.
4. **`docs/api-support.md`**: describe it in the first table, then regenerate the
   version table: `UPDATE_API_DOCS=1 go test ./internal/protocol -run TestAPISupportDoc`.
   The same test fails whenever the document and the table differ.

Error codes are the protocol library's (`kerr.UnknownTopicOrPartition.Code`), not a list
kept here; the list that used to be kept here had one number wrong.

Then test it with a raw `kmsg` request at the minimum, a middle and the maximum
version, and with a real client if one exercises it.

## The console

`DOZE_KAFKA_CONSOLE_DEV=./console` (what `task dev` sets) makes the console read
`templates/` and `static/` from disk on every request. There is no build step: the
front-end libraries are vendored (`console/static/VERSIONS`).

Guards keep it consistent, and they fail with instructions: every template class
must be styled, every `var(--x)` defined, font sizes come from the `--fs-*` tokens, and
inline styles are on a ratchet (`console/cssguard_test.go`); every text colour holds
4.5:1 on every surface in both themes (`TestThemeContrast`). Every GET route must render
(also with the broker down) and every POST must refuse a foreign origin and fail with a
4xx (`console/surfaces_test.go`); both read the route table from `console.go`, so a new
route is covered automatically.

## Commits

One logical change per commit, `area: what changed`, and say what you measured or
verified in the message. Do not weaken a test to make it pass; if a test is wrong, say
why in the commit.
