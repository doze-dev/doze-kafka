# Orientation (read this first; it replaces re-reading the whole plan)

Single-binary Kafka-compatible broker in Go, with a server-rendered web console built to the 137-board design (https://claude.ai/artifact/DFbFCyDEEbRsyFP7DRQuvx). Staged plan: `plan/README.md`, `00-foundation.md`, `01-protocol.md`, `stages/*.md`. Open those only for the stage you are working on.

## Rules
- The design is the specification. Never fake data.
- Make the better call, not the cheaper or faster one. Nothing left open or weak.
- A stage is done only when a reviewer who did not build it verifies each board on the live page.
- Tests are written red first.
- Claims about Kafka behaviour must be `recorded` against real `apache/kafka:4.3.1` in Docker, or cited as `spec`/`doc`. Nothing from memory.

## Layout
- `internal/protocol` wire, `internal/broker` handlers and coordinators, `internal/storage/{log,meta}`, `console/` UI, `server/` embed API, `cmd/doze-kafka` CLI.
- `conformance/`: records scenarios against Docker Kafka, replays against in-process doze-kafka. Claims in `conformance/claims/*.md`; Evidence format `conformance/recordings/<file>.json #<step>=<path>:<value>`, one file per row. Code comments cite `// claim: <file> <id>`.
- Replay sends each request once. Use `Env.Await` to wait without recording; `DoUntil` retries only against real Kafka.
- `e2e/design/`: board-vs-live design gate (shared `lib/`, `shell/`, `homes/`, `traffic/`, `boards/` with SHA256SUMS, allow-lists with reasons).

## Gates (`go tool task <name>`)
check, coverage, lightness, mutate, claims, api, conformance, conformance:stable, design, test:e2e, test:e2e:identity, test:compat, evidence, gate-report, docs:check, bench:smoke, vuln.
Run only the packages you touched while working. Run the full set once, at the end, on a quiet machine.

## Working rules (to save effort)
- Reports back: one page. Put detail in the repo.
- Give each worker its own files; run overlapping work in sequence.
- Do not re-run a suite a worker just ran on the same tree.
- Use a smaller model for mechanical work (assertions for survivors, claim checks); the large model for review and durability.
