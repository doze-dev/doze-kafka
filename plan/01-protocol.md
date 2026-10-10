# The protocol every stage goes through

Seven steps, in this order, for every stage. The stage documents in
[stages/](stages/) fill in the specifics; this page is what they all share.

A **stage** is one surface of the console together with the broker code under
it: the same unit the design is organised by (Traffic, Topics, Groups, Schemas,
Trace, Access, Cluster, Connect, Map, Bad days). The API and the console are
finished together, because a screen that is right about code that is wrong is
the failure this plan exists to prevent.

## The evidence standard

Every statement about how Kafka (or Confluent's Schema Registry) behaves carries
where it came from. There are four sources, and they are not interchangeable.

| Source | Meaning | Good enough to |
|---|---|---|
| `spec` | The wire protocol's own message definitions (the `kmsg` structs and the JSON specs they are generated from): fields, versions, flexible or not, nullable, tagged, error codes. For the registry: its published REST reference. | Enforce a shape and name its error. |
| `recorded` | A scenario run against a **real** `apache/kafka:4.0.0` (or Confluent SR 7.7.1) in Docker and stored under `conformance/recordings/`, replayed without Docker. | Settle a question. This is the oracle. |
| `doc` | The Kafka documentation or a named KIP, with the page or number. | Implement against, with the claim queued for recording. |
| `memory` | Somebody (a person or a model) recalls it. | Nothing. It goes on the recording queue and is not acted on until it is `doc` or `recorded`. |

**Docker is available here, so the recording queue is not a wish list.** The
`doze-kafka` Schema Registry work recorded about 1,500 schema pairs from a real
Confluent registry; the same method (script the call, record the answer, commit
it, replay it in Go) is how a Kafka claim becomes `recorded`. A stage whose
claims stay at `memory` has not finished step 2.

Every statement about how doze-kafka behaves is `run` (reproduced against a
built binary, or held by a test) or `read` (from the code). A `read` finding is
reproduced before it is fixed.

## The seven steps

### 1. Contract

List every Kafka API the stage's surface touches, and every console route, and
give each a tier.

- **F, functional.** It works locally and its effect is observable.
- **C, cosmetic.** Accepted and round-tripped with no local effect (a
  replication factor above 1 is reported as 1; a quota is stored and not
  applied). The ledger, and the console wherever the setting is shown, say so.
- **S, stub.** Refused honestly: the API is not advertised in `ApiVersions`, or
  the request is answered with the error real Kafka gives when the feature is
  off (`UNSUPPORTED_VERSION`, `UNSUPPORTED_FOR_MESSAGE_FORMAT`,
  `SECURITY_DISABLED`, `CLUSTER_AUTHORIZATION_FAILED`), never a dropped
  connection and never a success. Listed in `docs/limitations.md` with the
  scenario that shows it.

Nothing may be absent and nothing may be silently wrong: an API that returns
success and does nothing is C only if it says so, otherwise it is a bug. The
contract is checked in as the stage's rows of `docs/api-support.md`, which the
version-matrix test already reads (a row for every advertised key).

### 2. Red first

Every finding in the stage document becomes a failing test before any fix, in
the lowest layer that can show it: a store or handler test, a raw `kmsg` test,
a franz-go or Sarama client test, a `compat/` cell, or a browser test. A
finding nobody can make fail is not a finding yet; it goes back to "unverified"
with what was tried. Every fix is checked with a **mutation by hand**: revert
the fix, see the test fail, put it back (this has been the rule on
`audit-hardening` and the commits say which test discriminates).

### 3. Fix the code

Make the tests pass in the package that owns the behaviour. This includes
bounds on everything read from the network, error codes on every version in the
advertised range, state that survives a restart, and behaviour under both group
protocols and with authentication on and off.

### 4. Prove it

The gates, all green:

| Gate | Command | What it shows |
|---|---|---|
| Format, vet, Windows vet, race tests | `go tool task check` | The tree builds on all three platforms and the unit suites pass under `-race`. |
| Coverage floor | `go tool task coverage` (foundation B4) | No package's statement coverage fell below `testdata/coverage.txt`, and each package the stage owns reached the target in its document. |
| Mutation | `go tool task mutate:check -- ./pkg/...` (foundation B3) | The tests notice when the code is wrong, not only when it runs. No new surviving mutant against the recorded baseline. |
| Real clients | `go tool task test:compat` (the cells the stage names) | Java, librdkafka, KafkaJS, Streams, Spring, Connect, the registries and the two UIs still pass, plain and SCRAM over TLS. |
| Fuzz | `go tool task fuzz` | Every parser of client-supplied bytes the stage touches has a target, and it ran clean. |
| Browser | `go tool task test:e2e` | The stage's pages, with the route gate on (foundation B8). |
| Weight | `go tool task lightness` (foundation B9) | Idle heap, binary size and the module list did not move, or the commit says why. |
| Public surface | `go tool task api` (foundation B10) | The `server/` embedding API has no export that appeared or vanished by accident. |

### 5. Build the console

Implement the stage's page group from the design (the boards are named in the
stage document, with the template and handler each lands in). Every board
exists; every `new` item is built from the data it says it is built from;
every teaching line comes from a settings table or the ledger (foundation B7),
not from prose typed into a template.

### 6. Ledger and documents

`docs/api-support.md` and `docs/limitations.md` match the contract. The
relevant guide (`console.md`, `observability.md`, `transactions.md` …) matches
the behaviour. `CHANGELOG.md` names each behaviour change. The stage document's
findings table has every row closed or moved to "owed to integration" or
"recording queue" with a reason.

### 7. Hand over

Commit to `main`, CI green on every workflow (`ci`, `e2e`, `kcat`, `compat`,
`fuzz`). Update the status table in [README.md](README.md). Record the actual
time against the estimate.

## Acceptance criteria common to every stage

A stage is done when every box here and every box in its own document is
ticked. "N/A" is allowed only with a sentence saying why.

### Contract

- [ ] Every Kafka API the surface touches has a tier in `docs/api-support.md`,
      and the version-matrix test passes.
- [ ] Every C row is visible as cosmetic wherever the console shows that setting.
- [ ] Every S row answers as real Kafka does (or is not advertised) and is in
      `docs/limitations.md` with the scenario that shows it.
- [ ] Every console route the surface owns is in the route table (foundation B8)
      with its method, whether it mutates, and the test that drives it.

### Behaviour

- [ ] Every error code the surface can return is the one real Kafka returns
      for the same input, on every version in the advertised range, or the
      difference is in `docs/limitations.md`. Each is `spec` or `recorded`.
- [ ] Nothing real Kafka refuses is accepted. Each "accepted where Kafka
      refuses" finding is closed, or is on the recording queue as `memory` with
      the test written and skipped pending the recording.
- [ ] Every list-shaped answer pages or is bounded, in the API and in the console.
- [ ] Create, update and delete behave as Kafka does for a repeat, a missing
      target and a conflicting target.
- [ ] Everything read from the network is bounded (length, count, nesting,
      decompressed size) and a malformed input closes one connection and logs,
      never the process.
- [ ] State survives a restart: a data-dir test writes, restarts and reads, and
      a SIGKILL variant does the same for acknowledged writes.
- [ ] Works with authentication off and on, with TLS off and on, on a non-default
      listen address and port, and with a data directory whose path contains a
      space.
- [ ] Works under both consumer-group protocols where groups are involved.
- [ ] No identity is hard-coded: the surface's tests pass with the listener on
      `127.0.0.1:0` and the console under a non-default prefix.

### Proof

- [ ] Every finding in the stage document has a test that failed before its fix.
- [ ] All gates in step 4 are green.
- [ ] Coverage: each package the stage owns meets the target in its document
      (a target is never below the baseline in [README.md](README.md)).
- [ ] Mutation: no surviving mutant in the stage's packages that a reasonable
      test would kill; survivors that are left are listed with a reason.
- [ ] Differential: every F API family has a scenario run against a real broker
      and replayed in Go, and the replay reports zero "answered differently".
      The count still at `memory` or `doc` is written in the stage document.

### Console

- [ ] Every board in the stage's page group is implemented, or the document says
      which were dropped and why.
- [ ] Each refusal state shows the real error name and message in the
      foundation's vocabulary (Refused, Denied, Not implemented, Failed).
- [ ] Every destructive action confirms, and each confirm's sentence is true
      (a test asserts what the confirm says it will delete is what the request
      deletes).
- [ ] Every settings field carries the teaching line generated from the
      settings table, and a test fails for a form field with no record.
- [ ] Every `new` item works from the data its board names, or is cut from the
      board with a note.
- [ ] No console text contradicts the ledger. Every hint in the stage's
      templates was read against the code.
- [ ] The command preview on a form states exactly the request the form makes.
- [ ] `test:e2e` passes with the route gate on; the accessibility sweep is clean
      on the stage's pages; keyboard paths (`j` `k` Enter `/` `c` `p` Esc) work
      on its list and forms; the pages render at 960 px and 390 px.
- [ ] The pages work when the broker is down (foundation A7), when it is
      unreachable mid-poll, and with an empty data directory.

### Close

- [ ] `docs/api-support.md`, `docs/limitations.md`, the relevant guide and
      `CHANGELOG.md` updated.
- [ ] Findings table: every row closed, or moved with a reason.
- [ ] "Owed to integration" and "Recording queue" are complete enough that
      somebody else could pick them up.
- [ ] Committed to `main`, CI green, status table updated, actual time recorded.

## Severity

| | Meaning |
|---|---|
| **P0** | Loses or corrupts data, acknowledges a write that is not durable, hangs or kills the broker, lets an unauthorised request through, or makes a normal client's normal configuration fail. |
| **P1** | A parity gap a real client can observe, with no data wrong: a wrong error code, a missing API a framework calls, a limit real Kafka does not have. |
| **P2** | The console shows something false, loses data on a round trip, or cannot do what the API can. |
| **P3** | Wording, dead copy, a stale comment or document. |

Every finding is fixed; none is dropped for being small. P0 and P1 are never
deferred out of a stage. P2 and P3 may move to a later stage only when the fix
lives in a file that stage owns, and they are closed there.

## The shape of a stage document

Every file in [stages/](stages/) has these sections, in this order, so any two
can be read side by side.

1. **Summary**: what the surface is, its size, its peers, the packages it owns.
2. **Contract**: the API and route table with current and target tier, and what
   is declined.
3. **Where it stands**: the tests, scenarios and browser specs that exist, with
   the per-package coverage today.
4. **Findings**: the table: id, severity, what, where (`file:line`), evidence
   (`run` or `read`), source, the test to write first.
5. **What to check**: the stage-specific checklist beyond the common criteria.
6. **Console**: each board, the template and handler it lands in, the `new`
   items and what they are built from, and where each teaching line comes from.
7. **Tests to add**: by layer.
8. **Recording queue**: claims that are `doc` or `memory`, with the scenario that
   would record each.
9. **Owed to integration**: what needs a second stage to be true.
10. **Acceptance criteria**: the stage's own boxes, with coverage targets, on top
    of the common ones.
11. **Files and order of work**: the breakdown, and prior art (another broker, a
    KIP, Redpanda, Confluent) where someone solved the same problem.
