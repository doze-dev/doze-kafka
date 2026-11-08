# State (updated 2026-11-08)

Stages 3 (Topics) and 4 (Groups) are merged to main (PR #3, 2026-11-06). Stage 5 (Schemas) is done but for the items below, on branch `schemas-stage5`, draft PR #4 (open as a draft only so CI runs; not to be merged until the stage is done).

## Stage 5, Schemas (plan/stages/schemas.md): where it stands
All seven P1 findings are closed: SCH-1, 2, 3, 4, 5, 6, 7. Rows 1 to 5 of section 11 are done (the plan's "smallest useful cut"), with these P2/P3 findings closed as well: SCH-9, 10, 11, 12, 18, 27, 31, 37 (subject rules), 38.

- Bounds (row 1, 3): references resolved once each (cap 1,000 schemas, no depth cap); Avro and JSON Schema nesting capped at 256 JSON levels; bodies 20 MiB for schemas, 64 KiB for /config and /mode (413); subjects over 16 KiB or with control characters or named __GLOBAL refused (42208); one registry per Server; read timeout 30 s, idle 2 min. Tests: `schemaregistry/bounds_test.go`, `server` TestSchemaRegistryIsOneInstance, `cmd` TestRegistryTimesOutSlowAndIdleClients.
- Recordings (row 2): R1 to R14 all recorded against cp-schema-registry 7.7.1 on apache/kafka 4.3.1: 14 new scripts in `schemaregistry/testdata/conformance` (`scenarios_stage5_test.go`, `scenarios_protodeps_test.go`) and R9 as `testdata/compat/*-reasons.json`. Replay is green; every deliberate difference is in `knownDifferences` (conformance) or `corpusReasonDifferences` (reasons) with its reason, and in docs/schema-registry.md "Where it differs".
- Reasons (row 4, SCH-6): 2,287 of 2,291 recorded refusals give Confluent's reasons word for word (Avro all 501; JSON Schema 1,043 of 1,045; Protobuf 739 of 741); the conformance replay compares 409 details and verbose messages in full. Avro now uses our port of Avro's SchemaCompatibility (`avrocompat.go`), not hamba's check.
- Content type, READONLY, auth (row 5): 415 for non-JSON bodies (blocks cross-origin form posts); READONLY answers a held schema with its id and refuses a level change; soft delete drops level and mode; basic auth recorded; one log record per request; failed sign-ins slowed after five.
- Also fixed on the way: Avro unnamed types drop doc/aliases/name; Protobuf json_name and multi-option printing; stored schemas indexed under the current canonical form so an upgrade does not mint new ids; JSON Schema reference names scoped per schema; /schemas offset and limit; fetchMaxId; a flaky storage retention test (CI); the lightness binary budgets raised with reasons.

### Done since (2026-11-07 to 08)
All 19 boards (Sc-Home, Sc-01 to Sc-18) are built; 17 are held by the design gate (`e2e/design/schemas`, 19 tests), Sc-10 and Sc-13 by console tests with their deviations in plan/00-foundation.md. `e2e/tests/schemas.spec.ts` has 21 tests. Every finding is closed; plan/stages/schemas.md section 12 gives the status of each with the test that holds it. Overnight on 2026-11-08: the delete confirms checked against a real registry (they miscounted soft-deleted versions and called a shared id lost; fixed), the registry-down page (it said "Can't reach the broker"; fixed), SIGKILL and registry-off `cmd` tests, the registry under the second identity, kcat Avro decode and the two UIs listing subjects in the compat matrix (with a libserdes patch for GitHub's Ubuntu 26.04 runner), the register message (SCH-33), the subject list without schema text (SCH-8), coverage floors at the targets (schemaregistry 92, console 90), and the schemaregistry mutation baseline re-recorded with reasons.

### Left in Stage 5, for the owner
2. Console mutation run: about 8 hours on 2 workers; not started, by the 2026-10-24 decision (work over 1 to 2 hours comes back as a choice).
3. Compat cells not written: a wrong basic-auth password, `use.latest.version`, `normalize.schemas`, a Java READONLY refusal (Python has one), the UIs rendering a decoded value; and the decode differential over the corpus pairs.
4. Review and merge PR #4 (the owner merges it).

### For the owner to decide
- Decided 2026-11-08 (owner): the idle-heap gate judges the median of the starts, not the largest. Before that, on CI (linux/arm64) the median was steady across Stage 5 (1,370,872 to 1,373,640) while the largest went from 1,374,136 to 1,384,824 and failed the gate once (commit 7842097) with nothing grown. Budgets recorded before then are the largest of 20 starts and come down to the median when next recorded.
- Nothing blocking. Re-estimate after Stage 5: rows 1 to 5 took one long session (2026-11-06/07) against 13 planned half-days.
- Docker: the recording containers were removed (2026-11-07). To record again: apache/kafka:4.3.1 and cp-schema-registry:7.7.1 on a shared network, the registry on 127.0.0.1:18081 (SR_RECORD_URL), and for `basic-authentication` a second one with a JAAS password file (alice:alice-secret) on another port (SR_RECORD_AUTH_URL); copy the JAAS files in with docker cp, Docker Desktop does not share the scratchpad.

## Earlier state (2026-10-29)

## Done since the last update
- Mutation survivors: protocol and the small units 0 open; console 598 lived to 102 (all with reasons); broker data/admin/auth 0 open; broker groups 28 still open (24 in `groupmigrate.go`); storage log about 97 left (baseline stale, partly re-measured) and meta.
- Three real bugs fixed red-first: empty group id never adopted (`meta`), aborted record at the tape window edge, refused count and "N of M" on the Traffic page not refreshing (Go test only; no browser check yet).
- `scripts/mutate` unmutated-suite cap raised from 10 to 60 minutes.

## Full-gate findings (2026-10-29), all on the merged tree
- Passed: claims, api, docs:check, coverage, lightness (after the toolchain re-record), conformance replay, bench:smoke, evidence, test:e2e:identity (259), vuln (after Go 1.27.3), test:compat (24 of 24, after the telemetry scraper fix).
- Fixed: setClientMetrics races Kafka (conformance:stable found four telemetry recordings changing); Sy-08 and every brand-label board depended on the checkout directory's name (OwnBroker now passes --name); Go 1.27.3 for GO-2026-6809; compat telemetry scraper was stale against Stage 2's Traffic markup.
- OPEN: `endtxn-while-preparing` is not stable against real Kafka 4.3.1, and the cause is found: the scenario assumes `min.insync.replicas=2` on one broker keeps transaction markers from being written, but Kafka accepts `acks=all` produces to such a topic (547 of 547 probes), so a transaction is in Prepare only for the milliseconds the markers take, and the recording caught that window on some runs. Waiting for the config does not help. Q11b (`conformance/claims/bad-days.md`) now says so. To close: find a way to hold a transaction in Prepare on real Kafka (none known), or drop the recorded claim and keep Q11b as the protocol's definition, and take the scenario out of `conformance:stable`. `conformance:stable` fails until then.
- Evidence screenshots (`plan/evidence`) are regenerated by `task evidence` and differ from the committed ones for the same directory-name reason; regenerate them in one deliberate commit from a checkout named doze-kafka, or leave them.
- Second `check` on the quiet machine, Go 1.27.3: passed (the conformance scenario that failed once under load passes). Soak 20 min and chaos 30 min with SIGKILL restarts: passed. Fuzz 60 s per target: found a real Avro bug (a union value as decoded would not encode again), fixed with a test and the corpus file. After the fixes: lightness, coverage, test:e2e (259 + design), test:e2e:identity (259), vuln, test:compat (24 of 24) all pass.
- Flake: `tests/keys.spec.ts` "behind the key sheet a topic ignores p, ], [, j, k and ⌘↵" failed once in three identity runs (7 s, `toBeVisible`); passes otherwise. Make it wait for the sheet, not for time.
- Fresh install from a clean clone following `docs/getting-started.md`: works. 5,000-partition restart re-measured (docs updated). G1 to G4 recorded against Kafka 4.3.1 (scenario `group-requests-answers`; G4 was wrong and doze changed: a replaced parked JoinGroup is no longer answered, as Kafka does). Not run: a GitHub CI run.

## Still open for release confidence (rest of this list is done above)
1. One full gate on a quiet machine: check, coverage, lightness, conformance, conformance:stable, design, test:e2e, test:e2e:identity, evidence, docs:check, bench:smoke, vuln. This is the first run on the merged tree. Coverage floors and lightness budgets may need re-recording. Speed up the 300 s test (`TestEveryFileOperationFailingOrCutLosesNothingAcknowledged`) if it stays slow.
2. `task test:compat` (24 cells, needs Docker) on the merged tree.
3. Soak and SIGKILL chaos run, several hours.
4. Fresh-install check from `docs/getting-started.md` on a clean directory; one green GitHub CI run.
5. Browser check of the Traffic counts fix (Playwright).
6. Small items: Traf-10 contrast (muted ink applied, owner to confirm); TxnOffsetCommit from outside the group; DeleteGroups/OffsetDelete full-disk codes; `RecoverTransactions` logs a completed transaction at Debug; two group differences from Kafka found while recording G1 to G4 and not fixed (a group that only saw a MEMBER_ID_REQUIRED join is Empty in Kafka and Dead in doze; after a rejoin, DescribeGroups in CompletingRebalance still shows the old MemberAssignment in Kafka and "" in doze); other repos' CHANGELOG lines and doze `go mod tidy`.

## Decisions (owner, 2026-10-24 to 28)
- Stop extending mutation testing; the remaining survivors stay listed with reasons.
- Each piece of work has a time limit (about 1 to 2 hours); over that, come back with the choice.
- Stays in Go.

## Needs explicit approval
Pushing to origin; deleting remote `zone-hardening` in doze-names; starting Stage 3; starting workers.

## Housekeeping
Many stale worktrees under `.claude/worktrees/`; clean up when no worker needs them.
