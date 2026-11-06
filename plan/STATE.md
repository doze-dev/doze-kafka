# State (updated 2026-11-06)

Stages 3 (Topics) and 4 (Groups) are merged to main (PR #3, 2026-11-06, all CI green after fixing three flaky tests and moving klauspost/compress to v1.20.2 for GO-2026-6865). Stage 5 (Schemas) started on branch `schemas-stage5` with the owner's approval.

## Stage 5, Schemas (plan/stages/schemas.md)
- Done, row 1 and part of row 3 (red first, then fixed): SCH-1 (diamond references resolved once each, cap 1,000 schemas), SCH-2 (JSON nesting of Avro and JSON Schema schemas capped at 256; Protobuf measured linear and left alone), SCH-3 (bodies 8 MiB, 64 KiB for /config and /mode, 413), SCH-12 (one registry per Server), SCH-18 (subject over 16 KiB is 42207; the exact rule waits for R7). Tests: `schemaregistry/bounds_test.go`, `server` TestSchemaRegistryIsOneInstance.
- Closed before this stage by the foundation: SCH-4 (Host guard, internal/hostguard) and SCH-11 (registry serves https with the broker's certificate).
- Known residue of SCH-2: a wide Avro schema at depth 128 costs about 0.6 ms per deep field (8 MiB body, about 1.6 s worst case) because the comparison is hamba's and cannot be interrupted; row 4 (our own Avro checker for SCH-6's structured reasons) removes it.
- Done, row 2 (most of it): recorded R1 to R4, R6 to R8, R10, R12 to R14 and one found on the way (avro-attributes-on-unnamed-types) against cp-schema-registry 7.7.1 with apache/kafka 4.3.1 (Docker network `sr-rec`, containers `sr-rec-kafka`, `sr-rec-sr` on 127.0.0.1:18081; `SR_RECORD_ONLY` records named scripts only). Replay is green: every difference fixed or listed in `knownDifferences` with its reason and in docs/schema-registry.md. Fixed from them: SCH-5 (415), SCH-31 (READONLY refuses level change), READONLY answers a held schema, soft delete drops level and mode, subject rules (42208), /schemas offset and limit, fetchMaxId, verbose empty messages, 40402 on a missing subject's version, JSON Schema reference names scoped per schema, no depth limit on references beyond 1,000 schemas, Avro unnamed types drop doc/aliases/name. Answers that change the plan: Confluent ignores limit/offset on /subjects (SCH-8's console bound must be the console's own); its compatibility check ignores READONLY (SCH-14 is a console matter, the registry check stays as is); ids are never reused and version numbers are (R1 matches already).
- Done: R11 recorded (`protobuf-descriptor-with-import`, with json_name and multi-option printing found on the way) and SCH-7 fixed; stored schemas are also indexed under the current canonical form (`indexCurrentForms`), so an upgrade does not give a re-registered schema a new id.
- Done: R5 recorded (`basic-authentication`; second container `sr-rec-sra` on 127.0.0.1:18082, alice:alice-secret, JAAS files copied in with docker cp because Docker Desktop does not share the scratchpad; `SR_RECORD_AUTH_URL`), SCH-9 (one log record per request), SCH-27 (server test of the wiring; docs say authenticates, does not authorise), SCH-38 (slowed repeated failures).
- Still to record: R9 (Confluent's verbose reasons for every incompatible corpus pair, for SCH-6).
- Still to add from SCH-7: the compat cell (Python ProtobufSerializer on a message importing another file).
- Done: SCH-10 (registry read timeout 30 s, idle 2 min; header bytes stay at Go's 1 MiB default, which a 16 KiB subject escaped three times fits).
- Next: R9 and SCH-6 (row 4), with SCH-14/SCH-34 (one selection loop for register and check).

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
