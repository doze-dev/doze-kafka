# State (updated 2026-10-29)

Local main is ahead of origin and unpushed. Stages 0 to 2 fixed and merged after the verification pass. The mutation-survivor campaign is finished and merged; it is not to be extended (see below).

## Done since the last update
- Mutation survivors: protocol and the small units 0 open; console 598 lived to 102 (all with reasons); broker data/admin/auth 0 open; broker groups 28 still open (24 in `groupmigrate.go`); storage log about 97 left (baseline stale, partly re-measured) and meta.
- Three real bugs fixed red-first: empty group id never adopted (`meta`), aborted record at the tape window edge, refused count and "N of M" on the Traffic page not refreshing (Go test only; no browser check yet).
- `scripts/mutate` unmutated-suite cap raised from 10 to 60 minutes.

## Next (confidence for release), in order
1. One full gate on a quiet machine: check, coverage, lightness, conformance, conformance:stable, design, test:e2e, test:e2e:identity, evidence, docs:check, bench:smoke, vuln. This is the first run on the merged tree. Coverage floors and lightness budgets may need re-recording. Speed up the 300 s test (`TestEveryFileOperationFailingOrCutLosesNothingAcknowledged`) if it stays slow.
2. `task test:compat` (24 cells, needs Docker) on the merged tree.
3. Soak and SIGKILL chaos run, several hours.
4. Fresh-install check from `docs/getting-started.md` on a clean directory; one green GitHub CI run.
5. Browser check of the Traffic counts fix (Playwright).
6. Small items: re-measure 5,000-partition start-up after an unclean stop (11 to 58 s, noisy); G1/G3 `doze` claims cited from memory (`conformance/claims/groups-requests.md`); `conformance/claims_test.go:13` says 4.0.0; Traf-10 contrast (muted ink applied, owner to confirm); TxnOffsetCommit from outside the group; DeleteGroups/OffsetDelete full-disk codes; `RecoverTransactions` logs a completed transaction at Debug; other repos' CHANGELOG lines and doze `go mod tidy`.

## Decisions (owner, 2026-10-24 to 28)
- Stop extending mutation testing; the remaining survivors stay listed with reasons.
- Each piece of work has a time limit (about 1 to 2 hours); over that, come back with the choice.
- Stays in Go.

## Needs explicit approval
Pushing to origin; deleting remote `zone-hardening` in doze-names; starting Stage 3; starting workers.

## Housekeeping
Many stale worktrees under `.claude/worktrees/`; clean up when no worker needs them.
