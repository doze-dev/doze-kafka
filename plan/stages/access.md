# Stage: Access (ACC)

Boards: Ac-Home and Ac-01 to Ac-09. Foundation items are cited as `A2`, `B3` and so on ([foundation](../00-foundation.md)). The shape and the severity scale are in [the protocol](../01-protocol.md).

## 1. Summary

**The surface.** Who may do what on the broker: the ACL list, creating and deleting ACLs, the "could this principal do this?" checker, the refusals log, the principals page, and the default (`--acl-default`, super users). Underneath it sit SASL (PLAIN, SCRAM-SHA-256, SCRAM-SHA-512), TLS, the principal a connection carries, and the check every one of the 40-odd served Kafka APIs makes before it acts.

**Size.** The enforcement core is small and good: `internal/broker/authorizer.go` is 199 lines, `acls.go` about 290, `auth.go` 170, `scram.go` 215. The console today is one read-only page (`console/access.go`, `templates/access.html`, one unit test, one 12-line browser spec). The design asks for ten boards, seven of which need something that does not exist yet.

**Peers.** Redpanda and Confluent Control Center show ACLs as a table with a create form; none of them has a "could X do Y" checker or a refusals log, so the design goes past them. Kafka's own `kafka-acls.sh` is the reference for every field and every error.

**Packages owned.** `internal/broker` (`authorizer.go`, `acls.go`, `auth.go`, `scram.go`, the per-API checks and `TestEveryServedAPIIsAuthorized`), `internal/protocol` (the SASL gate, the pre-auth frame cap, TLS listener, `Conn.Principal`), `internal/storage/meta/acls.go`, `internal/config` (the ACL and SASL validation), `cmd/doze-kafka` (`provisionConsoleCredential`, the flags), `console/access.go`, `templates/access.html`, `docs/authorization.md`, `docs/auth-and-tls.md`.

**Verdict after the audit.** The rules are right where they were read against Kafka's published ones, and an ACL survives a restart (`TestACLsPersistAndAreEnforcedAfterARestart`). But the tests prove less than they look: of nine hand-made mutations of "which operation does this API need", **eight survived the whole authorization suite** (ACC-1). Three real defects were found: the authorizer can silently stop following the stored ACLs (ACC-2), `ListGroups` shows a group the principal is denied (ACC-3, reproduced), and `Metadata`/`DescribeCluster` never fill in authorized operations (ACC-4). The console is a super user with no login of its own (ACC-5), and creating ACLs from it makes that louder. Ten of the ten boards are new work; the new backend is: an explain function on the authorizer, a denial record, and a principals source. About 16 working days.

## 2. Contract

### Kafka APIs

| Key | API | Now | Target | Note |
|---|---|---|---|---|
| 29 | DescribeAcls (v0-3) | F | F | Needs Describe on the cluster. Filter semantics read against Kafka's `AclBindingFilter` (ANY, MATCH, null name); v0/v1 pattern default is queued (R7). |
| 30 | CreateAcls (v0-3) | F | F | Needs Alter on the cluster. Validation errors are `INVALID_REQUEST` with a message; texts queued (R6). |
| 31 | DeleteAcls (v0-3) | F | F | Needs Alter on the cluster. Atomic per call; a filter that matches nothing is not an error. |
| 17 | SaslHandshake (v1 only) | F | F | v0 refused by design (docs). |
| 36 | SaslAuthenticate (v2) | F | F | A failure ends the connection. `SessionLifetimeMillis` is 0: no re-authentication (ACC-12). |
| 18 | ApiVersions | F | F | Exempt from authorization (`authzExempt`). |
| 50, 51 | Describe/AlterUserScramCredentials | S (not advertised) | S | SCRAM users live in the config only; there is no API to add one (see section 5). |
| 38-41 | Delegation tokens | S (not advertised) | S | In `limitations.md`. |
| 48, 49 | Describe/AlterClientQuotas | S (not advertised) | S | Not this stage. |
| all others in `internal/protocol/versions.go` | the authorization check each makes | F | F | Table of required operation per API is in section 5. `GetTelemetrySubscriptions` and `PushTelemetry` are exempt on purpose (KIP-714). |

Tiers: ACLs are F (stored, enforced, restored). `--acl-default` is F. TLS client-certificate authentication (`ssl.client.auth`, a principal taken from the certificate) is **absent and not in `docs/limitations.md`** (ACC-24); it should be an S row.

### Console routes (prefix `p` is `/_console` by default)

| Route | Method | Mutates | Now | Target | Note |
|---|---|---|---|---|---|
| `p/access` | GET | no | F, flat list | F, grouped list with filters | Boards Home, 01, 02. |
| `p/access/new` | GET, POST | POST: yes (CreateAcls) | absent | F | Board 03, 04. Sends as `User:__console`. |
| `p/access/delete` | POST | yes (DeleteAcls) | absent | F | Board 09. The ACL is identified by its seven fields, never an index. |
| `p/access/check` | GET, POST | no | absent | F, new backend | Board 05. Pure function; no side effects, no traffic row marked as a refusal. |
| `p/access/refusals` and `/live` | GET | no | absent | F, new backend | Board 06. Polled partial. |
| `p/access/principals` | GET | no | absent | F, new backend | Board 07. |
| `p/access/default` | GET | no | absent | F | Board 08. Read-only: the default is set by a flag, not at run time. |
| `p/api/palette` entries | GET | no | one entry (Access) | add Create ACL, Check, Refusals | `console/console.go:271`. |

Declined: editing an ACL in place (Kafka has no such call; delete and create), changing `--acl-default` or super users from the console (they are start-up settings and the board says so), managing SASL users (no API; ACC-14).

## 3. Where it stands

**Coverage today (full suite, 2026-10-11).** `internal/broker` 89.0% (27.5% under `-short`; `TestEveryServedAPIIsAuthorized` skips under `-short`), `internal/protocol` 71.9%, `internal/storage/meta` 80.7%, `internal/config` 61.6%, `cmd/doze-kafka` 79.8%, `console` 87.8%.

Function level, from `go test ./internal/broker -run 'Auth|ACL|Authz|Authoriz|Transactional|Handshake|IdempotentWrite|Default' -coverprofile`: `authorizer.go` is 100% on every function. `acls.go`: `validate` 50.0%, `matches` 88.9%, `AnnounceACLs` 83.3%, `handleCreateACLs` 84.6%, `handleDeleteACLs` 75.6%. `auth.go`: `plainStep` 87.5%, `step` 90.0%, `handleSASLAuthenticate` 81.8%. `scram.go`: `newSCRAMCredential` 75.0%, `randomNonce` 75.0%, `scramFirst` 81.5%, `scramFinal` 82.6%, `deriveSCRAM` 85.7%. `internal/protocol/conn.go`: `SetPrincipal`, `RemoteIP`, `SetAuthenticated`, `SASLState`, `SetSASLState`, `SetSASLMechanism` 0.0% inside that package's own tests (the broker package covers them from outside; the number is low, not the behaviour), `server.go` `UseTLS` 0.0%.

**Tests that touch the surface.**

| Layer | What exists |
|---|---|
| Unit | `authorizer_internal_test.go`: `TestAuthorizerFollowsKafkasRules`, `TestIdempotentWriteIsImpliedByWritingSomewhere`. |
| Raw kmsg / client | `acls_test.go`: `TestCreateACLsRejectsEntriesThatAreNotConcrete`, `TestACLsRoundTripWithFilters`, `TestACLsPersistAndAreEnforcedAfterARestart`. `authz_test.go`: `TestCreatedACLsTakeEffect`, `TestDefaultDenyClosesWhatNoACLAllows`, `TestTransactionalIDsAreAuthorized`. `authz_apis_test.go`: `TestEveryServedAPIIsAuthorized` (default deny, one unauthorized principal, one super user, every API). `auth_test.go`: eleven `TestAuth_*` (PLAIN, both SCRAM, wrong password, unknown user, unsupported mechanism, no handshake, TLS, plaintext to a TLS port, SASL off). `protocol/server_test.go`: pre-auth frame cap, idle budget, trickle. |
| Console | `TestAccessPageListsACLsUnderTheEnforcedBanner` (one). |
| Browser | `e2e/tests/access.spec.ts` (one test: banner, rail, palette, "None stored"). |
| Compat | All 20 cells use one user (`dev`, SCRAM-SHA-512 over TLS) and **no ACL exists in any cell**. |
| Fuzz | None for `plainStep`, `scramFirst`, `scramFinal`, `scramAttrs`, the ACL filter. |
| Differential | None. No recording of real Kafka exists (foundation B2). |

## 4. Findings

Severity per the protocol. "run" means reproduced against a built binary or by a hand mutation in a scratch copy (`$SCRATCH/mut`, the repo is untouched); "read" means from the code. Source `spec` means the wire definitions, `doc` the Kafka documentation, `memory` goes on the recording queue (section 8) and is not acted on before it is recorded.

| ID | Sev | Finding | Where | Ev. | Src | Test to write first |
|---|---|---|---|---|---|---|
| ACC-1 | P1 | The authorization tests do not pin which operation each API needs. Hand mutations that survived `Authz\|ACL\|Authoriz\|Transactional\|Default\|Sarama\|Auth`: ListOffsets Describe to Read, JoinGroup Read to Describe, Heartbeat Read to Describe, OffsetCommit Read to Describe, DeleteGroups Delete to Read, CreatePartitions Alter to Write, DeleteRecords Delete to Write, EndTxn Write to Describe. Killed: Produce, Fetch, Metadata (by `TestCreatedACLsTakeEffect`). `TestEveryServedAPIIsAuthorized` only shows the API refuses under default deny. | `internal/broker/authz_apis_test.go:278`, handlers in `listoffsets.go:30`, `groups.go:72,134`, `offsets.go:22`, `groupdelete.go:29`, `createpartitions.go:36`, `deleterecords.go:29`, `txnapis.go:274` | run | memory (the tests), spec | Raw kmsg, table-driven: for every case in `authzCases()` add `needs int8` (the operation) and, per API, grant ALLOW of only that operation then assert success, and grant ALLOW of every other operation (not implying it) and assert the refusal code. Re-run the nine mutations; all must die. |
| ACC-2 | P1 | `refreshAuthorizer` swallows the error from `ListACLs`: if it fails after `AddACLs` or `RemoveACLs` succeeded, the in-memory authorizer keeps the old set while `CreateAcls` and `DeleteAcls` answer success. A new DENY is then not applied (an unauthorised request passes) and a deleted DENY still applies, until restart. Reachable when the store has an entry that fails to unmarshal or the database is closing. | `internal/broker/acls.go:127-131` | read | spec | Unit, with a `meta.Store` wrapper that fails `ListACLs` once: create a DENY, assert the response is an error code (not 0) and that a following `allowed()` refuses. Fix by applying the accepted/removed entries to the in-memory copy under `acls.mu` and returning `UNKNOWN_SERVER_ERROR` if the refresh fails. |
| ACC-3 | P1 | `ListGroups` lists a group the principal is denied. With `--acl-default allow` and `DENY ALL on Group g1 for User:ANONYMOUS`, `DescribeGroups g1` returns `GROUP_AUTHORIZATION_FAILED` (30) and `ListGroups` still lists g1, because the cluster-level Describe fallback is true when no ACL is about the cluster. Reproduced. Real Kafka's rule is queued (R1). | `internal/broker/groupadmin.go:156` | run | doc (Kafka docs list "Describe on Cluster or Group"), memory (the fallback) | Raw kmsg: the repro in `$SCRATCH/acc/main.go`. Expect the group absent; if the recording says the cluster fallback is real, change the test to assert it with a `DENY Describe cluster`. |
| ACC-4 | P1 | Authorized operations are never computed. `DescribeCluster` always answers `-2147483648` with the comment "we have no ACLs"; Metadata topic/cluster and DescribeGroups do not fill them either. A Java `Admin.describeTopics(..., includeAuthorizedOperations)` or `describeCluster().authorizedOperations()` returns nothing useful when ACLs exist. | `internal/broker/configs.go:152` (stale comment and sentinel); no use in `metadata.go`, `groupadmin.go` | read | spec (the fields exist) | Raw kmsg: with `ALLOW Read` on a topic, Metadata with `IncludeTopicAuthorizedOperations` must have the Read and Describe bits set, Write clear. Layer: add `authorizer.operationsFor(resource)`; the checker (ACC-27) shares it. |
| ACC-5 | P1 | The console is a super user and has no login. With SASL on, `provisionConsoleCredential` makes `User:__console` a super user; every HTTP request that passes the host and Origin guard acts as it. On `--console-addr 0.0.0.0:...` anyone on the network can read every topic and delete anything; the new Create/Delete ACL buttons would let them rewrite the access rules. Not said in `auth-and-tls.md`, `authorization.md` or `console.md`. | `cmd/doze-kafka/main.go:452`, `console/console.go:157-170`, `console/hostguard.go` | read | spec | Unit in `cmd/doze-kafka`: start with SASL on and `--console-addr 0.0.0.0:0`; assert a start-up warning line and that `POST p/access/new` is refused with 403 unless a flag opts in. Also e2e: the buttons are absent off loopback. Decision needed from the owner (section 9). |
| ACC-6 | P2 | The refusals are mis-attributed for authentication failures: a failed `SaslAuthenticate` resets the principal to "" before the observer reads `Principal()`, so the traffic row says `User:ANONYMOUS`. A refusals log or principals page built on traffic would count a wrong password as anonymous traffic. | `internal/protocol/conn.go:305`, `internal/broker/auth.go:163-164` | read | spec | Raw kmsg: wrong SCRAM password, then assert the recorded observation's principal is empty or the attempted user, not `User:ANONYMOUS`. |
| ACC-7 | P2 | The traffic recorder keeps only the first non-zero error of a response, so a Produce to two topics where the first is `UNKNOWN_TOPIC_OR_PARTITION` and the second is denied records no denial. The refusals log cannot be derived from `ErrorCode` alone. | `internal/protocol/summary.go:426`, `internal/protocol/observe.go:41` | read | spec | Unit on `firstError`/`Describe`: a response with codes 3 then 29 yields both. Becomes the `Denied []Resource` field of `Summary` (section 6, new backend N2). |
| ACC-8 | P2 | `ListGroups`'s authorization row in the guide omits the cluster-Describe fallback the code has: "shows the groups that may be described". | `docs/authorization.md` (What each API needs), `groupadmin.go:156` | read | doc | Doc test: the table row and the code agree (a test greps both for "cluster"). Settles with R1. |
| ACC-9 | P2 | No audit trail for access changes. `CreateAcls` and `DeleteAcls` log nothing; start-up logs "ACLs are enforced" only. The wire dock is in memory (2,000 requests). A changed rule cannot be traced after the fact. | `internal/broker/acls.go:146-290` | read | spec | Unit with a log handler: creating and deleting an ACL each produce one INFO line naming principal, API, and the entries (count and first). |
| ACC-10 | P2 | SASL log lines carry no user and no peer address: `SASL authentication failed mechanism=... err=...` and `succeeded mechanism=...`. Guessing attempts cannot be seen per source. | `internal/broker/auth.go:162,169` | read | spec | Unit with a log handler: the failure line has `remote` and, for SCRAM and PLAIN, the attempted user (not the password). |
| ACC-11 | P3 | No pacing of repeated failures: a failed attempt closes the connection, but a client can reconnect at once, 2,048 at a time (`defaultMaxConns`). Local-development risk is low; still undocumented. | `internal/protocol/server.go:16`, `auth.go:157-165` | read | spec | Raw: 200 sequential failures finish in under X seconds today; the test pins the chosen pause (for example 100 ms after the third consecutive failure from one IP) once decided. Or document it. |
| ACC-12 | P3 | `SaslAuthenticate` always answers `SessionLifetimeMillis` 0, so credentials removed from the config (needs a restart anyway) never expire a live connection and KIP-368 re-authentication is never triggered. Honest for a dev broker; not stated. | `auth.go:168-170` | read | doc | Raw: assert 0 and add one sentence to `auth-and-tls.md`. |
| ACC-13 | P3 | The comment says an unknown SCRAM user gets "the same answer as a wrong password: do not confirm which users exist", but the unknown user fails at step 1 while a known user gets a server-first message. A prober can tell the two apart. Real Kafka also fails at step 1 (memory), so this is parity; the comment is false. | `internal/broker/scram.go:120` | read | memory | Fix the comment, or answer with a deterministic fake salt (RFC 5802 section 5.1 discusses it). Raw test either way. |
| ACC-14 | P2 | Credentials exist only as plaintext in the config, flag or file. `--sasl-user alice:pw` is visible in `ps`; PLAIN keeps passwords in memory; SCRAM verifiers are re-derived at every start with a new random salt (4,096 iterations, `scramIterations`) and live only in memory. The users page cannot list users the broker was not asked about, and there is no `AlterUserScramCredentials` (S). Docs are honest about plaintext ("the broker itself still has it"), silent about `ps`. | `internal/broker/auth.go:31-65`, `scram.go:17`, `cmd/doze-kafka/cli.go:359,415` | read | doc | Doc test: `auth-and-tls.md` names the config file as the way to keep a password out of `ps` (it does) and says users cannot change at run time. Add `--sasl-user-file` only if the owner wants it (section 9). |
| ACC-15 | P3 | PLAIN ignores the authorization id: `[authzid] NUL authcid NUL passwd` is split into three and `parts[0]` is never checked. Kafka refuses an authzid that is non-empty and different from the user (memory). | `auth.go:109-118` | read | memory | Raw: authzid `bob`, authcid `alice`, right password: Kafka's answer (R5). |
| ACC-16 | P2 | Parsers of pre-auth, client-controlled bytes have no fuzz target: `plainStep`, `scramFirst`, `scramFinal`, `scramAttrs`, `scramUnescape`. | `scram.go:89-220`, `auth.go:108` | read | spec | Fuzz: `FuzzSASLPlain`, `FuzzSCRAMClientFirst`, `FuzzSCRAMClientFinal` (property: never panics, never returns done=true without the right proof). Add to `task fuzz`. |
| ACC-17 | P3 | Pre-auth frame cap is 1 MiB with a comment that says everything a stranger may send "is well under a kilobyte". 64 KiB would do; 2,048 connections times 1 MiB is 2 GiB of potential buffers before a login. | `internal/protocol/frame.go:18-22`, `conn.go:220` | read | spec | Server test: a pre-auth ApiVersions padded to 100 KiB is refused; SaslAuthenticate with a 70 KiB token is refused. |
| ACC-18 | P3 | `meta.ACL`'s doc comment says "They are NOT enforced: the broker never consults them". False since enforcement shipped. | `internal/storage/meta/acls.go:17-18` | read | doc | Doc test not needed; a lint check for the phrase "NOT enforced" in `internal/` (a `grep` in `task check`). |
| ACC-19 | P3 | The route comment says the Access page is shown "with the banner that they are not enforced". False. | `console/console.go:271` | read | doc | As ACC-18. |
| ACC-20 | P2 | The Access page fails whole when the console's own principal may not Describe the cluster (no auth and an ACL on the cluster resource, or `--acl-default deny` without a super user): `ListACLs` returns `CLUSTER_AUTHORIZATION_FAILED` and `c.fail` shows a generic error, on the one page that would explain it. | `console/access.go:44-52` | read | spec | Console test with a fake backend returning that error: the page renders the Refusal (A2) naming `--acl-super-user`, not an error banner. |
| ACC-21 | P3 | `openByDefault := true` is the fallback when `allow.everyone.if.no.acl.found` is absent from the broker configs, so the page can say "open to everyone" when it does not know. | `console/access.go:61-68` | read | spec | Console test: broker config without the key renders "unknown", not "open". |
| ACC-22 | P2 | Board claim "N requests refused in the last hour" (Home, Ac-02) is not true to the data: the recorder holds the last 2,000 requests in memory, with no time window. On a busy broker 2,000 requests are minutes. | boards Ac-Home, Ac-02, Ac-06; `internal/traffic/traffic.go` | read | spec | Console test: the copy says "among the last 2,000 requests, since HH:MM:SS" and the time is the oldest retained row. |
| ACC-23 | P2 | Board Ac-09 states consequences it cannot know: "g-order-validator goes from lag 0 to growing lag" and the list of requests that "start being refused". Those are a forecast from the last 2,000 requests, and what is refused also depends on other ACLs that cover the same topic (a prefixed or `*` ACL). | board Ac-09 | read | spec | The confirm test required by A6: the sentence lists only requests seen in the recorder, labelled "seen recently", and the count equals what `Explain` (N1) says would now be refused when those requests are replayed. |
| ACC-24 | P3 | TLS client-certificate authentication (mTLS, principal from the certificate) is not offered, not refused clearly, and not in `limitations.md`. A client configured for it connects (the server never asks for a certificate) and becomes `User:ANONYMOUS` or its SASL user. | `internal/config/config.go:460`, `docs/limitations.md` | read | doc | Doc test: a row in `limitations.md` and `api-support.md`; client test: franz-go with a client cert connects as `User:ANONYMOUS`. |
| ACC-25 | P3 | The TLS certificate is read once at start-up; no reload and no warning when it expires. With TLS but no SASL the first read happens under the 10-minute idle budget, not the 30-second pre-auth one, so a slow TLS handshake holds a connection slot for ten minutes. | `internal/protocol/conn.go:200-215`, `config.go:452` | read | spec | Server test: with TLS and no auth, a client that opens a TCP connection and sends nothing is dropped within `frameTimeout`. |
| ACC-26 | P2 | `CreateAcls` accepts a principal type that can never match (`Group:eng`, `RoleX:a`) and an empty name (`User:`). Kafka's CLI also lets it through (memory), so the broker is as Kafka; the console form must not. Board Ac-03 says "Only User: names exist here". | `internal/broker/acls.go:104-106` | read | memory | Console test: the form refuses `Group:eng` with a Refusal that names the rule; broker test records R6. |
| ACC-27 | P2 | The Create form (Ac-03) offers three operations (Read, Write, All) and four resource types. The API has 12 operations and 6 resource types; the cluster resource cannot meaningfully take Read or Write, and Create, Delete, Alter, Describe, DescribeConfigs, AlterConfigs, IdempotentWrite, ClusterAction are the ones the board's own text (Ac-01, Ac-04) depends on ("Alter on the cluster"). | board Ac-03, `authorizer.go:34-45` | read | spec | Console test: the form's operation list per resource type equals the table in `docs/authorization.md`; a ledger test fails for an operation the broker enforces and the form omits. |
| ACC-28 | P2 | A principal typed with a mistake (`User:chekout`) is stored without a word; with SASL on the broker knows the user list and could say "no such user". | `acls.go:98-122`, board Ac-03 | read | spec | Console test using `AccessSource.Users()`: a warning (not a refusal: an ACL for a future user is legal). |
| ACC-29 | P2 | `Permission DENY` with host `*` and principal `User:*` etc. is legal and can lock everything including the console (without SASL). The confirm on Create does not simulate the effect on `User:ANONYMOUS`. | board Ac-03, `console/access.go` | read | spec | Console test: creating a DENY that matches the console's own principal shows the lock-out sentence and requires the second confirmation. |
| ACC-30 | P3 | Linear authorization scan. `allowed()` loops over every stored ACL on every check, and a Fetch or Produce checks every partition. Fine at tens of ACLs; with Terraform-scale sets (thousands) the cost is per request. No benchmark exists; and the count is unbounded: each `CreateAcls` may carry up to a 1 MiB frame of entries, and every `DeleteAcls` re-reads and unmarshals all of them under the lock. | `authorizer.go:110-146`, `acls.go:251-262`, `meta/acls.go:56-69` | read | spec | Benchmark `BenchmarkAllowed_{10,1000,20000}ACLs` in `bench_test.go`; budget recorded in B9. If over, index by `(resourceType)` then name. |
| ACC-31 | P3 | The ACL key joins seven fields with NUL (`meta/acls.go:30`); names containing NUL can make two distinct entries collide. | `internal/storage/meta/acls.go:30-35` | read | spec | Fuzz `FuzzACLKey`: distinct entries, distinct keys. Or reject NUL in `validateCreation`. |
| ACC-32 | P2 | Hosts are compared as written. Kafka compares against `InetAddress.getHostAddress()`, which for IPv6 is the long form (`0:0:0:0:0:0:0:1`); doze-kafka gives `::1`. An ACL copied from a Kafka runbook for an IPv6 client would not match. | `internal/protocol/conn.go:134-143`, `authorizer.go:107` | read | memory | Raw over `[::1]`: an ACL with host `0:0:0:0:0:0:0:1` and with `::1`; the recording (R4) says which Kafka matches. |
| ACC-33 | P2 | The ACL surface has no real-client test. The 20 compat cells run with no ACL; no cell runs `kafka-acls.sh`, the Java `Admin` ACL calls, Terraform's provider, or a client that gets refused and must show the real exception. | `compat/` | read | spec | Compat cell `acl-java`: `kafka-acls.sh --add`, `--list`, `--remove`; a consumer refused with `TopicAuthorizationException`; a producer refused with the same; then allowed. Plain and SCRAM-over-TLS. |
| ACC-34 | P2 | The browser suite has one Access test (banner and empty state). Nothing drives a non-empty list, a keyboard path, a narrow width, the empty-vs-filtered distinction, or a destructive confirm. | `e2e/tests/access.spec.ts` | read | spec | See section 7, e2e. |
| ACC-35 | P3 | `writesSomewhere` grants `IdempotentWrite` to a principal with any ALLOW Write on a topic but does not consider a DENY on that same topic, nor a prefixed or `*` ACL by default. Real Kafka's exact rule is queued (R8). | `authorizer.go:149-158` | read | memory | Unit: ALLOW Write on `a`, DENY Write on `a`, `InitProducerId` without a transactional id: expected result from R8. |
| ACC-36 | P3 | The authorization guide's "Limits" says "no CIDR ranges", correct, but does not mention that host matching is by the address as the broker sees it (behind Docker NAT or a name-based loopback it is the gateway), which makes host ACLs unreliable in the Docker case the compat suite uses. | `docs/authorization.md` | read | doc | One sentence and a test that the sentence appears. |

**How the audit was done.** Read `authorizer.go`, `acls.go`, `auth.go`, `scram.go`, `meta/acls.go`, `conn.go`, `frame.go`, `main.go` (console credential), `console/access.go` and its template, every `canTopic/canGroup/canCluster/canTxnID` call site (grep, 52 sites), and the guide. Ran the existing authorization tests under a function-level coverage profile. Built the binary and ran `$SCRATCH/acc/main.go` (franz-go kmsg) for ACC-3. Hand mutations (ACC-1) were applied in a scratch copy of the module and reverted by copying the original back; the repo was not touched. Not found despite looking: a race in `acls.mu` use (create/delete take it; describe reads the store directly, which is safe), an ACL lost on restart (`TestACLsPersistAndAreEnforcedAfterARestart` covers it and the store write is one bbolt transaction), a leak of the SCRAM stored key, and a way past the pre-auth gate (`preAuthAllowed` is a short list).

## 5. What to check

**Kafka's rules against the implementation** (`authorizer.go`). Read against Kafka's documented rules; each "?" goes to the recording queue before it is relied on.

| Rule | doze-kafka | Confidence |
|---|---|---|
| Super users are never refused | yes (`supers[principal]`) | doc |
| A matching DENY wins over any ALLOW | yes, evaluated in the same loop | doc |
| DENY matches the exact operation or ALL | yes (`acl.Operation == op \|\| ALL`) | memory (R2) |
| ALLOW of Read, Write, Delete, Alter implies Describe; AlterConfigs implies DescribeConfigs | yes (`implied`) | doc |
| Resource applies by literal name, `*` literal, prefix | yes (`covers`) | doc |
| Default applies only when no ACL covers the resource, whatever its principal or operation | yes (`applies` is set before the principal test) | doc; a prefixed-only cover (R3) |
| `User:*` matches every principal | yes | doc |
| Host `*` or equal string | yes; IPv6 form differs (ACC-32) | memory (R4) |
| Cluster resource name is `kafka-cluster` | yes, enforced on create | spec |
| A DENY on the cluster blocks the idempotent shortcut | yes (`deniedOnCluster`) | memory (R8) |

**Required operation per API** (docs/authorization.md, confirmed against the call sites). The last column is `doc` when the Kafka documentation's authorization table says it, else the recording scenario that settles it.

| API | doze-kafka needs | Real Kafka | Settled by |
|---|---|---|---|
| Produce | Write topic; Write txn id when transactional | same | doc |
| Fetch | Read topic | same | doc |
| ListOffsets | Describe topic | same | doc |
| Metadata (named topic) | Describe topic; Create cluster or topic to auto-create | same | doc |
| CreateTopics | Create cluster or topic; refusal code `TOPIC_AUTHORIZATION_FAILED` | code on topic-level refusal | R10 |
| DeleteTopics | Delete topic; for an unknown topic | `TOPIC_AUTHORIZATION_FAILED` | R11 |
| CreatePartitions | Alter topic | same | doc |
| DeleteRecords | Delete topic | same | doc |
| DescribeConfigs / AlterConfigs / IncrementalAlterConfigs | DescribeConfigs / AlterConfigs on topic, or cluster for broker | same | doc |
| DescribeCluster | Describe cluster | same | doc |
| InitProducerId | Write txn id; else IdempotentWrite cluster or Write on any topic | the second part is memory | R8 |
| AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit | Write txn id (+ topic Write, group Read) | same | doc |
| FindCoordinator | Describe group or Describe txn id | same | doc |
| DescribeGroups, ConsumerGroupDescribe, OffsetFetch | Describe group (+ Describe topic for offsets) | same | doc |
| ListGroups | Describe group, or Describe cluster | cluster fallback is memory | R1 |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat | Read group | same | doc |
| OffsetCommit | Read group, Read topic | same | doc |
| DeleteGroups | Delete group | same | doc |
| OffsetDelete | Delete group, Read topic | same | doc |
| DescribeProducers | Read topic | same | doc |
| DescribeTransactions, ListTransactions | Describe txn id | same | doc |
| DescribeAcls | Describe cluster | same | doc |
| CreateAcls, DeleteAcls | Alter cluster | same | doc |

**Stage-specific checks.**

- Denial codes per API: `TOPIC_AUTHORIZATION_FAILED` (29), `GROUP_AUTHORIZATION_FAILED` (30), `CLUSTER_AUTHORIZATION_FAILED` (31), `TRANSACTIONAL_ID_AUTHORIZATION_FAILED` (53). `TestEveryServedAPIIsAuthorized` asserts one code per API; add the per-version check for the first and last advertised version (v0 Metadata has no per-topic error field? run it).
- DescribeAcls and DeleteAcls filters: ANY, MATCH, null name, prefixed name, v0/v1 default pattern type. Both the broker test and the recording (R7).
- ACLs after a restart: exists. Add: a SIGKILL variant after `CreateAcls` returned (B5), and one that checks the authorizer, not only the list.
- SCRAM storage: verifier derived at start, never written to disk (confirm with `strings` over `meta.db`); a test that greps the data directory for the password after a run with SCRAM-only.
- Console `__console`: super user only when SASL is on; renaming collision refused (`provisionConsoleCredential` has the check; test it).
- Limits pre-auth: 1 MiB cap and the 30 s budget are covered by `server_test.go`; add the SASL token size (ACC-17).
- Works with auth off and on, TLS off and on, a non-default port and a data directory with a space: parameterise `startAuthzBroker`.
- Both group protocols: the KIP-848 member that may not Describe a topic gets it left out of its assignment (`groups848.go:30`); test it.

## 6. Console

Foundation pieces (inspector, wire dock, Refusal vocabulary, confirms, route table) are used by name from [the foundation](../00-foundation.md) section A and B.

| Board | Template and handler | Status | Built from |
|---|---|---|---|
| Ac-Home | `templates/access.html` (split to `access_*` partials), `console/access.go` `access` | needs backend | The table is real (`DescribeAcls`). The principal rail with counts needs the principals source (N3). "5 requests refused in the last hour" needs N2. The wire dock is A4 filtered to ACL APIs and denials. "Open the checker" is N1. |
| Ac-01 | same, empty branch | buildable | ACL count 0; default and super users from the broker config (`allow.everyone.if.no.acl.found`, `super.users`). The worked example "with your topics" needs the topic list (exists). The lock-out warning is a rule from `authorization.md` ("Locking yourself out"), rendered from the settings table (B7). |
| Ac-02 | `access` with `?resource=&perm=` | needs backend | Grouping and filters are buildable from the list. The "Because of it, right now" panel (4 refused in 1 h, last request, client) needs N2. "Also true" sentences need N1 (`Explain`). |
| Ac-03 | new `accessNewPage`, `accessCreate` | needs backend (small) | Fields are the `CreateAcls` fields (exists). The "What this will do" sentence needs N1 (does the resource have an ACL after this one, with prefixed and `*` covers counted). The terminal command preview is the request's own seven fields rendered as `kafka-acls.sh` flags; test that it round-trips through the real CLI's argument model (compat cell). |
| Ac-04 | same form, Refusal partial | buildable | The two refusals are the real `INVALID_REQUEST` and `CLUSTER_AUTHORIZATION_FAILED` answers of `CreateAcls`, shown through A2. The "console does not hit this one" paragraph is true only with SASL on; the page states which case it is in. |
| Ac-05 | new `accessCheck` | needs backend | N1: `authorizer.Explain(principal, host, resourceType, name, op, defaultOverride)` returning the decision, the deciding ACL (or "default" or "super user"), and the four-step trace the board prints. Console reaches it through an `AccessSource` interface injected like `TrafficSource`. "A real request like this is refused with GROUP_AUTHORIZATION_FAILED" uses the per-resource code table (section 5). "Compare an allowed request" picks a stored ALLOW. |
| Ac-06 | new `accessRefusals` + live partial | needs backend | N2: a bounded ring of authorization denials written where `can*` returns false. Prefer this to deriving from traffic rows (ACC-7): the ring knows the operation, the resource and the ACL that decided; the traffic row is linked by correlation. Rows grouped by principal, resource, result and client. "Console's own requests are left out" is by client id (`doze-kafka-console*`) and principal, noted on the page. |
| Ac-07 | new `accessPrincipals` | needs backend | N3: `AccessSource.Principals()` from the traffic client list (principal, client id, software, port) joined with the ACL list (rules naming the principal) and N2 (refused counts). "SASL/SCRAM-SHA-512 on" from config. The page also lists configured SASL users who have not connected (the board says it cannot; it can, from the authenticator), so it is more honest than the board. |
| Ac-08 | new `accessDefault` | needs backend | The matrix is `Explain` run twice, once per default (override parameter). The reasons ("its ACL: ALLOW Write", "no ACL: the default decides") come from the explain trace. The command snippet is generated from the flag table (B7). |
| Ac-09 | `accessDelete` confirm | buildable | The DeleteAcls filter is the ACL's seven exact fields. "Read, which also covers Describe" is from `implied`. "These requests start being refused" is replayed through `Explain` against the last requests (ACC-23). Cancel has focus (A6). |

Teaching lines: every form field (principal, host, resource type, name, pattern, operation, permission) gets a settings-table record (A3, B7) carrying the rule from section 5; `--acl-default` and `--acl-super-user` records say start-up only. Counts on the rail (ACL count, per principal) come from the list.

Nothing on these boards is decorative-only: the "new" tags on Create, Checker, Refusals and Principals are the four new backend items below.

### New backend, sized

| | Work | Size |
|---|---|---|
| N1 | `Explain` on the authorizer: refactor `allowed()` so one function returns the decision and its trace, and `allowed()` is the same function minus the trace (no second copy of the rules). Add `operationsFor` for ACC-4. Table-driven test plus a property test against a reference model written independently from Kafka's rules. | 2 days |
| N2 | A denial ring: at each `can*` refusal record time, principal, host, client id, API, version, resource, operation, deciding ACL. Bounded (1,000), in memory, exposed through the existing server wiring. No change on the hot path when allowed. | 1.5 days |
| N3 | Principals source: traffic client list plus configured users plus counts. | 1 day |
| N4 | Console create/delete routes, with the lock-out check and confirm. | 1 day |
| N5 | Console pages for the seven boards. | 3 days |

## 7. Tests to add

**Unit / store.**
- `TestRefreshFailureIsNotSilent` (ACC-2); `TestACLKeyIsInjective` and `FuzzACLKey` (ACC-31).
- `TestExplainAgreesWithAllowed`: for every case in `authorizer_internal_test.go` and a generated set, `Explain(...).Allowed == allowed(...)`.
- `TestAuthorizerMatchesReferenceModel`: random ACL sets and requests against an independent implementation of the rules in section 5.
- `BenchmarkAllowed_*` (ACC-30).
- `TestAccessChangesAreLogged`, `TestSASLLogsNameUserAndPeer` (ACC-9, ACC-10).
- Store: `ListACLs`/`RemoveACLs` on a closed database return errors; two ACLs differing in one field are two entries.

**Raw kmsg.**
- `TestEachAPINeedsExactlyItsOperation` (ACC-1): per API, only the required operation allows, any non-implying other refuses. This is the test that kills the nine hand mutations.
- `TestListGroupsHidesDeniedGroups` (ACC-3), `TestAuthorizedOperationsReflectACLs` (ACC-4).
- `TestFailedSASLIsNotRecordedAsAnonymous` (ACC-6).
- `TestDenialsAreAllRecordedNotJustTheFirst` (ACC-7).
- DescribeAcls/DeleteAcls filters: every combination of ANY, MATCH, null name, prefixed, host, principal, operation (a generated table), and v0 and v1 request shapes.
- `TestPlainAuthzid` (ACC-15), pre-auth token size (ACC-17), SCRAM unknown user path (ACC-13).

**Client library (franz-go, Sarama).** A producer and consumer refused by ACL see `TOPIC_AUTHORIZATION_FAILED` and `GROUP_AUTHORIZATION_FAILED` as the library's own typed error, then succeed after an ALLOW. KIP-848 consumer refused on a topic it cannot Describe.

**Compat cell.** `acl-java` (ACC-33): `kafka-acls.sh` add, list, remove; Java consumer and producer refused and allowed; SCRAM over TLS and plain.

**Fuzz.** `FuzzSASLPlain`, `FuzzSCRAMClientFirst`, `FuzzSCRAMClientFinal`, `FuzzACLFilterMatches` (against the reference model), `FuzzACLKey`.

**Differential vs real Kafka.** The authorization matrix (R9) and the ACL API scenarios (R6, R7) recorded against `apache/kafka:4.0.0` configured with `StandardAuthorizer` (KRaft's authorizer; this is what 4.0 uses), replayed in Go with zero "answered differently".

**e2e (Playwright).** Replace the single spec with: `access.spec.ts` list, grouping, filters, empty vs filtered-empty vs none stored; create ACL (keyboard `c`, `Cmd+Enter`, refusal states, preview equals the request); delete with confirm (Cancel focused; the sentence's list equals the requests refused afterwards, run by a real client); the checker (the four-step trace for allowed, denied by DENY, denied by no-allow, default); refusals page (a client refused produces a row within one poll); principals; the default page. Run with SASL on, under a console prefix, at 960 and 390 px, with the broker stopped.

**Coverage targets (full suite).** `internal/broker` 89.0 to **90.5**, with `acls.go`, `authorizer.go`, `auth.go`, `scram.go` each at least 95% and these functions covered: `validate`, `matches`, `handleDeleteACLs`, `handleCreateACLs` (storage-failure branch), `handleSASLAuthenticate`, `plainStep`, `scramFirst`, `scramFinal`, `newSCRAMCredential`, `randomNonce`. `internal/protocol` 71.9 to **74.0** (a SASL-gate test in-package that exercises `SetPrincipal`, `SetAuthenticated`, `SASLState`, `RemoteIP`, `UseTLS`). `internal/storage/meta` 80.7 to **82.0** (the ACL functions' error branches). `internal/config` 61.6 to **63.0** (the ACL and SASL validation branches, `config.go:378-415`). `console` 87.8 to **88.5**. `cmd/doze-kafka` 79.8 to **80.5** (`provisionConsoleCredential` including the reserved-name refusal). Mutation: `mutate:check` on `internal/broker` (the `authorizer.go`, `acls.go`, `auth.go`, `scram.go` group) must not gain a survivor; the nine ACC-1 mutations must be dead.

## 8. Recording queue

All scenarios run against `apache/kafka:4.0.0` in KRaft mode with `authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer`, `super.users=User:admin`, `allow.everyone.if.no.acl.found=false` (and `true` where noted), SASL/PLAIN and SCRAM listeners. Driven by franz-go or raw kmsg, normalised, stored under `conformance/recordings/`.

| ID | Claim (currently `memory` or `doc`) | Scenario |
|---|---|---|
| R1 | `ListGroups` visibility: does Describe on the cluster show groups the principal is denied on, and which error/omission when it has neither | `acl-list-groups`: DENY Describe on `g1`, with and without a cluster ACL; ListGroups as three principals |
| R2 | DENY of an operation does not deny the operations it would imply (DENY Describe does not stop Read; DENY Read does not deny Describe), and ALL | `acl-deny-ops` |
| R3 | `allow.everyone.if.no.acl.found` when only a prefixed or `*`-literal ACL covers the resource, or only another principal's ACL | `acl-default-covers` |
| R4 | The host string compared for IPv6 and IPv4-mapped clients | `acl-host-ipv6`: client over `[::1]` against both spellings |
| R5 | PLAIN with an authorization id different from the user; the error code and message | `sasl-plain-authzid` |
| R6 | CreateAcls validation answers: pattern MATCH/ANY, empty name, empty host, `User:` with empty name, `Group:x`, cluster name other than `kafka-cluster`, ANY resource/operation; code and message text per case, v0 to v3 | `acl-create-validation` |
| R7 | DescribeAcls/DeleteAcls filters: v0 and v1 pattern default, ANY vs MATCH vs null name, prefixed, repeat delete, delete of a missing ACL | `acl-filters` |
| R8 | InitProducerId: IdempotentWrite versus Write on any topic; effect of a DENY; with a transactional id | `acl-init-producer-id` |
| R9 | The authorization matrix: for each served API, ALLOW exactly one operation and record success or the error code, for every operation (`kafka-acls` in a loop) | `acl-matrix` (the largest, feeds ACC-1) |
| R10 | CreateTopics refused: which error (`TOPIC_AUTHORIZATION_FAILED` or `CLUSTER_AUTHORIZATION_FAILED`) when Create is missing on cluster and on topic, and `validateOnly` | `acl-create-topics` |
| R11 | DeleteTopics for an unknown topic as an unauthorized principal | `acl-delete-unknown-topic` |
| R12 | Authorized-operations bitmaps in Metadata, DescribeCluster, DescribeGroups, with and without ACLs | `acl-authorized-ops` |
| R13 | SCRAM: unknown user fails at step 1 with which code and message; wrong password message; connection closed after failure | `sasl-scram-failures` |
| R14 | `SaslAuthenticate` `SessionLifetimeMs` and the reauth behaviour on a broker without `connections.max.reauth.ms` | `sasl-session` |
| R15 | Does `super.users` appear in `DescribeConfigs` of a broker to a principal with DescribeConfigs | `acl-describe-configs-super` |

## 9. Owed to integration

- **Traffic stage ([traffic](traffic.md)).** The recorder needs a filter by principal and by error code, and a cursor for the dock; N2 writes into the same wire. `Summary` needs a `Denied` list (ACC-7). Wire dock rows linking a refusal to its request use the same correlation id.
- **Groups and Topics ([groups](groups.md), [topics](topics.md)).** Their "Denied" boards (Gr-16, Tp-25) show a refusal; they should link to the ACC checker with the principal, resource and operation pre-filled, and use the same operation table (section 5).
- **Schemas ([schemas](schemas.md)).** The registry is not covered by ACLs: with SASL on, the console reaches it as `__console` with Basic auth, but a registry client has no principal and ACLs on topics do not extend to subjects. Sc-13 (Denied) needs a decision whether the registry has an authorization model at all; today it is authenticated or open.
- **Cluster ([cluster](cluster.md)).** `DescribeConfigs` reports `allow.everyone.if.no.acl.found` and `super.users`; Cl-01 should show them with the Access link; the listeners board (Cl-09) shows TLS and SASL state from the same config this stage reads.
- **Connect ([connect](connect.md)).** Cn-07 (SASL and TLS) prints `sasl.jaas.config` for the user; it must not print the `__console` password and should name the user types in section 5.
- **Bad days ([bad-days](bad-days.md)).** Bad-Auth shows the SASL failure story; it needs ACC-6 and ACC-10 (who, from where) to be true.
- **Foundation asks.** (1) A `Denied` Refusal kind that carries the deciding ACL (A2). (2) A route-table entry for each `p/access/*` route with its mutating flag (B8). (3) The B2 harness must support a SASL listener and `StandardAuthorizer` configuration; the recording scenarios in section 8 depend on it. (4) A settings-table record for `acl-default` and `acl-super-user` (B7). (5) `CHANGELOG.md` does not exist in the repository; the protocol's step 6 assumes it.
- **Owner decisions.** ACC-5 (console off loopback with SASL on: warn, require a flag, or read-only), ACC-14 (a `--sasl-user-file`), ACC-11 (failure pacing).

## 10. Acceptance criteria

The common criteria in [the protocol](../01-protocol.md) all apply. Stage-specific:

**Contract.**
- [ ] `docs/api-support.md` rows for 29, 30, 31, 17, 36 state tier F, and rows exist for 38-41 and 50-51 as S; `go test ./internal/broker -run VersionMatrix` passes.
- [ ] `docs/limitations.md` has the mTLS row (ACC-24) with its scenario (`TestMTLSIsNotRequested`).
- [ ] The route table (B8) lists the seven `p/access*` routes with `mutates` true for create and delete only.

**Behaviour.**
- [ ] ACC-1: `TestEachAPINeedsExactlyItsOperation` passes and the nine hand mutations are dead (`task mutate:check -- ./internal/broker/...`).
- [ ] ACC-2, ACC-3, ACC-4: the named raw tests fail on `main` and pass after the fix.
- [ ] R1 to R15 each recorded or marked unreachable with a reason; the replay reports zero "answered differently" for F rows, and the count still at `memory` is written here.
- [ ] `CreateAcls` and `DeleteAcls` each log one INFO line; the SASL lines carry user and peer.
- [ ] The ACL surface works with SASL off and on, TLS off and on, `127.0.0.1:0`, and a data dir with a space (`go test ./internal/broker -run Authz` parameterised).
- [ ] SIGKILL after `CreateAcls` returns: the ACL is present and enforced after restart (B5 helper).

**Proof.**
- [ ] `go tool task coverage`: `internal/broker` at least 90.5, `internal/protocol` 74.0, `internal/storage/meta` 82.0, `internal/config` 63.0, `console` 88.5, `cmd/doze-kafka` 80.5; each of `acls.go`, `authorizer.go`, `auth.go`, `scram.go` at least 95% by `go tool cover -func`.
- [ ] `task fuzz` runs the five new targets for 60 seconds each, clean.
- [ ] `BenchmarkAllowed_20000ACLs` has a recorded budget (B9) or the index of ACC-30 is in.
- [ ] `task test:compat` passes `acl-java` plain and SCRAM-over-TLS, plus the 20 existing cells.
- [ ] `task lightness` and `task api` unchanged, or the commit says why.

**Console.**
- [ ] The ten boards (Ac-Home, Ac-01 to Ac-09) are implemented; the run of `e2e/tests/access.spec.ts` covers each by name.
- [ ] Every `new` item works from its data: Create ACL (CreateAcls), checker (N1), refusals (N2), principals (N3). None is a static page.
- [ ] The Delete confirm's list equals what a real client is refused afterwards (test asserts).
- [ ] The copy says "among the last 2,000 requests" wherever the recorder is the source (ACC-22); no board claim about lag or time-window survives that the data cannot back (ACC-23).
- [ ] Every refusal state shows the real error name in the Refusal vocabulary (A2); the lock-out case (ACC-20) renders a Refusal naming `--acl-super-user`.
- [ ] The create form offers exactly the operations the broker enforces, per resource type (ACC-27); a principal that is not `User:` is refused by the form (ACC-26).
- [ ] `access.spec.ts` passes with the route gate on, at 960 and 390 px, with the broker down (A7), and keyboard `j k Enter / c Esc` work.
- [ ] ACC-18 and ACC-19 comments corrected; no source or doc contains the phrase "NOT enforced" or "not enforced" for ACLs.

**Close.**
- [ ] Findings table: every row closed or moved with a reason; ACC-5, ACC-11, ACC-14 closed by the owner's decision.
- [ ] `docs/authorization.md` (ListGroups row, host note ACC-36), `docs/auth-and-tls.md` (session lifetime ACC-12, `ps` note ACC-14, console exposure ACC-5, mTLS ACC-24), `docs/console.md` Access section updated.

## 11. Files and order of work

Half-day units (HD).

| # | HD | Work |
|---|---|---|
| 1 | 1 | Red tests for ACC-1 (the matrix with `needs`), ACC-2, ACC-3, ACC-6, ACC-7; confirm each fails. |
| 2 | 1 | Fix ACC-2 (apply delta, return a code on refresh failure), ACC-18/19 comments, ACC-9/10 log lines. |
| 3 | 1 | Fix ACC-6, ACC-7 (`Summary.Denied`, principal on failed SASL) with the traffic stage. |
| 4 | 1 | ACC-4: `operationsFor`, bitmaps in Metadata, DescribeCluster, DescribeGroups. |
| 5 | 2 | N1: `Explain`, refactor `allowed` onto it, reference-model property test. |
| 6 | 1 | ACC-3 after R1 is recorded (or documented), ACC-8 doc. |
| 7 | 2 | Recording harness work for section 8 (R1-R15), depends on foundation B2. |
| 8 | 1 | Fuzz targets (ACC-16, ACC-31), pre-auth sizes (ACC-17), TLS/no-SASL timeout (ACC-25). |
| 9 | 1 | N2 denial ring and its wiring. |
| 10 | 1 | N3 principals source. |
| 11 | 2 | Console: list, grouping, filters, empty states, the default page (boards Home, 01, 02, 08). |
| 12 | 2 | Console: create form, refusals, lock-out guard, delete confirm (03, 04, 09), N4. |
| 13 | 2 | Console: checker, refusals log, principals (05, 06, 07). |
| 14 | 2 | Compat cell `acl-java`, e2e specs (all ten boards), parameterised auth/TLS/port tests. |
| 15 | 1 | Docs: authorization, auth-and-tls, console, limitations, api-support; claims table (B1); coverage floors raised. |
| 16 | 1 | Owner decisions ACC-5 / ACC-14 / ACC-11 implemented; mutation baseline recorded. |

**Estimate: L, about 21 half-days (10.5 days of work, 16 with review, recording turnaround and the CI cycles).**

**Prior art.** Apache Kafka's `kafka-acls.sh` and `AclAuthorizer`/`StandardAuthorizer` are the reference for every rule here; KIP-290 (prefixed ACLs), KIP-368 (SASL re-authentication) and KIP-714 (client telemetry, already in the exempt list) are the KIPs that matter. Redpanda Console lists and creates ACLs with a principal, resource, pattern, operation, permission and host form, as board Ac-03 does. Confluent Control Center and Confluent Cloud show ACL lists and a role view; none of the three has a checker or a refusals log, so N1 and N2 are original and should be built as functions with a reference model, because they are where the console can be wrong about the broker.
