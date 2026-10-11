# Groups

Stage 3 of the console order. Size L. Follows [../01-protocol.md](../01-protocol.md);
the common acceptance criteria there apply in full, and this page adds the
Groups surface's own. Prefix `GRP`.

## 1. Summary

The Groups surface is the consumer-group coordinator for both protocols (classic
JoinGroup/SyncGroup/Heartbeat/LeaveGroup, and KIP-848 ConsumerGroupHeartbeat), the
durable committed offsets, the group admin APIs, the timeline of what the
coordinators did, and the console pages that show and change all of it. It is
the console's second most-visited surface after Topics, and the only one whose
"live" state (members, generations, timers) is in memory and rebuilt after a restart.

| Piece | Where | Size |
|---|---|---|
| Classic coordinator | `internal/broker/group.go` (1,077 lines), `groups.go` (handlers), `grouptimer.go` | state machine, session and join timers under one lock |
| KIP-848 coordinator | `internal/broker/group848.go` (643), `group848_assign.go`, `groups848.go` | broker-computed assignment, revoke-before-assign |
| Offsets | `internal/broker/offsets.go` (265), `groupdelete.go`, `internal/storage/meta/store.go` offsets bucket | one bbolt bucket, key `group\0topic\0partition`, value `{offset, metadata}` |
| Admin | `groupadmin.go` (ListGroups, DescribeGroups), `groups848.go` (ConsumerGroupDescribe), `groupdelete.go` (DeleteGroups, OffsetDelete) | |
| Timeline | `groupevents.go` (the sink), `internal/traffic/traffic.go` (the ring), `console/traffic.go` (labels) | |
| Console | `console/handlers_groups.go` (358), `backend_groups.go` (510), `templates/groups.html` (163), 8 routes | |

Peers: [topics](topics.md) (a topic's "Consumed by" strip, deleting a topic deletes
its offsets), [traffic](traffic.md) (owns the recorder the timeline and the wire
dock live in), [access](access.md) (group ACLs, the "could User:x read this" check
on Gr-16), [cluster](cluster.md) (open transactions, for Gr-14), [map](map.md)
(lag on the edges reads this stage's lag), [bad-days](bad-days.md) (flapping and
rebalance storms are the bad-day scripts for this stage).

**What is solid today.** The coordinators have been audited twice: stale-timer
identity (`expiry`), claim-before-join for the one-owner-per-id rule, parked
JoinGroup/SyncGroup answered on supersede, revoke-before-assign with an
acknowledgement fence, KIP-394 member-id-required, static membership in both
protocols. Sixty-odd tests pin these. The findings below are therefore mostly
at the edges: validation Kafka does and this broker does not, admin views that
disagree with the coordinators, and boards that claim data nobody records.

**Estimate:** L, about 25 working days (50 half-days, section 11). **Actual:**
_fill in when done._

## 2. Contract

### Kafka APIs

Versions are from `internal/protocol/versions.go` (columns are the four
`--kafka-version` profiles; the Kafka 4 profile is the default).

| Key | API | Kafka 4 range | Now | Target | Note |
|---|---|---|---|---|---|
| 10 | FindCoordinator | 0-4 | F | F | Always this node. Describe on the group is required (a deny answers `GROUP_AUTHORIZATION_FAILED` here, not at the first Join). |
| 11 | JoinGroup | 0-9 | F | F | No session-timeout bounds, no empty-group check: GRP-1, GRP-2. |
| 14 | SyncGroup | 0-5 | F | F | No protocol check, leader re-sync overwrites: GRP-11, GRP-27. |
| 12 | Heartbeat | 0-4 | F | F | |
| 13 | LeaveGroup | 0-5 | F | F | v3+ puts a member error in the top-level code: GRP-7. |
| 8 | OffsetCommit | 0-9 | F | F | Metadata unbounded, negative offsets stored: GRP-3, GRP-12. v10 (topic ids) is not offered, documented in `versions.go:87`. |
| 9 | OffsetFetch | 0-9 | F | F | Describe on the group; topics the principal may not describe are dropped from an "all" answer and refused by name otherwise. |
| 47 | OffsetDelete | 0-0 | F | F | Refuses every topic of any group with members: GRP-4. |
| 15 | DescribeGroups | 0-5 | F (classic only) | F | A live KIP-848 group answers Empty with no members: GRP-5. |
| 16 | ListGroups | 0-5 | F | F | Type of an offsets-only group is hard-coded `classic`: GRP-6. |
| 42 | DeleteGroups | 0-2 | F | F | `NON_EMPTY_GROUP`, `GROUP_ID_NOT_FOUND` as Kafka. |
| 68 | ConsumerGroupHeartbeat | 0-1 | F | F | Kafka-4 profile only. Regex subscription skips the Describe filter: GRP-9. |
| 69 | ConsumerGroupDescribe | 0-1 | F | F | Assignment shown is the coordinator's record (see GRP-20). |
| 28 | TxnOffsetCommit | 0-3 | F | F | Owned by the transactions stage (cluster); only noted here because it writes the same offsets bucket. |

The ledger rows exist in `docs/api-support.md:26-43,69-103`. `docs/api-support.md:31`
says "ConsumerGroupHeartbeat ... (version 4 only)", which is the Kafka major, not
the API version (the table at line 102 says 0-1); worded confusingly, see GRP-26.

### Console routes (`console/console.go:261-267`)

| Route | Method | Mutates | Now | Target | Note |
|---|---|---|---|---|---|
| `/groups` | GET | no | F | F | List and empty state. Not live-polled (the detail is). |
| `/groups/{id}` | GET | no | F | F | Id travels as one escaped segment; `.`, `..` and `~...` are hex-coded (`resourceurl.go:31`). |
| `/groups/{id}/live` | GET | no | F | F | Polled every 3 s, 204 when the hash is unchanged. |
| `/groups/{id}/reset` | POST | yes | F | F | Earliest/latest, all topics or one. Target adds a timestamp and per-row offsets (Gr-06). |
| `/groups/{id}/offset` | POST | yes | F | F | One partition, number or ends. |
| `/groups/{id}/offset/delete` | POST | yes | F | F | OffsetDelete. |
| `/groups/{id}/delete` | POST | yes | F | F | DeleteGroups. |
| (new) `/groups/{id}/members/{member}` | GET | no | none | F | The inspector body for a member (A1). |
| (new) `/groups/{id}/events/{seq}` | GET | no | none | F | The inspector body for a timeline event. |
| (new) `/groups/{id}/reset/preview` | POST | no | none | F | Gr-06 preview: committed now, after, lag after; sends nothing. |

### Declined

Admin commit of offsets to a group that has members (Kafka refuses; so does the
console). Creating a group from the console (a group exists when a consumer
joins). Classic `OffsetCommit` v10 and `OffsetFetch` v10 (topic ids): not offered,
so clients stay on v9.

## 3. Where it stands

| Package | Coverage today | Note |
|---|---|---|
| `internal/broker` | 89.0% full, 27.5% under `-short` | The group tests are skipped under `-short`. Group-focused run (`-run 'Group|Offset|...'`, 50 s): 64.7% of the whole package. |
| `internal/storage/meta` | 80.7% | Offsets bucket is covered by `store_test.go`; no retention, no timestamp. |
| `console` | 87.8% | Full run, 30 s. |
| `internal/traffic` | 96.7% | Group event ring covered by `TestGroupEventsAreKeptInOrderPerGroup`. |

Function-level coverage from the group-focused broker run and the full console
run (`go test -coverprofile`, then `go tool cover -func`), the functions below
85%:

| Function | Coverage | What is untested |
|---|---|---|
| `groups.go handleHeartbeat` | 60.0% | The denied path. |
| `groups.go handleLeaveGroup` | 64.7% | The denied path and the v3+ per-member list. |
| `groups.go handleSyncGroup` | 72.7% | Denied. |
| `groups.go handleFindCoordinator` | 70.4% | Denied for a transaction key. |
| `group.go forget` | 55.6% | A group with live members is left alone. |
| `group.go selectProtocol` | 62.5% | No common first choice; empty protocol list. |
| `group.go sameProtocols` | 66.7% | Different metadata bytes. |
| `group.go rebalanceTimeout` | 72.7% | Member with no session timeout and no rebalance timeout. |
| `group.go sync` | 75.0% | Leader sync in `Stable`; leader names an unknown member. |
| `group848.go memberCheck` | 72.7% | Empty group with a named member. |
| `groupdelete.go handleDeleteGroups` | 70.0% | The denied path. |
| `offsets.go handleOffsetFetch` | 74.5% | Batched denied; member-epoch fencing on fetch. |
| `offsets.go handleOffsetCommit` | 80.0% | Denied topics; storage fault path. |
| `console describeGroup848` | 0.0% | The console never renders a KIP-848 group in any test. |
| `console timelineKind` | 40.0% | Four of eight kinds. |
| `console groupOffsetDelete` | 50.0% | The redirect when the last offset goes. |

Scenarios and specs that touch the surface:

| Layer | What exists |
|---|---|
| Broker, classic | 20 tests in `group_classic_*_test.go` (join relay, parked answers, fencing codes, static membership, MEMBER_ID_REQUIRED by version, empty groups forgotten unless they hold offsets), plus `group_timers_internal_test.go` (7) and `groupexpiry_test.go` (2) for the stale-timer rules |
| Broker, KIP-848 | 20 tests in `group_kip848_*_test.go` including three model tests (`_Model_InstantClient`, `_LaggingClient`, `_TopicChurn`) that check no partition is ever held by two members |
| Admin | `group_admin_test.go` (6): offsets survive a restart, state is real, DeleteGroups, OffsetDelete |
| Timeline | `groupevents_test.go` (3) |
| Console | `console/groups_test.go` (7) and `traffic_test.go` (2 timeline tests) |
| Browser | `e2e/tests/groups.spec.ts`: 8 tests, all on groups with offsets and **no live member** (the seed header lists `g-timeline`, no test reads it) |
| Real clients | `compat/`: Java (classic and `group.protocol=consumer`), Python/librdkafka (groups, KIP-848), kcat, KafkaJS, Streams, Spring, Connect, Kafbat UI, Redpanda Console; plain and SCRAM over TLS |
| Fuzz | none of the 7 targets touches a group parser. The classic coordinator is fed opaque bytes it never parses; the console decodes `ConsumerMemberAssignment` from them (`backend_groups.go:172`) |
| Differential | none. No recording of a real Kafka exists, so every group error code below is `read` + `spec` or `memory` |

## 4. Findings

Evidence: `run` = reproduced against a built binary (probe programs under the
scratchpad, a franz-go `kmsg` client against `dkbin`); `read` = from the code.
Source: `spec` = kmsg message definition, `doc` = Kafka documentation or KIP,
`memory` = recalled (queued in section 8, not to be acted on until recorded).

### P1: parity a real client can observe

| ID | Sev | Finding | Where | Evidence | Source | Test to write first |
|---|---|---|---|---|---|---|
| GRP-1 | P1 | JoinGroup accepts any session timeout. A 1 ms session was accepted and the member was evicted 1 ms after its join was answered (`gc after 600ms: Dead 0`). Kafka refuses a value outside `group.min.session.timeout.ms`..`group.max.session.timeout.ms` with `INVALID_SESSION_TIMEOUT` (26). A client misconfigured to `session.timeout.ms=1000` fails against Kafka with a clear error and silently flaps here. | `group.go:393`, `groups.go:71` | run | doc (broker configs), defaults `memory` (R1) | Raw kmsg: JoinGroup with session 1 ms and 100 days returns 26 on every version 0-9; in range returns 0. |
| GRP-2 | P1 | JoinGroup with an empty group id answered `MEMBER_ID_REQUIRED` (79) and handed out a member id. Kafka answers `INVALID_GROUP_ID` (24); so do Sync, Heartbeat and Leave. OffsetCommit already does (`commitFence`). | `group.go:289`, `groups.go:71` | run | doc + memory (R2) | Raw kmsg: Join/Sync/Heartbeat/Leave with `Group=""` return 24 on all versions. |
| GRP-3 | P1 | OffsetCommit stores metadata of any size (a 100,000-byte string was accepted). Kafka refuses a value over `offset.metadata.max.bytes` (4096) per partition with `OFFSET_METADATA_TOO_LARGE` (12). | `offsets.go:52-56` | run | doc, default `memory` (R3) | Raw kmsg: commit with 4,096 and 4,097 bytes of metadata, per partition, others in the request still stored. |
| GRP-4 | P1 | OffsetDelete refuses **every** topic of a group that has any member with `GROUP_SUBSCRIBED_TO_TOPIC` (86). Run: a live KIP-848 member subscribed to `tp1` only; deleting the offset of `other` got 86 on the partition. Kafka refuses only topics a member is subscribed to. For KIP-848 the subscription is known (`mem.subscribes`); for a classic `consumer` protocol group it is in the member's JoinGroup metadata (`ConsumerProtocolSubscription`), which the broker already stores. | `groupdelete.go:77-95` (comment at 67-71 says "cannot be told") | run | doc (KIP-496 describes the error), exact semantics `memory` (R5) | Raw kmsg + client: group with a live member on A; delete offset on B succeeds, on A answers 86; both protocols. |
| GRP-5 | P1 | DescribeGroups on a live KIP-848 group answers `State=Empty`, no members, `ProtocolType=consumer` (run: member `m1` at epoch 2). Tools that call DescribeGroups on every group (older Admin clients, some UIs) see a live group as empty. Kafka 4.0 describes consumer-protocol groups through DescribeGroups too. | `groupadmin.go:186-196` | run | doc (KIP-848) + memory (R6) | Raw kmsg: join a group with ConsumerGroupHeartbeat, DescribeGroups returns state `Stable`, one member, assignment bytes in the classic encoding. |
| GRP-6 | P1 | A KIP-848 group is listed as `classic` after a restart: the group exists only as offsets, and `listAllGroups` hard-codes `groupType: "classic"`. Run: `g848-commit` listed `Empty consumer` before the restart and `Empty classic` after. `kafka-consumer-groups --list --type consumer` loses it, Gr-Home's Protocol column is wrong, and the console's `DescribeGroup` picks the classic path. The type is not persisted. | `groupadmin.go:87-90`, `store.go:232` (no field) | run | read | Restart test with the data dir: a KIP-848 group commits, restart, ListGroups says `consumer`. Needs the type stored with the group's offsets (or in a small groups bucket). |
| GRP-7 | P1 | LeaveGroup v3+ copies the first member's error into the top-level `ErrorCode`. Run: unknown group, v3 batched form, got top-level 25 and member 25. The v3+ response reports per-member errors; the top level is for the whole request. | `groups.go:157-159` | run | memory (R7), `spec` for the field | Raw kmsg: LeaveGroup v0-v2 single member error at top level; v3-v5 per-member, top-level 0 for an unknown member. |
| GRP-8 | P1 | One protocol owns a group id, always. Kafka 4.0's default `group.consumer.migration.policy` lets classic and consumer-protocol members share a group (rolling upgrade of an application, one instance at a time). Here the second protocol is refused: classic Join answers `INCONSISTENT_GROUP_PROTOCOL` (23, run), ConsumerGroupHeartbeat answers `GROUP_ID_NOT_FOUND` (69). A mixed fleet fails to start. The one-owner rule is a deliberate safety choice (`groupClaims`), documented nowhere as a difference. | `group.go:39-75,302`, `group848.go:291` | run | doc (KIP-848 migration) + memory (R8) | Differential: two clients, one `group.protocol=classic`, one `consumer`, same group id, against real Kafka first; then decide between supporting it and writing it in `docs/limitations.md` with this scenario. **Closed:** D7: recorded (`mixed-protocol-groups`, `mixed-protocol-rolling`) and built; see `docs/consumer-groups.md` and the two limitations it leaves. |
| GRP-9 | P1 | A KIP-848 regex subscription bypasses the topic Describe filter. `handleConsumerGroupHeartbeat` removes undescribable names from `SubscribedTopicNames` only; `mem.subscribes` then matches the regex against every topic, so a principal with no Describe on `payroll` is assigned `payroll` partitions and sees its topic id and name in `ConsumerGroupDescribe`. Fetch still checks Read, so no record leaks; topic existence does. | `groups848.go:29-31`, `group848.go:196-198`, `group848_assign.go:22` | read | read | Authz test with ACLs: user may Describe `a.*` only, regex `.*`, topics `a.1` and `payroll` exist; assignment contains `a.1` only. |

### P2: console shows something false or cannot do what the API can; small broker defects

| ID | Sev | Finding | Where | Evidence | Source | Test to write first |
|---|---|---|---|---|---|---|
| GRP-10 | P2 | An empty KIP-848 group with no offsets stays listed until the broker restarts (run: `g848-nocommit` joined and left, listed `Empty consumer`). The classic side forgets it (`dropIfUnused`). The rail count (`Counts`, `backend_groups.go:127`) and Gr-Home count it. Whether Kafka lists such a group until its offsets would expire is not established. | `group848.go:507-513`, `groupadmin.go:45-53` | run | memory (R20) | Unit: join and leave with no commit, then ListGroups; expectation set from the recording. |
| GRP-11 | P2 | A leader's SyncGroup is honoured in any state. A retried leader sync while the group is `Stable` rewrites every member's assignment from the new request. Kafka answers the current assignment and changes nothing in `Stable`. | `group.go:559-575` | read | memory (R13) | Raw kmsg: leader syncs twice with different assignments; DescribeGroups shows the first. |
| GRP-12 | P2 | A negative offset (-5) is committed and returned by OffsetFetch (run). Clients read anything below 0 as "none" or break; the console hides it (`GroupOffsets` skips `Offset < 0`) so the row vanishes. | `offsets.go:56`, `backend_groups.go:223` | run | memory (R4) | Raw kmsg: commit -5; expectation from the recording (refused, or stored). |
| GRP-13 | P2 | `CommitOffsets` skips a partition whose topic is gone by the time the transaction runs and still returns success; the response says the offset was stored. The window is a delete racing a commit, and the store's own comment calls it the reason for the check. Kafka would answer `UNKNOWN_TOPIC_OR_PARTITION` for that partition. | `store.go:437-441`, `offsets.go:63` | read | read | Store test: commit to a topic deleted between lookup and write returns an error naming the partition, and the response carries code 3 for it. |
| GRP-14 | P2 | Gr-11 says "Kafka keeps a group's committed offsets after its topic is deleted until the group's offsets expire", and Gr-10 tells a story of a topic "deleted and created again" while the group "still remembers offsets from around 1,200". Neither can happen through the API: `DeleteTopic` removes the topic's offsets in the same bolt transaction (`store.go:383-401`), so a recreated topic has no committed offsets to be ahead of. The broker's own comment says Kafka "never keeps such an offset" (`groupdelete.go:99-103`). The leftover-offset row exists only for a data directory from an older version; the ahead row arises only from a commit past the log end (which Kafka also accepts). | `store.go:383`, `groupdelete.go:99`, board Gr-10, Gr-11 | read | memory (R10) | Record R10 first. Then either rewrite both boards' prose, or reproduce the scenario with a console test that commits past the end. Console test: deleting a topic removes the group's rows for it (the row never reaches `Deleted`). |
| GRP-15 | P2 | Gr-08 says an offset outside the range "is refused with `OFFSET_OUT_OF_RANGE` before it is sent", and shows the range as 0 to log end. The console refuses with a plain bad-input sentence; the lower bound is the log start, not 0; and `OFFSET_OUT_OF_RANGE` is a Fetch error, not an OffsetCommit one (the broker never range-checks a commit, as Kafka does not). The form must use the foundation's Refused kind (A2) with a console rule, not a Kafka name. | `backend_groups.go:339-342` | read | read | Console test: setting offset `latest+1` shows Refused, the rule text, no error name; setting `earliest-1` the same. |
| GRP-16 | P2 | Gr-07 and Gr-15 show `NON_EMPTY_GROUP` as the refusal for an OffsetCommit from outside a group with members, with a wire row `OffsetCommit ... NON_EMPTY_GROUP`. The console never sends that commit (`requireIdle` refuses first, in prose), and the broker would answer `UNKNOWN_MEMBER_ID` (classic) or `UNKNOWN_MEMBER_ID` (KIP-848, `memberCheck`), not `NON_EMPTY_GROUP`. `NON_EMPTY_GROUP` is DeleteGroups' answer. The name on the board does not come from any path. | `backend_groups.go:250-259`, `group.go:690-697`, `group848.go:577-593` | read | memory (R9) | Raw kmsg: OffsetCommit generation -1, no member, on a group with a member: assert the code; console test asserts the Refusal name equals the wire name. |
| GRP-17 | P2 | Gr-15 says a single offset can be removed "while the group is Empty or when no member owns the partition". The console shows Remove only when the group is Empty (`CanAct`), and the broker refuses any topic while any member exists (GRP-4). Three different rules. | `handlers_groups.go:187`, `groupdelete.go:94` | read | read | After GRP-4: console test with a live member on topic A removes the offset on topic B. |
| GRP-18 | P2 | Gr-14 shows lag against the last stable offset for a `read_committed` group, a `read_committed` chip, and a held-back count. The console computes lag from `PartitionBounds`, a ListOffsets with the default isolation (log end), and a coordinator does not know a group's isolation level: Fetch carries no group id. So the number is the log end minus committed, which is also what `kafka-consumer-groups.sh` prints (to be recorded, R16), and the template says exactly that ("computed here the same way"). The chip and the LSO column are new backend work: infer isolation from the member connections' Fetch requests (the recorder links connections to groups, `traffic.Client.Groups`), ask ListOffsets with isolation 1 for those partitions, and say it is inferred. | `handlers_groups.go:148-154`, `backend.go:466`, `groups.html:111` | read | memory (R16) | Console test with a fake backend: group flagged read_committed, LSO 4803, log end 4812, committed 4803 shows lag 0 and "9 held". Broker test: an open transaction holds LSO and ListOffsets isolation 1 returns it (already covered, `lastStableOffset` 66.7%). |
| GRP-19 | P2 | Gr-18's offsets-retention countdown ("expire Oct 13, 09:12", "3 of 7 days used", "disappears from this list") has no backend. Offsets never expire: `offsetRecord` holds only `{offset, metadata}` (`store.go:232`), nothing records a commit time or when a group became empty, nothing sweeps. The board says the countdown shows "only if the broker reports the retention"; the broker does not. Kafka expires an empty group's offsets after `offsets.retention.minutes`. Decision for the owner (section 9): build it (store `committedAt`, track `emptySince` across restarts, sweep, report both) or cut the countdown and write "offsets are never expired" in the guide. Until then no text in the console may say offsets expire. | `store.go:232`, `group.go` (no emptySince), board Gr-18 | read | doc (`offsets.retention.minutes`), semantics `memory` (R11) | If built: store test that an offset older than the retention is gone after a sweep and a group that was non-empty recently keeps its offsets; restart variant. If cut: doc test that the guide says offsets are kept. **Closed:** D7: recorded (`offsets-retention`) and built (`--offsets-retention`, `internal/broker/offsetexpiry.go`). |
| GRP-20 | P2 | Gr-12 says "current is what the member says it owns". The `Assignment` that ConsumerGroupDescribe returns is the coordinator's `assigned` set, deliberately not the member's report (the header of `group848.go:23-40` explains why). Per-member `STABLE`/`RECONCILING`, the previous epoch ("12 -> 13") and the `releasing 2` / `waiting for 2` notes need `revoking`, `prevEpoch` and the group epoch per member, none of which `describe` returns (`group848.go:609-619`). | `group848.go:609-642`, `groups848.go:82` | read | read | Unit: group with two members mid-handoff: the snapshot shows member 1 `Reconciling` with `revoking={2}`, member 2 `Reconciling` waiting. Console test renders both lines from the snapshot. |
| GRP-21 | P2 | Gr-16 explains an OffsetFetch refusal with "an ACL denies Read ... a deny wins". OffsetFetch needs **Describe** on the group, and the authorizer's DENY is exact: DENY Read does not deny Describe (`authorizer.go:128-133`, comment included). A DENY Read breaks JoinGroup, Heartbeat, OffsetCommit; the refused OffsetFetch comes from a DENY Describe, or from the absence of an Allow under a default-deny. The "deciding ACL" panel must name the operation the API needs (the ACL checker is in [access](access.md)). | `offsets.go:81,124`, `authorizer.go:74-90,128` | read | read | Authz test: DENY Read on a group leaves OffsetFetch and ListGroups working; Join answers 30. Board copy test: the panel is generated from the API's operation. |
| GRP-22 | P2 | Gr-03, Gr-05 and Gr-13 show member facts the broker holds in memory and no API returns: host **with port** (`RemoteHost` returns `/127.0.0.1` with no port, `conn.go:96`), last heartbeat, joined time, session and rebalance timeouts, the offered protocols ("range, cooperative-sticky"), the leader, the software name and the principal. DescribeGroups has no leader or timeout fields. `groupMember` (`group.go:210-226`) holds client, timeouts and protocols but no timestamps. This is the largest backend item of the stage: a `GroupInspector` snapshot on the broker, read in-process by the console (as `TrafficSource` is). | `group.go:191-226`, `groupadmin.go:101-137` | read | read | Unit: join three members, heartbeat one; the snapshot returns joined times, heartbeat times, timeouts, offered protocols, leader and connection ids for each; a restart returns an empty snapshot, not an error. |
| GRP-23 | P2 | The timeline vocabulary is smaller than the boards use. The coordinators emit eight kinds (`joined left expired rebalance generation assigned dropped epoch`) and the console names all eight. Missing: durations ("waited 3.0 s", "no heartbeat for 10.0 s"), the reason a member left ("said goodbye (LeaveGroup)" vs the same member never heard from), "rejoined with a new member id", a KIP-848 handoff (revocation sent, acknowledged, assigned: `assigned` is classic-only), a static member leaving at epoch -2, group deleted, offsets reset/set/removed from the console. `rebalance` after an expiry carries no member. | `group.go:403-405,921,954`, `group848.go:246,481`, `traffic.go:293` | read | read | Event tests per kind with the exact `detail`: the table of kinds in section 6 is the spec; each coordinator test asserts the sequence for one story (flap, clean leave, revoke). |
| GRP-24 | P2 | The group event ring is global and shared: capacity equals the traffic request ring (`traffic.New(trafficRows, ...)`, `server.go:302`), one slice for all groups, with an O(n) `slices.Delete` on every event once full (`traffic.go:307-310`). A flapping group evicts every other group's history, "in the last hour it rebalanced 3 times" is unanswerable, and the console reads only the latest 60 of a group (`traffic.go:283`) while Gr-04 says the broker keeps 2,000. | `traffic.go:301-311`, `console/traffic.go:283` | read | read | Unit: group A emits 3 events, group B emits capacity events; A's three are still there. Console test: window and counts ("3 rebalances in the last hour") come from the events with a fake clock. |

### P3: wording, dead copy, undocumented behaviour, test gaps

| ID | Sev | Finding | Where | Evidence | Source | Test to write first |
|---|---|---|---|---|---|---|
| GRP-25 | P3 | Gr-05 says the rebalance timeout is "the longest `max.poll.interval` any member asked for, capped by the broker". There is no cap: it is the longest any member asked for, each falling back to its session timeout, then 30 s (`group.go:751-767`). Also the "31 s left of 45 s" countdown needs the join timer's deadline, which `expiry` does not keep. | `group.go:747-767`, `grouptimer.go` | read | read | Unit: `rebalanceTimeout()` for members {none, 10 s, 45 s} is 45 s; deadline exposed by the snapshot equals start plus that. |
| GRP-26 | P3 | `docs/limitations.md:70-72` and `docs/consumer-groups.md` ("Limits") say static membership "is accepted on the wire but not acted on". It is implemented in both coordinators and tested (`TestGroupClassic_StaticMembership`, `TestGroupKIP848_StaticMembership`). The guide also says `uniform` is "a plain, even round-robin" while `assignUniform` keeps a member's partitions where it can (its comment, `group848_assign.go:112`). `api-support.md:31` writes "version 4 only" for an API whose range is 0-1. The initial rebalance delay (200 ms, `group.go:27`) is a variable with no flag and no mention anywhere (Kafka's default is 3 s). | docs; `group.go:27` | read | read | Doc check (B14): every claim in the guide's Limits section has a test or is deleted; the 200 ms is stated with its reason. |
| GRP-27 | P3 | SyncGroup v5 carries `ProtocolType` and `Protocol`; the handler echoes them and never compares them with the group's (`groups.go:125-126`). Kafka answers `INCONSISTENT_GROUP_PROTOCOL` for a mismatch. | `groups.go:113-128`, `group.go:533` | read | memory (R13) | Raw kmsg v5: Sync with a wrong protocol name. |
| GRP-28 | P3 | DescribeGroups for a group that does not exist answers `Dead` with `ProtocolType=consumer`; Kafka answers an empty protocol type for a dead group. | `groupadmin.go:179,190` | read | memory (R14) | Raw kmsg v0-v5 on an unknown id. |
| GRP-29 | P3 | OffsetFetch v0-v7 with explicit topics iterates a Go map (`for topic, parts := range topics`), so the topic order in the response changes between calls. Harmless to clients, noise to a recorder and a differential test. | `offsets.go:193-203` | read | read | Same request twice returns the same bytes. |
| GRP-30 | P3 | `pending` (member ids issued with `MEMBER_ID_REQUIRED`) is pruned only when another id is issued, and holds up to `rate x 5 min` entries; an unauthenticated flood of empty-id joins grows it. Minor on a dev broker; the fix is a bound. | `group.go:130-147` | read | read | Unit: 100,000 empty-id joins against one group keep `pending` under a stated cap. |
| GRP-31 | P3 | A JoinGroup that arrives while DeleteGroups holds the id (`claims.claim(id, "deleting")`) fails the protocol claim and answers `INCONSISTENT_GROUP_PROTOCOL` (classic) or "in use by a classic group" `GROUP_ID_NOT_FOUND` (KIP-848). Neither says "try again". | `groupdelete.go:48`, `group.go:347`, `group848.go:291` | read | memory (R17) | Concurrency test: join during delete returns a retriable code, then succeeds on retry. |
| GRP-32 | P3 | Confirm sentences are weaker than the boards'. Delete confirms "Delete group X and its committed offsets?" (no count, no lag consequence, no `auto.offset.reset` line); Remove names the partition but not the offset it forgets. A6 requires the sentence to be true and tested. | `groups.html:48,111` | read | read | A6 table row per route; test asserts "N committed offsets" equals the count removed. |
| GRP-33 | P3 | Test gaps: `describeGroup848` 0%, `timelineKind` 40%, `groupOffsetDelete` 50%; the browser suite never drives a live member, a KIP-848 group, the timeline or a rebalance; no fuzz target decodes consumer metadata or assignment bytes (the console does, `backend_groups.go:172`); no differential test of any group code. | section 3 | read | read | Section 7. |
| GRP-34 | P3 | `ListGroups` and the rail count both list every group and the home page will add lag per group (`TopicConsumers` already costs one OffsetFetch + ListOffsets per group). With hundreds of leftover test groups the 3 s poll becomes hundreds of requests. The home table needs a single in-process "group summaries" call (members, generation, total lag) rather than N wire calls. | `backend_groups.go:54-105,477-510` | read | read | Bench: 500 groups, home render makes a constant number of broker calls. |

**Counts:** 34 findings. P0: 0. P1: 9. P2: 15. P3: 10.

**How the P0 question was answered.** Looked for acknowledged-but-lost commits
(only GRP-13, a delete race, tiny), unbounded input (frame limit holds; GRP-30 is
the only growth), goroutine leaks on parked joins (every supersede/remove path
answers the parked channel; `TestGroupClassic_ParkedJoinsAreReleased` covers
them), and double ownership under KIP-848 (three model tests). None found.

## 5. What to check

Beyond the common criteria:

- [ ] **Rebalance timers.** `joinExpired` after a completed rebalance does nothing; a join that arrives after the deadline starts a new one; a member that never rejoins is dropped after the longest rebalance timeout and `dropped` is emitted (`group.go:837`).
- [ ] **Rebalance timeout.** A JoinGroup v0 member (no rebalance timeout) uses its session timeout; a member with `max.poll.interval` of 5 minutes is waited for 5 minutes and is not evicted by its own session timer while parked (`TestGroupClassic_ParkedMemberIsNotEvictedByItsOwnSession`).
- [ ] **Parked answers.** Every parked JoinGroup and SyncGroup is answered when superseded (retry on a new connection), when the member is removed, when a rebalance starts, when the broker stops. Count goroutines before and after in a test.
- [ ] **Generation fencing.** Sync, Heartbeat and OffsetCommit with a stale generation answer `ILLEGAL_GENERATION`; an unknown member `UNKNOWN_MEMBER_ID`; a replaced static instance `FENCED_INSTANCE_ID`, on every version.
- [ ] **KIP-848.** The epoch never goes backwards; a lost response is repaired by `prevEpoch`; a partition is never in two members' `assigned` or `revoking`; revoke is sent before assign; a member that never acknowledges is removed at its rebalance timeout; a deleted-and-recreated topic is a new topic id.
- [ ] **One owner per id** across protocols under concurrent first joins and under DeleteGroups (`TestGroupCoordinators_ConcurrentFirstJoinsCannotBothWin` extended with delete).
- [ ] **Restart.** Groups with offsets list after restart with the right type (GRP-6); members do not; a consumer resuming finds its offsets; a SIGKILL variant after an acknowledged OffsetCommit keeps it.
- [ ] **Offsets.** Commit, fetch (explicit and all), delete one, delete group, delete topic; metadata round trip; a group id with a space, a slash, `~`, `.` and unicode reaches the console and every route.
- [ ] **Auth on and off.** For each group API the operation it needs (Read: Join, Sync, Heartbeat, Leave, Commit, ConsumerGroupHeartbeat; Describe: FindCoordinator, OffsetFetch, DescribeGroups, ConsumerGroupDescribe, ListGroups; Delete: DeleteGroups, OffsetDelete) and the topic operation beside it. The console user is a super user.
- [ ] **Lag.** Committed beyond the end is "ahead", not negative; committed below the start is "lost to retention"; no commit is "no commits", not 0; a deleted topic never counts toward the total.
- [ ] **Reset/set/delete.** Allowed only with no members (`requireIdle`), and the real guard is the broker's refusal if a consumer joins in between; set range is log start to log end inclusive; reset to a timestamp uses ListOffsets by time and clamps to the end.
- [ ] **Timeline vocabulary.** Each kind has one story test; the same stories exist for both protocols (`epoch` stands for `generation`; the label says so).
- [ ] **Flapping.** Detected from three or more expired-then-joined cycles in 10 minutes; steady gaps reported as steady; a clean leave (`left`) never counts as flapping.
- [ ] **Console when the broker is down:** frozen group page, no toast loop (A7); empty data dir; 960 and 390 px.

## 6. Console

Boards: `Gr-Home` and `Gr-01` .. `Gr-18` (19 files). The right-hand inspector,
the wire dock and the neighbours sentence come from the foundation (A1, A4, A5);
confirms from A6; refusals from A2. Status: **real** (exists), **buildable**
(data exists, screen does not), **needs backend** (data or behaviour does not exist).

| Board | Template + handler | Status | Built from |
|---|---|---|---|
| Gr-Home: all groups | `groups_home`, `group_lp` / `groups` | needs backend (small) | Rows, protocol, state, members: `ListGroups` (real). Topics: offsets. Lag column: per-group offsets and bounds (GRP-34). "epoch 12 / generation 7" column: the KIP-848 epoch is on the wire, the classic generation is not (snapshot, GRP-22). "Two groups need a look": rule over lag and empty-since (GRP-19). Filter tabs (all, with members, empty, behind) client-side. "Refreshes every 3 s": the list is not polled today. |
| Gr-01: none | `lp_empty` (`groups.none.*`) / `groups` | buildable | Copy and the kcat/console-consumer snippets from foundation B7/connect. Snippets must use the live bootstrap (A8), not `127.0.0.1:9092`. |
| Gr-02: lag | `group_live` / `groupData` | buildable | Offsets table, total, bars: real. "Three members on generation 7" and "offsets committed every 5 s" need the snapshot; the 5 s is a client setting and must not be printed as fact. Neighbours: A5. Wire dock: A4 with `Filter{Group}`. "Try it, Set offset" form: GRP-15, GRP-17. |
| Gr-03: members | new `group_members`, member inspector / `groupData` | needs backend | Member, client id, assigned: real. Host with port, heartbeat age, joined, timeouts, subscription topics (decode `ConsumerMemberMetadata`), offered protocols, software, principal: snapshot (GRP-22). Tabs `Members / Timeline / Offsets` and keys `1-4` are new. "Members are in memory only" is true and stays. |
| Gr-04: timeline | `group_timeline` / `groupTimeline` | needs backend (small) | Exists as a list of the latest 60 events. Needs durations and reasons, a per-group ring, the filter chips (all / rebalances / members / problems), "rebalanced N times in the last hour" and the "since the broker started" honesty line (GRP-23, GRP-24). |
| Gr-05: rebalancing | `group_live` state banner / `groupData` | needs backend | `PreparingRebalance` state is real. "31 s left of 45 s", "rejoined 14:36:15.401 / not yet / last heartbeat 4 s ago": deadline and per-member `rejoined` and heartbeat time in the snapshot (GRP-22, GRP-25). "Capped by the broker" is false, delete it. |
| Gr-06: reset | `group_reset` inspector form / `groupReset` + new preview | buildable | Earliest/latest: real. Timestamp: `ListOffsets` by time (exists, `offsetByTime`). Specific offsets: per-row Set (real). Preview table (committed now, after, lag after): computed from `GroupOffsets` + `PartitionBounds`, sends nothing. The wire dock "what Apply will send" is the command preview (A6): it must show exactly the OffsetCommit the handler makes (generation -1, as `__console`). |
| Gr-07: reset refused | `Refusal` partial / `groupReset` | buildable | A2 Refusal with the console's own rule. The name `NON_EMPTY_GROUP` is not what any path returns (GRP-16): show `UNKNOWN_MEMBER_ID` only if recorded (R9), otherwise Refused with the rule "a commit from outside the group is only accepted while it has no members". The three steps use members from the snapshot. |
| Gr-08: offset set | offset inspector / `groupOffsetSet` | buildable | Real route and range check. Text fixes in GRP-15. |
| Gr-09: no commits | `group_live` `NoCommits` / `groupData` | buildable | `NoCommits` and the "members but no offsets" line exist. Add the Owner and Log end table with "no commit yet" (owners from the assignment, log end from bounds). Never print 0. |
| Gr-10: ahead | `group_live` Ahead chip | real | Chip, `AheadBy`, `AnyAhead` exist. Suggested reset text and "re-read the 239 that exist" are arithmetic over the bounds. The narrative about a recreated topic is not reachable (GRP-14). |
| Gr-11: deleted topic | `group_live` Deleted chip, Remove | real | Chip and Remove exist. Reachable only from an old data dir (GRP-14); "also remembered" neighbour needs A5. |
| Gr-12: protocol 848 | `group_live` / `describeGroup848` | needs backend | Member id, epoch, target, current: `ConsumerGroupDescribe` (real, but `current` is the coordinator's record, GRP-20). Per-member state, previous epoch, releasing/waiting: snapshot. The "what reconciling looks like" example is static teaching copy and must say it is an example. `describeGroup848` is 0% covered. |
| Gr-13: classic | `group_live` / `describeGroupClassic` | needs backend | Protocol type and name: DescribeGroups (real). Generation, leader, session and rebalance timeouts, role column: snapshot (GRP-22). "(chosen from range, cooperative-sticky)": offered protocols. |
| Gr-14: read committed | `group_live` / new | needs backend | LSO, held-back count, isolation chip, open transaction (id, producer id, epoch, first offset, age): ListOffsets isolation 1 (real), isolation inferred from fetchers (GRP-18), open transactions from the transactions API (cluster stage, `DescribeTransactions`). |
| Gr-15: confirms | confirm dialogs (A6) | buildable | Delete group, Remove offset: routes real. Sentences with counts and consequences generated from the data and tested (GRP-32). Remove's "allowed when no member owns the partition" depends on GRP-4/17. |
| Gr-16: denied | `Refusal` + ACL panel | needs backend | The recorder keeps refused requests (`ErrorsOnly`, `Filter.Group`); "1 client refused / 4 times since 14:34" is a rollup of those. "Check User:loyalty on this group" is the ACL checker of the access stage (new backend). Text per GRP-21. |
| Gr-17: flapping | `group_timeline` + pill / new rule | needs backend (small) | The pattern is in the events (expired, joined, generation). Gaps between expiries, "30.1 s steady" are arithmetic. "no heartbeat for 10.0 s (its session timeout)" needs the duration in the event (GRP-23). Pill "rebalancing every 30 s" is a rule over the last 10 minutes of events. |
| Gr-18: empty, expiring | `group_live` banner | needs backend | Empty since: not recorded. Expiry: not implemented. Whole countdown depends on the decision in GRP-19. Without it the board keeps the "reset to latest" advice and drops the countdown. |

**Counts:** 19 boards. real 2, buildable 7, needs backend 10.

### New items and what they are built from

| `new` item | Built from | Backend work |
|---|---|---|
| Tabs Members / Timeline / Offsets, keys 1-4 | existing page sections | none |
| Member inspector | `GroupInspector` snapshot | GRP-22 |
| Event inspector ("Its events") | event ring + seq | GRP-24 |
| Reset preview and timestamp reset | `GroupOffsets`, `PartitionBounds`, ListOffsets by time | none (console only) |
| LSO lag | ListOffsets isolation 1 + inferred isolation | GRP-18 |
| Offsets countdown | commit time, empty-since, retention | GRP-19 |
| Flapping pill and "where to look" | events | GRP-23 |
| ACL panel | access-stage checker | outside this stage |
| Wire dock, neighbours | recorder filter by group; A5 graph | traffic stage |

### Timeline kinds (the spec for GRP-23)

| Kind | Classic emits | KIP-848 emits | Label | Detail must carry |
|---|---|---|---|---|
| joined | yes (`group.go:381`) | yes (`group848.go:330`) | joined | client id, host with port |
| left | LeaveGroup | epoch -1 | left | "said goodbye" vs timed out is the `expired` kind |
| expired | session timer | session timer, revoke timer | session expired | duration since last heartbeat and the timeout |
| dropped | did not rejoin in time | none | dropped | the rebalance timeout |
| rebalance | yes | no (see `epoch`) | rebalance | reason, who started it, how long it waited |
| generation | yes | no | new generation | members, leader, protocol |
| assigned | leader's sync | **never** | assigned | count; for 848: per-member revoke/assign |
| epoch | no | yes | new epoch | reason, members reassigned |

### Teaching lines

None of this stage's forms has a settings record yet. Fields to add to the
settings table (B7): `reset to` (earliest, latest, timestamp, specific), `new
offset` (range: log start..log end, applied), `isolation` (inferred, not a
setting), `offsets retention` (stored/applied per GRP-19), `session timeout` and
`rebalance timeout` (per-client, shown, not edited), `consumer session timeout`
(the broker's own flag, `--consumer-session-timeout`, default 45 s). Each line
comes from that table; a test fails for a form field without one (A3).

## 7. Tests to add

Every finding gets a failing test first, at the lowest layer that can show it.

**Unit / store**
- `TestGroupClassic_SessionTimeoutBounds` (GRP-1), `TestGroupClassic_EmptyGroupID` (GRP-2).
- `TestGroupClassic_LeaderResyncInStable` (GRP-11), `TestGroupClassic_SyncProtocolMismatch` (GRP-27).
- `TestGroupClassic_RebalanceTimeoutTable` (GRP-25).
- `TestGroupEvents_RingIsPerGroup` in `internal/traffic` (GRP-24).
- Store: `TestOffsets_MetadataLimit` (GRP-3), `TestOffsets_CommitToVanishedTopicReportsIt` (GRP-13), `TestOffsets_GroupTypeSurvivesRestart` (GRP-6), and if GRP-19 is built `TestOffsets_RetentionSweep`, `TestOffsets_EmptySinceSurvivesRestart`.
- `TestGroupSnapshot_*` for the inspector (GRP-22, GRP-20, GRP-25).
- Coordinator model test extended: random sequence of join, leave, heartbeat, expire, delete, topic churn, for both protocols and for both at once, asserting the invariants (one owner per id, no partition in two members, generation/epoch monotonic, no parked channel left unanswered). This replaces three hand-written scenarios with a property.

**Raw kmsg (on every version in range)**
- Session timeout bounds (GRP-1), empty group id on Join/Sync/Heartbeat/Leave (GRP-2), metadata size (GRP-3), negative offset (GRP-12), OffsetDelete by subscription for both protocols (GRP-4), DescribeGroups on a KIP-848 group (GRP-5), ListGroups type across a restart (GRP-6), LeaveGroup v3+ top-level code (GRP-7), SyncGroup protocol (GRP-27), DescribeGroups on a dead id (GRP-28), OffsetFetch determinism (GRP-29), commit with generation -1 against a group with members (GRP-16).
- Auth matrix: the operation each group API needs, with DENY Read leaving Describe-only APIs working (GRP-21); regex subscription with partial Describe (GRP-9).

**Client library (franz-go and Sarama)**
- Two members join, one's process is paused past its session; timeline shows `expired` then `joined`.
- A classic and a consumer-protocol member share one id (after the decision in GRP-8).
- Resume after broker restart, plain and SIGKILL.

**Compat cells** (existing cells that must stay green: `java`, `python`, `kcat`, `kafkajs`, `streams`, `spring`, `connect`, `kafbat-ui`, `redpanda-console`). Add to `java`: `group.protocol=consumer` with a second classic member on the same group (GRP-8, after the decision); to `python`: session timeout below the minimum fails with the error Kafka gives.

**Fuzz**
- `FuzzConsumerProtocolDecode` in `console`: member metadata and assignment bytes into `kmsg.ConsumerMemberMetadata/Assignment.ReadFrom` and the render path (they come from clients, `backend_groups.go:172`).
- `FuzzGroupSequence` in `internal/broker`: a byte string drives a sequence of coordinator calls (the same operations as the model test); invariants checked after each. It is the fuzz target for the state machine, since the coordinator parses nothing itself.

**Console**
- `describeGroup848` rendering with two members mid-handoff (GRP-20); `timelineKind` for all eight kinds; `groupOffsetDelete` last-offset redirect.
- Refusal tests: every failing route returns a `Refusal` (A2); the confirm table (A6/GRP-32).
- Ahead / deleted / trimmed / no-commits / read-committed rows (GRP-14, GRP-18).
- Group ids `.`, `..`, `~x`, `a/b`, `a b`, unicode, `%`: open, reset, set, delete.

**Browser (new `groups-live.spec.ts`, seed gets a live member helper)**
- A live classic member: members tab, host, heartbeat age ticks; stop it, the page goes to Empty without reload and Reset enables.
- A live KIP-848 member, Gr-12 table.
- Rebalance in progress banner (Gr-05) with a held-open join.
- Flapping seed (`g-flap`): pill and the "steady gap" line.
- Reset preview then Apply; Reset refused with a member connected; Set offset out of range; Remove offset confirm.
- Denied group (Gr-16) with ACLs on.
- Route gate on (B8), accessibility sweep, 960 and 390 px.

**Differential vs real Kafka (B2)**: scenarios R1-R20 in section 8, recorded once under `conformance/recordings/groups/`, replayed in Go.

**Coverage targets** (statement coverage, full suite)

| Package | Baseline | Target | Functions that must be covered to get there |
|---|---|---|---|
| `internal/broker` | 89.0% | 91.0% | `handleHeartbeat`, `handleLeaveGroup`, `handleSyncGroup`, `handleFindCoordinator` denied paths; `group.go forget`, `selectProtocol`, `sameProtocols`, `rebalanceTimeout`, `sync`; `memberCheck`; `handleDeleteGroups` denied; `handleOffsetFetch` batched denied and fenced; the new snapshot and bounds code |
| `internal/storage/meta` | 80.7% | 83.0% | offsets metadata limit, vanished-topic commit, group type, retention if built |
| `console` | 87.8% | 90.0% | `describeGroup848`, `timelineKind`, `groupOffsetDelete`, `groupChanged`, preview and member handlers |
| `internal/traffic` | 96.7% | 96.7% | per-group ring |

Mutation (B3): `go tool task mutate:check -- ./internal/broker/ ./internal/storage/meta/ ./console/` with no new survivor in `group.go`, `group848.go`, `offsets.go`, `groupadmin.go`, `groupdelete.go`, `handlers_groups.go`, `backend_groups.go`. Likely survivors to kill first: the `>=`/`>` at `group.go:332` (`GetVersion() >= 4`), the `<=` in `issued` (`group.go:153`), `reconcile`'s `reported == nil || reported.intersects` (`group848.go:399`), the `Latest`/`Earliest` choice in `ResetGroupOffsets` (`backend_groups.go:300`), and `offset > latest` in `SetGroupOffset` (`backend_groups.go:339`).

## 8. Recording queue

Docker: `apache/kafka:4.0.0`; each scenario is a script that issues the request with a raw client and stores the response bytes.

| # | Claim (currently `memory` or `doc`) | Scenario |
|---|---|---|
| R1 | `INVALID_SESSION_TIMEOUT` (26) outside `group.min/max.session.timeout.ms`; the default bounds (6 s, 1800 s). | JoinGroup v0, v5, v9 with session 1 s, 6 s, 30 min, 31 min. |
| R2 | `INVALID_GROUP_ID` (24) for an empty group on Join/Sync/Heartbeat/Leave/OffsetCommit/OffsetFetch/DescribeGroups/DeleteGroups. | One request per API, empty id. |
| R3 | `OFFSET_METADATA_TOO_LARGE` per partition at 4096 bytes; sibling partitions stored. | OffsetCommit with metadata 4096 and 4097. |
| R4 | A negative committed offset: refused or stored? | OffsetCommit -1, -5, then OffsetFetch. |
| R5 | OffsetDelete with a live member: error only for subscribed topics; classic and consumer protocol; top-level code. | One member on A; delete B, then A. |
| R6 | DescribeGroups on a consumer-protocol group: state, members, assignment encoding. | Join with `group.protocol=consumer`, call DescribeGroups. |
| R7 | LeaveGroup v3+ with an unknown group or member: top-level code vs per-member. | Leave v1, v3, v5. |
| R8 | A classic member and a consumer member in one group: allowed by the default migration policy? Error codes each way. | Two clients, same id. |
| R9 | OffsetCommit generation -1 on a group with a member: `UNKNOWN_MEMBER_ID`, `ILLEGAL_GENERATION`, `NON_EMPTY_GROUP`? And what `kafka-consumer-groups --reset-offsets` and `Admin.alterConsumerGroupOffsets` report. | Commit as an admin client with a member present; also the CLI. |
| R10 | Committed offsets removed when their topic is deleted? And after re-creation? | Commit, delete topic, OffsetFetch all, recreate. |
| R11 | Offsets retention: emptied-at start, `offsets.retention.minutes`, listing after expiry. | Container with a 2 minute retention and 1 minute check interval; empty a group; poll ListGroups and OffsetFetch. |
| R12 | Heartbeat to an unknown group, a dead group, and a group on the other protocol, per version. | Heartbeat v0-v4, ConsumerGroupHeartbeat to a classic id. |
| R13 | SyncGroup protocol mismatch; leader re-sync in `Stable`. | Raw classic client. |
| R14 | DescribeGroups for an unknown id: state, protocol type, protocol. | v0-v5. |
| R15 | ListGroups type of a consumer group after a broker restart (it is persisted in Kafka). | Restart the container with a volume. |
| R16 | Lag basis in `kafka-consumer-groups.sh --describe` with an open transaction: log end or last stable offset. | Transaction held open, consumer read_committed, the CLI. |
| R17 | JoinGroup during DeleteGroups. | Two clients racing. |
| R18 | DeleteGroups on a missing id, a group with members, a group in each state. | Each. |
| R19 | KIP-848 regex subscription vs topic ACLs. | ACL-enabled container (SASL), user with partial Describe. |
| R20 | An empty consumer-protocol group with no offsets: listed, and for how long. | Join, leave, ListGroups, wait. |

## 9. Owed to integration

- **Traffic stage** ([traffic](traffic.md)): the recorder filter by group (newest first, cursor) for the wire dock; the per-group event ring (GRP-24) lives in `internal/traffic`, a file that stage owns, so the two agree on one design. A connection-to-member link (the recorder already has `Client.Groups`; the snapshot needs the member id per connection) for the principal and software on Gr-03.
- **Access stage** ([access](access.md)): the ACL checker behind Gr-16's panel, and the operation each group API needs (GRP-21).
- **Cluster stage** ([cluster](cluster.md)): open-transaction details for Gr-14 (id, producer id, epoch, first offset, age).
- **Topics stage** ([topics](topics.md)): the "Consumed by" strip uses `TopicConsumers`; the Gr-14 held-back count and a topic's own LSO belong to its partition table. Deleting a topic deletes the group's offsets for it, and its confirm must say so ("N groups lose their offsets").
- **Map stage** ([map](map.md)): lag on edges reads the group summaries of GRP-34.
- **Bad-days stage** ([bad-days](bad-days.md)): the flapping group script (a client that pauses longer than its session) and a rebalance storm.

**Decisions for the owner**

1. GRP-19: build offsets expiry (M, 4 half-days) or cut the countdown and document "offsets are never expired". Recommendation: cut, unless a test needs the Kafka behaviour; a dev broker that silently forgets a week-old position is a surprise, and the dev flow resets groups by hand.
2. GRP-8: support mixed-protocol groups or document the refusal. Needs R8 first; supporting it is a rewrite of the claim rule, not a patch.
3. GRP-22: confirm the in-process `GroupInspector` approach (same as `TrafficSource`), over extending the wire with doze-specific fields.

**Asks of the foundation**

- A1: inspector kinds `member` and `event` (title, kind, URL) alongside message, request, client, subject and transaction.
- A2: the `Refusal.Name` table must include console-rule refusals with no Kafka name (a "Refused (console rule)" kind), so Gr-07/Gr-08 do not invent `NON_EMPTY_GROUP` or `OFFSET_OUT_OF_RANGE`.
- B2: the real-broker oracle needs a restart step (R15) and a flag-setting step (R1, R3, R11 change broker configs).
- B7: settings records for the fields listed in section 6.
- B12: `FuzzConsumerProtocolDecode` and `FuzzGroupSequence` registered in the fuzz target list.

## 10. Acceptance criteria

- [ ] Every finding GRP-1 .. GRP-34 has a test that failed first and is closed, or is moved with a reason (GRP-8, GRP-10, GRP-12, GRP-19 may wait on R8, R20, R4, the owner decision).
- [ ] `go tool task check` green (race, Windows vet).
- [ ] Coverage: `internal/broker` >= 91.0%, `internal/storage/meta` >= 83.0%, `console` >= 90.0%, `internal/traffic` >= 96.7%, and the floor file updated (B4).
- [ ] `go tool task mutate:check -- ./internal/broker/ ./internal/storage/meta/ ./console/` shows no new surviving mutant in the group files listed in section 7; survivors left are listed with a reason.
- [ ] `go tool task test:compat` green for `java`, `python`, `kcat`, `kafkajs`, `streams`, `spring`, `connect`, `kafbat-ui`, `redpanda-console`, plain and SCRAM over TLS.
- [ ] `go tool task fuzz` runs `FuzzConsumerProtocolDecode` and `FuzzGroupSequence` clean.
- [ ] `go tool task test:e2e` green with the route gate on; `groups-live.spec.ts` has the specs in section 7; accessibility sweep clean on `/groups` and `/groups/{id}` for classic, KIP-848, empty, rebalancing, denied; 960 and 390 px.
- [ ] Differential: scenarios R1-R20 recorded under `conformance/recordings/groups/` and replayed with zero "answered differently"; the count still at `memory` or `doc` is written here.
- [ ] Every API of section 2 has its row in `docs/api-support.md` and the version-matrix test passes; every difference left (GRP-8 if kept, the 200 ms initial delay, one-owner rule) is in `docs/limitations.md` with its scenario.
- [ ] Boards implemented: Gr-Home, Gr-01 .. Gr-18 (19), or the document says which were dropped and why (Gr-18's countdown if GRP-19 is cut).
- [ ] The group page works with the broker down, mid-poll, and with an empty data dir (A7).
- [ ] A restart test with the data dir: a KIP-848 group and a classic group with offsets list with the right types; a SIGKILL variant keeps an acknowledged commit.
- [ ] Every confirm sentence is tested against the effect (A6): delete group, remove offset, reset, set.
- [ ] No console text contradicts the ledger: GRP-14, 15, 16, 17, 18, 21, 25, 26 sentences removed or made true.
- [ ] `docs/consumer-groups.md` and `docs/limitations.md` no longer say static membership is unimplemented (GRP-26); `CHANGELOG.md` names every behaviour change (GRP-1, 2, 3, 4, 5, 6, 7, 9).
- [ ] Status table in `plan/README.md` updated; actual time recorded above.

## 11. Files and order of work

Half-day units (hd). Order matters: the recordings come first because five P1s
and three P2s are `memory` until recorded.

| # | Work | Files | hd |
|---|---|---|---|
| 1 | Record R1-R20 against `apache/kafka:4.0.0`; settle GRP-8, 10, 12, 14, 16, 19 questions | `conformance/` | 3 |
| 2 | Red tests for GRP-1, 2, 3, 7, 11, 27, 28, 29 (raw kmsg, every version) then fixes | `group.go`, `groups.go`, `offsets.go`, `groupadmin.go` | 5 |
| 3 | GRP-4: subscription-aware OffsetDelete (848 subscribed set; classic decode of `ConsumerProtocolSubscription`); GRP-17 | `groupdelete.go` | 2 |
| 4 | GRP-5: DescribeGroups for consumer-protocol groups | `groupadmin.go`, `group848.go` | 2 |
| 5 | GRP-6: persist the group type with the offsets (small groups record), restart tests; GRP-10 per R20; GRP-13 | `store.go`, `groupadmin.go` | 3 |
| 6 | GRP-9: topic filter applied to regex matches; authz tests | `groups848.go`, `group848_assign.go` | 1 |
| 7 | GRP-8: implement mixed groups or document, per decision | `group.go`, `group848.go` | 0 or 6 |
| 8 | `GroupInspector` snapshot: members, timeouts, heartbeat and joined times, protocols, leader, connection ids, join deadline, 848 member state and revoking, group generation, empty-since (GRP-20, 22, 25) | new `groupinspect.go`, `group.go`, `group848.go`, `server/` wiring | 5 |
| 9 | Events: vocabulary and durations (GRP-23), per-group ring (GRP-24), console actions emit events | `groupevents.go`, `internal/traffic`, `console/traffic.go` | 3 |
| 10 | Offsets retention, if built (GRP-19): `committedAt`, sweeper, flag `--offsets-retention`, docs | `store.go`, `offsets.go`, `cmd/` | 4 or 0 |
| 11 | Isolation inference and LSO lag (GRP-18) | `console/backend_groups.go`, `internal/traffic` | 3 |
| 12 | Console: Home, 01, 02, 09, 10, 11 (list, empty, lag, no-commits, ahead, deleted) on the new shell; home summaries call (GRP-34) | `templates/groups.html`, `handlers_groups.go` | 4 |
| 13 | Console: Members, Rebalancing, Classic, 848 boards (03, 05, 12, 13) and member inspector | same | 4 |
| 14 | Console: Timeline, Flapping (04, 17), Reset and preview (06, 07, 08), Confirms (15), Denied (16), Read committed (14), Expiry (18) | same | 5 |
| 15 | Tests: model and fuzz targets, coverage to targets, e2e `groups-live.spec.ts`, compat additions, mutation survivors | tests | 6 |
| 16 | Docs and close: `api-support.md`, `limitations.md`, `consumer-groups.md`, `console.md`, `CHANGELOG.md`, README status | `docs/` | 2 |
| | **Total** (with items 7 and 10 at their lower bound) | | **52, about 26 days; L** |

**Prior art.**

- Apache Kafka's classic coordinator (`GroupCoordinator`, `GroupMetadataManager`): session bounds, `INVALID_GROUP_ID`, `OFFSET_METADATA_TOO_LARGE`, empty-since and offsets retention, delayed join/heartbeat purgatory. The rules in GRP-1 to GRP-3 and GRP-19 come from there; each is recorded (section 8) before it is copied.
- KIP-848 ("The Next Generation of the Consumer Rebalance Protocol") and the migration policy for mixed groups: GRP-8.
- Redpanda: keeps groups in a coordinator with persisted membership and offsets, and its Console shows members with host, assignments and lag; it does not show a rebalance timeline.
- Confluent Control Center and Kafbat UI show state, members and lag; neither shows why a group rebalanced. The timeline and flapping board are the part with no prior art; the events are the contract.
- doze-aws `services/sqs.md` for the shape: a solid core, boards that over-claim, and a recording queue settled before behaviour is changed.
