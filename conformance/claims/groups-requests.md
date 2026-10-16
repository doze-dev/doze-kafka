# Claims: group requests not yet recorded

Rows about how Apache Kafka answers group requests that no recording under
`conformance/recordings/` shows yet, cited by the comments in `internal/broker/group.go`
and `groupdelete.go`. They are kept apart from `groups.md` (the Groups stage's recorded
rows) so that the two do not collide; a row moves there when a scenario records it.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| G1 | DeleteGroups of a group that has members is NON_EMPTY_GROUP (68) for that group, of a group that does not exist GROUP_ID_NOT_FOUND (69); an empty group is deleted (0, recorded in R11j) | doc | Kafka protocol guide, DeleteGroups response error codes; KIP-229 (DeleteGroups API) | A scenario deleting a group that holds a member, and an id never used, at DeleteGroups v0 to v2. |
| G2 | OffsetDelete of an offset of a topic Kafka does not have is UNKNOWN_TOPIC_OR_PARTITION for that partition, and Kafka never keeps an offset for a topic that is gone (doze-kafka deletes one a data directory of an earlier version of it can hold) | memory | | A scenario committing for a topic, deleting the topic, then OffsetDelete of the group naming it. |
| G3 | A first JoinGroup with no member id is MEMBER_ID_REQUIRED (79) with the id to join with only from JoinGroup v4; at v0 to v3 the broker gives the member an id and joins it in one step (R6 records v7) | doc | KIP-394 (require member.id for initial join group request), "Compatibility, Deprecation, and Migration Plan" | A scenario sending JoinGroup v0 to v3 with an empty member id into a new group. |
| G4 | A JoinGroup from a member that has one still parked (the rebalance is waiting for another member) replaces it: the parked request is answered UNKNOWN_MEMBER_ID (25) | memory | | A scenario with two classic members, the first's JoinGroup parked, the same member id sent again on another connection, the first's answer read. |
