# Claims: Groups (decision D7)

The two Groups-stage questions D7 left open, recorded against `apache/kafka:4.3.1`
(`plan/stages/groups.md`, GRP-8 and GRP-19). The rest of the stage's recording queue (R1
to R20) is still at `memory`.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| R8a | A consumer-protocol member (ConsumerGroupHeartbeat) joining a classic group of protocol type "consumer" with members is accepted: the group becomes a consumer group, the classic member stays in it with the partitions it held, and a classic Heartbeat from it is then REBALANCE_IN_PROGRESS (27) (GRP-8) | recorded | conformance/recordings/mixed-protocol-groups.json | mixed-protocol-groups, steps c1-* |
| R8b | A classic JoinGroup to a consumer group is accepted (MEMBER_ID_REQUIRED, then success) with no leader and no member list; ConsumerGroupDescribe has the classic member among the others and DescribeGroups answers Dead | recorded | conformance/recordings/mixed-protocol-groups.json | mixed-protocol-groups, steps n1-* |
| R8c | A classic group whose protocol type is not "consumer" is not upgraded: ConsumerGroupHeartbeat is GROUP_ID_NOT_FOUND (69), "Cannot upgrade classic group ... because the group does not use the consumer embedded protocol."; ConsumerGroupDescribe says "is not a consumer group." | recorded | conformance/recordings/mixed-protocol-groups.json | mixed-protocol-groups, steps x-* |
| R8d | A classic member that rejoins a consumer group is given the partitions the assignor left for it, a heartbeat of a member whose partitions are all held by another is 0 and not 27, and a consumer-protocol member that takes partitions the classic member gave up on its rejoin | recorded | conformance/recordings/mixed-protocol-rolling.json | mixed-protocol-rolling, steps up-*, down-c-* |
| R8e | When the last consumer-protocol member leaves a consumer group of classic members it is a classic group again: Heartbeat is ILLEGAL_GENERATION (22), DescribeGroups is PreparingRebalance with the member, ConsumerGroupDescribe is GROUP_ID_NOT_FOUND | recorded | conformance/recordings/mixed-protocol-rolling.json | mixed-protocol-rolling, steps down-n-leaves, down-c-heartbeat-3, down-describe-* |
| R8f | An emptied group moves protocol whole and keeps its committed offsets, in both directions | recorded | conformance/recordings/mixed-protocol-rolling.json | mixed-protocol-rolling, steps emptyup-*, emptydown-* |
| R11a | A group with no members, that had some, keeps its offsets for offsets.retention.minutes from the moment it became empty: a commit made into it afterwards does not extend it (GRP-19) | recorded | conformance/recordings/offsets-retention.json | offsets-retention, groups g-late (still there at 75 s, gone at 115 s) and g-recommit (gone at 75 s) |
| R11b | A group that never had a member (a standalone commit) keeps each offset for the retention from its own commit | recorded | conformance/recordings/offsets-retention.json | offsets-retention, group g-standalone |
| R11c | A classic or consumer group with members keeps the offsets of the topics it is subscribed to and loses, from their commit, those of a topic it is not | recorded | conformance/recordings/offsets-retention.json | offsets-retention, groups g-active and g-c-active (ret-b) |
| R11d | A group whose offsets have all expired is no longer listed, and DescribeGroups says Dead | recorded | conformance/recordings/offsets-retention.json | offsets-retention, steps t75-list, t115-list, t75-describe-g-empty |
| R11e | An OffsetFetch of an expired offset is not an error: the partition is -1 | recorded | conformance/recordings/offsets-retention.json | offsets-retention, steps t75-fetch-*, t115-fetch-* |
