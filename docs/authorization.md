# Authorization (ACLs)

doze-kafka enforces the ACLs you create, with Kafka's rules. If `kafka-acls`,
Terraform or an admin client says an operation is denied, it is.

Nothing changes until you create one. With no ACLs a client can do everything, as
before. An ACL created through `CreateAcls` takes effect on the next request.

## Who is asking

Every connection has a **principal**: the SASL user as `User:alice`, or
`User:ANONYMOUS` when the broker does not ask for authentication (every client, then,
including the console). An ACL names a principal (or `User:*` for any), a host (or `*`),
an operation, and allows or denies it on a resource.

## The rules

A request is an operation on a resource by a principal from an address. In order:

1. A **super user** is never refused.
2. A matching **DENY** refuses it, whatever else allows it.
3. A matching **ALLOW** permits it.
4. If no ACL at all is about the resource, the **default** decides: `allow` (the
   default here, so a topic nobody wrote an ACL for stays open) or `deny` (Kafka's own
   default, set with `--acl-default deny`).
5. Otherwise it is refused.

An ACL applies to a resource **by name** (literal), **by prefix**, or to every resource
of its type (`*`). `ALL` covers every operation; `Read`, `Write`, `Delete` and `Alter`
each imply `Describe`, and `AlterConfigs` implies `DescribeConfigs`.

A resource with ACLs is closed to everyone they do not allow, even with the default of
`allow`: writing one ACL for `orders` makes `orders` closed to everyone else, and leaves
every other topic as it was.

## What each API needs

| API | Needs |
|---|---|
| Produce | Write on the topic |
| Fetch | Read on the topic |
| ListOffsets, Metadata (a named topic) | Describe on the topic (a topic list shows only what may be described) |
| Metadata that would create a topic, CreateTopics | Create on the cluster or the topic |
| DeleteTopics, DeleteRecords | Delete on the topic |
| CreatePartitions | Alter on the topic |
| DescribeConfigs, AlterConfigs, IncrementalAlterConfigs | DescribeConfigs / AlterConfigs on the topic, or on the cluster for the broker |
| DescribeCluster | Describe on the cluster |
| InitProducerId | IdempotentWrite on the cluster, or Write on any topic; with a `transactional.id`, Write on that id |
| AddPartitionsToTxn, AddOffsetsToTxn, EndTxn, TxnOffsetCommit | Write on the transactional id (and Write on each topic, Read on the group / topics for the offsets) |
| Produce (transactional batch) | Write on the topic and on the transactional id |
| FindCoordinator (transaction), DescribeTransactions, ListTransactions | Describe on the transactional id (a listing shows what may be described) |
| DescribeProducers | Read on the topic |
| FindCoordinator, DescribeGroups, ConsumerGroupDescribe, OffsetFetch | Describe on the group (OffsetFetch also Describe on each topic) |
| JoinGroup, SyncGroup, Heartbeat, LeaveGroup, ConsumerGroupHeartbeat | Read on the group (a KIP-848 member also leaves out topics it may not Describe) |
| OffsetCommit | Read on the group and on each topic |
| ListGroups | shows the groups that may be described |
| DeleteGroups, OffsetDelete | Delete on the group (OffsetDelete also Read on each topic) |
| DescribeAcls | Describe on the cluster |
| CreateAcls, DeleteAcls | Alter on the cluster |

A refusal is the code Kafka uses for that API: `TOPIC_AUTHORIZATION_FAILED`,
`GROUP_AUTHORIZATION_FAILED` or `CLUSTER_AUTHORIZATION_FAILED`.
`ApiVersions`, `SaslHandshake` and `SaslAuthenticate` need nothing: they are how a
client says who it is.

## Settings

| Flag | Default | |
|---|---|---|
| `--acl-default` | `allow` | What a resource with no ACL allows: `allow` or `deny`. |
| `--acl-super-user` | *(none)* | A principal never refused, as `User:admin`. Repeatable. In the file: `acl-super-user = ["User:admin"]`. |

The console's own user (`__console`, created when authentication is on) is always a
super user. Without authentication the console is `User:ANONYMOUS` like any other
client, so an ACL that restricts `User:ANONYMOUS` restricts the console too.

`DescribeConfigs` on the broker reports `allow.everyone.if.no.acl.found` and
`super.users`, as Kafka names them, and the console's **Access** page lists the ACLs
and says how they are applied.

## Locking yourself out

With `--acl-default deny` and no super user, nothing is allowed, including creating the
ACLs that would allow it. Name a super user (`--acl-super-user User:admin`) and
authenticate as it to set things up. With the default of `allow` this cannot happen
until an ACL is created on the cluster resource; then only principals it names may
create or delete ACLs, so name a super user first.

## Limits

Principals are `User:` names only (no groups or roles), the host is matched as written
(no CIDR ranges), and delegation tokens are not resources here (they do not exist). A transactional
id is a resource: see [Transactions](transactions.md). ACLs are stored in the metadata database and survive restarts.
