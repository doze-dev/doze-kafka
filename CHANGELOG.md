# Changelog

User-visible changes, newest first. A change to the exported surface of
`server/` (the embedding API, pinned in `testdata/api/server.txt`) needs a line
here that contains "server API"; `task api` fails without it.

## Unreleased

- Console: the Traffic pages are drawn as their boards and say only what the broker knows.
  A design gate (`go tool task design`) renders each board's own files and compares the
  live page to them element by element, with real clients (classic and KIP-848 groups,
  telemetry), against a ledger of what they sent; a difference that is not on its
  allow-list, each entry quoting the plan's record, fails. Wrong before: a client's
  refusal streak crossed its connections' requests; a fold claimed 169 identical
  heartbeats across six rebalances; `client=checkout` listed `checkout-api`; "last
  minute" was 55 to 60 s; the ApiVersions downgrade claimed the client asks again;
  Metadata said "auto-create on" from the request's flag on a broker with it off; a
  client's requests a minute fell when one of its connections closed; a client that hung
  up before its answer, or an API key no Kafka has, counted as the broker failing; times
  under a millisecond read `0.4 ms`, where the boards print `0 ms` (the exact value is in
  the title). Esc clears the filter; the keys strip follows each state; the rows are one
  tab stop of a grid; the inspector announces what it holds; a request's decoded fields
  are written in the specification's snake case. The keys strip is a region, and the
  phone's brand link keeps its name.

- Groups: a classic JoinGroup into a consumer group whose subscription cannot be read is
  answered UNKNOWN_SERVER_ERROR with no member id, as Kafka answers it (it fails to read
  it with an exception it does not catch), where doze answered INCONSISTENT_GROUP_PROTOCOL;
  a static join and a JoinGroup v3 too (recorded: unreadable-subscription).

- Groups: DescribeGroups, ConsumerGroupDescribe and DeleteGroups that name several
  groups answer them in Kafka's order, which is not the request's (it follows the
  groups' __consumer_offsets partitions); doze answered in the request's order. A client
  that pairs the answers with what it asked by position now pairs them as on Kafka
  (recorded: describe-groups-order).

- Groups: ConsumerGroupDescribe shows a static member that has left for a restart
  (epoch -2) at epoch -2, and its group Reconciling, as Kafka does; it showed the epoch
  it had left at and the group Stable (recorded: consumer-static-leave).

- Groups: the first rebalance of an empty classic group waits as Kafka's does: a member
  that joins during the delay starts another (cut to what the rebalance timeout
  leaves), and the first member to join is the leader; it used to wait one delay from
  the first join and pick a leader by member id. The delay is a setting,
  `--group-initial-rebalance-delay` (TOML `group-initial-rebalance-delay`; Kafka's
  `group.initial.rebalance.delay.ms`), 200 ms by default where Kafka's is 3 s. server API:
  `Options.GroupInitialRebalanceDelay` (0 is the default, negative does not wait).
  DescribeGroups shows a group's protocol and its members' metadata only once it is
  Stable, as Kafka does (recorded: classic-initial-rebalance-delay).

- Groups: a consumer group's new targets are computed at most once a second, by a
  member's heartbeat (or a classic member's JoinGroup), as on Kafka
  (`group.consumer.assignment.interval.ms`). A member that joins or leaves within a
  second of the last targets leaves the group `Assigning`, the joiner at the targets'
  epoch, until such a heartbeat; a leave or an expired session no longer moves anyone's
  partitions by itself. Before, the targets were computed when the change was made, so
  epochs and generations read one higher than Kafka's and the group was never
  `Assigning`. A describe now shows a member's partitions with those it has been told to
  give up and has not yet (recorded: consumer-assignment-interval, consumer-assignment-expiry).

- Console: Traffic holds a client's long names (client id, software, group, transactional
  id, a 249-character topic) in its columns, cut with an ellipsis and whole in the title
  and the inspector. A client with 90-character names made the clients table 3,675 px
  wide, the inspector scroll sideways and a group's pill push its controls off the page;
  at a phone's width the requests' cards lost their time and request to the client id.
  Between 641 and 1179 px the clients table leaves out principal, opened and last request.

- Console: the wire dock at the foot of a topic or group page pages back through the
  requests it holds (Older, Newest); it showed the newest eight and no more.

- Console: with the list of keys (`?`) open, the page's keys are off. They used to act
  on the page behind it: `p` paused the live view, `j` and `k` moved the cursor, `c`
  and the `g` chords left the page.

- Console: a topic's message tape draws its own marks again (8 px, the recent ones
  inked). The Traffic inspector's fold tick had the same class name and redrew them as
  4 px of ink. The cards of the create forms are 12 px apart, as on the boards.

- Console: a client flagged because most of its requests in five minutes were refused is
  told so ("was refused 3 of its 5 requests in the last five minutes"). The page said
  "has been refused 0 times in a row" whenever its latest request had gone well. A streak
  on one of a client's several connections says it is one connection's.

- Console: the wire dock's "Traffic page" link lands on that topic's or group's requests
  (it carried `topic%3dorders`, which is no filter, and showed everything). The Older,
  Newer and Latest links of a topic's committed-only view keep the view; they wrote
  `from=0%26committed%3d1`. A subject's dock no longer links to a Traffic page that has
  no filter for a subject.

- Produce reads every batch before storing it, as Apache Kafka does, on every kind of
  topic and every Produce version (recorded against `apache/kafka:4.3.1`:
  `produce-validation`, `produce-inflated-size`). Batches Kafka refuses used to be
  acknowledged and stored on any topic that did not compact: a records section that is
  not the codec it names (now UNKNOWN_SERVER_ERROR, as Kafka answers), a header claiming
  more or fewer records than it holds (it advanced the log by the claimed count), two
  batches for one partition, an empty record set, a base offset other than 0, a producer
  id with sequence -1 (INVALID_RECORD); a transactional batch with no transactional id
  (TRANSACTIONAL_ID_AUTHORIZATION_FAILED); a timestamp outside
  `message.timestamp.before.max.ms` / `after.max.ms`, now applied, or a client-set
  LogAppendTime bit (INVALID_TIMESTAMP). Refusals Kafka also made now carry Kafka's code
  (INVALID_RECORD where this said CORRUPT_MESSAGE for a zero-count batch, a legacy magic, a
  control batch or a short record set), base offset -1 and Kafka's message or none.
  **A topic with no `max.message.bytes` is now held to the broker's `message.max.bytes`,
  Kafka's default of 1048588** (`--message-max-bytes`, `message-max-bytes` in the file):
  a batch over 1 MB was accepted here and refused by every production broker. A topic
  whose `compression.type` is not the batch's stores it re-written in its codec and holds
  that size to the limit. A batch inflating past 64 MiB, a zstd window past 8 MiB and a
  compressed record whose length disagrees with its fields are refused here and kept by
  Kafka (`docs/limitations.md`). A log from an earlier version holding an unreadable batch
  still opens: compaction and timestamp lookups go past it instead of failing for good.
  A retry of an idempotent batch is answered with the timestamp it was stored with.
- Security: an Avro value is checked against its schema before the Avro decoder sees it.
  The console decodes registry-framed record values, which any producer can write, with
  `hamba/avro`, which is archived and has three open advisories against its decoder
  (GO-2026-5046, 5047, 5048; no fixed release). 328 bytes naming 64 arrays of a million
  nulls made the console allocate 5 GiB, and a registered `fixed` of 512 MiB allocated
  2 GiB for a seven-byte value. Every length and count is now checked first: the arrays
  and maps of one value hold at most 1,048,576 elements between them, nothing nests more
  than 1,024 deep, and nothing claims more bytes than are there. A truncated value is no
  longer shown as if it had decoded. `go tool task vuln` (govulncheck, in CI on every push
  and daily) fails on any reachable vulnerability that is not accepted with a guarding
  test in `testdata/vulncheck-accepted.txt`; it found five more in the doze module's
  dependencies, fixed by upgrades (`golang.org/x/net` 0.60.0, `google.golang.org/grpc`
  1.83.2).
- `go tool task check` runs every benchmark once (`bench:smoke`).

- The doze module's console (the one `doze` starts beside a `kafka` instance) has the
  Traffic page and the clients' own metrics, as the `doze-kafka` command's console has.
  Its broker was built as one with no console, so nothing was recorded, `/traffic` was a
  404 and the client-telemetry APIs (71, 72) were not offered. A module instance started
  with no console address still keeps and offers neither, and neither does a broker built
  with `server.New`.

- CreateTopics: a request for many topics no longer compares each name with every topic
  (twice: the handler listed them all and the store looked at them all, so a batch was
  quadratic); both ask an index kept by the metadata store. Which names collide is now
  exactly Apache Kafka's, from a recording of 4.3.1 (`createtopics-collisions`): a name is
  held against the topics that existed before the request and not against the others in
  it, so `a.b` and `a_b` sent in one request are both created, where this broker used to
  refuse the second with `INVALID_TOPIC_EXCEPTION`. A name that collides with an existing
  topic is still refused, with or without `validate_only`, and names that differ only in
  letter case are refused everywhere.

- Embedding (`server/`): nothing outlives a failure. A `Serve` that cannot bind closes the
  `AlsoListen` listeners too (they stayed bound and accepting); a `New` that fails closes
  what it had opened, the lock on the data directory included (a broken record in the
  metadata database left the directory "in use" by a broker that did not exist).
  `Close` on a server that is serving now stops it, as cancelling its context does, and
  returns once `Serve` has drained and closed storage; before, it closed storage under
  the connections still using it. `Ready()` is closed by `Close` as well, so waiting on it
  never hangs, and what it means is read from `Addr()`: not nil, the listener is bound;
  nil, the server ended without ever binding. `ServeListener` owns the listener it is
  given and closes it when it returns, except when the call is refused. No change to the
  exported surface.

- With `--listen` on port 0 the console and the schema registry take free loopback
  ports too, so the documented throwaway broker runs twice at once (the second one
  failed on 8081). A fixed `--listen` keeps `127.0.0.1:9093` and `127.0.0.1:8081`.
- The start-up summary's last-but-one line is an `env` command that gives this
  broker: under `--listen` it carries the bound addresses (`eval "$(doze-kafka env
  --listen 127.0.0.1:… --config '')"`), where the bare `doze-kafka env` printed the
  `.doze` name. `doze-kafka env` accepts `--sasl-mechanisms` without `--sasl-user`.
- `--zone=false` (`zone = false`, `DOZE_ZONE=off`) without `--listen` is a configuration
  error, one line and exit 2, in every command; it used to fail after start-up began,
  exit 1, and `doctor` and `env` reported a name that was not served.
- `config print` prints every setting at its effective value, `console-prefix` and
  `acl-default` included, and `meta-db` as a comment while it follows `data-dir`.
  `DOZE_KAFKA_CONSOLE_PREFIX` is now validated like the flag.
- `--console-prefix` (`console-prefix`, or `DOZE_KAFKA_CONSOLE_PREFIX`) serves the console
  under another path, for a reverse proxy; `/` serves it at the root of its port.
- Console: the home page is Traffic, or the first-run page on a broker with nothing in
  it that has heard from no client (it was Topics); `/connect` is new and says how to
  connect an app; `/trace` without an id is a page with a field for one (it was a 400);
  the rail is grouped Observe, Data, Broker; the inspector, in the page's flow, replaces
  the drawer; a high-contrast theme joins light and dark, and the palette no longer has
  theme rows (the top bar's switch does it).
- Protocol answers: CreateTopics refuses a manual assignment with a replication factor
  or partition count set (`INVALID_REQUEST`) and a malformed one, such as a repeated or
  missing partition, an empty replica list or an unknown broker
  (`INVALID_REPLICA_ASSIGNMENT`); FindCoordinator answers `INVALID_REQUEST` for an
  unknown key type; OffsetCommit answers `OFFSET_METADATA_TOO_LARGE` for metadata above
  4096 bytes; `cleanup.policy` accepts a repeated value, as Kafka's list validator does.
- Go 1.27 is required to build doze-kafka or embed it (`go.mod` says `go 1.27.0`).
- The schema registry is served over TLS when the broker is: with `--tls-cert` and
  `--tls-key` it answers `https://` with the same certificate (it stayed plain HTTP
  before, so `schema.registry.url=https://...` failed and basic-auth passwords crossed in
  the clear beside an encrypted broker). The summary and `doze-kafka env` print the
  `https://` URL; by name that is `https://kafka.<name>.doze:8081`.
  `--schema-registry-tls=false` (`schema-registry-tls`) keeps it plain HTTP, with a
  warning at start-up when it is then reachable from other machines. **If you run with
  a certificate, change `http://` to `https://` in your registry URL.**
- With SASL or TLS on, a console address beyond loopback is refused at start-up (exit
  2): the console has no login, is plain HTTP and acts as a super user.
  `--console-unprotected` (`console-unprotected`) accepts it, with a warning.
  [What SASL and TLS cover](docs/auth-and-tls.md#what-sasl-and-tls-cover) says what
  each setting reaches.

- Groups: an empty group with committed offsets is listed and described after a restart
  as before it (recorded: `groups-listed-after-restart`): a classic group kept its
  offsets but lost its protocol type, and an emptied consumer-protocol group was gone
  from `ConsumerGroupDescribe` and listed as classic. The store's record of a group now
  carries its protocol type, or its epoch and assignor (records of earlier versions are
  still read).

- Groups: requests Kafka refuses are refused, with Kafka's codes (recorded:
  `group-requests-refused`). An empty group id is `INVALID_GROUP_ID` on every classic
  group API and `OffsetDelete` (a v0 `JoinGroup` used to join a group named ""). A
  classic `JoinGroup` with no protocol type is `INCONSISTENT_GROUP_PROTOCOL`. A
  `ConsumerGroupHeartbeat` join that says it owns partitions, or does not say, is
  `INVALID_REQUEST`, as is a static leave with no instance id and an empty instance id;
  a rebalance timeout of 0 is accepted; a later epoch in a group that does not exist is
  `GROUP_ID_NOT_FOUND`; the messages are Kafka's. `OffsetCommit` below v9 from a
  consumer-protocol member is `UNSUPPORTED_VERSION`. In a consumer group, a classic
  `SyncGroup` with the wrong protocol type or name is `INCONSISTENT_GROUP_PROTOCOL`, a
  classic `JoinGroup` must share a protocol with the other classic members, an unknown
  member id is seated as Kafka seats it, a classic member's `ConsumerGroupHeartbeat` is
  answered, and a classic `LeaveGroup` can remove a consumer-protocol member. A
  `LeaveGroup` from v3 answers 0 at the top level and each member's code beside it.
- Groups: static membership fencing. A request naming a static instance nobody holds is
  `UNKNOWN_MEMBER_ID`, and one naming an instance another member holds is
  `FENCED_INSTANCE_ID`, on `Heartbeat`, `SyncGroup`, `OffsetCommit` and
  `ConsumerGroupHeartbeat` (a member heartbeating with the wrong instance id used to be
  answered 0; the member a restarted consumer-protocol instance replaced was
  `UNKNOWN_MEMBER_ID`). An empty `group.instance.id` is an instance id on a classic
  `JoinGroup`, as on Kafka. The docs no longer say static membership is not acted on.

- server API: `Options.OffsetsRetention` sets how long committed offsets are kept once
  nothing reads them, as the CLI's `--offsets-retention`. An embedder had expiry at the
  7-day default with no way to change or turn it off; zero is still that default and a
  negative value keeps offsets for ever.

- Groups: a classic member of a consumer group that rejoins keeps the partitions its
  `JoinGroup` says it still owns, as Apache Kafka 4.3.1 does (recorded:
  `mixed-protocol-cooperative`). Every rejoin used to give up everything, which is
  true of an eager consumer only: a cooperative-sticky one goes on reading until its
  `SyncGroup` returns, and its partitions could be handed to another member meanwhile.
  Its assignment is written at the version of its subscription. A static classic
  member (`group.instance.id`) that restarts takes the place of the member holding its
  instance instead of being seated twice, and the old member id is
  `FENCED_INSTANCE_ID` (recorded: `mixed-protocol-static`).

- Groups: a classic group upgraded in the middle of a rebalance answers what it had
  parked as Apache Kafka 4.3.1 does (recorded: `mixed-protocol-parked`). A `JoinGroup`
  waiting for the others to rejoin was answered `UNKNOWN_MEMBER_ID`, which makes a
  client throw its member id away, while that id was kept as a member of the new
  consumer group and held a share of the partitions until its session ran out (45 s
  for a Java client). It is now `REBALANCE_IN_PROGRESS`, the member keeps its id and
  its next `JoinGroup` finds its seat. A group upgraded between its `JoinGroup`s and
  the leader's `SyncGroup` keeps each member's partitions of the generation before
  (they were dropped, which freed them for the new member while a cooperative consumer
  was still reading them), and the `SyncGroup`s that follow are
  `REBALANCE_IN_PROGRESS`. A refused `SyncGroup` carries no protocol type and no
  protocol name.

- Offsets retention: a group that is gone leaves nothing behind. `DeleteGroups` removed
  the group's offsets and left "empty since" in memory, so the same id used afterwards
  by a standalone committer had its new offsets judged by when the deleted group had
  emptied, and lost them early. The record now goes with the group, before its offsets
  do. As on Apache Kafka 4.3.1 (recorded: `offsets-retention-recreate`), a group with no
  members and no offsets is dropped by the next sweep, classic or consumer-protocol:
  until then it is listed `Empty` and a commit made into it is a commit into an empty
  group; after it, the id is a new group. Before this a classic group with no offsets
  was unlisted the moment its last member left (Kafka lists it until its next check),
  an emptied consumer-protocol group with no offsets stayed listed until the broker
  restarted, and the emptiness of a group that then lost its last offset was counted
  against later commits for a whole retention. A classic group that empties moves on a
  generation, as Kafka's does: the next member to join is told two more than the last
  one had.

- Offsets retention: a restart no longer expires the offsets of a group that was live
  when the broker stopped. Members are not kept across a restart, and a group with none
  in memory and no record of having emptied was taken for one nobody had ever joined:
  each offset expired from its own commit, so the first sweep after a restart could
  delete an old, valid offset on a quiet partition before its consumer was back. The
  metadata database now records that a group has members (and their longest session)
  before the first member's join is answered, so it is there after a crash too. After a
  restart such a group is treated as Apache Kafka 4.3.1 treats the members it loads
  back (recorded: `offsets-retention-restart`): they are there until their session has
  passed with nobody joining; from then a classic group has its retention to go, and a
  consumer-protocol group's offsets are as old as their commits. That last is Kafka's
  rule for any consumer-protocol group with no members, and is new here: such a group
  used to keep its offsets for the retention from when it emptied. A data directory
  from an earlier version is adopted on first start (every group with offsets is taken
  to have had members). The record is kept with `--offsets-retention 0` as well. A join
  that cannot be recorded is `COORDINATOR_NOT_AVAILABLE`. The sweep judges a group from
  one view of it, taken with both coordinators held still, where it read three things
  under three locks; a failed write of the record is tried again; and a group being
  deleted is skipped. ConsumerGroupDescribe of a group that does not exist carries
  Kafka's message, `Group <id> not found.`

- A Fetch at an offset out of range (above the log end or below its start) answers
  `OFFSET_OUT_OF_RANGE` with high watermark, last stable offset and log start offset
  -1, as `apache/kafka:4.3.1` does (it used to carry the partition's real offsets), and
  DescribeConfigs answers a resource with an empty error message rather than null
  (recorded: `fetch-above-the-end`, `list-config-append`; `conformance/claims/api.md`
  V6, V7). Every code comment that says what Kafka does now names the claims row
  that shows it, and a test holds new ones to that.

- Transactions and ACLs refuse malformed requests as `apache/kafka:4.3.1` does
  (recorded: `txn-empty-id`, `acl-unknown-codes`; `conformance/claims/api.md` V4, V5).
  An empty transactional id is `INVALID_REQUEST` in EndTxn, AddOffsetsToTxn,
  AddPartitionsToTxn and InitProducerId (InitProducerId used to hand out a producer id
  for it, as for a null id); AddOffsetsToTxn no longer refuses an empty group id.
  An ACL request naming the UNKNOWN (0) code for resource type, pattern type, operation
  or permission closes the connection; a code that is no value matches nothing in
  DescribeAcls and is `INVALID_REQUEST` in Kafka's words in DeleteAcls and CreateAcls;
  CreateAcls with ANY or MATCH answers `UNKNOWN_SERVER_ERROR`.

- API versions: no `--kafka-version` profile offers a version the real broker of that
  major does not serve. At the default, `--kafka-version 4`, nine old versions that
  Kafka 4 removed (KIP-896) are no longer served: ListOffsets v0, OffsetCommit v0–1,
  OffsetFetch v0, CreateTopics v0–1, DeleteTopics v0, DescribeAcls, CreateAcls and
  DeleteAcls v0, DescribeConfigs v0. A request at one closes the connection, as on
  `apache/kafka:4.3.1` (recorded: `versions-not-served`). A client that old works at
  `--kafka-version 3`, as it does against a Kafka 3 broker. At `--kafka-version 1`,
  six maxima come down to what Kafka 1.1.1 has (Metadata v5, FindCoordinator v1,
  ListGroups v1, InitProducerId v0, SaslAuthenticate v0, DeleteGroups v0). Every
  column of the version table is now held by a test to the recorded `ApiVersions`
  answer of a real broker of that major (`conformance/recordings/api-versions.json`;
  `docs/api-support.md` says which image stands for which).

- **Security: SASL.** A connection logs in once, as the user whose password it proved,
  and stays that user. Before this (a) a PLAIN token naming another user as its
  authorization id (`bob NUL alice NUL alice's password`) was accepted, where Kafka
  refuses it; SCRAM ignored `a=` the same way; (b) a connection that had logged in could
  send a second `SaslAuthenticate` and become that token's user (alice became bob with
  bob's password), and a wrong password on it logged the connection out; (c) a refused
  `SaslHandshake` left the connection open for another try. Every answer to a login now
  is the one `apache/kafka:4.3.1` gives, from two recordings of it (`sasl-plain`, 143
  steps; `sasl-scram`, 293 steps; `conformance/claims/auth.md`): the authorization id
  must be empty or the user's own name; after a login `SaslHandshake` and
  `SaslAuthenticate` are `ILLEGAL_SASL_STATE` and change nothing; a `SaslAuthenticate`
  with no handshake, a second `ApiVersions` before the handshake and a request in the
  middle of a login close the connection; a second handshake mid-login is
  `ILLEGAL_SASL_STATE` and closed; the refusal texts are Kafka's ("Authentication
  failed: Invalid username or password", the same for an unknown user); an accepted
  token answers an empty error message, not a null one; SCRAM takes only the `n`
  channel-binding flag (it accepted `y`), does not sign a reserved `m=` attribute,
  refuses a bad `=XX` escape in a user name and anything around the three attributes of
  a client-final message, and closes without an answer on a proof of the wrong length.
  Two answers stay stricter than Kafka's and are written in `docs/limitations.md`.
  `CreateAcls` answers an empty error message for an ACL it created, as Kafka does.
- Traffic inspector: for the requests that carry a credential (`SaslHandshake`,
  `SaslAuthenticate`, the SCRAM-credential and delegation-token requests) the decoded
  fields are held to a list of what may be shown; every other field reads `"(hidden)"`.
  The length of a SASL token is no longer shown (`"AuthBytes": "29 bytes"` gave the
  length of a PLAIN password), and neither is the size of a `SaslAuthenticate` request,
  which gave it too: its row reads `0 B in`.

- Config file errors name the line and the key and never repeat what the file says. An
  unquoted value outside `[credentials]` used to be quoted back (`expected value but
  found "hunter2"`), as did a bad duration, and a password typed where the user's name
  goes was printed whole; under `[credentials]` the key is no longer named, only the
  line. `credentials` written as anything but a table (`credentials = "..."`) is refused:
  it used to load as a broker with no users.

- Credentials: `--pprof-addr` no longer serves `/debug/pprof/cmdline` (the command line,
  which is where `--sasl-user user:password` is); the other profiles are unchanged. The log
  line for a handler that panicked during a SASL exchange names the type of the panic's
  value and the stack, not its text. The test that reads every output for a planted
  password now reads text and JSON logs at every level, logins with PLAIN and both SCRAM
  mechanisms (right and wrong password), malformed SASL input, the panic line, the pprof
  pages, the schema registry's answers to HTTP Basic credentials, `/healthz`, the console
  and its Traffic inspector, every file of the data directory (and its mode), `env`,
  `doctor`, `--help`, `config print`, and a restart over the same data; a surface it did
  not reach fails it.

- Durability: an idempotent `acks=all` retry after the broker was killed and restarted
  is fsynced before it is acknowledged, and a roll after a restart flushes the segment
  it seals; after an unclean stop nothing read back is taken to be on disk until it has
  been flushed (a clean stop writes `data/clean-shutdown`). EndTxn is answered only
  with every marker on disk, and the opposite decision of a decided transaction is
  answered as Kafka answers it (`CONCURRENT_TRANSACTIONS`, then `INVALID_TXN_STATE`)
  instead of being acknowledged. On a full or failing disk OffsetCommit and the
  transaction APIs answer `NOT_COORDINATOR`, as Kafka does, and a refused Produce
  answers -1 offsets and no message. A cleaning pass that fails part-way leaves the log
  readable without a restart. A multi-batch produce whose write fails part-way leaves
  nothing in the log.

- Console: every button reads weight 400 whether it is a button or a link drawn as one (the
  boards draw them all as buttons). A request that took under a millisecond shows its
  measured time (`42 µs`, `0.4 ms`) rather than the design boards' `0 ms`: a broker
  answering from memory does that constantly, and `0 ms` says nothing was measured (AD6).

- Traffic inspector: "Result per partition" pairs each partition with its own answer. A
  fetch of several topics used to give every topic the result of the last topic that had
  a partition of the same number (a refused `b-0` read as a refused `a-0`, with its
  record count and bytes). A fetch that names its topics by id (v13) now lists each
  partition under its topic's name; they were blank. ListOffsets, OffsetFetch,
  TxnOffsetCommit, DeleteRecords and OffsetDelete list their partitions and results too.
- Traffic recorder: a request naming thousands of partitions no longer stalls every
  other connection. Describing a request was quadratic in its partitions (48 ms for a
  4,000-partition fetch) and ran under the recorder's one lock, which every connection
  takes after every request; it now costs the same whatever the request names (about
  16 µs) and runs outside the lock. A Traffic page poll copies the rows it draws (about
  110 KB) where it copied everything held, twice (3.5 MB each time), under that lock.
  The paused page's "N new requests" now honours the search field as the table does.
- `ApiVersions` answers as Apache Kafka 4.3.1 does, from recordings of it
  (`conformance/recordings/apiversions-*.json`). From v3, a client software name or
  version that is empty, or is not letters and digits with dots and dashes only between
  them, is `INVALID_REQUEST` with no APIs listed (it was answered 0 and the whole list;
  a client that sends ApiVersions v3 by hand with no name is now refused, as it is by
  Kafka). A version of ApiVersions that is not served is answered `UNSUPPORTED_VERSION`
  with one row, ApiVersions itself (it was the whole table). ApiVersions is offered to
  v4 when emulating Kafka 4.
- Traffic leaves out the console and nobody else. The console was known by its client
  id, so a client that called itself `doze-kafka-console` (or `-fetch`, `-trace`) had
  no rows and was not among the connected clients. The console is now known by its
  connections, which the broker process marks in memory when it opens them to itself;
  a client id is only a name, and a client using one of the console's is listed.
- What a client can make the broker keep is bounded. A client's software name and
  version are kept to 128 bytes (300 connections each sending a 1 MiB name held 500 MB).
  A group id, group instance id, rack id, client-chosen member id, protocol name,
  subscribed topic name or pattern, or transactional id longer than 4,096 bytes is
  refused, and so is a SCRAM first message that long (Kafka has no such bounds:
  `docs/limitations.md`). A group id with a NUL byte is refused by a classic JoinGroup
  as it already was by a commit.

- Client telemetry answers as Apache Kafka 4.3.1 does, from recordings of it
  (`conformance/recordings/telemetry-*.json`, `client-telemetry.json`): a push before the
  interval has passed, and a second subscription request inside it, is
  `THROTTLING_QUOTA_EXCEEDED`; the push after a terminating one is `INVALID_REQUEST`
  once; a body that does not decompress is `INVALID_RECORD`, and one that is not
  OpenTelemetry is taken and not read; the byte cap is 1 MiB and the compression types
  are listed zstd, lz4, gzip, snappy; the subscription id is a function of the instance
  id, so a push holding the id from before a restart is taken; an instance is tied to no
  connection and no principal. The Traffic inspector's decoded fields carry the names of
  Kafka's JSON specification of the request ("TransactionalId", "TopicData") and no
  longer the client library's. A Produce partition that does not exist answers a base
  offset of -1.
- Client telemetry, two more answers made Kafka's (`telemetry-terminating.json`,
  `telemetry-ask-again.json`): every push is `INVALID_REQUEST` while the push before it
  said it was terminating, whatever became of that one (the third terminating push in a
  row was taken; a push after a refused terminating one was throttled), and a
  terminating push inside the interval no longer sets the push clock; the subscription
  request a client sends after a push refused `UNKNOWN_SUBSCRIPTION_ID` or
  `UNSUPPORTED_COMPRESSION_TYPE` is answered, once, where it was
  `THROTTLING_QUOTA_EXCEEDED`. Setting a `CLIENT_METRICS` resource is refused, and a
  broker without a console closes the connection of a client that sends the two calls;
  both are in `docs/limitations.md`.

- Groups: classic and consumer-protocol members can share a group id, as on Kafka 4 with
  its default migration policy (a rolling upgrade or downgrade of an application, one
  instance at a time). A consumer-protocol join turns a classic group into a consumer
  group with its members kept; a classic join becomes a member of a consumer group; the
  last consumer-protocol member leaving makes it classic again. Recorded against
  `apache/kafka:4.3.1` (`mixed-protocol-groups`, `mixed-protocol-rolling`). Before this
  the second protocol was refused (`INCONSISTENT_GROUP_PROTOCOL` / `GROUP_ID_NOT_FOUND`).
  A new consumer-protocol group's first member epoch is 2, as Kafka's; a group nobody
  joined is listed with an empty protocol type, as Kafka lists it, and DescribeGroups of
  an Empty group answers no protocol.
- Committed offsets expire, as Kafka's `offsets.retention.minutes`: `--offsets-retention`
  (default 7 days, 0 keeps them for ever). A group that had members keeps its offsets
  from the moment it became empty, a standalone commit from its own commit, a group with
  members keeps those of the topics it reads and loses the rest; the rules are recorded
  (`offsets-retention`) and the time a group emptied is kept across a restart. A group
  whose offsets are all gone is no longer listed.

- `doze-kafka config print` redacts passwords: every `[credentials]` entry is written as
  `"<redacted>"`, and `config print --show-secrets` writes the file to run from. A file
  that still holds the placeholder is refused when loaded, by user. A malformed
  `--sasl-user` and a bad credentials line in a config file no longer repeat the value
  in the error. Nothing else printed a secret: a test plants a password, a wrong one, the
  console credential and TLS key material and reads the summary, the debug log, every
  console page it links to, `DescribeConfigs` and the errors for each.

- Produce: what a compressed batch may inflate to is bounded by a stream, not a buffer.
  A keyed-topic check used to inflate a batch whole (up to 256 MiB, held again by the
  record decoder) before it looked at a key; it now streams, holding a window and a
  buffer, and refuses a batch that inflates past 64 MiB, or a zstd frame declaring a
  window past 8 MiB, with `MESSAGE_TOO_LARGE`. A topic with `compression.type=uncompressed`
  is held to `max.message.bytes` as the batch inflates, as Kafka holds it (recorded
  against `apache/kafka:4.3.1`, `produce-decompressed-size`); a batch Kafka accepts
  because it weighs little still works.

- Console: a write refused with `KAFKA_STORAGE_ERROR` no longer reads as if nothing
  was written. When the fsync after the append failed the batch is in the log (BAD-2),
  so the refusal, the Traffic inspector's cause and the error table say it may already
  be there and that a non-idempotent retry can store it twice.


- server API: `NewFromConfig` is removed. It took `internal/config.Config`, so no embedder
  could call it; `New(Options)` is the constructor, and this module's commands reach the
  config path through `internal/embed`.
- Traffic, third review: Enter on a link inside the inspector (the client link, "See a
  client that reports", a topic's neighbours) opens its selection; a refusal's name
  never breaks inside a word (the Result column is 118 px, the board's 84 cut
  `UNSUPPORTED_` and `AUTHORIZATION_`); a story row is 28 px; the story files a join
  or sync under the rebalance that produced the generation it names, and a LeaveGroup
  under the rebalance it triggers; the Clients view draws the frozen line when the
  broker is lost.

- Traffic, second review: Enter on a row (after `j`, or on a focused row) opens the
  inspector and leaves it open (the key's default pressed the Close button the loading
  inspector had taken focus for); a join request goes under the earliest rebalance that
  begins just after it, so two rebalances 1.5 s apart each keep their own FindCoordinator
  and JoinGroup; a refusal's name wraps at its underscores, in the table and the story,
  and is never cut; the frozen line is the row under the header; the newest generation
  is drawn open; the Clients view draws no Connected-clients panel, so its nine columns
  fit at 1440.

- Traffic, review fixes: the open row is drawn open (and `aria-selected`) in the
  requests, clients and story tables, through polls and a reload; clients are counted
  by client id, with their connections per client, in the summary, the tab, the rail
  and the table; the story files a rebalance's triggering requests (FindCoordinator,
  JoinGroup) under it, in the order they happened, and a folded heartbeat under the
  generation it was last seen in; a new member's cause is "a member joined"; the
  story's "since" is a localised time; the lost-broker page dims only the rows, hides
  what it cannot count and binds `r` to Retry now; the empty page draws only its card
  and its keys (`c` connects an app); Metadata, ListOffsets, OffsetFetch and refused
  CreateTopics say what they were about; a known API at an unserved version reads
  "Produce v99 is not served by this broker"; a zero topic id is left out of the
  decoded fields.

- Traffic: everything a client says about itself is clipped before it is kept (a
  900 KiB join reason was kept whole in each of 2,000 rows); zstd telemetry and record
  payloads are read through their limit instead of being inflated first (a 6 KB frame
  allocated 60 MiB); a telemetry instance's resource attributes, names and series are
  bounded (200 pushes left 20 MB in one instance) (TRF-1, 2, 4). A cut-off snappy
  header is an error, not a panic (TRF-3).
- Traffic: `MEMBER_ID_REQUIRED` and `REBALANCE_IN_PROGRESS` are normal flow, not errors:
  *errors only*, the error count and a client's error rate leave them out; every error
  the broker returns has a class (refused, denied, not implemented, failed), a cause and a
  fix in one table, which the console's Refused, Denied, Not implemented and Failed also
  read (TRF-8).
- Traffic: a transactional id is no longer listed as a consumer group (TRF-10); a request
  that ended its connection is a row with the reason (TRF-11); a row keeps its id when
  it folds, and a folded count survives a busy broker (TRF-12, 13); a group's rows
  include its members' fetches (TRF-18); the console's own client ids are matched
  exactly (TRF-21); a connection that has sent nothing is not listed as a client
  (TRF-30); errors nested deeper than six levels (`OffsetFetch` v8+) are found (TRF-7).
- Traffic: the page is the design's Main and Traf-02 to Traf-12: filter pills and "N of M
  requests match", errors with what each means and what to do, a request with its
  partitions and decoded fields, folded rows with ticks, a Clients view, a client's own
  metrics and why one has none, a paused page that counts what it missed, a lost broker
  that says when it was last heard, and a group's rebalance told in order. Keys `e`,
  `u`, `m` and `c` act on it.
- telemetry: APIs 71 and 72 are offered only with the console on and not
  `--client-telemetry=false`; an embedded broker advertised them and kept a store nobody
  read (TRF-14). The store is bounded at 100 instances, 150 series and 60 points (under
  32 MB), pushes past a burst are counted and not read (TRF-5, 6), a cut-off point is
  refused (TRF-24), a timestamp far from now is the broker's (TRF-25), and another
  principal cannot take an instance over (TRF-27).
- The embedding surface of `server/` is unchanged by this: no export appeared or
  vanished (`task api` passes).
- durability: a retry of an idempotent `acks=all` batch that the log already holds is
  now fsynced before it is acknowledged; it was acknowledged straight from the page
  cache (BAD-1).
- durability: after a failed `fsync` a partition refuses writes until the broker
  restarts, instead of retrying the sync and acknowledging data the kernel may have
  dropped. Kafka takes the log directory offline for the same reason (BAD-3).
- error codes: a full or failing disk is `KAFKA_STORAGE_ERROR` (retriable) from
  CreateTopics, CreatePartitions, IncrementalAlterConfigs, DeleteGroups and
  OffsetDelete as well as Produce, OffsetCommit and Fetch, where it was
  `UNKNOWN_SERVER_ERROR`; an `acks=0` produce that the disk could not take closes
  the connection, as Kafka does for any refused partition (BAD-4).
- JoinGroup: a `session.timeout.ms` below 6000 or above 1800000 is
  `INVALID_SESSION_TIMEOUT`, as in a stock Kafka broker; any value was accepted
  before (BAD-29).
- tests: the power-cut model keeps only what was fsynced and checks every `acks=all`
  record survives; `SIGKILL` could not see a missing fsync (BAD-13).
- console: Traffic shows the whole time, result and duration of every request (a long
  error name wraps instead of being cut), and every number on its summary line says its
  window: "313 requests and 3 errors in the last minute". At 390 px requests are cards
  and the keys strip is not drawn.
- console: the top bar names the broker by its instance name (the one it answers to
  as `kafka.<name>.doze`), not by the data directory's basename.
- console: the empty Topics page and the create form follow the design: what exists,
  what it is not, the `kafka-topics.sh` command that follows the form, the replication
  factor, what happens next, and a name that is taken is said so while it is typed.
  Field help is a sentence, no longer `unit: ms; range: -1 or more; applied`.
- console: a group's Lag tab ends in Try it (set one partition's offset), and its
  neighbours name its members and the subject of what it reads.
- console: ACL type, operation and permission are written as Topic, Write and ALLOW;
  the Topics table says "no group has committed" instead of "nobody", and "written by"
  says it comes from the recent requests and how old they are.
- console: the command palette has Topics, Groups, Subjects and Actions and no "Go to"
  group; the sections are the rail's and the `g` chords'.
- console: the templates are parsed on the first console request, not at start, so a
  broker nobody opens the console on holds about 1 MB less (idle heap 2.70 MB to 1.71 MB).

- server API: `Server.Traffic()` and `Server.ClientMetrics()` are removed. They
  returned types from `internal/` that a caller outside the module could not
  name; this module's own commands read them through `internal/serverhook`.
- server API: the exported surface is now pinned (`task api`).
