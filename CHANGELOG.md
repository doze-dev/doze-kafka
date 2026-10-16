# Changelog

User-visible changes, newest first. A change to the exported surface of
`server/` (the embedding API, pinned in `testdata/api/server.txt`) needs a line
here that contains "server API"; `task api` fails without it.

## Unreleased

- Names: a by-name address is printed only when something answers at it. The start-up
  summary, `doze-kafka env` and the console and registry URLs used to give the name
  whenever it resolved on the machine; on a Mac a name can go on resolving after the
  zone's network daemon has gone, and clients were sent to a name nothing listened on.
  Now the name is also dialled (`names.Reachable`) and, when it does not answer, the
  address is given instead, with one log line saying why (`doze-kafka doctor` to fix).

- Built against `doze-names` `dc00aee` (from `fcf6a9e`): one network daemon on macOS
  that serves the home it was installed for, a second name on one address
  (`Lease.Twin`), `Registry.Listen` by address.

- `--zone` (and `zone` in the config file): `--zone=false` keeps the instance out of the
  shared `.doze` zone (no name, no front door), the same as `DOZE_ZONE=off`.

- Console: a third theme (high contrast) beside light and dark; one key map for the
  whole console, with a help overlay generated from it (`?`), and the palette's actions
  (themes, rail, pause, help) beside its search; destructive actions (delete a topic,
  delete records, reset or delete a group's offsets) confirm in a sentence built from
  what the action will do on this broker (partitions, records, the groups that lose
  offsets); deleting a topic says that its groups' committed offsets go with it. Topic
  and group pages have a wire dock at their foot: the requests about that topic or
  group, live.

- Schema registry: it answers only requests whose `Host` is an IP address, `localhost`,
  a `.doze` name, the `--advertised-host` or a `--console-allow-host` name (the same
  list as the console), and refuses any other with 403 in the registry's error shape,
  before asking for credentials. An unauthenticated registry on loopback could
  otherwise be read and written by a web page through DNS rebinding.

- Console: a B3 span or parent id in a record's headers is shown only when it is one (16
  hex digits). A producer can write anything there, of any length, and the Trace page
  printed it as "span …"; such a value is now left out, and the trace id still finds
  the record. The console's Host check takes only digits after the last colon as a
  port: a `Host` like `localhost:x@other.example` was judged as `localhost` and served,
  and is now refused. Both were found by fuzz targets added for the trace-header parser
  and the Host check.

- Console: every button reads weight 400 whether it is a button or a link drawn as one (the
  boards draw them all as buttons). A request that took under a millisecond shows its
  measured time (`42 µs`, `0.4 ms`) rather than the design boards' `0 ms`: a broker
  answering from memory does that constantly, and `0 ms` says nothing was measured (AD6).

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
