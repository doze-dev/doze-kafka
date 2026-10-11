# Changelog

User-visible changes, newest first. A change to the exported surface of
`server/` (the embedding API, pinned in `testdata/api/server.txt`) needs a line
here that contains "server API"; `task api` fails without it.

## Unreleased

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
