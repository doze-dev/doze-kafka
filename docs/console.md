# The web console

doze-kafka serves a small web console beside the broker. It is on by default at
**http://kafka.&lt;name&gt;.doze** (port-less, through the shared front door; the start-up
summary prints the address). Under `--listen` there is no name and it is on
`127.0.0.1:9093` (a free loopback port when `--listen` is on port 0). Turn it off with
`--console=false`; move it with `--console-addr`.

It is a development tool, not an operations product. It answers the questions you
have while building: *did my message arrive, what does it look like, is my consumer
keeping up, why is this topic empty.*

The console is a **client of the broker** over the Kafka wire protocol, like any
other. It sees what your applications see, and it needs the same credentials when
SASL is on (doze-kafka provisions it a private one at start-up).

## Topics

- A list on the left, filtered as you type (`/`), with message counts.
- A topic's page shows its id, partition count, **effective config**, and a
  **tape** per partition: each tick is a record on disk, newest on the right, tall
  in proportion to its size. Tabs (or a select, above a couple of dozen partitions)
  switch partition and show per-partition counts.
- A **message table** that follows the log live. Pause it with the toggle. Click a
  row for the full record
  in a inspector: key, value, headers, timestamp, size. The inspector's address is in the
  URL, so a refresh keeps it open and you can share it. Older/newer buttons walk
  the log.
- Records are shown **as they are**. Valid UTF-8 appears as text (JSON is
  formatted); anything else gets a hex view and base64. A null key or value is
  distinguished from an empty one, and a tombstone is labelled as one. **Download value** gives the exact bytes.
- **Jump to offset** ("go to offset 120") lands on that record, or tells you the
  partition ends at N.
- **Search** looks for text in keys, values and headers, ignoring case. Kafka keeps no
  index of what records contain, so this is a scan: it reads the newest 5,000 records
  of the partition, says exactly which offsets it read and how many matched, and
  offers **Search older records** to carry on further back. Tick **all partitions**
  to search every partition's newest records and see the matches together, newest
  first; with no search text that is simply the latest records across the whole
  topic.
- **Send a message** from the dock at the bottom: key, value, headers, partition
  (or "auto", which partitions by key exactly as a real client does), acks. The
  composer stays open while you change partition and closes when you move to another
  topic. Values
  can be sent as text, JSON, base64, or a tombstone. **Send again** on any record
  produces an exact copy (the same key, value and headers, byte for byte), and
  **Edit a copy** opens the composer filled in with it, to change first.
- **Consumed by** lists the groups reading the topic, with their lag.
- **Settings** shows what is in force for the topic (how long records are kept, how
  much, and the segment size), marks the ones that are the topic's own, and lets you
  change them: type `7d`, `1GB`, `forever`, or leave a field blank to follow the
  broker. The largest record batch the topic accepts, and whether records take the
  producer's time or the broker's, are set there too. The same fields are on the
  create-topic form. Other Kafka topic
  settings are listed for reading; a topic that is compacted says so, and that its offset
  counts can exceed its records.
- **Transactions** show on the tape. A record of a transaction that aborted is dimmed and
  badged *aborted*, and one whose transaction has not ended is badged *open* (a
  read-committed consumer cannot see either); a commit or abort marker takes an offset
  and is drawn as a thin divider, not a record. **Committed only** shows the partition as
  a read-committed consumer would. The **Cluster** page lists the transactions in
  progress: transactional id, state, producer, how long open, and the partitions touched.
  See [Transactions](transactions.md).
- **Access** lists the ACLs on the broker, read-only, under a banner that says they are
  enforced and how a resource with no ACL is treated (see
  [Authorization](authorization.md)). They are managed with `kafka-acls` or your
  provisioning tool, not here.
- **Go to offset** and **go to time** sit above the messages. Time takes `15m`, `2h` or
  `1d` (that long ago), a date and time such as `2026-10-07 10:30` (read as UTC unless it
  carries a zone), or an epoch in milliseconds, and lands on the first message at or
  after it.
- **Partitions** shows the count and adds more. Partitions can only be added, never
  removed, and a key's partition depends on the count, so records with the same key
  stop being ordered together across the change; the page says so before it asks.
  Consumers in a group pick the new partitions up on their own.
- **Purge** empties a topic with `DeleteRecords`: the topic keeps its id, settings and
  its groups' committed offsets, so a group that had read everything carries on with
  the next record. **Delete** removes the topic. Both ask first, naming the action.

- **Follow this trace**, in the inspector of a record that carries a trace id
  (`traceparent`, B3, or the header named by `--trace-header`): every record of the
  same trace in every topic, in order, with the groups that have read each. See
  [Observability](observability.md).

## Consumer groups

- State (Stable, Rebalancing, Empty…), protocol (classic or KIP-848) and members.
- Each **member** shows its client id, the host it connected from, and the
  partitions it owns, as readable ranges.
- **Offsets and lag** per partition, with the owner, updating live. Unusual states
  are explained rather than hidden: *no commits yet*, *ahead of the log end by N*
  (the topic was purged or recreated), *trimmed* (the committed offset has been
  deleted by retention), and offsets whose topic no longer exists.
- **Reset offsets** to earliest or latest, for the whole group or for one topic, and
  **delete group**.
- **One partition's offset**: each row has **Set…** (a number between the partition's
  oldest record and its end, or either end) and **Remove** (the group then has no
  position there, and a consumer starts from its `auto.offset.reset`). Remove also
  clears an offset left behind by a topic that no longer exists.
- All of these are for groups with no active members. A group that is running commits
  its own offsets, so changing them is refused, with the reason.

- A **Timeline** of what the group's coordinator did (joins, rebalances, expiries,
  generations), newest first: the answer to "why does my consumer keep rebalancing".
  See [Observability](observability.md).

## Traffic

What clients have asked the broker, newest first: who sent each request, what it was
about, how it went, and the Kafka error name when it did not. Repeats of idle requests
fold into one row, and the tail filters by client, topic, group, request and kind.
Select a row for the request in full (how each partition went, its decoded fields, why
a refusal happened and what to do), a folded row for its ticks, or a client for its own
page, with what it reports about itself (KIP-714: metrics, not traces). **Clients**
(`?view=clients`) lists who is connected and how each is behaving; a group filter tells
the group's rebalances in order, in epochs for a KIP-848 group. Pausing keeps the list
still and counts what it missed; Esc clears a filter, and the strip of keys at the foot
is the page's own for the state it is in. The page is drawn as its boards (design gate:
`go tool task design`, [`e2e/design/README.md`](../e2e/design/README.md)); each row is
one tab stop of a grid, moved with the arrows. See [Observability](observability.md) for what each number is counted over and
what is kept.

## Schemas

With the built-in [schema registry](schema-registry.md) running, the console grows a
**Schemas** section (its subjects are in the command palette, under Subjects).

- The list shows every subject with its language (Avro, JSON Schema, Protobuf) and
  its newest version.
- A subject's page has its **versions** as tabs, and for any version after the first
  a **Changes from vN** view: the schema as text, compared with the version before,
  added lines in green and removed lines in red. The compatibility level is shown, and
  can be set for the subject or handed back to the registry's default.
- **New version** starts from the latest schema. **Check compatibility** says
  whether it may be registered under the level in force and, if not, why; **Register**
  does it. A schema that does not parse is reported as such, beside the form.
- **Delete** removes a version, or the whole subject, after asking.
- **Register schema** (the list's *New*) creates a subject from a pasted schema.

Where a topic has `<topic>-key` or `<topic>-value` subjects, its page links them.

**Records written by a registry-aware serializer are shown as what they mean.** A value
that starts with the registry's wire-format header (a zero byte and a schema id) is read
with that schema, in the table and in the inspector, which names the schema and links its
version. The Hex and Base64 views still show the stored bytes, *Send again* still
resends them exactly, and a value whose schema the registry no longer has is shown as
binary, as before. *Edit a copy* of a decoded record brings back the JSON and chooses
the same subject.

**Sending a message** can encode the value: the composer's value field has an
*encode with* menu of subjects (the topic's own `-value` subject is chosen when it has
one). Write the value as JSON; it is checked against the subject's latest schema and
sent in the wire format, and a value the schema does not accept is refused with the
reason instead of being sent. Choose *No schema* to send the text as it is.

The console reaches the registry over HTTP, like any client, using the same
credentials it uses for the broker when SASL is on.

## Cluster

The single broker: its node id and advertised address, the cluster id, how *this*
connection is secured, and the **Kafka APIs it advertises** with their version ranges
(asked of the broker, with `ApiVersions`). Below that are the broker settings that
Kafka's `DescribeConfigs` reports: topic auto-creation, default partitions, retention,
segment and index sizes. Settings Kafka has no name for (the data directory, the fsync
interval, the emulated version) are not there; `doze-kafka config print` shows those.
It links to [what is not implemented](limitations.md).

## The inspector

Selecting a record, a request, a client, a group member, a schema subject, an open
transaction or a group event opens it in the inspector, the column at the right of
the page. The page narrows to make room: the inspector is part of the layout and
nothing is drawn over it (below 640px it is a sheet from the foot). All of them look
the same: a title, a body and a close control. With nothing selected it says what you
can select here. The selection is in the address (`?open=`), so a refresh or a shared
link reopens it, Back returns to the previous selection (or to none), and `Esc`
closes it and puts focus back on the row.

## Pages and tabs

A topic is one page with tabs: Messages, Partitions, Readers, Config and Schema. A
group has Lag, Members, Timeline and Offsets; a subject has Versions, Compatibility
and Register. The tab is in the address (`?tab=`), and `[` and `]` move between
tabs. The Cluster page has Overview, Config, APIs and Transactions (`?view=`).

The sections are Traffic and Trace (Observe), Topics, Groups and Schemas (Data), and
Access and Cluster (Broker), each in the rail with a count where there is one. A
section's home lists its resources with the page's own summary beside them. The
first thing a broker with nothing in it shows is a first-run page with the address
and a way to make a first topic; **Connect an app** (`/connect`) gives the address
to paste and how to check it.

## The wire dock and neighbours

A topic or group page ends with the requests that touched it, live, with the same
pause control as the tape. It shows the newest eight; **Older** pages back through the
rest in place (the older page keeps refreshing as itself) and **Newest** returns, and
"Traffic page" opens Traffic filtered to that topic or group. Under a topic's title one sentence says who writes it, who
reads it and which schema subject describes it ("Written by checkout-api, read by
g-order-validator, schema subject orders-value"), each name a link.

## Keyboard

Press `?` for the list. It is generated from the one table the console binds its keys
from, so it is always the keys that work. The strip under the page shows the few that
matter on the page you are on.

| Key | Does |
|---|---|
| `⌘K` / `Ctrl+K` | Command palette: jump to a topic, group or subject, or act |
| `g` then `r` `t` `g` `s` `a` `c` | Go to Traffic, Topics, Groups, Schemas, Access, Cluster |
| `?` | Show the keys |
| `/` | Filter this table |
| `j` / `k`, `Enter` | Move through the list and open |
| `[` / `]` | Previous or next tab |
| `c` | New topic, or register a schema |
| `p` | Produce, on a topic; otherwise pause or resume live updates |
| `e` | On Traffic: errors only, and back |
| `u` / `m` / `c` | On Traffic, with a request or client open: unfold a folded row, all of a client's metrics, copy a request's decoded fields |
| `⌘Enter` / `Ctrl+Enter` | Send, from the composer |
| `Esc` | Close a dialog or the inspector, or clear the filter |

Single keys act only when you are not typing in a field, with no modifier held, and
no dialog open. The list of keys is a dialog too: while it is open only `Esc` and `?`
(close it), `Tab` and `⌘K` (the palette instead) do anything. The inspector is not a
dialog; it is part of the page, and the page's keys work beside it.

## The palette

`⌘K` opens a palette with results under Topics, Groups, Subjects and Actions. A row
says what it is (a topic its partitions and messages, a group its state, members and
lag), the part of the name you typed is bold, and an action carries its key. On a
topic it offers to produce to it; on a group, to reset its offsets, which still asks
before it does anything. The sections themselves are not in the palette: the rail
and `g` then a letter go to them.

## Appearance

Light, dark and high-contrast themes, or follow the system (dark, or a request for
more contrast); the control at the top right is four words: system, light, dark, high
contrast. High contrast also outlines the selection, so it does not rely on a fill. Every text and background pair is
measured in all three, and the ratios are in `console/testdata/contrast.txt`. The
fonts (IBM Plex Sans and Mono) are part of the binary, so the console looks the same
offline. It works down to phone widths: at 960px the rail is icons and the list is one tap
away; below 640px each page is one screen, the top bar is a menu, the page title and
the palette, and messages are cards.

Times are shown in your browser's zone, with the zone and the UTC instant on hover;
"3m ago" keeps counting on a paused page.

## Safety

- The console accepts only requests addressed to a host it knows — `localhost`, IP
  addresses, `*.doze`, and anything you add with `--console-allow-host` — and
  refuses state-changing requests from other origins. That keeps a web page open in
  your browser from reaching it through DNS rebinding or a forged form.
- `--console-prefix` (`console-prefix` in the file) mounts it somewhere other than
  `/_console`, behind a reverse proxy; `/` serves it at the root of its port.
  `DOZE_KAFKA_CONSOLE_PREFIX` does the same when neither is set (the test suites use it
  to show nothing assumes the default).
- It is plain HTTP with no login of its own, and with SASL on it acts on the broker as
  a super user. It listens on loopback. With SASL or TLS on, a `--console-addr` beyond
  loopback is refused at start-up unless `--console-unprotected` accepts it; what to
  put in front of it instead is in
  [What SASL and TLS cover](auth-and-tls.md#what-sasl-and-tls-cover). Without either,
  bind it beyond loopback only on a network you trust.

## Errors

Every error is one of four words, each with its own colour: **Refused** (real Kafka
would say no too), **Denied** (an ACL or a login decided), **Not implemented** (this
broker does not do it) and **Failed** (the broker is unhappy). It names what said no:
the Kafka error name, the registry's `error_code`, or "by the console" for a rule of
the console's own, with the broker's own explanation where it gave one.

When the broker is unreachable the live pill at the top right says **Not connected**
once, with the last time anything was heard and one Retry; what was on screen stays,
dimmed, and comes back by itself when the broker does. A topic or group that does not
exist gives a real 404 page with near-miss suggestions ("did you mean `orders`?").

Every destructive action confirms, and the confirm says what goes with it, measured
at the moment you press the button: "deletes its 3 partitions and 12 records, and the
committed offsets of 2 groups".
