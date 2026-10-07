# The web console

doze-kafka serves a small web console beside the broker. It is on by default at
**http://kafka.&lt;name&gt;.doze** (port-less, through the shared front door; the start-up
summary prints the address). Under `--listen` there is no name and it is on
`127.0.0.1:9093`. Turn it off with
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
  in a drawer: key, value, headers, timestamp, size. The drawer's address is in the
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
  settings are listed for reading; a topic that asks for compaction is told it is not
  compacted.
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

## Cluster

The single broker: its node id and advertised address, the cluster id, how *this*
connection is secured, and the **Kafka APIs it advertises** with their version ranges
(asked of the broker, with `ApiVersions`). Below that are the broker settings that
Kafka's `DescribeConfigs` reports: topic auto-creation, default partitions, retention,
segment and index sizes. Settings Kafka has no name for (the data directory, the fsync
interval, the emulated version) are not there; `doze-kafka config print` shows those.
It links to [what is not implemented](limitations.md).

## Keyboard

| Key | Does |
|---|---|
| `⌘K` / `Ctrl+K` | Command palette: jump to any topic, group or view |
| `/` | Focus the list filter |
| `j` / `k`, `Enter` | Move through the list and open |
| `c` | New topic |
| `m` | Open the composer |
| `[` | Collapse or expand the rail |
| `⌘Enter` / `Ctrl+Enter` | Send, from the composer |
| `Esc` | Close a dialog or the drawer |

Single keys act only when you are not typing in a field, with no modifier held, and
no dialog open.

## Appearance

Light and dark themes, or follow the system (the switch is at the top right). The
fonts (IBM Plex Sans and Mono) are part of the binary, so the console looks the same
offline. It works down to phone widths: below about 640px the list and the detail
become two screens.

## Safety

- The console accepts only requests addressed to a host it knows — `localhost`, IP
  addresses, `*.doze`, and anything you add with `--console-allow-host` — and
  refuses state-changing requests from other origins. That keeps a web page open in
  your browser from reaching it through DNS rebinding or a forged form.
- It is plain HTTP with no login of its own. Bind it beyond loopback only on a
  network you trust.

## Errors

When the broker is unreachable the console says so, in the page, and keeps the live
views waiting rather than flashing errors. A topic or group that does not exist gives
a real 404 page with near-miss suggestions ("did you mean `orders`?").
