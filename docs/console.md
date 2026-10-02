# The web console

doze-kafka serves a small web console beside the broker. It is on by default at
**http://127.0.0.1:9093** (the start-up summary prints the address; with the
[`.doze` zone](endpoints.md) set up, also `http://kafka.doze`). Turn it off with
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
- **Send a message** from the dock at the bottom: key, value, headers, partition
  (or "auto", which partitions by key exactly as a real client does), acks. Values
  can be sent as text, JSON, base64, or a tombstone. **Send again** on any record
  carries its headers and its exact bytes.
- **Consumed by** lists the groups reading the topic, with their lag.
- **Purge** empties a topic (it is deleted and recreated, so it gets a new id) and
  **Delete** removes it. Both ask first, naming the action.

## Consumer groups

- State (Stable, Rebalancing, Empty…), protocol (classic or KIP-848) and members.
- Each **member** shows its client id, the host it connected from, and the
  partitions it owns, as readable ranges.
- **Offsets and lag** per partition, with the owner, updating live. Unusual states
  are explained rather than hidden: *no commits yet*, *ahead of the log end by N*
  (the topic was purged or recreated), *trimmed* (the committed offset has been
  deleted by retention), and offsets whose topic no longer exists.
- **Reset offsets** (to earliest or latest) and **delete group**, for groups with no
  active members. A group that is running is refused, with the reason.

## Cluster

The single broker: its node id and advertised address, the cluster id, how *this*
connection is secured, the **Kafka API versions it advertises** (read from the same
table the broker uses), and its **effective configuration** (retention, segment
size, fsync interval, data directory, emulated version, auth). It links to
[what is not implemented](limitations.md).

## Keyboard

| Key | Does |
|---|---|
| `⌘K` / `Ctrl+K` | Command palette: jump to any topic, group or view |
| `/` | Focus the list filter |
| `j` / `k`, `Enter` | Move through the list and open |
| `c` | New topic |
| `m` | Open the composer |
| `[` | Collapse or expand the rail |
| `Esc` | Close a dialog or the drawer |

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
