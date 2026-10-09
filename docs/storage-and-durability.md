# Storage & durability

The thing that separates doze-kafka from an in-memory fake is that your data is
really on disk. This page explains what's stored where, what a restart or crash
keeps, and why idle memory stays flat no matter how much data you have.

## What's on disk

Two things:

- **`--data-dir`** (default `./data`) — the message logs. One directory per
  partition, e.g. `data/demo-0/`, holding append-only segment files (`.log`) and
  their sparse indexes (`.index`).
- **`meta.db`**, inside the data directory by default (`--meta-db` moves it) — a small
  embedded database (bbolt) holding the topic registry, committed consumer-group
  offsets, and the producer-id counter.

Delete the data directory and you have a fresh broker. Copy it elsewhere and you've
moved your broker's state. (Earlier versions kept `meta.db` beside `./data`; that
file is still used, with a notice, until you move it in.)

## How messages are stored

When a client produces, doze-kafka assigns offsets and appends the record batch to
the active segment **exactly as it arrived on the wire** — it never decodes or
re-encodes your payload. That's why compression (gzip, snappy, lz4, zstd) just works:
the compressed batch is stored verbatim and handed back byte-for-byte to the consumer,
which decompresses it. It also means the on-disk format is doze-kafka's own; it is not
byte-compatible with Apache Kafka's segment files, and doesn't need to be.

A segment rolls to a new file once it passes `--segment-bytes` (default 128 MiB). A
sparse index records a position every `--index-interval-bytes` (default 4 KiB) so a
fetch can seek near an offset without scanning the whole file.

## Flat memory

Reads are served straight off the filesystem with `pread` — the log is never pulled
into the process heap. The practical result: **idle memory stays in the tens of MB**
whether a partition holds a megabyte or many gigabytes. The operating system's page
cache does the caching, and the kernel reclaims it under pressure. This is the
property that lets doze-kafka sit quietly in the background without ballooning.

## Retention

By default doze-kafka keeps everything. Bound a partition's disk use, for every topic,
with either or both of:

- `--retention-bytes` — delete the oldest whole segments once a partition exceeds this
  total size.
- `--retention-ms` — delete segments whose newest record is older than this.

A topic can have its own: `retention.ms`, `retention.bytes` and `segment.bytes` set
when it is created, or changed later (`kafka-configs --alter`, any admin client, or the
console's topic page), apply to that topic and take precedence over the flags above,
in both directions. `-1` means no limit, so one topic can keep everything on a broker
that otherwise trims, and one topic can be trimmed on a broker that otherwise keeps
everything. A change applies at once, and survives a restart.

The active segment is never deleted, so a partition always keeps its most recent data. A background sweep enforces
the bounds roughly once a second for partitions in use, and once a minute for those
nothing has read or written since the broker started; because it only ever removes
whole non-active
segments, a partition can briefly sit a little above its byte bound (by up to one
segment) before the next roll. Deleted data becomes unreadable and the partition's
earliest offset advances.

## Compaction

A topic with `cleanup.policy=compact` (or `compact,delete`) keeps the latest record of each
key and drops the older ones. A background cleaner does it, about once a second, for
partitions in use, and it follows these rules:

- It works on **sealed segments only**, never the active one, and only below the last
  stable offset, so a record that could still be superseded by the active segment is kept,
  and an open transaction is never touched.
- It reads the uncleaned part of the log into a map of key → latest offset (a million keys
  at a time, so a bigger log is cleaned over several passes), then **rewrites** every
  eligible segment, keeping a record only if it is the latest for its key. A record with
  no key is refused at produce, as in Kafka, and one that is somehow there is kept.
- **Offsets never move.** A rewritten batch keeps its base offset and offset span; removed
  records leave gaps, as on a real broker. A consumer positioned in a gap is given the
  next record. The log-start offset does not move either: an emptied segment is deleted
  except the first.
- A **tombstone** (a null value) stays until its segment has been cleaned before and is
  older than `delete.retention.ms` (24 hours unless the topic sets it), then goes.
- Records of **aborted transactions** are dropped, and a commit or abort marker stays only
  while a record of its transaction does.
- `min.cleanable.dirty.ratio` (here 0: clean as soon as a segment has rolled, rather than
  Kafka's 0.5), `min.compaction.lag.ms` and `max.compaction.lag.ms` decide when. Rewritten
  batches are uncompressed unless the topic's `compression.type` names a codec.
- A rewrite is swapped in without a moment when the log cannot be read, and a stop at any
  point leaves a log that opens: a half-written file is discarded, and an index that no
  longer matches its log is rebuilt from it. The segment keeps its original age, so a
  rewrite does not postpone retention.

For `compact,delete`, compaction runs first and retention after.

## Durability: restart vs crash

There are two failure modes worth separating.

**Clean shutdown / restart** (Ctrl-C, `SIGTERM`): the broker stops accepting
connections, lets in-flight requests drain, flushes and fsyncs all logs, and closes
the metadata store. Nothing acknowledged is lost. Start it again over the same
`--data-dir` and everything — messages, topics, committed offsets — is exactly as you
left it. This is verified end-to-end against the real binary.

**Crash** (`kill -9`, power loss): writes reach the operating system immediately (so a
mere process crash loses nothing — the data is already in the page cache), but they
only reach the physical disk on fsync. What a *power loss* can cost depends on the
producer's `acks` setting, which is exactly the Kafka knob for durability:

- **`acks=all`** — doze-kafka **fsyncs the written partitions before it
  acknowledges**, so an acknowledged record has reached stable storage, not just the
  page cache. On macOS that fsync is a full device-cache flush (`F_FULLFSYNC`); on
  Linux it's `fsync`/`fdatasync`. The usual caveat every database carries applies:
  the guarantee is only as strong as your disk actually honoring the flush (some
  consumer SSDs and virtualized disks have volatile caches that don't). This is the
  setting idempotent producers use by default, so most modern clients get durable
  acks for free.
- **`acks=1`** — the record is in the page cache and acknowledged immediately, but
  reaches disk only on the next coarse fsync (the timer, every `--fsync-interval`,
  default 500 ms) or when the segment rolls. A power loss can lose the last
  sub-second of these. This is the fast path — you're trading the durability you
  didn't ask for.
- **`acks=0`** — fire and forget; no acknowledgement at all.

Either way, on the next start recovery scans the active segment, truncates any
half-written tail, and resumes at the last intact record — it comes back without
corruption. This is exercised directly by a chaos harness (`go tool task chaos`) that
`SIGKILL`s and restarts the broker under sustained load: acknowledged `acks=all`
records survive every crash, with no loss and no gaps.

Tighten the `acks=1` window by lowering `--fsync-interval` (at some throughput cost),
or set it to `0` to rely purely on `acks=all` fsyncs plus the flush-on-shutdown.

## Committed offsets

Consumer-group offsets are the other durable thing. They're written to `meta.db`,
not to a `__consumer_offsets` topic (doze-kafka doesn't emulate that topic). A
consumer that restarts, or a broker that does, resumes each group from its last
committed offset. See [Consumer groups](consumer-groups.md).
