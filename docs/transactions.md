# Transactions (exactly-once)

doze-kafka runs transactions. A producer configured with a `transactional.id` can write
to several partitions and commit consumer offsets atomically, and a consumer that reads
`read_committed` sees all of it or none of it. That is what exactly-once processing
rests on: Kafka Streams with `exactly_once_v2`, Spring Kafka transactions, Flink's
exactly-once sink and franz-go's `GroupTransactSession` all work, and Sarama's
transactional producer is tested.

## The protocol

The classic protocol, from before KIP-890: what current Java, librdkafka and franz-go
clients use when the broker offers Produce no newer than v9, which it does. The newer
"transaction v2" (Produce v12 and later) is not offered, so a client falls back to this
one by itself.

| API | |
|---|---|
| InitProducerId (with a `transactional.id`) | Becomes the owner of the id, fencing whoever held it; a transaction the previous owner left open is aborted, and the epoch is raised. |
| FindCoordinator (type transaction) | This node. |
| AddPartitionsToTxn, AddOffsetsToTxn | What the transaction will touch. |
| Produce (transactional batches) | Refused unless the producer owns the id and the partition was added. |
| TxnOffsetCommit | Consumer offsets staged with the transaction. |
| EndTxn | Commit or abort. |
| DescribeTransactions, ListTransactions, DescribeProducers | For tooling, and the console's Cluster page. |

## What commit and abort do

Ending a transaction appends a **marker** (a control batch carrying the producer's id
and epoch) to every partition the transaction wrote to, then, for a commit, applies the
consumer offsets it carried. The decision is stored before the markers are written, so a
stop in the middle is finished on the next start: a stored commit is carried out. A
transaction still open is left open, as in Kafka: its producer may be alive and retrying,
and can go on to commit. Its timeout starts again from the restart, and a producer that
really is gone is found by that. (Aborting it at start-up would fence a live producer for
good, since a client treats being fenced as fatal, and every restart of the broker would
kill the application's producer.)

EndTxn is answered with success, and the transaction recorded complete, only when every
one of its markers is on disk (each partition is flushed, whoever appended the marker)
and, for a commit, its offsets are stored: a power cut the moment after cannot take a
committed transaction back. When a marker cannot be written or flushed the decision
stands and EndTxn answers `CONCURRENT_TRANSACTIONS`, which is what Kafka answers while a
decided transaction's markers are being written; the coordinator keeps trying on its
own, and a client that asks again gets the outcome. The other decision is never carried
out instead: an EndTxn that names it is answered `CONCURRENT_TRANSACTIONS` while the
first is being carried out and `INVALID_TXN_STATE` once it has been, and a repeat of the
same decision succeeds, as Kafka 4.3.1 answers them (recorded:
`conformance/recordings/endtxn-decisions.json`, `endtxn-while-preparing.json`). The
coordinator's state in memory never runs ahead of `meta.db`: a write that fails changes
nothing, and the request is answered `NOT_COORDINATOR`.

A marker takes an offset, so a partition's offsets have gaps where transactions ended.
Consumers skip them; the console shows them as dividers.

## What a consumer sees

- `read_uncommitted` (the default) reads everything, including records of transactions
  that later abort.
- `read_committed` is never given anything at or above the **last stable offset**, the
  first offset of the oldest open transaction (or the log end when none is open), and
  with each fetch gets the aborted transactions touching what it returned, so it drops
  their records. `ListOffsets` for the latest offset returns the last stable offset.

An open transaction therefore holds `read_committed` consumers of that partition back
until it ends. If its producer vanishes, the timeout ends it.

## Timeouts and fencing

`transaction.timeout.ms` (at most 15 minutes) is counted from when the transaction last
changed state. One that outlives it is aborted by the broker and its producer fenced: the
epoch is raised, so the producer's next request fails with `PRODUCER_FENCED` or
`INVALID_PRODUCER_EPOCH` instead of writing into a transaction that is already decided.
A second producer with the same `transactional.id` fences the first the same way.

## Authorization

A transactional id is an ACL resource: `Write` on it to take it over and run
transactions, `Describe` to find its coordinator or describe it. See
[Authorization](authorization.md).

## Limits

- The classic protocol only (above).
- Transaction state is a record per `transactional.id` in the metadata database, and
  per-partition state (open and aborted transactions) is rebuilt from the batch headers
  of the log. A partition that has never held a transactional batch is not scanned.
- Exactly-once across a broker crash needs a resent batch to be recognised, so each
  producer's recent sequence numbers are rebuilt from the last 16 MiB of the log on first
  use after a restart. A producer idle for longer than that window loses the protection.
