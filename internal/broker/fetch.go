package broker

import (
	"context"
	"errors"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/log"

	"github.com/twmb/franz-go/pkg/kerr"
)

const defaultFetchMaxBytes = 1 << 20 // 1 MiB when a client sets no bound.

// maxFetchWait caps a long-poll. A fetch waits inside its connection's goroutine,
// which cannot notice the client going away until it writes the answer; with no
// cap, a client that asked for 24 days and disconnected left that goroutine (and
// its socket) parked for 24 days. Answering early with no data is always correct:
// the client simply fetches again.
var maxFetchWait = 30 * time.Second

// handleFetch serves raw record batches from each partition's fetch offset,
// resolving topic IDs (modern versions address topics by UUID). It long-polls: if
// nothing is available it waits up to MaxWaitMillis for a produce, then returns
// whatever accumulated. highWatermark and logStartOffset come straight from the
// partition (single-node: highWatermark == LEO).
func (b *Broker) handleFetch(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	fr := kreq.(*kmsg.FetchRequest)

	// Fetch sessions (KIP-227) are not kept: every response says session 0, so a
	// conforming client keeps sending full fetches. One that continues a session
	// anyway must be told it does not exist, not handed an empty success.
	if fr.SessionID != 0 && fr.SessionEpoch > 0 {
		resp := kmsg.NewPtrFetchResponse()
		resp.SetVersion(req.Header.APIVersion)
		resp.ErrorCode = kerr.FetchSessionIDNotFound.Code
		return protocol.EncodeResponse(req.Header, resp), nil
	}

	minBytes := max(int(fr.MinBytes), 1)
	wait := min(time.Duration(fr.MaxWaitMillis)*time.Millisecond, maxFetchWait)
	deadline := time.Now().Add(wait)

	var sub *subscription
	for {
		// Subscribe BEFORE reading. A produce that lands between the
		// read and the wait would otherwise close a channel nobody holds yet, and
		// this fetch would sleep the full MaxWait with data already there.
		if sub == nil {
			sub = b.produce.subscribe(b.fetchTopics(fr))
			defer sub.Close()
		}
		resp, total, failed := b.buildFetchResponse(fr, req.Header.APIVersion)
		// An error is an answer: the client has to act on it (reset its offset,
		// refresh metadata), and making it wait MaxWait first cost every consumer
		// offset reset a full fetch.max.wait.ms.
		if failed || total >= minBytes || wait <= 0 || !time.Now().Before(deadline) {
			return encodeFetchResponse(req.Header, resp, total), nil
		}
		// Wait for a produce to one of these topics or the deadline, then re-read.
		select {
		case <-sub.C:
		case <-time.After(time.Until(deadline)):
		case <-ctx.Done():
			return encodeFetchResponse(req.Header, resp, total), nil
		}
	}
}

// fetchTopics is the set of topic names a fetch reads, for subscribing to wakeups.
func (b *Broker) fetchTopics(fr *kmsg.FetchRequest) []string {
	seen := map[string]struct{}{}
	var out []string
	for _, t := range fr.Topics {
		name, _, ok := b.resolveTopic(t.Topic, t.TopicID)
		if !ok {
			continue
		}
		if _, dup := seen[name]; !dup {
			seen[name] = struct{}{}
			out = append(out, name)
		}
	}
	return out
}

// encodeFetchResponse frames a fetch response into a buffer sized for it. The
// default starts at 128 bytes and grows by doubling, copying a multi-megabyte
// record payload several times over on its way to the socket.
func encodeFetchResponse(header protocol.RequestHeader, resp *kmsg.FetchResponse, recordBytes int) []byte {
	parts := 0
	for _, t := range resp.Topics {
		parts += len(t.Partitions)
	}
	return protocol.EncodeResponseSized(header, resp, recordBytes+256+96*parts)
}

// buildFetchResponse reads every requested partition once and returns the response
// plus the total record bytes gathered (used to decide long-poll satisfaction) and
// whether any partition answered with an error.
func (b *Broker) buildFetchResponse(fr *kmsg.FetchRequest, version int16) (*kmsg.FetchResponse, int, bool) {
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(version)

	total, failed := 0, false
	budget := newFetchBudget(fr.MaxBytes)
	for _, t := range fr.Topics {
		rt := kmsg.NewFetchResponseTopic()
		rt.TopicID = t.TopicID
		name, topic, ok := b.resolveTopic(t.Topic, t.TopicID)
		rt.Topic = name

		for _, part := range t.Partitions {
			rp := kmsg.NewFetchResponseTopicPartition()
			rp.Partition = part.Partition

			if !ok || part.Partition < 0 || part.Partition >= topic.Partitions {
				rp.ErrorCode = int16(b.unknownTopicErr(t.Topic))
				rt.Partitions = append(rt.Partitions, rp)
				failed = true
				continue
			}
			total += b.readPartition(name, part, fr, &rp, budget)
			failed = failed || rp.ErrorCode != 0
			rt.Partitions = append(rt.Partitions, rp)
		}
		resp.Topics = append(resp.Topics, rt)
	}
	if afterFetchRead != nil {
		afterFetchRead()
	}
	return resp, total, failed
}

// afterFetchRead is a test hook that runs once the partitions have been read and
// before the caller decides whether to wait: the instant a produce must not be
// missed.
var afterFetchRead func()

// fetchBudget enforces the request-level MaxBytes across partitions, as Kafka
// does: each partition may read up to min(its own limit, what the request has
// left). The limit is soft in exactly one way: the first batch of the first
// partition with data is returned even if it alone is larger (so a large batch can
// always be fetched). Reading each partition against what is left, rather than
// reading its full allowance and discarding it when it does not fit, is what lets
// a later partition make progress while an earlier one keeps producing a little.
type fetchBudget struct {
	remaining int64
	gaveData  bool // a partition has already returned data.
}

func newFetchBudget(maxBytes int32) *fetchBudget {
	if maxBytes <= 0 {
		return &fetchBudget{remaining: 1<<62 - 1}
	}
	return &fetchBudget{remaining: int64(maxBytes)}
}

// readPartition fills rp from one partition and returns the record bytes read.
func (b *Broker) readPartition(topic string, part kmsg.FetchRequestTopicPartition, fr *kmsg.FetchRequest, rp *kmsg.FetchResponseTopicPartition, budget *fetchBudget) int {
	plog, err := b.logs.ExistingPartition(topic, part.Partition)
	if errors.Is(err, log.ErrNoSuchPartition) {
		// The topic was deleted between resolving it and opening its log.
		rp.ErrorCode = kerr.UnknownTopicOrPartition.Code
		return 0
	}
	if err != nil {
		rp.ErrorCode = b.storageFault("fetch: open partition", err, "topic", topic, "partition", part.Partition)
		return 0
	}

	maxBytes := int64(part.PartitionMaxBytes)
	if maxBytes <= 0 {
		maxBytes = int64(fr.MaxBytes)
	}
	if maxBytes <= 0 {
		maxBytes = defaultFetchMaxBytes
	}

	if maxBytes > budget.remaining {
		maxBytes = budget.remaining
	}
	if maxBytes <= 0 {
		maxBytes = 1 // nothing left: Read still reports offsets and errors; data is dropped below.
	}

	data, err := plog.Read(part.FetchOffset, maxBytes)
	rp.HighWatermark = plog.LogEndOffset()
	rp.LastStableOffset = rp.HighWatermark
	rp.LogStartOffset = plog.LogStartOffset()

	switch {
	case errors.Is(err, log.ErrOffsetOutOfRange):
		rp.ErrorCode = kerr.OffsetOutOfRange.Code
		return 0
	case err != nil:
		rp.ErrorCode = b.storageFault("fetch: read", err, "topic", topic, "partition", part.Partition, "offset", part.FetchOffset)
		return 0
	}
	if budget.gaveData && int64(len(data)) > budget.remaining {
		// This partition's first batch does not fit in what the request has left,
		// and an earlier partition already took the one oversize allowance.
		return 0
	}
	budget.remaining -= int64(len(data))
	if len(data) > 0 {
		budget.gaveData = true
	}
	rp.RecordBatches = data
	return len(data)
}

// unknownTopicErr picks the right "unknown topic" code depending on whether the
// request addressed the topic by name or by id.
func (b *Broker) unknownTopicErr(nameInRequest string) int16 {
	if nameInRequest == "" {
		return kerr.UnknownTopicID.Code
	}
	return kerr.UnknownTopicOrPartition.Code
}
