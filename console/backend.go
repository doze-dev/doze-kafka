package console

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// backend is the console's connection to the broker — a real Kafka client
// (franz-go), never a bypass. It dials opts.Addr over loopback TCP exactly
// as any SDK would, authenticating with SASL when the broker requires it.
//
// Four clients, not one: admin/metadata calls share cl, but "acks" is a
// producer-wide franz-go option (not a per-record one), so each acks level
// the compose panel offers gets its own pre-built client. All are cheap,
// idle TCP connections to a local broker.
type backend struct {
	addr string
	opts Options

	cl                                     *kgo.Client // metadata/admin
	produceLeader, produceAll, produceNone *kgo.Client // acks=1, acks=all, acks=0

	mu    sync.Mutex
	rrctr map[string]int // topic -> next partition, for auto-partition round robin
}

// seedOpts is a client's dial target: a TCP seed, or — for a "unix://" Addr,
// the embedded (doze module) topology — a placeholder seed plus a dialer to
// the broker's own socket. Unix matters there: a TCP dial would go through the
// supervisor's proxy, where an idle console client counts as a live connection
// and pins the instance awake forever. EVERY client build must go through this
// (the tape's short-lived fetch consumer included), or a unix Addr leaks into
// SeedBrokers and fails host:port parsing.
func seedOpts(addr string) []kgo.Opt {
	if path, ok := strings.CutPrefix(addr, "unix://"); ok {
		return []kgo.Opt{
			kgo.SeedBrokers("kafka.local:9092"), // placeholder; the dialer ignores it
			kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			}),
		}
	}
	return []kgo.Opt{kgo.SeedBrokers(addr)}
}

// displayAddr is the bootstrap address pages show clients: DisplayAddr when
// set (the embedded topology, where the dial addr is a unix socket apps can't
// use), the dial addr otherwise.
func (b *backend) displayAddr() string {
	if b.opts.DisplayAddr != "" {
		return b.opts.DisplayAddr
	}
	return b.addr
}

// newBackend dials the broker. Connections are lazy (franz-go connects on
// first request), so this only validates options and builds the clients.
func newBackend(opts Options) (*backend, error) {
	if opts.Addr == "" {
		return nil, fmt.Errorf("console: Addr is required")
	}
	base := append([]kgo.Opt{kgo.ClientID("doze-kafka-console")}, seedOpts(opts.Addr)...)
	if opts.SASLUser != "" {
		mech, err := saslMechanism(opts.SASLMechanism, opts.SASLUser, opts.SASLPass)
		if err != nil {
			return nil, err
		}
		base = append(base, kgo.SASL(mech))
	}
	if opts.TLSConfig != nil {
		base = append(base, kgo.DialTLSConfig(opts.TLSConfig))
	}

	cl, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("console: connect to %s: %w", opts.Addr, err)
	}
	// Partition is always chosen explicitly (see pickPartition) so the console
	// can honestly report which partition a message landed on, rather than
	// leaving it to a client-side partitioner it doesn't control. Idempotent
	// producing (franz-go's default) requires acks=all, so the acks=1/acks=0
	// clients must explicitly opt out of it.
	produce := func(acks kgo.Acks, idempotent bool) (*kgo.Client, error) {
		popts := append(append([]kgo.Opt{}, base...),
			kgo.RequiredAcks(acks), kgo.RecordPartitioner(kgo.ManualPartitioner()))
		if !idempotent {
			popts = append(popts, kgo.DisableIdempotentWrite())
		}
		c, err := kgo.NewClient(popts...)
		if err != nil {
			cl.Close()
			return nil, fmt.Errorf("console: connect to %s: %w", opts.Addr, err)
		}
		return c, nil
	}
	produceLeader, err := produce(kgo.LeaderAck(), false)
	if err != nil {
		return nil, err
	}
	produceAll, err := produce(kgo.AllISRAcks(), true)
	if err != nil {
		return nil, err
	}
	produceNone, err := produce(kgo.NoAck(), false)
	if err != nil {
		return nil, err
	}

	return &backend{
		addr: opts.Addr, opts: opts,
		cl: cl, produceLeader: produceLeader, produceAll: produceAll, produceNone: produceNone,
		rrctr: map[string]int{},
	}, nil
}

func (b *backend) close() {
	b.cl.Close()
	b.produceLeader.Close()
	b.produceAll.Close()
	b.produceNone.Close()
}

// saslMechanism builds the sasl.Mechanism the console authenticates with.
// The console always knows exactly which mechanism its own credential was
// registered for (main.go picks one when it provisions the credential), so
// this isn't a negotiation — just a lookup.
func saslMechanism(name, user, pass string) (sasl.Mechanism, error) {
	switch name {
	case "", "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("console: unsupported SASL mechanism %q", name)
	}
}

// ---- topics ----

// TopicSummary is one row in the topics list. Msgs is the number of records
// currently on disk across all partitions (Σ latest−earliest) — exact, not an
// estimate for an ordinary topic. Compaction and transaction markers leave gaps in
// the offset space, so for those it is an upper bound.
type TopicSummary struct {
	Name       string
	Partitions int32
	Msgs       int64
}

// TopicDetail is a topic's identity plus its effective (read-only) config.
// doze-kafka has no AlterConfigs — see console's honesty constraints — so
// there is deliberately no corresponding "set config" method.
type TopicDetail struct {
	Name       string
	ID         string // hex-encoded topic UUID
	Partitions int32
	Config     []ConfigEntry // stable order: the order DescribeConfigs returned
}

// ConfigEntry is one effective-config key/value pair.
type ConfigEntry struct {
	Name, Value string
	Own         bool // set on the topic itself, rather than the broker's default.
}

// ListTopics returns every topic, sorted by name. AllowAutoTopicCreation is
// deliberately left false: this is a read, and doze-kafka would otherwise
// create a topic just because the console asked about it.
func (b *backend) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = nil // null = "all topics" (v1+)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	out := make([]TopicSummary, 0, len(resp.Topics))
	loReq := kmsg.NewPtrListOffsetsRequest()
	loReq.ReplicaID = -1
	for _, t := range resp.Topics {
		if t.IsInternal || t.Topic == nil {
			continue
		}
		if t.ErrorCode != 0 {
			return nil, fmt.Errorf("metadata for %s: %w", *t.Topic, kerr.ErrorForCode(t.ErrorCode))
		}
		out = append(out, TopicSummary{Name: *t.Topic, Partitions: int32(len(t.Partitions))})
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = *t.Topic
		for _, p := range t.Partitions {
			for _, ts := range []int64{-2, -1} { // earliest, latest
				rp := kmsg.NewListOffsetsRequestTopicPartition()
				rp.Partition = p.Partition
				rp.Timestamp = ts
				rt.Partitions = append(rt.Partitions, rp)
			}
		}
		loReq.Topics = append(loReq.Topics, rt)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })

	// One batched ListOffsets round trip covers every partition of every
	// topic; message counts fill in best-effort (a topic deleted mid-flight
	// just shows 0 rather than failing the whole list).
	if len(loReq.Topics) > 0 {
		loResp, err := loReq.RequestWith(ctx, b.cl)
		if err == nil {
			counts := map[string]int64{}
			for _, t := range loResp.Topics {
				// Request order per partition is preserved: earliest then
				// latest. Sum latest−earliest by pairing successive entries.
				byPart := map[int32][]int64{}
				for _, p := range t.Partitions {
					if p.ErrorCode == 0 {
						byPart[p.Partition] = append(byPart[p.Partition], p.Offset)
					}
				}
				for _, offs := range byPart {
					if len(offs) == 2 {
						counts[t.Topic] += offs[1] - offs[0]
					}
				}
			}
			for i := range out {
				out[i].Msgs = counts[out[i].Name]
			}
		}
	}
	return out, nil
}

// PartitionCount is a cheap Metadata-only lookup (no DescribeConfigs round
// trip) — for call sites like the tape's live poll that need to know how
// many partition tabs to draw but not the full effective config.
func (b *backend) PartitionCount(ctx context.Context, name string) (int32, error) {
	_, n, err := b.TopicShape(ctx, name)
	return n, err
}

// TopicShape returns a topic's id and partition count from one Metadata request.
func (b *backend) TopicShape(ctx context.Context, name string) (id string, partitions int32, err error) {
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &name
	req.Topics = []kmsg.MetadataRequestTopic{mt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return "", 0, err
	}
	if len(resp.Topics) == 0 {
		return "", 0, fmt.Errorf("%w: topic %q", errNotFound, name)
	}
	if resp.Topics[0].ErrorCode != 0 {
		return "", 0, kerr.ErrorForCode(resp.Topics[0].ErrorCode)
	}
	return topicIDString(resp.Topics[0].TopicID), int32(len(resp.Topics[0].Partitions)), nil
}

// CreateTopic creates a topic. Replication factor is always 1 — doze-kafka
// is single-node, so there's no meaningful choice to offer here.
func (b *backend) CreateTopic(ctx context.Context, name string, partitions int32, config map[string]string) error {
	return b.createTopic(ctx, name, partitions, config)
}

// createTopic creates a topic with per-topic config overrides.
func (b *backend) createTopic(ctx context.Context, name string, partitions int32, config map[string]string) error {
	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = name
	rt.NumPartitions = partitions
	rt.ReplicationFactor = 1
	for _, k := range sortedConfigKeys(config) {
		rc := kmsg.NewCreateTopicsRequestTopicConfig()
		v := config[k]
		rc.Name, rc.Value = k, &v
		rt.Configs = append(rt.Configs, rc)
	}
	req.Topics = []kmsg.CreateTopicsRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if t.ErrorCode == kerr.TopicAlreadyExists.Code {
			return badInput("a topic named %q already exists", name)
		}
		if err := refusal(t.ErrorCode, t.ErrorMessage); err != nil {
			return err
		}
	}
	return nil
}

// DeleteTopic deletes a topic.
func (b *backend) DeleteTopic(ctx context.Context, name string) error {
	req := kmsg.NewPtrDeleteTopicsRequest()
	req.TopicNames = []string{name}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = &name
	req.Topics = []kmsg.DeleteTopicsRequestTopic{dt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if t.ErrorCode != 0 {
			return kerr.ErrorForCode(t.ErrorCode)
		}
	}
	return nil
}

// PurgeTopic empties a topic by moving every partition's log-start offset to its
// end (DeleteRecords). The topic keeps its id, its settings and what its consumers
// have committed: a group that had read everything carries on with the next record.
// It used to delete and recreate the topic, which gave it a new id, was not atomic,
// and dropped every committed offset, so consumers started over from their reset
// policy.
func (b *backend) PurgeTopic(ctx context.Context, name string) error {
	partitions, err := b.PartitionCount(ctx, name)
	if err != nil {
		return err
	}
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = name
	for p := int32(0); p < partitions; p++ {
		rp := kmsg.NewDeleteRecordsRequestTopicPartition()
		rp.Partition, rp.Offset = p, -1 // the high watermark: everything.
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
				return fmt.Errorf("purge partition %d: %w", p.Partition, err)
			}
		}
	}
	return nil
}

// topicOverrides returns the config a topic was created or altered with, as
// distinct from the broker defaults it would otherwise inherit.
func (b *backend) topicOverrides(ctx context.Context, name string) (map[string]string, error) {
	req := kmsg.NewPtrDescribeConfigsRequest()
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeTopic, name
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	out := map[string]string{}
	for _, r := range resp.Resources {
		if r.ErrorCode != 0 {
			return nil, kerr.ErrorForCode(r.ErrorCode)
		}
		for _, c := range r.Configs {
			if c.Source == kmsg.ConfigSourceDynamicTopicConfig && c.Value != nil {
				out[c.Name] = *c.Value
			}
		}
	}
	return out, nil
}

func sortedConfigKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// DescribeTopic returns a topic's identity and effective config.
func (b *backend) DescribeTopic(ctx context.Context, name string) (*TopicDetail, error) {
	mreq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = &name
	mreq.Topics = []kmsg.MetadataRequestTopic{mt}
	mresp, err := mreq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(mresp.Topics) == 0 {
		return nil, fmt.Errorf("%w: topic %q", errNotFound, name)
	}
	mt0 := mresp.Topics[0]
	if mt0.ErrorCode != 0 {
		return nil, kerr.ErrorForCode(mt0.ErrorCode)
	}

	creq := kmsg.NewPtrDescribeConfigsRequest()
	cr := kmsg.NewDescribeConfigsRequestResource()
	cr.ResourceType = kmsg.ConfigResourceTypeTopic
	cr.ResourceName = name
	creq.Resources = []kmsg.DescribeConfigsRequestResource{cr}
	cresp, err := creq.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	var cfg []ConfigEntry
	if len(cresp.Resources) > 0 {
		res := cresp.Resources[0]
		if res.ErrorCode != 0 {
			return nil, kerr.ErrorForCode(res.ErrorCode)
		}
		for _, c := range res.Configs {
			v := ""
			if c.Value != nil {
				v = *c.Value
			}
			cfg = append(cfg, ConfigEntry{Name: c.Name, Value: v, Own: c.Source == kmsg.ConfigSourceDynamicTopicConfig})
		}
	}

	return &TopicDetail{
		Name:       name,
		ID:         topicIDString(mt0.TopicID),
		Partitions: int32(len(mt0.Partitions)),
		Config:     cfg,
	}, nil
}

// PartitionOffsets returns a partition's earliest and latest (log-end)
// offsets, one round trip, via the -2/-1 special ListOffsets timestamps.
// topicIDString renders a topic's UUID the way Kafka prints it (kafka-topics.sh,
// the Java client's Uuid.toString): URL-safe base64 without padding, 22 characters.
// It used to be raw hex, which matches nothing else a developer will see.
func topicIDString(id [16]byte) string { return base64.RawURLEncoding.EncodeToString(id[:]) }

// PartitionBound is one partition's retained range of offsets.
type PartitionBound struct {
	Partition        int32
	Earliest, Latest int64
}

// Count is how many offsets the partition holds (latest minus earliest): the number of
// records unless compaction or a transaction marker left gaps. It is NOT the log-end offset, which is what
// the tape used to label "total" — wrong as soon as retention trims the log.
func (p PartitionBound) Count() int64 { return p.Latest - p.Earliest }

// PartitionBounds returns the retained range of every partition of a topic in one
// ListOffsets round trip.
func (b *backend) PartitionBounds(ctx context.Context, topic string, partitions int32) ([]PartitionBound, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for p := int32(0); p < partitions; p++ {
		for _, ts := range []int64{-2, -1} { // earliest, latest
			rp := kmsg.NewListOffsetsRequestTopicPartition()
			rp.Partition, rp.Timestamp = p, ts
			rt.Partitions = append(rt.Partitions, rp)
		}
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return nil, err
	}
	if len(resp.Topics) != 1 {
		return nil, fmt.Errorf("unexpected ListOffsets response shape")
	}
	out := make([]PartitionBound, partitions)
	for i := range out {
		out[i].Partition = int32(i)
	}
	seen := make([]int, partitions)
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			return nil, kerr.ErrorForCode(p.ErrorCode)
		}
		if p.Partition < 0 || p.Partition >= partitions {
			continue
		}
		// Each partition was asked for twice, earliest first.
		if seen[p.Partition] == 0 {
			out[p.Partition].Earliest = p.Offset
		} else {
			out[p.Partition].Latest = p.Offset
		}
		seen[p.Partition]++
	}
	return out, nil
}

func (b *backend) PartitionOffsets(ctx context.Context, topic string, partition int32) (earliest, latest int64, err error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, ts := range []int64{-2, -1} {
		p := kmsg.NewListOffsetsRequestTopicPartition()
		p.Partition = partition
		p.Timestamp = ts
		rt.Partitions = append(rt.Partitions, p)
	}
	req.Topics = []kmsg.ListOffsetsRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return 0, 0, err
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) != 2 {
		return 0, 0, fmt.Errorf("unexpected ListOffsets response shape")
	}
	for _, p := range resp.Topics[0].Partitions {
		if p.ErrorCode != 0 {
			return 0, 0, kerr.ErrorForCode(p.ErrorCode)
		}
	}
	// Request order is preserved: [0]=earliest(-2), [1]=latest(-1).
	return resp.Topics[0].Partitions[0].Offset, resp.Topics[0].Partitions[1].Offset, nil
}

// ---- messages ----

// Message is one decoded record.
type Message struct {
	Offset    int64
	Partition int32
	Timestamp time.Time
	Key       []byte
	Value     []byte
	Headers   []MessageHeader
}

// MessageHeader is one record header.
type MessageHeader struct {
	Key   string
	Value []byte
}

// FetchWindow returns up to limit records starting at fromOffset, ascending
// by offset. It opens a short-lived direct-partition consumer (not a
// subscribing consumer group) and polls briefly — the console wants a
// bounded snapshot for its live-polled tape, not an ongoing subscription.
// Using kgo's own PollFetches (rather than hand-decoding raw record
// batches) means decompression and batch parsing are the same
// battle-tested code path every real consumer uses.
func (b *backend) FetchWindow(ctx context.Context, topic string, partition int32, fromOffset int64, limit int) ([]Message, error) {
	return b.fetchWindow(ctx, topic, partition, fromOffset, limit, false)
}

// FetchWindowCommitted is FetchWindow as a read-committed consumer would see it: no
// records of transactions that aborted or are still open.
func (b *backend) FetchWindowCommitted(ctx context.Context, topic string, partition int32, fromOffset int64, limit int) ([]Message, error) {
	return b.fetchWindow(ctx, topic, partition, fromOffset, limit, true)
}

// txnTail is how long a window that has records waits for more before it concludes
// that what is left is transaction markers, which a consumer never sees as records.
const txnTail = 400 * time.Millisecond

// fetchWindow reads the records in the offsets [fromOffset, fromOffset+limit). The
// bound is on OFFSETS, not on a count: a transaction's commit or abort marker takes an
// offset and is not a record, so a window of sixty offsets can hold fewer than sixty
// records, and counting would have waited for ones that do not exist.
func (b *backend) fetchWindow(ctx context.Context, topic string, partition int32, fromOffset int64, limit int, committed bool) ([]Message, error) {
	if limit <= 0 {
		limit = 1
	}
	// Ask where the partition's records are first. A fetch at an offset that holds
	// nothing waits out its whole timeout, and one for a topic or partition that
	// does not exist returned "no records" after 800 ms, which the caller then
	// described as a record lost to retention. Both are answered here at once.
	earliest, latest, err := b.PartitionOffsets(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	if fromOffset < earliest || fromOffset >= latest {
		return nil, nil
	}
	end := min(fromOffset+int64(limit), latest) // exclusive.
	iso := kgo.ReadUncommitted()
	if committed {
		iso = kgo.ReadCommitted()
	}
	kopts := append(seedOpts(b.addr), kgo.ClientID("doze-kafka-console-fetch"),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {partition: kgo.NewOffset().At(fromOffset)}}),
		kgo.FetchMaxBytes(4<<20), kgo.FetchIsolationLevel(iso),
	)
	if b.opts.SASLUser != "" {
		mech, err := saslMechanism(b.opts.SASLMechanism, b.opts.SASLUser, b.opts.SASLPass)
		if err != nil {
			return nil, err
		}
		kopts = append(kopts, kgo.SASL(mech))
	}
	if b.opts.TLSConfig != nil {
		kopts = append(kopts, kgo.DialTLSConfig(b.opts.TLSConfig))
	}
	cl, err := kgo.NewClient(kopts...)
	if err != nil {
		return nil, err
	}
	defer cl.Close()

	// The records are known to exist, so this is a bound on a healthy broker's
	// answer, not a guess at how long "nothing there" takes.
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	var out []Message
	for {
		pctx, pcancel := fctx, context.CancelFunc(func() {})
		if len(out) > 0 {
			pctx, pcancel = context.WithTimeout(fctx, txnTail)
		}
		fs := cl.PollFetches(pctx)
		pcancel()
		if fs.IsClientClosed() {
			break
		}
		if err := fs.Err0(); err != nil && pctx.Err() == nil {
			return nil, err
		}
		reached := false
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset >= end {
				reached = true
				return
			}
			hdrs := make([]MessageHeader, len(r.Headers))
			for i, h := range r.Headers {
				hdrs[i] = MessageHeader{Key: h.Key, Value: h.Value}
			}
			out = append(out, Message{
				Offset: r.Offset, Partition: r.Partition, Timestamp: r.Timestamp,
				Key: r.Key, Value: r.Value, Headers: hdrs,
			})
			reached = reached || r.Offset >= end-1
		})
		if reached || fctx.Err() != nil || (len(out) > 0 && pctx.Err() != nil) {
			break
		}
	}
	if len(out) == 0 && fctx.Err() != nil && ctx.Err() == nil {
		// Records are there and none arrived in time: a failure, not an empty topic.
		return nil, fmt.Errorf("fetching %s partition %d from offset %d: %w", topic, partition, fromOffset, context.DeadlineExceeded)
	}
	return out, nil
}

// newReader is a consumer of exactly the partitions given, from exactly the offsets
// given, as the console's other short-lived readers are: no group, no commits. The
// caller closes it.
func (b *backend) newReader(assign map[string]map[int32]kgo.Offset) (*kgo.Client, error) {
	kopts := append(seedOpts(b.addr), kgo.ClientID("doze-kafka-console-trace"),
		kgo.ConsumePartitions(assign), kgo.FetchMaxBytes(8<<20), kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	if b.opts.SASLUser != "" {
		mech, err := saslMechanism(b.opts.SASLMechanism, b.opts.SASLUser, b.opts.SASLPass)
		if err != nil {
			return nil, err
		}
		kopts = append(kopts, kgo.SASL(mech))
	}
	if b.opts.TLSConfig != nil {
		kopts = append(kopts, kgo.DialTLSConfig(b.opts.TLSConfig))
	}
	return kgo.NewClient(kopts...)
}

// StableOffset is the partition's last stable offset: what a read-committed consumer
// may read up to. Records at or above it belong to a transaction still open.
func (b *backend) StableOffset(ctx context.Context, topic string, partition int32) (int64, error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID, req.IsolationLevel = -1, 1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = partition, -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = []kmsg.ListOffsetsRequestTopic{rt}
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return 0, err
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return 0, fmt.Errorf("unexpected ListOffsets response shape")
	}
	p := resp.Topics[0].Partitions[0]
	if e := kerr.ErrorForCode(p.ErrorCode); e != nil {
		return 0, e
	}
	return p.Offset, nil
}

// ProduceResult reports where a message landed.
type ProduceResult struct {
	Partition int32
	Offset    int64 // -1 when Confirmed is false (acks=0: the broker sent no response)
	Confirmed bool
}

// Produce sends one message. acks is "1" (default), "0", or "all" — see the
// compose panel. Partition is always explicit (see pickPartition): the
// console never leaves partition assignment to an opaque client-side
// partitioner it can't report back accurately.
func (b *backend) Produce(ctx context.Context, topic string, partition int32, key, value []byte, headers []MessageHeader, acks string) (ProduceResult, error) {
	cl := b.produceLeader
	confirmed := true
	switch acks {
	case "0":
		cl, confirmed = b.produceNone, false
	case "all":
		cl = b.produceAll
	}
	hdrs := make([]kgo.RecordHeader, len(headers))
	for i, h := range headers {
		hdrs[i] = kgo.RecordHeader{Key: h.Key, Value: h.Value}
	}
	rec := &kgo.Record{Topic: topic, Partition: partition, Key: key, Value: value, Headers: hdrs}
	results := cl.ProduceSync(ctx, rec)
	if err := results.FirstErr(); err != nil {
		return ProduceResult{}, err
	}
	if !confirmed {
		return ProduceResult{Partition: partition, Offset: -1, Confirmed: false}, nil
	}
	got, _ := results.First()
	return ProduceResult{Partition: got.Partition, Offset: got.Offset, Confirmed: true}, nil
}

// keyPartitioner hashes keys the way Kafka's default partitioner does (murmur2),
// so a record the console sends with key K lands on the partition a real Java or
// franz-go producer would pick for K. The console used to use FNV-1a, so "the same
// key" went to a different partition from the application's own records.
var keyPartitioner = kgo.StickyKeyPartitioner(nil)

// PickPartition chooses a partition for "Auto" in the compose panel: the Kafka
// hash of the key if one was given (so same-key messages land together, exactly
// as a real keyed producer would place them), otherwise simple round robin.
func (b *backend) PickPartition(topic string, key []byte, partitionCount int32) int32 {
	if partitionCount <= 1 {
		return 0
	}
	if len(key) > 0 {
		tp := keyPartitioner.ForTopic(topic)
		return int32(tp.Partition(&kgo.Record{Key: key}, int(partitionCount)))
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	p := b.rrctr[topic] % int(partitionCount)
	b.rrctr[topic]++
	return int32(p)
}
