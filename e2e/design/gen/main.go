//go:build !windows
// (SIGUSR1, which the design gate signals it with, does not exist on Windows; the gate runs on Linux and macOS.)

// Command gen is the design gate's traffic: real franz-go clients that do what the
// Traffic boards draw (Main, Traf-02 to Traf-12), each writing a ledger of what it
// sent. The ledger is the truth the page is compared with: the gate counts the
// requests, clients and connections it finds there and the page must say the same.
//
// Every client writes $LEDGER/<client-id>.jsonl, one JSON object a line:
//
//	{"ev":"req","api":"Produce","key":0,"start":<unix ms>,"end":<unix ms>,"err":"..."}
//	  one per request the client sent (kgo.HookBrokerE2E): start is when its first byte
//	  was written, end when its answer was read.
//	{"ev":"connect","local":"127.0.0.1:55102","t":<unix ms>}, {"ev":"disconnect",...}
//	{"ev":"sync","t":<unix ms>,"open":[{"api":...,"start":...}]}
//	  written when the process gets SIGUSR1: every request that ended before it is above
//	  it in this file, and "open" lists the ones written to the broker and not yet answered
//	  (the broker may have read them and the console shown them).
//	{"ev":"software","name":"kgo","version":"v1.22.1"}  what it says in ApiVersions.
//	{"ev":"push","t":<unix ms>,"metrics":[{"name":...,"unit":...,"value":...}]}
//	  a telemetry report it pushed and the broker took.
//	{"ev":"note",...} what the program did (a phase, a code it was answered).
//
// Usage: gen <command> <host:port> <client-id> [args]
//
//	seed                          create the three topics and the ACL that denies
//	                              User:ANONYMOUS the group g-loyalty-etl, ask Metadata
//	                              for the three topics with auto-create allowed, then
//	                              stay connected, asking nothing (a short-lived loader)
//	producer <topic> <everyMs> [txid]
//	                              one record per interval (in a transaction when txid is
//	                              given); REFUSE=<topic> also sends one Produce to that
//	                              topic, which does not exist, after the third record
//	member <group> <topic> classic|next
//	                              a consumer in the group: the classic protocol, or
//	                              KIP-848 (kgo.ServerSideBalancer)
//	reporter <group> <topic>      a classic consumer that reports its metrics (KIP-714)
//	                              under the Java consumer's names, every interval the
//	                              broker asks for
//	quiet                         a client that asks for metadata every 2 s and reports
//	                              no metrics
//
// NOMETRICS=1 turns franz-go's own metric reporting off. Every command prints "ready"
// on stdout once it has done its first round, and runs until it is killed (SIGTERM
// leaves cleanly: a member leaves its group; SIGKILL does not).
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime/debug"
	"sync"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type ledger struct {
	mu sync.Mutex
	f  *os.File
	// The requests written to the broker and not yet answered, by API key, in the order
	// they were written (a connection answers in order): what a sync line lists. Each is
	// the time taken just before the write call, which is before the broker can have read
	// a byte of it (see stampConn).
	flying map[int16][]float64
}

// ledgers are the clients of this process, which SIGUSR1 asks to sync.
var (
	ledgersMu sync.Mutex
	ledgers   []*ledger
)

func newLedger(id string) *ledger {
	dir := os.Getenv("LEDGER")
	if dir == "" {
		dir = "."
	}
	f, err := os.OpenFile(dir+"/"+id+".jsonl", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	l := &ledger{f: f, flying: map[int16][]float64{}}
	ledgersMu.Lock()
	ledgers = append(ledgers, l)
	ledgersMu.Unlock()
	return l
}

// sync writes the line the gate waits for: every request that ended before it is in the
// ledger above it (a request's end is stamped under the same lock that writes it), and
// the requests the client has written to the broker and not had answered are listed
// with it, since the broker may already have read them and the page shown them.
func (l *ledger) sync() {
	l.mu.Lock()
	defer l.mu.Unlock()
	var open []map[string]any
	for k, starts := range l.flying {
		for _, s := range starts {
			open = append(open, map[string]any{"key": k, "api": kmsg.NameForKey(k), "start": s})
		}
	}
	b, _ := json.Marshal(map[string]any{"ev": "sync", "t": ms(time.Now()), "open": open})
	l.f.Write(append(b, '\n'))
}

func ms(t time.Time) float64 { return float64(t.UnixMicro()) / 1000 }

func (l *ledger) write(m map[string]any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.line(m)
}

// line writes one line; the caller holds the lock.
func (l *ledger) line(m map[string]any) {
	if _, ok := m["t"]; !ok {
		m["t"] = ms(time.Now())
	}
	b, _ := json.Marshal(m)
	l.f.Write(append(b, '\n'))
}

// OnBrokerE2E is every request the client sent, with when it began and ended: begun is
// when its first byte was written (kgo's end-to-end duration starts there, after the
// time the request waited in the client's queue); the broker's time for it is when it
// read it, which is in between.
func (l *ledger) OnBrokerE2E(_ kgo.BrokerMetadata, key int16, e kgo.BrokerE2E) {
	l.mu.Lock()
	defer l.mu.Unlock()
	end := time.Now()
	// start is the time taken before the write, never later than the broker's reading of
	// the request. end is read here, after the answer and whatever the process was doing
	// before this hook ran, so it is never earlier than the broker's answering: both
	// errors widen the span the request may have been read in, and neither narrows it.
	// (kgo's own duration taken back from here put start late by as long as the hook
	// waited to run: on a loaded machine, tens of milliseconds after the broker had the
	// request.)
	start := ms(end.Add(-e.DurationE2E()))
	if q := l.flying[key]; len(q) > 0 {
		start = min(start, q[0])
		l.flying[key] = q[1:]
	}
	m := map[string]any{"ev": "req", "key": key, "api": kmsg.NameForKey(key), "start": start, "end": ms(end), "t": ms(end), "wr": e.BytesWritten, "rd": e.BytesRead}
	if err := e.Err(); err != nil {
		m["err"] = err.Error()
	}
	l.line(m)
}

// stampConn takes the time just before each write to the broker. A request is one write
// (size, API key, version, correlation id, ...): the time before it is a lower bound of
// when the broker read it.
type stampConn struct {
	net.Conn
	l *ledger
}

func (c *stampConn) Write(p []byte) (int, error) {
	t := time.Now()
	if len(p) >= 6 {
		key := int16(binary.BigEndian.Uint16(p[4:6]))
		c.l.mu.Lock()
		c.l.flying[key] = append(c.l.flying[key], ms(t))
		c.l.mu.Unlock()
	}
	return c.Conn.Write(p)
}

func (l *ledger) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, c net.Conn, err error) {
	if c != nil && err == nil {
		l.write(map[string]any{"ev": "connect", "local": c.LocalAddr().String()})
	}
}

func (l *ledger) OnBrokerDisconnect(_ kgo.BrokerMetadata, c net.Conn) {
	l.write(map[string]any{"ev": "disconnect", "local": c.LocalAddr().String()})
}

// software is what franz-go says it is in ApiVersions: "kgo" and its module version.
func software() (string, string) {
	if bi, ok := debug.ReadBuildInfo(); ok {
		for _, d := range bi.Deps {
			if d.Path == "github.com/twmb/franz-go" {
				return "kgo", d.Version
			}
		}
	}
	return "kgo", "unknown"
}

func client(addr, id string, opts ...kgo.Opt) (*kgo.Client, *ledger) {
	l := newLedger(id)
	name, version := software()
	l.write(map[string]any{"ev": "software", "name": name, "version": version})
	dial := func(ctx context.Context, network, host string) (net.Conn, error) {
		c, err := (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, host)
		if err != nil {
			return nil, err
		}
		return &stampConn{Conn: c, l: l}, nil
	}
	base := []kgo.Opt{kgo.SeedBrokers(addr), kgo.ClientID(id), kgo.WithHooks(l), kgo.Dialer(dial)}
	if os.Getenv("NOMETRICS") != "" {
		base = append(base, kgo.DisableClientMetrics())
	}
	cl, err := kgo.NewClient(append(base, opts...)...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
	return cl, l
}

func must(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, "gen:", err)
		os.Exit(1)
	}
}

func wait(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}

func main() {
	if len(os.Args) < 4 {
		fmt.Fprintln(os.Stderr, "usage: gen <command> <host:port> <client-id> [args]")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	// SIGUSR1: the gate asks every client of this process to write a sync line.
	usr := make(chan os.Signal, 4)
	signal.Notify(usr, syscall.SIGUSR1)
	go func() {
		for range usr {
			ledgersMu.Lock()
			ls := append([]*ledger(nil), ledgers...)
			ledgersMu.Unlock()
			for _, l := range ls {
				l.sync()
			}
		}
	}()
	cmd, addr, id, a := os.Args[1], os.Args[2], os.Args[3], os.Args[4:]
	switch cmd {
	case "seed":
		seed(ctx, addr, id)
	case "producer":
		producer(ctx, addr, id, a)
	case "member":
		member(ctx, addr, id, a[0], a[1], a[2], nil)
	case "reporter":
		member(ctx, addr, id, a[0], a[1], "classic", report)
	case "quiet":
		quiet(ctx, addr, id)
	default:
		fmt.Fprintln(os.Stderr, "gen: no command", cmd)
		os.Exit(2)
	}
}

// Topics are the boards' own.
var topics = []struct {
	name  string
	parts int32
}{{"harbour.checkout.orders", 3}, {"harbour.depot.dispatch", 3}, {"harbour.loyalty.points", 1}}

func seed(ctx context.Context, addr, id string) {
	cl, l := client(addr, id)
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	for _, t := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic, rt.NumPartitions, rt.ReplicationFactor = t.name, t.parts, 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	must(err)
	for _, t := range resp.Topics {
		l.write(map[string]any{"ev": "note", "what": "create", "topic": t.Topic, "code": t.ErrorCode})
	}
	acl := kmsg.NewPtrCreateACLsRequest()
	c := kmsg.NewCreateACLsRequestCreation()
	c.ResourceType, c.ResourceName, c.ResourcePatternType = kmsg.ACLResourceTypeGroup, "g-loyalty-etl", kmsg.ACLResourcePatternTypeLiteral
	c.Principal, c.Host, c.Operation, c.PermissionType = "User:ANONYMOUS", "*", kmsg.ACLOperationAll, kmsg.ACLPermissionTypeDeny
	acl.Creations = append(acl.Creations, c)
	ar, err := acl.RequestWith(ctx, cl)
	must(err)
	l.write(map[string]any{"ev": "note", "what": "acl", "code": ar.Results[0].ErrorCode})
	meta := kmsg.NewPtrMetadataRequest()
	meta.AllowAutoTopicCreation = true
	for _, t := range topics {
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(t.name)
		meta.Topics = append(meta.Topics, mt)
	}
	_, err = meta.RequestWith(ctx, cl)
	must(err)
	fmt.Println("ready")
	<-ctx.Done()
}

func producer(ctx context.Context, addr, id string, a []string) {
	topic := a[0]
	var every int
	fmt.Sscan(a[1], &every)
	opts := []kgo.Opt{kgo.RecordPartitioner(kgo.RoundRobinPartitioner()), kgo.ProducerLinger(0)}
	tx := len(a) > 2
	if tx {
		opts = append(opts, kgo.TransactionalID(a[2]))
	}
	cl, l := client(addr, id, opts...)
	defer cl.Close()
	refuse := os.Getenv("REFUSE")
	for i := 0; ctx.Err() == nil; i++ {
		if tx {
			must(cl.BeginTransaction())
		}
		r := &kgo.Record{Topic: topic, Key: fmt.Appendf(nil, "order-%04d", i), Value: fmt.Appendf(nil, `{"order":%d,"total":"%d.50"}`, i, 10+i%90)}
		res := cl.ProduceSync(ctx, r)
		if err := res.FirstErr(); err != nil {
			l.write(map[string]any{"ev": "note", "what": "produce-error", "err": err.Error()})
		} else {
			l.write(map[string]any{"ev": "note", "what": "produced", "p": res[0].Record.Partition, "off": res[0].Record.Offset})
		}
		if tx {
			if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
				l.write(map[string]any{"ev": "note", "what": "commit-error", "err": err.Error()})
			}
		}
		// The app's other write goes to a topic that is not there, every round: the
		// refusal the boards draw (Main, Traf-03), and the one seen most.
		if refuse != "" {
			refused(ctx, cl, l, refuse)
		}
		if i == 0 {
			fmt.Println("ready")
		}
		if !wait(ctx, time.Duration(every)*time.Millisecond) {
			return
		}
	}
}

// refused sends one Produce to a topic that does not exist, straight to the broker (a
// client's own producer would ask Metadata first and never send it): the refusal of
// Traf-03.
func refused(ctx context.Context, cl *kgo.Client, l *ledger, topic string) {
	req := kmsg.NewPtrProduceRequest()
	req.Acks, req.TimeoutMillis = -1, 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	brokers := cl.DiscoveredBrokers()
	if len(brokers) == 0 {
		l.write(map[string]any{"ev": "note", "what": "refused-produce", "err": "no broker known"})
		return
	}
	resp, err := brokers[0].Request(ctx, req)
	if err != nil {
		l.write(map[string]any{"ev": "note", "what": "refused-produce", "err": err.Error()})
		return
	}
	pr := resp.(*kmsg.ProduceResponse)
	code := int16(0)
	if len(pr.Topics) > 0 && len(pr.Topics[0].Partitions) > 0 {
		code = pr.Topics[0].Partitions[0].ErrorCode
	}
	l.write(map[string]any{"ev": "note", "what": "refused-produce", "topic": topic, "code": code})
}

func member(ctx context.Context, addr, id, group, topic, proto string, extra func(context.Context, *kgo.Client, *ledger)) {
	opts := []kgo.Opt{kgo.ConsumerGroup(group), kgo.ConsumeTopics(topic), kgo.FetchMaxWait(500 * time.Millisecond)}
	if proto == "next" {
		opts = append(opts, kgo.ServerSideBalancer())
	} else {
		// The eager range assignor: one rebalance a change of membership, as the boards'
		// group has (the cooperative one rebalances twice for a join).
		opts = append(opts, kgo.SessionTimeout(6*time.Second), kgo.HeartbeatInterval(time.Second), kgo.Balancers(kgo.RangeBalancer()))
	}
	if extra != nil {
		// It reports through report(), as the one instance this client is.
		opts = append(opts, kgo.DisableClientMetrics())
	}
	var l *ledger
	readyOnce := sync.Once{}
	opts = append(opts,
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			l.write(map[string]any{"ev": "note", "what": "assigned", "parts": m})
			readyOnce.Do(func() { fmt.Println("ready") })
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) {
			l.write(map[string]any{"ev": "note", "what": "revoked", "parts": m})
		}))
	var cl *kgo.Client
	cl, l = client(addr, id, opts...)
	if extra != nil {
		go extra(ctx, cl, l)
	}
	// A member the broker refuses (an ACL) is never assigned: it says ready once it has
	// been answered at all.
	go func() {
		if wait(ctx, 3*time.Second) {
			readyOnce.Do(func() { fmt.Println("ready") })
		}
	}()
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		fs := cl.PollFetches(pctx)
		cancel()
		if n := fs.NumRecords(); n > 0 {
			l.write(map[string]any{"ev": "note", "what": "polled", "n": n})
		}
	}
	l.write(map[string]any{"ev": "note", "what": "leaving"})
	cl.Close()
}

// report plays what a KIP-714 client does: subscribe, push a report of its metrics,
// and push again on the interval it was given. The names are the Java consumer's.
func report(ctx context.Context, cl *kgo.Client, l *ledger) {
	var sub *kmsg.GetTelemetrySubscriptionsResponse
	var id [16]byte
	for i := 0; ctx.Err() == nil; i++ {
		if sub == nil {
			req := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
			req.ClientInstanceID = id
			resp, err := req.RequestWith(ctx, cl)
			if err != nil || resp.ErrorCode != 0 {
				if !wait(ctx, time.Second) {
					return
				}
				continue
			}
			sub, id = resp, resp.ClientInstanceID
			// The first push may come at once; the next waits the interval.
		}
		now := time.Now()
		ms := []struct {
			name, unit string
			v          float64
		}{
			{"org.apache.kafka.consumer.request.error.rate", "1/s", 0},
			{"org.apache.kafka.consumer.request.retry.rate", "1/s", 0},
			{"org.apache.kafka.consumer.request.latency.avg", "ms", 1.2 + float64(i%4)/5},
			{"org.apache.kafka.consumer.poll.idle.ratio.avg", "", 0.9 + float64(i%4)/100},
			{"org.apache.kafka.consumer.connection.count", "", 1},
			{"org.apache.kafka.consumer.fetch.manager.bytes.consumed.rate", "By/s", 1200 + float64(i%3)*100},
		}
		// More of what a consumer reports, so the page has more than it lists at once
		// ("all N metrics", the m key).
		for n, name := range []string{"fetch.manager.fetch.latency.avg", "fetch.manager.records.lag.max", "coordinator.commit.latency.avg",
			"io.wait.ratio", "network.io.rate", "outgoing.byte.rate", "incoming.byte.rate", "request.rate"} {
			ms = append(ms, struct {
				name, unit string
				v          float64
			}{"org.apache.kafka.consumer." + name, "", float64(n + 1)})
		}
		var metrics []telemetry.Metric
		var logged []map[string]any
		for _, m := range ms {
			metrics = append(metrics, telemetry.Metric{Name: m.name, Unit: m.unit, Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: now, Value: m.v}}})
			logged = append(logged, map[string]any{"name": m.name, "unit": m.unit, "value": m.v})
		}
		payload := telemetry.Encode(telemetry.Payload{Resource: map[string]string{}, Metrics: metrics})
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		zw.Write(payload)
		zw.Close()
		push := kmsg.NewPtrPushTelemetryRequest()
		push.ClientInstanceID, push.SubscriptionID, push.CompressionType, push.Metrics = id, sub.SubscriptionID, 1, zipped.Bytes()
		resp, err := push.RequestWith(ctx, cl)
		switch {
		case err != nil:
			sub = nil
		case resp.ErrorCode != 0:
			l.write(map[string]any{"ev": "note", "what": "push-refused", "code": resp.ErrorCode})
			sub = nil
		default:
			l.write(map[string]any{"ev": "push", "metrics": logged})
		}
		every := time.Second
		if sub != nil {
			every = max(time.Duration(sub.PushIntervalMillis)*time.Millisecond, time.Second)
		}
		if !wait(ctx, every) {
			return
		}
	}
}

func quiet(ctx context.Context, addr, id string) {
	cl, _ := client(addr, id, kgo.DisableClientMetrics())
	defer cl.Close()
	for i := 0; ctx.Err() == nil; i++ {
		req := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr("harbour.checkout.orders")
		req.Topics = append(req.Topics, mt)
		if _, err := req.RequestWith(ctx, cl); err == nil && i == 0 {
			fmt.Println("ready")
		}
		if !wait(ctx, 2*time.Second) {
			return
		}
	}
}
