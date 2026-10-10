package protocol

import (
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// A row in the API table with no summariser would be an API the console's traffic
// page cannot describe, and nothing else would notice.
func TestEveryAPIDescribesItself(t *testing.T) {
	for _, a := range APIs {
		if a.Summary == nil {
			t.Errorf("%s has no Summary in the API table", APIName(a.Key))
			continue
		}
		// It must also survive being handed a request nobody filled in and no
		// response, which is what a handler that failed leaves behind.
		q := kmsg.RequestForKey(int16(a.Key))
		func() {
			defer func() {
				if v := recover(); v != nil {
					t.Errorf("%s summariser panicked on an empty request: %v", APIName(a.Key), v)
				}
			}()
			a.Summary(q, nil)
			a.Summary(q, q.ResponseKind())
		}()
	}
}

func TestFirstErrorFindsTheCodeWhereverItIs(t *testing.T) {
	if got := firstError(nil); got != 0 {
		t.Errorf("nil response: %d", got)
	}
	ok := kmsg.NewPtrProduceResponse()
	ok.SetVersion(7)
	if got := firstError(ok); got != 0 {
		t.Errorf("an empty produce response has error %d", got)
	}

	// A partition error three levels down.
	pr := kmsg.NewPtrProduceResponse()
	topic := kmsg.NewProduceResponseTopic()
	topic.Topic = "orders"
	good, bad := kmsg.NewProduceResponseTopicPartition(), kmsg.NewProduceResponseTopicPartition()
	bad.Partition, bad.ErrorCode = 1, kerr.UnknownTopicOrPartition.Code
	topic.Partitions = append(topic.Partitions, good, bad)
	pr.Topics = append(pr.Topics, topic)
	if got := firstError(pr); got != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("nested partition error = %d, want %d", got, kerr.UnknownTopicOrPartition.Code)
	}

	// The top-level error explains the nested ones, so it wins.
	fr := kmsg.NewPtrFetchResponse()
	fr.ErrorCode = kerr.FetchSessionIDNotFound.Code
	ft := kmsg.NewFetchResponseTopic()
	fp := kmsg.NewFetchResponseTopicPartition()
	fp.ErrorCode = kerr.OffsetOutOfRange.Code
	ft.Partitions = append(ft.Partitions, fp)
	fr.Topics = append(fr.Topics, ft)
	if got := firstError(fr); got != kerr.FetchSessionIDNotFound.Code {
		t.Errorf("top-level error = %d, want %d", got, kerr.FetchSessionIDNotFound.Code)
	}

	if ErrorName(kerr.OffsetOutOfRange.Code) != "OFFSET_OUT_OF_RANGE" || ErrorName(0) != "" {
		t.Errorf("names: %q %q", ErrorName(kerr.OffsetOutOfRange.Code), ErrorName(0))
	}
}

func TestDescribeSaysWhatTheRequestWasAbout(t *testing.T) {
	q := kmsg.NewPtrProduceRequest()
	q.SetVersion(7)
	q.Acks = -1
	for _, name := range []string{"orders", "payments", "orders"} {
		topic := kmsg.NewProduceRequestTopic()
		topic.Topic = name
		part := kmsg.NewProduceRequestTopicPartition()
		part.Records = make([]byte, 2048)
		topic.Partitions = append(topic.Partitions, part)
		q.Topics = append(q.Topics, topic)
	}
	r := &Request{Header: RequestHeader{APIKey: kmsg.Produce}}
	r.Noted(q, nil)
	s := r.Describe()
	if len(s.Topics) != 2 || s.Topics[0] != "orders" || s.Topics[1] != "payments" {
		t.Errorf("topics = %v, want [orders payments]", s.Topics)
	}
	if s.Detail != "3 partitions, 6.0 KiB" {
		t.Errorf("detail = %q", s.Detail)
	}

	// A Fetch that returned nothing is idle; one that returned data is not.
	fq := kmsg.NewPtrFetchRequest()
	fq.SetVersion(12)
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = "orders"
	fq.Topics = append(fq.Topics, ft)
	fresp := kmsg.NewPtrFetchResponse()
	fresp.SetVersion(12)
	fr := &Request{Header: RequestHeader{APIKey: kmsg.Fetch}}
	fr.Noted(fq, fresp)
	if s := fr.Describe(); !s.Idle || s.Detail != "nothing new" {
		t.Errorf("empty fetch: idle=%v detail=%q", s.Idle, s.Detail)
	}
	rt := kmsg.NewFetchResponseTopic()
	rp := kmsg.NewFetchResponseTopicPartition()
	rp.RecordBatches = make([]byte, 100)
	rt.Partitions = append(rt.Partitions, rp)
	fresp.Topics = append(fresp.Topics, rt)
	if s := fr.Describe(); s.Idle {
		t.Errorf("a fetch that returned data is idle: %+v", s)
	}

	// A fetch by topic id carries the id for the observer to resolve.
	id := [16]byte{1, 2, 3}
	fq.Topics[0].Topic, fq.Topics[0].TopicID = "", id
	if s := fr.Describe(); len(s.TopicIDs) != 1 || s.TopicIDs[0] != id || len(s.Topics) != 0 {
		t.Errorf("by id: %+v", s)
	}
}

type recordingObserver struct {
	mu     sync.Mutex
	opened []uint64
	closed []uint64
	reqs   []Observation
}

func (o *recordingObserver) ConnOpened(id uint64, _ string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.opened = append(o.opened, id)
}
func (o *recordingObserver) ConnClosed(id uint64) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.closed = append(o.closed, id)
}
func (o *recordingObserver) Request(x Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.reqs = append(o.reqs, x)
}

func (o *recordingObserver) snapshot() (opened, closed []uint64, reqs []Observation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]uint64(nil), o.opened...), append([]uint64(nil), o.closed...), append([]Observation(nil), o.reqs...)
}

// The observer hears connections open and close, and every request with what it was
// about, who sent it and how it went, including the one that was never answered.
func TestObserverHearsConnectionsAndRequests(t *testing.T) {
	router := NewRouter()
	router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
		q, err := req.Decode()
		if err != nil {
			return nil, err
		}
		resp := q.ResponseKind().(*kmsg.HeartbeatResponse)
		resp.SetVersion(req.Header.APIVersion)
		resp.ErrorCode = kerr.RebalanceInProgress.Code
		return req.Reply(resp), nil
	})
	router.Register(kmsg.Produce, func(_ context.Context, req *Request) ([]byte, error) {
		if _, err := req.Decode(); err != nil { // acks=0: decoded, never answered.
			return nil, err
		}
		return nil, nil
	})

	obs := &recordingObserver{}
	srv := NewServer("127.0.0.1:0", router, quietLogger())
	srv.SetObserver(obs)
	addr, err := srv.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.Serve(ctx); close(done) }()
	defer func() { cancel(); <-done }()

	conn, err := net.Dial("tcp", addr.String())
	if err != nil {
		t.Fatal(err)
	}

	send := func(key APIKey, version int16, corr int32, clientID string, body []byte) {
		payload := binary.BigEndian.AppendUint16(nil, uint16(key))
		payload = binary.BigEndian.AppendUint16(payload, uint16(version))
		payload = binary.BigEndian.AppendUint32(payload, uint32(corr))
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(clientID)))
		payload = append(payload, clientID...)
		payload = append(payload, body...)
		if err := WriteFrame(conn, payload); err != nil {
			t.Fatal(err)
		}
	}
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.SetVersion(1)
	hb.Group, hb.MemberID, hb.Generation = "billing", "m-1", 3
	send(kmsg.Heartbeat, 1, 7, "billing-app", hb.AppendTo(nil))
	if _, err := ReadFrame(conn); err != nil {
		t.Fatal(err)
	}
	pq := kmsg.NewPtrProduceRequest()
	pq.SetVersion(7)
	pq.Acks = 0
	topic := kmsg.NewProduceRequestTopic()
	topic.Topic = "orders"
	pq.Topics = append(pq.Topics, topic)
	send(kmsg.Produce, 7, 8, "billing-app", pq.AppendTo(nil))

	var reqs []Observation
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, reqs = obs.snapshot(); len(reqs) >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(reqs) != 2 {
		t.Fatalf("observed %d requests, want 2", len(reqs))
	}
	h := reqs[0]
	if h.API != kmsg.Heartbeat || h.ClientID != "billing-app" || h.Group != "billing" || h.Version != 1 {
		t.Errorf("heartbeat observed as %+v", h)
	}
	if h.ErrorCode != kerr.RebalanceInProgress.Code {
		t.Errorf("heartbeat error = %d, want REBALANCE_IN_PROGRESS", h.ErrorCode)
	}
	if h.RespBytes == 0 || h.ReqBytes == 0 || h.Duration <= 0 || h.Principal != AnonymousPrincipal {
		t.Errorf("sizes, duration or principal missing: %+v", h)
	}
	p := reqs[1]
	if p.API != kmsg.Produce || len(p.Topics) != 1 || p.Topics[0] != "orders" || p.RespBytes != 0 {
		t.Errorf("an unanswered produce observed as %+v", p)
	}
	if opened, _, _ := obs.snapshot(); len(opened) != 1 || opened[0] != h.Conn {
		t.Errorf("opened = %v, request conn = %d", opened, h.Conn)
	}

	conn.Close()
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if _, closed, _ := obs.snapshot(); len(closed) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Error("the observer was not told the connection closed")
}

// Every client newer than the broker is told UNSUPPORTED_VERSION by ApiVersions and
// asks again; that is the protocol working, and must not look like a failure.
func TestTheApiVersionsDowngradeIsNotAnError(t *testing.T) {
	q := kmsg.NewPtrApiVersionsRequest()
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ErrorCode = kerr.UnsupportedVersion.Code
	r := &Request{Header: RequestHeader{APIKey: kmsg.ApiVersions, APIVersion: 4}}
	r.Noted(q, resp)
	if s := r.Describe(); s.ErrorCode != 0 || s.Detail == "" {
		t.Errorf("the downgrade is described as %+v", s)
	}
	// Any other API with that code is a real error.
	hb := &Request{Header: RequestHeader{APIKey: kmsg.Heartbeat}}
	hresp := kmsg.NewPtrHeartbeatResponse()
	hresp.ErrorCode = kerr.UnsupportedVersion.Code
	hb.Noted(kmsg.NewPtrHeartbeatRequest(), hresp)
	if s := hb.Describe(); s.ErrorCode != kerr.UnsupportedVersion.Code {
		t.Errorf("a heartbeat answered UNSUPPORTED_VERSION lost its error: %+v", s)
	}
}

// What watching costs per request, with a console attached: describing a 10-partition
// fetch and recording it. Without a console nothing runs at all.
func BenchmarkDescribeFetch(b *testing.B) {
	q := kmsg.NewPtrFetchRequest()
	q.SetVersion(12)
	resp := kmsg.NewPtrFetchResponse()
	resp.SetVersion(12)
	ft, rt := kmsg.NewFetchRequestTopic(), kmsg.NewFetchResponseTopic()
	ft.Topic, rt.Topic = "orders", "orders"
	for p := int32(0); p < 10; p++ {
		fp := kmsg.NewFetchRequestTopicPartition()
		fp.Partition = p
		ft.Partitions = append(ft.Partitions, fp)
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition, rp.RecordBatches = p, make([]byte, 4096)
		rt.Partitions = append(rt.Partitions, rp)
	}
	q.Topics, resp.Topics = append(q.Topics, ft), append(resp.Topics, rt)
	r := &Request{Header: RequestHeader{APIKey: kmsg.Fetch}}
	r.Noted(q, resp)
	b.ReportAllocs()
	for b.Loop() {
		_ = r.Describe()
	}
}
