package broker_test

import (
	"encoding/binary"
	"io"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
)

// What a client names is kept: a group lives until it is deleted, a member until it
// leaves, a transactional id for ever, the first message of a SCRAM login until the
// second. A flexible request may carry a string as long as its frame (1 MiB), so each of
// these tests sends the most a frame allows in one field, many times over, and holds
// the broker to what it then keeps. A name is bounded at 4,096 bytes; nothing real is
// near it.

const (
	longestName  = 4096         // the broker's bound on a name it keeps (maxNameBytes).
	fillsAFrame  = 1<<20 - 1024 // with the rest of the request, a control frame's limit.
	boundedHeap  = 8 << 20      // what thirty refused requests may leave behind.
	thirtyFrames = 30
)

func heapNow() uint64 {
	var m runtime.MemStats
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m.HeapAlloc
}

// keepsLittle runs send thirty times and fails if the broker's heap grew by more than
// a few megabytes: thirty names of a megabyte each, kept, are thirty megabytes.
func keepsLittle(t *testing.T, what string, send func(i int)) {
	t.Helper()
	before := heapNow()
	for i := range thirtyFrames {
		send(i)
	}
	if grew := int64(heapNow()) - int64(before); grew > boundedHeap {
		t.Errorf("%s: thirty of them left %d MiB on the heap, want under %d", what, grew>>20, boundedHeap>>20)
	}
}

func classicJoin(group, member string, instance *string, protocolType, protocolName string) *kmsg.JoinGroupRequest {
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group, req.MemberID, req.InstanceID = group, member, instance
	req.SessionTimeoutMillis, req.RebalanceTimeoutMillis, req.ProtocolType = 10000, 10000, protocolType
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = protocolName
	req.Protocols = append(req.Protocols, p)
	return req
}

// another is s with its last byte changed, so thirty names of one length are thirty names.
func another(i int, s string) string { return s[:len(s)-1] + string(rune('a'+i)) }

func TestALongNameInAClassicJoinIsRefusedAndNotKept(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	big := strings.Repeat("x", fillsAFrame)
	for _, tc := range []struct {
		field string
		code  int16
		build func(s string) *kmsg.JoinGroupRequest
	}{
		{"group id", 24, func(s string) *kmsg.JoinGroupRequest { return classicJoin(s, "", nil, "consumer", "range") }},
		{"group instance id", 42, func(s string) *kmsg.JoinGroupRequest { return classicJoin("g-instance", "", &s, "consumer", "range") }},
		{"member id", 42, func(s string) *kmsg.JoinGroupRequest { return classicJoin("g-member", s, nil, "consumer", "range") }},
		{"protocol type", 42, func(s string) *kmsg.JoinGroupRequest { return classicJoin("g-type", "", nil, s, "range") }},
		{"protocol name", 42, func(s string) *kmsg.JoinGroupRequest { return classicJoin("g-proto", "", nil, "consumer", s) }},
	} {
		keepsLittle(t, "a JoinGroup whose "+tc.field+" fills a frame", func(i int) {
			resp := ask(t, c, tc.build(another(i, big)), 9, kmsg.NewPtrJoinGroupResponse())
			if resp.ErrorCode != tc.code {
				t.Fatalf("a %s of %d bytes: error %d, want %d", tc.field, fillsAFrame, resp.ErrorCode, tc.code)
			}
		})
		// One byte over the bound is refused, and the bound itself is not.
		bound := longestName
		if tc.field == "member id" {
			bound += 64 // a static member's id is its instance id and a suffix of the broker's.
		}
		if resp := ask(t, c, tc.build(strings.Repeat("y", bound+1)), 9, kmsg.NewPtrJoinGroupResponse()); resp.ErrorCode != tc.code {
			t.Errorf("a %s of %d bytes: error %d, want %d", tc.field, bound+1, resp.ErrorCode, tc.code)
		}
		if resp := ask(t, c, tc.build(strings.Repeat("y", bound)), 9, kmsg.NewPtrJoinGroupResponse()); resp.ErrorCode == tc.code {
			t.Errorf("a %s of %d bytes is refused with %d: that is the bound, and is taken", tc.field, bound, resp.ErrorCode)
		}
	}
	// None of the refused groups exists.
	groups := ask(t, c, kmsg.NewPtrListGroupsRequest(), 4, kmsg.NewPtrListGroupsResponse())
	for _, g := range groups.Groups {
		if len(g.Group) > longestName {
			t.Errorf("a group with an id of %d bytes is listed", len(g.Group))
		}
	}
}

func consumerBeat(group, member string, instance, rack, assignor *string, topics []string, regex *string) *kmsg.ConsumerGroupHeartbeatRequest {
	req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	req.Group, req.MemberID, req.MemberEpoch = group, member, 0
	req.InstanceID, req.RackID, req.ServerAssignor = instance, rack, assignor
	req.RebalanceTimeoutMillis = 10000
	req.SubscribedTopicNames, req.SubscribedTopicRegex = topics, regex
	req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	return req
}

func TestALongNameInAConsumerGroupHeartbeatIsRefusedAndNotKept(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	big := strings.Repeat("x", fillsAFrame)
	orders := []string{"orders"}
	member := 0
	next := func() string { member++; return "member-" + string(rune('a'+member%26)) + string(rune('a'+member/26)) }
	for _, tc := range []struct {
		field string
		code  int16
		build func(s string) *kmsg.ConsumerGroupHeartbeatRequest
	}{
		{"group id", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat(s, "m", nil, nil, nil, orders, nil)
		}},
		{"member id", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat("c-member", s, nil, nil, nil, orders, nil)
		}},
		{"group instance id", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat("c-instance", next(), &s, nil, nil, orders, nil)
		}},
		{"rack id", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat("c-rack", next(), nil, &s, nil, orders, nil)
		}},
		{"subscribed topic name", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat("c-topic", next(), nil, nil, nil, []string{s}, nil)
		}},
		{"subscribed topic pattern", 42, func(s string) *kmsg.ConsumerGroupHeartbeatRequest {
			return consumerBeat("c-regex", next(), nil, nil, nil, nil, &s)
		}},
	} {
		keepsLittle(t, "a ConsumerGroupHeartbeat whose "+tc.field+" fills a frame", func(i int) {
			resp := ask(t, c, tc.build(another(i, big)), 1, kmsg.NewPtrConsumerGroupHeartbeatResponse())
			if resp.ErrorCode != tc.code {
				t.Fatalf("a %s of %d bytes: error %d, want %d", tc.field, fillsAFrame, resp.ErrorCode, tc.code)
			}
		})
		bound := longestName
		if tc.field == "member id" {
			bound += 64 // a static member's id is its instance id and a suffix of the broker's.
		}
		if resp := ask(t, c, tc.build(strings.Repeat("y", bound+1)), 1, kmsg.NewPtrConsumerGroupHeartbeatResponse()); resp.ErrorCode != tc.code {
			t.Errorf("a %s of %d bytes: error %d, want %d", tc.field, bound+1, resp.ErrorCode, tc.code)
		}
	}
	// An assignor is one of two names, and no other is kept.
	if resp := ask(t, c, consumerBeat("c-assignor", "m", nil, nil, &big, orders, nil), 1, kmsg.NewPtrConsumerGroupHeartbeatResponse()); resp.ErrorCode != 112 {
		t.Errorf("an assignor of %d bytes: error %d, want UNSUPPORTED_ASSIGNOR (112)", fillsAFrame, resp.ErrorCode)
	}
	// What is at the bound is taken: a group, a member id, a rack and an instance id of
	// 4,096 bytes each.
	at := strings.Repeat("z", longestName)
	if resp := ask(t, c, consumerBeat(at, at, &at, &at, nil, orders, nil), 1, kmsg.NewPtrConsumerGroupHeartbeatResponse()); resp.ErrorCode != 0 {
		t.Errorf("names of exactly %d bytes: error %d, want them taken", longestName, resp.ErrorCode)
	}
}

func TestALongTransactionalIDIsRefusedAndNotKept(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	big := strings.Repeat("x", fillsAFrame)
	initProducer := func(id string) int16 {
		req := kmsg.NewPtrInitProducerIDRequest()
		req.TransactionalID, req.TransactionTimeoutMillis = &id, 60000
		req.ProducerID, req.ProducerEpoch = -1, -1
		return ask(t, c, req, 4, kmsg.NewPtrInitProducerIDResponse()).ErrorCode
	}
	keepsLittle(t, "an InitProducerId whose transactional id fills a frame", func(i int) {
		if code := initProducer(another(i, big)); code != 42 {
			t.Fatalf("a transactional id of %d bytes: error %d, want INVALID_REQUEST (42)", fillsAFrame, code)
		}
	})
	if code := initProducer(strings.Repeat("y", longestName+1)); code != 42 {
		t.Errorf("a transactional id of %d bytes: error %d, want 42", longestName+1, code)
	}
	if code := initProducer(strings.Repeat("y", longestName)); code != 0 {
		t.Errorf("a transactional id of %d bytes: error %d, want it taken", longestName, code)
	}
	list := ask(t, c, kmsg.NewPtrListTransactionsRequest(), 0, kmsg.NewPtrListTransactionsResponse())
	if len(list.TransactionStates) != 1 {
		t.Errorf("%d transactional ids are kept, want the one at the bound", len(list.TransactionStates))
	}
}

func TestALongGroupIDInAnOffsetCommitIsRefusedAndNotStored(t *testing.T) {
	_, addr := bootServer(t, config.Default())
	c := openRaw(t, addr)
	commit := func(group string) *kmsg.OffsetCommitResponse {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.Generation = group, -1
		tp := kmsg.NewOffsetCommitRequestTopic()
		tp.Topic = "orders"
		p := kmsg.NewOffsetCommitRequestTopicPartition()
		p.Offset = 1
		tp.Partitions = append(tp.Partitions, p)
		req.Topics = append(req.Topics, tp)
		return ask(t, c, req, 8, kmsg.NewPtrOffsetCommitResponse())
	}
	big := strings.Repeat("x", fillsAFrame)
	keepsLittle(t, "an OffsetCommit whose group id fills a frame", func(i int) {
		if code := commit(another(i, big)).Topics[0].Partitions[0].ErrorCode; code != 24 {
			t.Fatalf("a group id of %d bytes: error %d, want INVALID_GROUP_ID (24)", fillsAFrame, code)
		}
	})
	if code := commit(strings.Repeat("y", longestName+1)).Topics[0].Partitions[0].ErrorCode; code != 24 {
		t.Errorf("a group id of %d bytes: error %d, want 24", longestName+1, code)
	}
	groups := ask(t, c, kmsg.NewPtrListGroupsRequest(), 4, kmsg.NewPtrListGroupsResponse())
	if len(groups.Groups) != 0 {
		t.Errorf("%d groups exist after the refused commits", len(groups.Groups))
	}
}

// The first message of a SCRAM login is kept on the connection until the second. It
// holds a user name and a nonce a client makes up: a few dozen bytes. One that fills
// the megabyte a frame may be before authentication is not a login, and thirty
// connections holding one each must not hold thirty megabytes.
func TestALongSCRAMFirstMessageIsRefusedAndNotKept(t *testing.T) {
	addr := startAuthBroker(t, []string{config.SASLScramSHA256, config.SASLPlain}, map[string]string{"bob": "hunter2"}, "", "")
	keepsLittle(t, "a SCRAM client-first whose nonce fills a frame", func(int) {
		c := openRaw(t, addr) // held open, mid-login, until the test ends.
		hs := kmsg.NewPtrSASLHandshakeRequest()
		hs.Mechanism = config.SASLScramSHA256
		if code := ask(t, c, hs, 1, kmsg.NewPtrSASLHandshakeResponse()).ErrorCode; code != 0 {
			t.Fatalf("handshake: %d", code)
		}
		au := kmsg.NewPtrSASLAuthenticateRequest()
		au.SASLAuthBytes = []byte("n,,n=bob,r=" + strings.Repeat("n", fillsAFrame-64))
		au.SetVersion(2)
		resp := kmsg.NewPtrSASLAuthenticateResponse()
		if c.do(au, resp) && resp.ErrorCode != 58 {
			t.Fatalf("a client-first of %d bytes: error %d, want SASL_AUTHENTICATION_FAILED (58)", fillsAFrame, resp.ErrorCode)
		}
	})

	// A mechanism that is not offered is not what the connection is then set to, however
	// long its name; and a user name that is nobody's is nobody's principal.
	c := openRaw(t, addr)
	hs := kmsg.NewPtrSASLHandshakeRequest()
	hs.Mechanism = strings.Repeat("M", 32767)
	if code := ask(t, c, hs, 1, kmsg.NewPtrSASLHandshakeResponse()).ErrorCode; code != 33 {
		t.Errorf("a mechanism of %d bytes: error %d, want UNSUPPORTED_SASL_MECHANISM (33)", 32767, code)
	}
	au := kmsg.NewPtrSASLAuthenticateRequest()
	au.SASLAuthBytes = []byte("\x00bob\x00hunter2")
	// Kafka closes the connection on a SaslAuthenticate that follows a refused handshake
	// (recorded: sasl-plain, claim auth A9); if the long name had been kept as the
	// connection's mechanism it would be served.
	if c.do(au, kmsg.NewPtrSASLAuthenticateResponse()) {
		t.Errorf("authenticating after a refused handshake was answered: the long mechanism was kept")
	}
	p := openRaw(t, addr)
	hs.Mechanism = config.SASLPlain
	ask(t, p, hs, 1, kmsg.NewPtrSASLHandshakeResponse())
	au.SASLAuthBytes = []byte("\x00" + strings.Repeat("u", fillsAFrame-64) + "\x00hunter2")
	if code := ask(t, p, au, 2, kmsg.NewPtrSASLAuthenticateResponse()).ErrorCode; code != 58 {
		t.Errorf("a user name of %d bytes: error %d, want SASL_AUTHENTICATION_FAILED (58)", fillsAFrame, code)
	}
}

// A client id is in every request's header, in a form that cannot say more than 32,767
// bytes, and a group member's is given back whole by DescribeGroups, as Kafka gives it.
// That is its bound where a member keeps it; the Traffic page keeps the first 128.
func TestTheLongestClientIDIsKeptWholeByItsMemberAndShortByTraffic(t *testing.T) {
	srv, addr := bootServer(t, config.Default())
	clientID := strings.Repeat("c", 32767)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	send := func(req kmsg.Request, version int16, resp kmsg.Response) {
		t.Helper()
		req.SetVersion(version)
		resp.SetVersion(version)
		hdr := []byte{byte(req.Key() >> 8), byte(req.Key()), byte(version >> 8), byte(version), 0, 0, 0, 9}
		hdr = binary.BigEndian.AppendUint16(hdr, uint16(len(clientID)))
		hdr = append(hdr, clientID...)
		if req.IsFlexible() {
			hdr = append(hdr, 0)
		}
		body := req.AppendTo(nil)
		frame := binary.BigEndian.AppendUint32(nil, uint32(len(hdr)+len(body)))
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Write(append(append(frame, hdr...), body...)); err != nil {
			t.Fatal(err)
		}
		var n [4]byte
		if _, err := io.ReadFull(conn, n[:]); err != nil {
			t.Fatalf("%T: %v", req, err)
		}
		buf := make([]byte, binary.BigEndian.Uint32(n[:]))
		if _, err := io.ReadFull(conn, buf); err != nil {
			t.Fatal(err)
		}
		skip := 4
		if resp.IsFlexible() {
			skip++
		}
		if err := resp.ReadFrom(buf[skip:]); err != nil {
			t.Fatalf("decode %T: %v", resp, err)
		}
	}
	first := kmsg.NewPtrJoinGroupResponse()
	send(classicJoin("g-client", "", nil, "consumer", "range"), 9, first)
	if first.ErrorCode != 79 {
		t.Fatalf("the first join: error %d, want MEMBER_ID_REQUIRED", first.ErrorCode)
	}
	joined := kmsg.NewPtrJoinGroupResponse()
	send(classicJoin("g-client", first.MemberID, nil, "consumer", "range"), 9, joined)
	if joined.ErrorCode != 0 {
		t.Fatalf("the join: error %d", joined.ErrorCode)
	}
	describe := kmsg.NewPtrDescribeGroupsRequest()
	describe.Groups = []string{"g-client"}
	described := kmsg.NewPtrDescribeGroupsResponse()
	send(describe, 5, described)
	if len(described.Groups) != 1 || len(described.Groups[0].Members) != 1 || described.Groups[0].Members[0].ClientID != clientID {
		t.Fatalf("the member's client id is not the %d bytes it sent", 32767)
	}
	eventually(t, 5*time.Second, "the requests to be recorded", func() bool { return len(serverhook.Traffic(srv).Clients()) == 1 })
	if got := serverhook.Traffic(srv).Clients()[0].ClientID; len(got) != 128 {
		t.Errorf("the Traffic page keeps %d bytes of the client id, want 128", len(got))
	}
}
