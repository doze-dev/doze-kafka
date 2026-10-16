package protocol

import (
	"context"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/twmb/franz-go/pkg/kerr"
)

// What the Traffic page writes about a request must be what happened, not what a
// client is expected to do next or what the request hoped for. Each test here failed
// on the code before its fix (the fix reverted by hand fails it again).

// A1: an ApiVersions newer than this broker serves is answered UNSUPPORTED_VERSION with
// the version to ask at. What the client does next is the client's: the row said "the
// client asks again", which is a claim about another program.
func TestApiVersionsDowngradeSaysWhatTheBrokerAnswered(t *testing.T) {
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ErrorCode = kerr.UnsupportedVersion.Code
	k := kmsg.NewApiVersionsResponseApiKey()
	k.ApiKey, k.MinVersion, k.MaxVersion = int16(kmsg.ApiVersions), 0, 4
	resp.ApiKeys = append(resp.ApiKeys, k)
	r := &Request{Header: RequestHeader{APIKey: kmsg.ApiVersions, APIVersion: 5}, answered: resp}
	s := r.Describe()
	if strings.Contains(s.Detail, "asks again") {
		t.Errorf("detail claims what the client does next: %q", s.Detail)
	}
	if want := "v5 is newer than this broker's v0 to v4: answered UNSUPPORTED_VERSION with the versions to ask at"; s.Detail != want {
		t.Errorf("detail = %q, want %q", s.Detail, want)
	}
	if s.ErrorCode != 0 || s.Class.IsError() {
		t.Errorf("the downgrade is how every client learns what to speak: code %d class %q", s.ErrorCode, s.Class)
	}
}

// A8: "auto-create on" was the request's flag, written on a broker started with
// --auto-create-topics=false. Whether a topic is created is the broker's to say: the
// summary says the request asked, and the page says what the broker does with it.
func TestMetadataLeavesAutoCreateToTheBroker(t *testing.T) {
	req := kmsg.NewPtrMetadataRequest()
	req.AllowAutoTopicCreation = true
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("invoices")
	req.Topics = append(req.Topics, mt)
	resp := kmsg.NewPtrMetadataResponse()
	rt := kmsg.NewMetadataResponseTopic()
	rt.Topic = kmsg.StringPtr("invoices")
	resp.Topics = append(resp.Topics, rt)
	s := sumMetadata(req, resp)
	if strings.Contains(s.Detail, "auto-create") {
		t.Errorf("the detail says what the broker did not: %q", s.Detail)
	}
	if !s.AutoCreate {
		t.Error("the summary does not say the request allowed auto-create")
	}
	req.AllowAutoTopicCreation = false
	if s := sumMetadata(req, resp); s.AutoCreate {
		t.Error("AutoCreate set for a request that did not allow it")
	}
}

// A commit's partitions and generation are facts of their own, so the page can write
// them where its board does: "harbour.depot.dispatch p0–p2" beside the group (Main),
// "p0–p2 · g-order-validator" beside the topic (Traf-02), ", generation 7" in the
// story (Traf-12).
func TestOffsetCommitSaysItsPartitionsAndGeneration(t *testing.T) {
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group, req.Generation = "g-depot-router", 7
	tp := kmsg.NewOffsetCommitRequestTopic()
	tp.Topic = "harbour.depot.dispatch"
	for _, p := range []int32{2, 0, 1} {
		pp := kmsg.NewOffsetCommitRequestTopicPartition()
		pp.Partition = p
		tp.Partitions = append(tp.Partitions, pp)
	}
	req.Topics = append(req.Topics, tp)
	s := sumOffsetCommit(req, nil)
	if s.Parts != "p0–p2" || s.Gen != 7 || !s.HasGen {
		t.Errorf("parts %q generation %d (%v), want p0–p2 and 7", s.Parts, s.Gen, s.HasGen)
	}
}

// A8: the advice for a topic that does not exist told a broker with auto-create off to
// "let the producer's first Metadata create it".
func TestUnknownTopicAdviceFollowsTheBroker(t *testing.T) {
	off := ExplainFor(kerr.UnknownTopicOrPartition.Code, BrokerFacts{AutoCreateTopics: false, Known: true})
	if strings.Contains(off.Fix, "create it") && strings.Contains(off.Fix, "Metadata") {
		t.Errorf("auto-create is off, and the fix says Metadata creates it: %q", off.Fix)
	}
	if !strings.Contains(off.Fix, "auto-create is off") {
		t.Errorf("the fix does not say auto-create is off: %q", off.Fix)
	}
	on := ExplainFor(kerr.UnknownTopicOrPartition.Code, BrokerFacts{AutoCreateTopics: true, Known: true})
	if !strings.Contains(on.Fix, "auto-create is on") {
		t.Errorf("the fix does not say auto-create is on: %q", on.Fix)
	}
	if got := ExplainFor(kerr.GroupAuthorizationFailed.Code, BrokerFacts{}); got != Explain(kerr.GroupAuthorizationFailed.Code) {
		t.Errorf("a code the broker's settings do not touch is explained differently: %+v", got)
	}
}

// A9: "Size in / out" was 4 bytes under the wire on each side: the frame's length
// prefix was not counted. A client's own count (kgo's BytesWritten and BytesRead) has
// it, so the page and the client disagreed.
func TestObservedSizesAreWhatCrossedTheWire(t *testing.T) {
	router := NewRouter()
	router.Register(kmsg.Heartbeat, func(_ context.Context, req *Request) ([]byte, error) {
		q, err := req.Decode()
		if err != nil {
			return nil, err
		}
		resp := q.ResponseKind().(*kmsg.HeartbeatResponse)
		resp.SetVersion(req.Header.APIVersion)
		return req.Reply(resp), nil
	})
	conn, obs := serveObserved(t, router)
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.SetVersion(1)
	hb.Group, hb.MemberID, hb.Generation = "g", "m", 1
	body := hb.AppendTo(nil)
	const id = "sizes"
	sendRaw(t, conn, kmsg.Heartbeat, 1, 1, id, body)
	frame, err := ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	o := waitObservations(t, obs, 1)[0]
	wantIn := 4 + 2 + 2 + 4 + 2 + len(id) + len(body) // the length, the header, the body.
	if o.ReqBytes != wantIn || o.RespBytes != 4+len(frame) {
		t.Errorf("in %d out %d, want %d and %d: the bytes on the wire, length prefix included", o.ReqBytes, o.RespBytes, wantIn, 4+len(frame))
	}
}
