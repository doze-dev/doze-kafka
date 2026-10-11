package console

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPartRanges(t *testing.T) {
	for _, tc := range []struct {
		in   []int32
		want string
	}{
		{nil, ""}, {[]int32{3}, "3"}, {[]int32{0, 1, 2}, "0–2"}, {[]int32{0, 1, 2, 5, 7, 8}, "0–2, 5, 7–8"},
		{[]int32{8, 1, 2, 0}, "0–2, 8"}, {[]int32{4, 6}, "4, 6"},
	} {
		if got := partRanges(tc.in); got != tc.want {
			t.Errorf("partRanges(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func commitFor(t *testing.T, addr, group, topic string, offsets map[int32]int64) {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	oc := kmsg.NewPtrOffsetCommitRequest()
	oc.Group, oc.Generation = group, -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	for p, o := range offsets {
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition, rp.Offset = p, o
		rt.Partitions = append(rt.Partitions, rp)
	}
	oc.Topics = append(oc.Topics, rt)
	resp, err := oc.RequestWith(context.Background(), cl)
	if err != nil || resp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatalf("commit: %v %+v", err, resp)
	}
}

func topicWith(t *testing.T, c *Console, name string, partitions, records int) {
	t.Helper()
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {name}, "partitions": {itoa(partitions)}})
	produceTo(t, c, name, 0, records)
}

// A group page names who is in the group and what they own, links the topics, and
// says which member holds each partition: the things you open the page to find out.
func TestGroupPageShowsMembersOwnersAndLinks(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "orders", 2, 3)

	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("billing-service"), kgo.ConsumerGroup("billing"), kgo.ConsumeTopics("orders"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.AutoCommitInterval(100*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	cl.PollRecords(ctx, 3)
	cancel()
	cl.CommitUncommittedOffsets(context.Background())

	var body string
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		body = req(t, c, "GET", "/_console/groups/billing", nil).Body.String()
		if strings.Contains(body, "billing-service") && strings.Contains(body, `id="lag-table"`) {
			break
		}
		time.Sleep(150 * time.Millisecond)
	}
	members := req(t, c, "GET", "/_console/groups/billing?tab=members", nil).Body.String()
	for _, want := range []string{
		"billing-service", // the client id.
		"127.0.0.1",       // the host it connected from.
		`orders:0–1`,      // a readable assignment (its topic link is in the member's inspector, not in the row that opens it).
	} {
		if !strings.Contains(members, want) {
			t.Errorf("the members tab is missing %q", want)
		}
	}
	for _, want := range []string{
		"billing-service", // each partition's owner.
		"caught up",
		`data-live="/_console/groups/billing/live?tab=lag"`, // the lag is live.
	} {
		if !strings.Contains(body, want) {
			t.Errorf("the group page is missing %q", want)
		}
	}
	if strings.Contains(body+members, "[0 1]") {
		t.Error("the assignment is printed as a Go slice")
	}
	// The owner of each partition is shown beside it.
	if strings.Count(body, `<span class="m el">billing-service</span>`) < 1 {
		t.Error("each partition's owner should be shown")
	}
}

func TestGroupLivePollAnswers204UntilSomethingChanges(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 5)
	commitFor(t, addr, "g", "t", map[int32]int64{0: 2})

	first := doReq(t, c, "GET", "/_console/groups/g/live", map[string]string{"HX-Request": "true"})
	hash := first.Header().Get("HX-Live-Hash")
	if first.Code != 200 || hash == "" || !strings.Contains(first.Body.String(), `>3</span> records behind`) {
		t.Fatalf("first poll: %d, hash %q, lag shown: %v", first.Code, hash, strings.Contains(first.Body.String(), `>3</span> records behind`))
	}
	if rec := doReq(t, c, "GET", "/_console/groups/g/live?h="+hash, map[string]string{"HX-Request": "true"}); rec.Code != http.StatusNoContent {
		t.Fatalf("an unchanged group should answer 204, got %d", rec.Code)
	}
	produceTo(t, c, "t", 0, 2) // the lag grows.
	rec := doReq(t, c, "GET", "/_console/groups/g/live?h="+hash, map[string]string{"HX-Request": "true"})
	if rec.Code != 200 || !strings.Contains(rec.Body.String(), `>5</span> records behind`) {
		t.Fatalf("lag changed from 3 to 5 but the poll said %d", rec.Code)
	}
}

func TestGroupEdgeStatesAreExplained(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 5)
	commitFor(t, addr, "ahead", "t", map[int32]int64{0: 50})
	body := req(t, c, "GET", "/_console/groups/ahead", nil).Body.String()
	if !strings.Contains(body, "ahead of log end by 45") {
		t.Error("a committed offset beyond the log end should say how far ahead it is")
	}
	if strings.Contains(body, "lag 0") || strings.Contains(body, "caught up") {
		t.Error("an offset ahead of the log is not 'caught up'")
	}

	// A group with members but no commits explains why there is no lag.
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("fresh"), kgo.ConsumeTopics("t"), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cl.PollFetches(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		body = req(t, c, "GET", "/_console/groups/fresh", nil).Body.String()
		if strings.Contains(body, "No committed offsets yet") {
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
	t.Error("a group with members and no commits should say there is nothing to show yet")
}

func TestResetAndDeleteAnInactiveGroup(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 6)
	commitFor(t, addr, "idle", "t", map[int32]int64{0: 2})
	post := func(path string, form url.Values) (int, string) {
		rec := doPost(t, c, path, form)
		return rec.Code, rec.Body.String()
	}

	if code, body := post("/_console/groups/idle/reset", url.Values{"to": {"latest"}}); code != 200 || !strings.Contains(body, "caught up") {
		t.Fatalf("reset to latest: %d %.200s", code, body)
	}
	if offs, _ := c.be.GroupOffsets(context.Background(), "idle"); len(offs) != 1 || offs[0].Committed != 6 {
		t.Fatalf("after reset to latest the committed offset is %+v, want 6", offs)
	}
	post("/_console/groups/idle/reset", url.Values{"to": {"earliest"}})
	if offs, _ := c.be.GroupOffsets(context.Background(), "idle"); offs[0].Committed != 0 {
		t.Fatalf("after reset to earliest the committed offset is %d, want 0", offs[0].Committed)
	}
	if code, body := post("/_console/groups/idle/reset", url.Values{"to": {"sideways"}}); code != 400 || !strings.Contains(body, "earliest or latest") {
		t.Errorf("a bad target: %d %q", code, body)
	}

	// An active group is refused, and says why.
	// (Committed before the member joins: a commit naming no member is refused by
	// the broker once the group has one.)
	commitFor(t, addr, "busy", "t", map[int32]int64{0: 1})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ConsumerGroup("busy"), kgo.ConsumeTopics("t"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	cl.PollFetches(ctx)
	cancel()
	if code, body := post("/_console/groups/busy/reset", url.Values{"to": {"latest"}}); code != 400 || !strings.Contains(body, "active member") {
		t.Errorf("reset of an active group: %d %q, want a refusal naming the members", code, body)
	}
	if code, body := post("/_console/groups/busy/delete", url.Values{}); code != 400 || !strings.Contains(body, "active members") {
		t.Errorf("delete of an active group: %d %q", code, body)
	}

	// An inactive one is deleted and gone from the list.
	rec := doPost(t, c, "/_console/groups/idle/delete", url.Values{})
	if rec.Code != http.StatusSeeOther {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	if list := req(t, c, "GET", "/_console/groups", nil).Body.String(); strings.Contains(list, `href="/_console/groups/idle"`) {
		t.Error("the deleted group is still listed")
	}
}

func doPost(t *testing.T, c *Console, path string, form url.Values) *httptest.ResponseRecorder {
	t.Helper()
	return req(t, c, "POST", path, form)
}

// Who consumes a topic is part of its page. It used to be fetched after the page
// had been shown, and arriving a moment later it pushed everything below it down.
func TestTopicPageListsItsConsumers(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 4)
	topicWith(t, c, "unread", 1, 1)
	commitFor(t, addr, "reader", "t", map[int32]int64{0: 1})
	page := req(t, c, "GET", "/_console/topics/t?tab=readers", nil).Body.String()
	if !strings.Contains(page, "Groups reading this topic") || !strings.Contains(page, `href="/_console/groups/reader"`) || !strings.Contains(page, `warn">3</td>`) {
		t.Error("the Readers tab should name its consumer and its lag")
	}
	if strings.Contains(page, `hx-trigger="load"`) {
		t.Error("something on the topic page is still loaded after it is shown")
	}
	// The tab's number, and the neighbours well, are on every tab.
	if main := req(t, c, "GET", "/_console/topics/t", nil).Body.String(); !strings.Contains(main, `read by</span><span class="m">reader</span>`) && !strings.Contains(main, ">reader</span>") {
		t.Error("the topic page should name the group that reads it among its neighbours")
	}
	// A topic nobody reads says so in the same place.
	if other := req(t, c, "GET", "/_console/topics/unread?tab=readers", nil).Body.String(); !strings.Contains(other, "No group has committed an offset here") {
		t.Error("a topic with no consumers should say so in the same place")
	}
}

func TestClusterPageShowsRealAPIsAndConfig(t *testing.T) {
	c := newTestConsole(t)
	body := req(t, c, "GET", "/_console/cluster?view=apis", nil).Body.String() + req(t, c, "GET", "/_console/cluster?view=config", nil).Body.String() + req(t, c, "GET", "/_console/cluster", nil).Body.String()
	for _, want := range []string{"DeleteGroups", "OffsetDelete", "ConsumerGroupHeartbeat", "Produce <span class=\"apiv\">v3–", "Broker configuration", "log.retention.bytes", "auto.create.topics.enable", "limitations</a>"} {
		if !strings.Contains(body, want) {
			t.Errorf("the cluster page is missing %q", want)
		}
	}
}
