package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestQueryParameters(t *testing.T) {
	read := func(q string) (int32, int64, int64, int64) {
		rq := httptest.NewRequest("GET", "/x?"+q, nil)
		return partitionParam(rq, 3), fromParam(rq), hlParam(rq), jumpParam(rq)
	}
	for q, want := range map[string][4]int64{
		"":                       {0, -1, -1, -1},
		"partition=2":            {2, -1, -1, -1},
		"partition=3":            {0, -1, -1, -1},
		"partition=-1":           {0, -1, -1, -1},
		"partition=x":            {0, -1, -1, -1},
		"from=0&hl=0&jump=0":     {0, 0, 0, 0},
		"from=5&hl=6&jump=7":     {0, 5, 6, 7},
		"from=-1&hl=-2&jump=-3":  {0, -1, -1, jumpMalformed},
		"from=x&hl=y&jump=12abc": {0, -1, -1, jumpMalformed},
		"jump=+":                 {0, -1, -1, -1},
		"jump=%2042%20":          {0, -1, -1, 42},
	} {
		p, f, h, j := read(q)
		if int64(p) != want[0] || f != want[1] || h != want[2] || j != want[3] {
			t.Errorf("%q: partition %d from %d hl %d jump %d; want %v", q, p, f, h, j, want)
		}
	}
	for _, tc := range []struct {
		p, o string
		wp   int32
		wo   int64
		bad  bool
	}{
		{"", "0", 0, 0, false}, {"2", "7", 2, 7, false}, {"0", "0", 0, 0, false}, {"-1", "0", 0, 0, true}, {"x", "0", 0, 0, true},
		{"0", "-1", 0, 0, true}, {"0", "", 0, 0, true}, {"0", "x", 0, 0, true}, {"4294967296", "0", 0, 0, true},
	} {
		p, o, err := recordCoordinates(tc.p, tc.o)
		if (err != nil) != tc.bad || (!tc.bad && (p != tc.wp || o != tc.wo)) {
			t.Errorf("recordCoordinates(%q, %q) = %d, %d, %v", tc.p, tc.o, p, o, err)
		}
	}
}

func TestSafeFilename(t *testing.T) {
	for in, want := range map[string]string{"orders": "orders", "a.b-c_d": "a.b-c_d", "az09AZ": "az09AZ", "a b/c": "a_b_c", "`{@[": "____", "é": "_", "a\"b": "a_b", "a;b": "a_b"} {
		if got := safeFilename(in); got != want {
			t.Errorf("safeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPreviewOnly(t *testing.T) {
	long := strings.Repeat("é", previewRunes*4)
	for _, tc := range []struct {
		in   []byte
		want func(bytesView) bool
	}{
		{nil, func(v bytesView) bool { return v.Null && v.Preview == "∅ null" }},
		{[]byte{}, func(v bytesView) bool { return v.Empty && v.Preview == `""` }},
		{[]byte{0xff, 0, 1}, func(v bytesView) bool { return v.Binary && strings.HasPrefix(v.Preview, "⟨binary") }},
		{[]byte("a\nb"), func(v bytesView) bool { return v.Preview == "a b" && v.Size == 3 }},
		{[]byte(long + "tail"), func(v bytesView) bool {
			return strings.HasSuffix(v.Preview, "…") && len([]rune(v.Preview)) == previewRunes+1
		}},
	} {
		if v := previewOnly(tc.in); !tc.want(v) {
			t.Errorf("previewOnly(%q...) = %+v", string(tc.in[:min(len(tc.in), 6)]), v)
		}
	}
}

// A record's inspector offers its neighbours, within what the log still holds.
func TestMessageInspectorNeighbours(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(ctx, "nb", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "nb", 1, 6, vals("m"), time.Time{})
	deleteBefore(t, cl, "nb", 1, 2)
	open := func(off string) string {
		rec := req(t, c, "GET", "/_console/topics/nb/message?partition=1&offset="+off, nil)
		if rec.Code != 200 {
			t.Fatalf("offset %s: %d", off, rec.Code)
		}
		return rec.Body.String()
	}
	for off, want := range map[string][2]string{
		"2": {"", "(offset 3)"}, "3": {"(offset 2)", "(offset 4)"}, "5": {"(offset 4)", ""},
	} {
		body := open(off)
		for i, w := range want {
			word := []string{"Older", "Newer"}[i]
			if w == "" {
				if strings.Contains(body, word+" (offset ") && !strings.Contains(body, word+" (offset )") {
					t.Errorf("offset %s offers a %s record", off, word)
				}
				continue
			}
			if !strings.Contains(body, word+" "+w) || !strings.Contains(body, "partition=1&amp;offset="+strings.Trim(strings.TrimPrefix(w, "(offset "), ")")) {
				t.Errorf("offset %s lacks %s %s", off, word, w)
			}
		}
	}
	for _, bad := range []string{"1", "6", "99"} {
		if rec := req(t, c, "GET", "/_console/topics/nb/message?partition=1&offset="+bad, nil); rec.Code != 404 {
			t.Errorf("offset %s: %d", bad, rec.Code)
		}
	}
}

// The clients that wrote a topic are listed once each, with how many requests they made.
func TestTopicWritersAreCountedPerClient(t *testing.T) {
	src := &fakeTraffic{events: []TrafficEvent{
		{ID: "r1", Seq: 1, API: "Produce", ClientID: "a", Topics: []string{"t"}, Count: 3, Principal: "User:alice", Detail: "1 partition", Time: tt0},
		{ID: "r2", Seq: 2, API: "Produce", ClientID: "a", Topics: []string{"t"}, Count: 0},
		{ID: "r3", Seq: 3, API: "Produce", Remote: "10.0.0.9:4000", Topics: []string{"t"}, Count: 2, Error: "boom"},
		{ID: "r4", Seq: 4, API: "Fetch", ClientID: "reader", Topics: []string{"t"}},
		{ID: "r5", Seq: 5, API: "Produce", ClientID: "other", Topics: []string{"u"}},
	}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	got := c.topicWriters("t")
	if len(got) != 2 {
		t.Fatalf("writers %+v", got)
	}
	if got[0].Client != "a" || got[0].Count != 4 || got[0].Principal != "User:alice" || got[0].Detail != "1 partition" {
		t.Errorf("a: %+v", got[0])
	}
	if got[1].Client != "10.0.0.9:4000" || got[1].Count != 2 || got[1].Error != "boom" {
		t.Errorf("by address: %+v", got[1])
	}
	if w := newTestConsole(t).topicWriters("t"); w != nil {
		t.Errorf("without a recorder: %+v", w)
	}
}

// What the console says it did when a record is sent.
func TestProduceToastsSayWhereTheRecordWent(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "ptoast", 3, 0)
	toastOf := func(form url.Values) (int, string) {
		rec := req(t, c, "POST", "/_console/topics/ptoast/produce", form)
		return rec.Code, rec.Header().Get("HX-Trigger")
	}
	code, trig := toastOf(url.Values{"partition": {"1"}, "value": {"a"}, "acks": {"1"}, "partition_ctx": {"1"}})
	if code != 200 || !strings.Contains(trig, `Sent to partition 1 \u00b7 offset 0"`) {
		t.Errorf("viewing the partition it went to: %d %s", code, trig)
	}
	_, trig = toastOf(url.Values{"partition": {"1"}, "value": {"b"}, "acks": {"all"}, "partition_ctx": {"0"}})
	if !strings.Contains(trig, `Sent to partition 1 \u00b7 offset 1 (you are viewing partition 0)`) {
		t.Errorf("viewing another: %s", trig)
	}
	for _, ctx := range []string{"3", "-1", "x", ""} {
		_, trig = toastOf(url.Values{"partition": {"2"}, "value": {"c"}, "acks": {"1"}, "partition_ctx": {ctx}})
		if strings.Contains(trig, "you are viewing") {
			t.Errorf("partition_ctx=%q is not a partition of the topic but is believed: %s", ctx, trig)
		}
	}
	_, trig = toastOf(url.Values{"partition": {"2"}, "value": {"d"}, "acks": {"1"}, "partition_ctx": {"2"}})
	if strings.Contains(trig, "you are viewing") {
		t.Errorf("the last partition: %s", trig)
	}
	_, trig = toastOf(url.Values{"partition": {"0"}, "value": {"e"}, "acks": {"0"}})
	if !strings.Contains(trig, "no confirmation requested (acks=0)") {
		t.Errorf("acks 0: %s", trig)
	}
	rec := req(t, c, "POST", "/_console/topics/ptoast/produce", url.Values{"partition": {"3"}, "value": {"f"}})
	if rec.Code != 400 {
		t.Errorf("a partition that does not exist: %d", rec.Code)
	}
}

func TestResendAndRawBytesOfARecord(t *testing.T) {
	c := newTestConsole(t)
	topicWith(t, c, "raw", 2, 0)
	req(t, c, "POST", "/_console/topics/raw/produce", url.Values{"partition": {"1"}, "key": {"k1"}, "value": {"v1"}, "acks": {"1"}})
	rec := req(t, c, "POST", "/_console/topics/raw/resend", url.Values{"partition": {"1"}, "offset": {"0"}})
	if rec.Code != 200 || !strings.Contains(rec.Header().Get("HX-Trigger"), `Sent an exact copy to partition 1 \u00b7 offset 1`) {
		t.Errorf("resend: %d %s", rec.Code, rec.Header().Get("HX-Trigger"))
	}
	for _, bad := range []url.Values{{"partition": {"1"}, "offset": {"9"}}, {"partition": {"x"}, "offset": {"0"}}, {"partition": {"1"}, "offset": {"-1"}}} {
		if rec := req(t, c, "POST", "/_console/topics/raw/resend", bad); rec.Code < 400 {
			t.Errorf("resend %v: %d", bad, rec.Code)
		}
	}
	for field, want := range map[string]string{"": "v1", "value": "v1", "key": "k1"} {
		rec := req(t, c, "GET", "/_console/topics/raw/message/raw?partition=1&offset=0&field="+field, nil)
		if rec.Code != 200 || rec.Body.String() != want || rec.Header().Get("Content-Type") != "application/octet-stream" ||
			!strings.Contains(rec.Header().Get("Content-Disposition"), "raw-1-0."+map[string]string{"": "value", "value": "value", "key": "key"}[field]+".bin") {
			t.Errorf("field %q: %d %q %s", field, rec.Code, rec.Body.String(), rec.Header().Get("Content-Disposition"))
		}
	}
}
