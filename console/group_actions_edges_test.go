package console

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func TestGroupActionsAnswerWithToastsAndFreshRows(t *testing.T) {
	c, addr, _ := groupFixture(t)
	ctx := context.Background()
	post := func(path string, form url.Values) (int, string, string) {
		rec := req(t, c, "POST", path, form)
		return rec.Code, rec.Header().Get("HX-Trigger"), rec.Body.String()
	}
	// Reset one topic, then all.
	code, trig, body := post("/_console/groups/g/reset", url.Values{"to": {"latest"}, "topic": {"gt"}})
	if code != 200 || !strings.Contains(trig, `Reset 2 offsets to latest on gt`) || !strings.Contains(body, "gt") {
		t.Errorf("reset one topic: %d %s", code, trig)
	}
	if got := groupOffsetsText(t, c, "g"); got != "gt/0=5 gt/1=3 gu/0=1" {
		t.Errorf("offsets %s", got)
	}
	code, trig, _ = post("/_console/groups/g/reset", url.Values{"to": {"earliest"}})
	if code != 200 || !strings.Contains(trig, "Reset 3 offsets to earliest") || strings.Contains(trig, " on ") {
		t.Errorf("reset all: %d %s", code, trig)
	}
	if code, _, _ = post("/_console/groups/g/reset", url.Values{"to": {"sideways"}}); code != 400 {
		t.Errorf("reset to nowhere: %d", code)
	}

	// Setting one offset: to a number, or to an end.
	code, trig, _ = post("/_console/groups/g/offset", url.Values{"topic": {"gt"}, "partition": {"1"}, "offset": {"2"}})
	if code != 200 || !strings.Contains(trig, `gt \u00b7 partition 1 is now at offset 2`) {
		t.Errorf("set: %d %s", code, trig)
	}
	code, trig, _ = post("/_console/groups/g/offset", url.Values{"topic": {"gt"}, "partition": {"0"}, "to": {"latest"}})
	if code != 200 || !strings.Contains(trig, `gt \u00b7 partition 0 is now at offset 5`) {
		t.Errorf("set to latest: %d %s", code, trig)
	}
	for name, form := range map[string]url.Values{
		"no topic":          {"partition": {"0"}, "offset": {"1"}},
		"bad partition":     {"topic": {"gt"}, "partition": {"x"}, "offset": {"1"}},
		"negative":          {"topic": {"gt"}, "partition": {"-1"}, "offset": {"1"}},
		"no offset":         {"topic": {"gt"}, "partition": {"0"}},
		"negative offset":   {"topic": {"gt"}, "partition": {"0"}, "offset": {"-1"}},
		"past the end":      {"topic": {"gt"}, "partition": {"0"}, "offset": {"99"}},
		"no such partition": {"topic": {"gt"}, "partition": {"9"}, "offset": {"0"}},
	} {
		if code, _, _ = post("/_console/groups/g/offset", form); code != 400 {
			t.Errorf("%s: %d", name, code)
		}
	}
	// An offset forgotten; and the last one leaves the list.
	code, trig, _ = post("/_console/groups/g/offset/delete", url.Values{"topic": {"gu"}, "partition": {"0"}})
	if code != 200 || !strings.Contains(trig, `Removed the committed offset for gu \u00b7 partition 0`) {
		t.Errorf("delete one: %d %s", code, trig)
	}
	if code, _, _ = post("/_console/groups/g/offset/delete", url.Values{"topic": {"gu"}}); code != 400 {
		t.Errorf("delete with no partition: %d", code)
	}
	post("/_console/groups/g/offset/delete", url.Values{"topic": {"gt"}, "partition": {"0"}})
	rec := req(t, c, "POST", "/_console/groups/g/offset/delete", url.Values{"topic": {"gt"}, "partition": {"1"}})
	if rec.Code != 303 || rec.Header().Get("Location") != "/_console/groups?flash=Removed+the+group%27s+last+committed+offset" {
		t.Errorf("delete the last: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if c.groupListed(httptest.NewRequest("GET", "/x", nil).WithContext(ctx), "g") {
		t.Error("a group with no offsets is still listed")
	}

	// A whole group goes; a group with a member cannot.
	commitFor(t, addr, "h", "gt", map[int32]int64{0: 1})
	rec = req(t, c, "POST", "/_console/groups/h/delete", nil)
	if rec.Code != 303 || rec.Header().Get("Location") != "/_console/groups?flash=Group+deleted" {
		t.Errorf("delete: %d %s", rec.Code, rec.Header().Get("Location"))
	}
	if rec = req(t, c, "POST", "/_console/groups/h/delete", nil); rec.Code != 404 {
		t.Errorf("delete again: %d", rec.Code)
	}
}

func TestOffsetCoordinatesAndTopicsOfAGroup(t *testing.T) {
	read := func(wantOffset bool, kv ...string) (int32, int64, error) {
		v := url.Values{}
		for i := 0; i < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		r := httptest.NewRequest("POST", "/x", strings.NewReader(v.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		return offsetCoordinates(r, wantOffset)
	}
	if p, o, err := read(true, "topic", "t", "partition", " 3 ", "offset", " 17 "); err != nil || p != 3 || o != 17 {
		t.Errorf("%d %d %v", p, o, err)
	}
	if p, o, err := read(false, "topic", "t", "partition", "0"); err != nil || p != 0 || o != 0 {
		t.Errorf("%d %d %v", p, o, err)
	}
	if p, o, err := read(true, "topic", "t", "partition", "0", "offset", "0"); err != nil || p != 0 || o != 0 {
		t.Errorf("zeros: %d %d %v", p, o, err)
	}
	for _, kv := range [][]string{{"partition", "0", "offset", "1"}, {"topic", "t", "partition", "-1", "offset", "1"}, {"topic", "t", "partition", "4294967296", "offset", "1"}, {"topic", "t", "partition", "0", "offset", "-1"}, {"topic", "t", "partition", "0", "offset", "1.5"}} {
		if _, _, err := read(true, kv...); err == nil {
			t.Errorf("%v accepted", kv)
		}
	}
	rows := []groupLagRow{{Topic: "a"}, {Topic: "a"}, {Topic: "gone", Deleted: true}, {Topic: "b"}, {Topic: "b"}, {Topic: "a"}}
	if got := strings.Join(offsetTopics(rows), ","); got != "a,b,a" {
		t.Errorf("%s", got)
	}
	if len(offsetTopics(nil)) != 0 {
		t.Error("topics of nothing")
	}
	if got := tryRow([]groupLagRow{{Topic: "gone", Deleted: true}, {Topic: "t", Partition: 2}}); got.Topic != "t" || got.Partition != 2 {
		t.Errorf("%+v", got)
	}
	if got := tryRow([]groupLagRow{{Topic: "gone", Deleted: true}}); got.Topic != "" {
		t.Errorf("%+v", got)
	}
}
