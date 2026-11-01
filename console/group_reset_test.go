package console

import (
	"context"
	"errors"
	"html"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// resetFixture: topic rt with two partitions (10 and 6 records, one millisecond
// apart from t0), and a group g committed at 7 and 2 with no member.
var resetT0 = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func resetFixture(t *testing.T) (*Console, string) {
	t.Helper()
	c, addr := newTestConsoleAddr(t)
	cl := rawClient(t, addr)
	if err := c.be.CreateTopic(context.Background(), "rt", 2, nil); err != nil {
		t.Fatal(err)
	}
	bulkProduce(t, cl, "rt", 0, 10, vals("a"), resetT0)
	bulkProduce(t, cl, "rt", 1, 6, vals("b"), resetT0)
	commitFor(t, addr, "g", "rt", map[int32]int64{0: 7, 1: 2})
	return c, addr
}

func afters(rows []ResetRow) map[int32]int64 {
	out := map[int32]int64{}
	for _, r := range rows {
		out[r.Partition] = r.After
	}
	return out
}

// The preview is the plan: every number from the broker, nothing sent.
func TestPlanResetPerMode(t *testing.T) {
	c, _ := resetFixture(t)
	ctx := context.Background()
	plan := func(s ResetSpec) []ResetRow {
		t.Helper()
		rows, err := c.be.PlanReset(ctx, "g", s)
		if err != nil {
			t.Fatalf("%+v: %v", s, err)
		}
		return rows
	}

	rows := plan(ResetSpec{To: "earliest"})
	if len(rows) != 2 || rows[0].Committed != 7 || rows[0].Latest != 10 || rows[0].After != 0 || rows[1].Committed != 2 || rows[1].After != 0 {
		t.Errorf("earliest: %+v", rows)
	}
	if got := afters(plan(ResetSpec{To: "latest"})); got[0] != 10 || got[1] != 6 {
		t.Errorf("latest: %v", got)
	}
	// By time: the first record at or after it, per partition.
	if got := afters(plan(ResetSpec{To: "timestamp", At: resetT0.Add(4 * time.Millisecond)})); got[0] != 4 || got[1] != 4 {
		t.Errorf("timestamp +4ms: %v", got)
	}
	// After the last record there is none: the end of the log, and the row says so.
	late := plan(ResetSpec{To: "timestamp", At: resetT0.Add(time.Hour)})
	if afters(late)[0] != 10 || afters(late)[1] != 6 || late[0].Note == "" {
		t.Errorf("timestamp past the last record: %+v", late)
	}
	// Specific offsets: only the named partitions move.
	one := plan(ResetSpec{To: "offsets", Offsets: map[TopicPartition]int64{{"rt", 0}: 3}})
	if !one[0].Moves || one[0].After != 3 || one[1].Moves || one[1].After != 2 {
		t.Errorf("offsets: %+v", one)
	}
	// A preview sends nothing.
	if got := groupOffsetsText(t, c, "g"); got != "rt/0=7 rt/1=2" {
		t.Errorf("a plan moved offsets: %s", got)
	}
}

// GRP-15: outside the log is the console's own rule, in the Refused kind, naming no Kafka error.
func TestPlanResetRefusesAnOffsetOutsideTheLogWithTheConsolesRule(t *testing.T) {
	c, _ := resetFixture(t)
	for _, bad := range []int64{11, -1} {
		_, err := c.be.PlanReset(context.Background(), "g", ResetSpec{To: "offsets", Offsets: map[TopicPartition]int64{{"rt", 0}: bad}})
		if !errors.Is(err, errBadInput) {
			t.Fatalf("offset %d: %v", bad, err)
		}
		f := refusalFor(err)
		if f.Kind != Refused || f.Source != SourceConsole || f.Name != "" {
			t.Errorf("offset %d: %+v", bad, f)
		}
		if !strings.Contains(f.Message, "outside partition 0") || !strings.Contains(f.Rule, "first retained record") || !strings.Contains(f.Rule, "10") {
			t.Errorf("offset %d: message %q rule %q", bad, f.Message, f.Rule)
		}
		if strings.Contains(f.Message+f.Rule, "OFFSET_OUT_OF_RANGE ") && !strings.Contains(f.Rule, "Fetch") {
			t.Errorf("OFFSET_OUT_OF_RANGE is a Fetch answer: %q", f.Rule)
		}
	}
}

func TestApplyResetByTimeAndByOffsets(t *testing.T) {
	c, _ := resetFixture(t)
	ctx := context.Background()
	if n, err := c.be.ApplyReset(ctx, "g", ResetSpec{To: "timestamp", At: resetT0.Add(4 * time.Millisecond)}); err != nil || n != 2 {
		t.Fatalf("by time: %d %v", n, err)
	}
	if got := groupOffsetsText(t, c, "g"); got != "rt/0=4 rt/1=4" {
		t.Errorf("after the time reset: %s", got)
	}
	if n, err := c.be.ApplyReset(ctx, "g", ResetSpec{To: "offsets", Offsets: map[TopicPartition]int64{{"rt", 0}: 9, {"rt", 1}: 4}}); err != nil || n != 1 {
		t.Fatalf("by offsets (one unchanged): %d %v", n, err)
	}
	if got := groupOffsetsText(t, c, "g"); got != "rt/0=9 rt/1=4" {
		t.Errorf("after the offsets reset: %s", got)
	}
	if _, err := c.be.ApplyReset(ctx, "g", ResetSpec{To: "offsets", Offsets: map[TopicPartition]int64{{"rt", 0}: 9}}); !errors.Is(err, errBadInput) {
		t.Errorf("nothing changed should be refused: %v", err)
	}
}

func TestResetPreviewRouteSendsNothingAndResetByTimeWorks(t *testing.T) {
	c, _ := resetFixture(t)
	prev := req(t, c, "POST", "/_console/groups/g/reset/preview", url.Values{"to": {"timestamp"}, "at": {resetT0.Add(4 * time.Millisecond).Format(time.RFC3339Nano)}, "topic": {"rt"}})
	if prev.Code != 200 {
		t.Fatalf("preview: %d %s", prev.Code, prev.Body)
	}
	text := plainOf(prev.Body.String())
	for _, want := range []string{"Preview", "Nothing has changed yet", "Committed now", "After reset", "Lag after", "7 → 4", "2 → 4"} {
		if !strings.Contains(text, want) {
			t.Errorf("the preview lacks %q: %s", want, text)
		}
	}
	if got := groupOffsetsText(t, c, "g"); got != "rt/0=7 rt/1=2" {
		t.Errorf("a preview moved offsets: %s", got)
	}
	for _, bad := range []url.Values{{"to": {"timestamp"}, "at": {"not a time"}}, {"to": {"sideways"}}} {
		if rec := req(t, c, "POST", "/_console/groups/g/reset/preview", bad); rec.Code != 400 {
			t.Errorf("%v: %d", bad, rec.Code)
		}
	}
	rec := req(t, c, "POST", "/_console/groups/g/reset", url.Values{"to": {"timestamp"}, "at": {"2026-01-01T00:00:00.004Z"}, "topic": {"rt"}})
	if rec.Code != 200 || !strings.Contains(rec.Header().Get("HX-Trigger"), "Reset 2 offsets") {
		t.Fatalf("reset by time: %d %s", rec.Code, rec.Header())
	}
	if got := groupOffsetsText(t, c, "g"); got != "rt/0=4 rt/1=4" {
		t.Errorf("after: %s", got)
	}
}

// plainOf is a fragment as it reads: tags gone, runs of space one.
func plainOf(h string) string {
	return spaceBeforeStop.ReplaceAllString(strings.Join(strings.Fields(html.UnescapeString(tagRe.ReplaceAllString(h, " "))), " "), "$1")
}

var spaceBeforeStop = regexp.MustCompile(` ([.,;)])`)

func TestOffsetsTabOfAnEmptyGroupIsTheResetFormWithItsPreview(t *testing.T) {
	c, _ := resetFixture(t)
	page := req(t, c, "GET", "/_console/groups/g?tab=offsets", nil).Body.String()
	for _, want := range []string{`id="reset-form"`, `name="to" value="earliest"`, `name="to" value="latest"`, `name="to" value="timestamp"`, `name="to" value="offsets"`,
		`name="at"`, `<option value="rt" selected>rt (2 partitions)</option>`, `id="reset-count">2 partitions</span>`, `id="reset-preview"`, `hx-post="/_console/groups/g/reset/preview"`} {
		if !strings.Contains(page, want) {
			t.Errorf("the Offsets tab lacks %s", want)
		}
	}
	facts := plainOf(regexp.MustCompile(`(?s)id="group-facts">(.*?)</div>`).FindStringSubmatch(page)[1])
	if want := "No members; offsets kept on 2 partitions of one topic, 7 records behind. Resetting is allowed because the group is Empty."; facts != want {
		t.Errorf("facts %q, want %q", facts, want)
	}
	// The preview is the table of the plan: 7 and 2 of 10 and 6 move to 0, and the lag after is the whole log.
	for _, want := range []string{"7 → 0", "2 → 0", "10 1", "6 Total", "16"} {
		if !strings.Contains(plainOf(page), want) {
			t.Errorf("the preview lacks %q", want)
		}
	}
	if !strings.Contains(plainOf(page), "Going to earliest makes the group re-read 16 records instead of 7.") {
		t.Errorf("the sentence under the preview: %s", plainOf(page))
	}
}

// Gr-07: a group with members is refused, with the way out, and nothing is sent.
func TestResetOfAGroupWithMembersIsRefusedWithTheWayOut(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	topicWith(t, c, "t", 1, 5)
	commitFor(t, addr, "busy", "t", map[int32]int64{0: 1})
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("depot-router"), kgo.ConsumerGroup("busy"), kgo.ConsumeTopics("t"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	cl.PollRecords(ctx, 1)
	cancel()

	page := req(t, c, "GET", "/_console/groups/busy?tab=offsets", nil).Body.String()
	text := plainOf(page)
	for _, want := range []string{"Refused", "The group has 1 active member. Offsets can only be reset or changed while a group has no members.", "What to do",
		"Stop the 1 consumer that uses this group", "depot-router", "UNKNOWN_MEMBER_ID", "Show the 1 member", "Watch the timeline"} {
		if !strings.Contains(text, want) {
			t.Errorf("the refusal lacks %q", want)
		}
	}
	if strings.Contains(page, `id="reset-form"`) || strings.Contains(page, `data-refusal-name=`) {
		t.Error("a refused group offers the form, or the refusal names a Kafka error it was not given")
	}
	for path, form := range map[string]url.Values{"/reset": {"to": {"earliest"}}, "/reset/preview": {"to": {"latest"}}} {
		rec := req(t, c, "POST", "/_console/groups/busy"+path, form)
		if rec.Code != 400 || rec.Header().Get("X-Doze-Refusal") != "reset" || rec.Header().Get("HX-Retarget") != "#reset-result" {
			t.Errorf("%s: %d %v", path, rec.Code, rec.Header())
		}
		if got := plainOf(rec.Body.String()); !strings.Contains(got, "What to do") || !strings.Contains(got, "active member") {
			t.Errorf("%s: %s", path, got)
		}
	}
	if got := groupOffsetsText(t, c, "busy"); got != "t/0=1" {
		t.Errorf("a refused reset moved the offsets: %s", got)
	}
}

// GRP-15: the range check is the console's rule, in the Refused kind, naming no Kafka error.
func TestSettingAnOffsetOutsideTheLogIsRefusedByTheConsolesRule(t *testing.T) {
	c, _ := resetFixture(t)
	for _, bad := range []string{"11", "-1"} {
		rec := req(t, c, "POST", "/_console/groups/g/offset", url.Values{"topic": {"rt"}, "partition": {"0"}, "offset": {bad}})
		body := rec.Body.String()
		if rec.Code != 400 || !strings.Contains(body, `data-refusal-kind="refused"`) || strings.Contains(body, "data-refusal-name=") {
			t.Errorf("offset %s: %d %s", bad, rec.Code, body)
		}
	}
	rec := req(t, c, "POST", "/_console/groups/g/offset", url.Values{"topic": {"rt"}, "partition": {"0"}, "offset": {"11"}})
	for _, want := range []string{"Offset 11 is outside partition 0 of rt.", "first retained record (0)", "end of the log (10"} {
		if !strings.Contains(rec.Body.String(), want) {
			t.Errorf("the refusal lacks %q: %s", want, rec.Body)
		}
	}
}
