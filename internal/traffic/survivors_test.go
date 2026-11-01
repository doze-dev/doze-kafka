package traffic

import (
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func TestClipKeepsAStringThatExactlyFits(t *testing.T) {
	if got := clip("abcde", 5); got != "abcde" {
		t.Errorf("got %q", got)
	}
	if got := clip("abcdef", 5); got != "ab…" {
		t.Errorf("got %q", got)
	}
	// Cutting at the start of the string, which is not at a character boundary, leaves
	// only the ellipsis.
	if got := clip("\x80\x80\x80\x80", 3); got != "…" {
		t.Errorf("got %q", got)
	}
}

func TestTopicsFillingTheBudgetExactlyAreAllKept(t *testing.T) {
	var topics []string
	for i := 0; i < 8; i++ {
		topics = append(topics, strings.Repeat(string(rune('a'+i)), MaxTopicBytes/8))
	}
	kept, more := clipTopics(topics)
	if len(kept) != 8 || more != 0 {
		t.Errorf("kept %d, left out %d; want 8 and 0", len(kept), more)
	}
	kept, more = clipTopics(append(topics, "x"))
	if len(kept) != 8 || more != 1 {
		t.Errorf("kept %d, left out %d; want 8 and 1", len(kept), more)
	}
}

func TestContainsFoldFoldsCaseAtTheEdgesOfTheAlphabet(t *testing.T) {
	for _, c := range []struct {
		s, lower string
		want     bool
	}{
		{"Z", "z", true},
		{"A", "a", true},
		{"xyZ", "yz", true},
		{"CAFÉ", "café", true},
		{"naïve", "ï", true},
		{"É", "é", true}, // every byte non-ASCII.
		{"ÉÉ", "éé", true},
		{"a\x80", "\x80", false}, // not UTF-8: compared as text, where it is not there.
		{"abc", "d", false},
	} {
		if got := containsFold(c.s, c.lower); got != c.want {
			t.Errorf("containsFold(%q, %q) = %v, want %v", c.s, c.lower, got, c.want)
		}
	}
}

// A counter holds 65535 at most in a bucket and does not wrap round to nothing.
func TestABucketSaturatesInsteadOfWrapping(t *testing.T) {
	var c counters
	now := t0.Add(time.Hour)
	at := now.Add(-3 * time.Second)
	for i := 0; i < 0xffff+10; i++ {
		c.add(at, true)
	}
	if total, errs := c.sum(now, time.Minute); total != 0xffff || errs != 0xffff {
		t.Errorf("total %d errors %d, want 65535 each", total, errs)
	}
}

// A window longer than the ring is cut to the sixty whole buckets it holds.
func TestASpanIsAtMostSixtyBuckets(t *testing.T) {
	lo, hi, from, to := span(t0, time.Hour)
	if hi-lo != bucketCount-1 || to.Sub(from) != (bucketCount-1)*bucketWidth {
		t.Errorf("span of %d buckets (%v), want %d", hi-lo, to.Sub(from), bucketCount-1)
	}
	var c counters
	c.add(t0.Add(-time.Duration(bucketCount)*bucketWidth+time.Second), false) // 61 buckets back.
	if total, _ := c.sum(t0, time.Hour); total != 0 {
		t.Errorf("a request 61 buckets old is counted in a window of the last 60: %d", total)
	}
}

// Rows more than the limit are cut; a negative limit is no limit, as zero is.
func TestAQueryWithANegativeLimitReturnsEverything(t *testing.T) {
	r := New(10, nil)
	r.ConnOpened(1, "a")
	for i := 0; i < 3; i++ {
		r.Request(obs(1, "app", kmsg.Produce, time.Duration(i)*time.Second))
	}
	if res := r.Query(Query{Limit: -1}); len(res.Rows) != 3 || res.Total != 3 {
		t.Errorf("rows %d total %d, want 3", len(res.Rows), res.Total)
	}
	if res := r.Query(Query{Limit: 2}); len(res.Rows) != 2 || res.Total != 3 || !res.Rows[0].Time.After(res.Rows[1].Time) {
		t.Errorf("rows %d total %d", len(res.Rows), res.Total)
	}
}

// Who got a refusal, the one with most first.
func TestRefusalClientsAreMostFirst(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	bad := func(o *protocol.Observation) { o.ErrorCode, o.Class = 3, protocol.ClassRefused }
	n := 0
	for client, times := range map[string]int{"low": 1, "high": 3, "mid": 2} {
		for i := 0; i < times; i++ {
			n++
			r.Request(obs(1, client, kmsg.Produce, time.Duration(n)*time.Second, bad))
		}
	}
	got := r.Refusals()[0].Clients
	if len(got) != 3 || got[0] != (ClientCount{"high", 3}) || got[1] != (ClientCount{"mid", 2}) || got[2] != (ClientCount{"low", 1}) {
		t.Errorf("clients = %+v", got)
	}
}

// A connection remembers the last four groups it spoke to, and whether it asked for its
// telemetry subscription only if it did.
func TestAClientKeepsItsLastFourGroupsAndWhetherItAskedForTelemetry(t *testing.T) {
	r := New(50, nil)
	r.ConnOpened(1, "a")
	in := func(g string) func(*protocol.Observation) {
		return func(o *protocol.Observation) { o.Group, o.Kind = g, "groups" }
	}
	for i, g := range []string{"g1", "g2", "g3", "g4"} {
		r.Request(obs(1, "app", kmsg.Heartbeat, time.Duration(i)*time.Second, in(g)))
	}
	cs := r.Clients()
	if got := strings.Join(cs[0].Groups, ","); got != "g1,g2,g3,g4" {
		t.Errorf("groups = %s, want all four", got)
	}
	if cs[0].AskedTelemetry {
		t.Error("a client that asked for nothing is marked as having asked for telemetry")
	}
	r.Request(obs(1, "app", kmsg.Heartbeat, 5*time.Second, in("g5")))
	r.Request(obs(1, "app", kmsg.GetTelemetrySubscriptions, 6*time.Second))
	cs = r.Clients()
	if got := strings.Join(cs[0].Groups, ","); got != "g2,g3,g4,g5" {
		t.Errorf("groups = %s, want the last four", got)
	}
	if !cs[0].AskedTelemetry {
		t.Error("a client that asked for its telemetry subscription is not marked")
	}
}

// Identical requests exactly the collapse window apart are two rows; a nanosecond short
// of it, one.
func TestRepeatsFoldOnlyWithinTheCollapseWindow(t *testing.T) {
	for _, c := range []struct {
		gap  time.Duration
		rows int
	}{{collapseWindow - time.Nanosecond, 1}, {collapseWindow, 2}} {
		r := New(50, nil)
		r.ConnOpened(1, "a")
		r.Request(obs(1, "app", kmsg.Heartbeat, 0, idle("g")))
		r.Request(obs(1, "app", kmsg.Heartbeat, c.gap, idle("g")))
		if got := len(r.Events(Filter{}, 0)); got != c.rows {
			t.Errorf("a gap of %v: %d rows, want %d", c.gap, got, c.rows)
		}
	}
}
