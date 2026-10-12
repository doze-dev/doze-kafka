package traffic

import (
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// The page's one search field is matched by the recorder, so it can count what matched
// without handing the rows over. It finds a row by any of the things a row names, in
// any case.
func TestTheSearchFieldMatchesAnythingARowNames(t *testing.T) {
	r := New(50, nil)
	r.Request(obs(1, "Billing-App", kmsg.Produce, 0, func(o *protocol.Observation) {
		o.Remote, o.Principal, o.Software = "10.1.2.3:4000", "User:alice", "apache-kafka-java 3.7.0"
		o.Topics, o.TxnID, o.Kind = []string{"Orders", "payments"}, "checkout-1", "produce"
	}))
	r.Request(obs(2, "audit", kmsg.JoinGroup, time.Second, func(o *protocol.Observation) {
		o.Group, o.ErrorCode, o.Class = "Workers", 25, protocol.ClassRefused
	}))
	r.Request(obs(2, "audit", kmsg.Fetch, 2*time.Second, func(o *protocol.Observation) { o.Kind = "fetch" }))
	r.Request(obs(3, "größe", kmsg.Metadata, 3*time.Second))

	for q, want := range map[string]int{
		"billing":         1, // the client id, whatever its case.
		"KAFKA-JAVA":      1, // the software.
		"10.1.2":          1, // the address.
		"user:alice":      1, // the principal.
		"produce":         1, // the request.
		"payments":        1, // a topic that is not the first.
		"checkout":        1, // the transactional id.
		"workers":         2, // the group, and the fetch of its member.
		"unknown_member":  1, // the error.
		"GRÖSSE":          0, // not this spelling,
		"GRÖßE":           1, // but this one, which lower-cases to the client's.
		"audit":           2,
		"nothing-has-it":  0,
		"":                4,
		"e":               4,
		"127.0.0.1:50000": 3,
	} {
		got := r.Query(Query{Filter: Filter{Any: q}})
		if got.Total != want || len(got.Rows) != want {
			t.Errorf("%q matches %d rows, want %d", q, got.Total, want)
		}
		if got.Held != 4 {
			t.Errorf("%q: held %d, want 4 whatever the filter", q, got.Held)
		}
	}
	// It narrows together with the other fields, and the paused page's count uses it too.
	if got := r.Query(Query{Filter: Filter{Any: "audit", Kind: "fetch"}}); got.Total != 1 {
		t.Errorf("audit fetches: %d", got.Total)
	}
	if n, _ := r.NewSince(Filter{Any: "AUDIT"}, t0); n != 2 {
		t.Errorf("new audit requests since the start: %d, want 2", n)
	}
}

func TestContainsFold(t *testing.T) {
	for _, c := range []struct {
		s, lower string
		want     bool
	}{
		{"JoinGroup", "group", true}, {"JoinGroup", "joingroup", true}, {"JoinGroup", "joingroups", false},
		{"", "", true}, {"", "a", false}, {"abc", "", true}, {"aab", "ab", true}, {"ABAB", "bab", true},
		{"Größe", "größe", true}, {"GRÖSSE", "größe", false}, {"K-kelvin", "k-kelvin", true},
	} {
		if got := containsFold(c.s, c.lower); got != c.want {
			t.Errorf("containsFold(%q, %q) = %v, want %v", c.s, c.lower, got, c.want)
		}
	}
}
