package traffic

import (
	"fmt"
	"runtime"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// Everything a client says about itself is kept, so none of it may be kept whole: a
// 900 KiB join reason in each of 2,000 rows was 1.8 GiB (TRF-1).
func TestRowFieldsAreClipped(t *testing.T) {
	r := New(10, nil)
	huge := strings.Repeat("é", 450_000) // 900 KB of two-byte runes, so a cut in the middle of one is visible.
	var topics []string
	for i := 0; i < 50; i++ {
		topics = append(topics, fmt.Sprintf("topic-%02d-%s", i, strings.Repeat("x", 400)))
	}
	r.ConnOpened(1, huge)
	r.Request(obs(1, huge, kmsg.JoinGroup, 0, func(o *protocol.Observation) {
		o.Remote, o.Principal, o.Software = huge, huge, huge
		o.Group, o.Detail, o.Topics = huge, huge, topics
	}))
	evs := r.Events(Filter{}, 0)
	if len(evs) != 1 {
		t.Fatalf("%d rows, want 1", len(evs))
	}
	e := evs[0]
	for name, s := range map[string]string{"Detail": e.Detail, "ClientID": e.ClientID, "Software": e.Software, "Group": e.Group, "Principal": e.Principal, "Remote": e.Remote} {
		limit := 128
		switch name {
		case "Detail":
			limit = 256
		case "Remote":
			limit = 64
		}
		if len(s) > limit {
			t.Errorf("%s kept %d bytes, want at most %d", name, len(s), limit)
		}
		if !utf8.ValidString(s) {
			t.Errorf("%s was cut in the middle of a character", name)
		}
	}
	// 50 names of 400 bytes: each is clipped to 249, and four fit in 1,024.
	if len(e.Topics) != 4 || e.TopicsMore != 46 {
		t.Errorf("topics kept %d with %d more, want 4 and 46", len(e.Topics), e.TopicsMore)
	}
	for _, tp := range e.Topics {
		if len(tp) > 249 {
			t.Errorf("a topic name of %d bytes kept (a topic is at most 249)", len(tp))
		}
	}
	cs := r.Clients()
	if len(cs) != 1 {
		t.Fatalf("%d clients, want 1", len(cs))
	}
	if c := cs[0]; len(c.ClientID) > 128 || len(c.Software) > 128 || len(c.Remote) > 64 || len(c.Principal) > 128 {
		t.Errorf("the client was kept whole: client id %d, software %d, remote %d, principal %d", len(c.ClientID), len(c.Software), len(c.Remote), len(c.Principal))
	}
	// The coordinators' events carry a client id in a detail, and a member id.
	r.GroupEvent("g", "joined", huge, 1, huge, 0, "")
	ge := r.GroupEvents("g", 0)
	if len(ge) != 1 || len(ge[0].Detail) > 256 || len(ge[0].Member) > 128 {
		t.Errorf("a group event was kept whole: %d events", len(ge))
	}
}

// The bound is on what is held, in total: a ring filled with maximal rows (every
// string at its limit, in distinct strings so that nothing is shared) from a hundred
// maximal clients, and two thousand group events, stays under 8 MiB. That is the
// number the lightness budget takes. A connection costs about two kilobytes on top
// (its counters and its strings), which the number of connections sets, not the
// recorder.
func TestRecorderMemoryWithMaximalInput(t *testing.T) {
	const rows, clients = 2000, 100
	big := func(i int) string { return fmt.Sprintf("%06d", i) + strings.Repeat("é", 8<<10) }
	heap := func() uint64 {
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	r := New(rows, nil)
	before := heap()
	for c := 0; c < clients; c++ {
		r.ConnOpened(uint64(c), big(c))
	}
	for i := 0; i < rows; i++ {
		var topics []string
		for j := 0; j < 40; j++ {
			topics = append(topics, fmt.Sprintf("%02d%06d", j, i)+strings.Repeat("t", 200))
		}
		h, who := big(i), big(i%clients)
		r.Request(obs(uint64(i%clients), who, kmsg.JoinGroup, time.Duration(i)*time.Second, func(o *protocol.Observation) {
			o.Remote, o.Principal, o.Software, o.Group, o.Detail, o.Topics = who, who, who, h, h, topics
		}))
		r.GroupEvent(fmt.Sprintf("g%d", i%50), "joined", h, 1, h, 3, h)
	}
	after := heap()
	runtime.KeepAlive(r)
	if got := int64(after) - int64(before); got > 8<<20 {
		t.Errorf("2,000 maximal rows hold %d bytes, want under 8 MiB", got)
	}
}
