package broker

import (
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// recordedLiveness is a livenessRecorder that remembers what it was told.
type recordedLiveness struct {
	consumerOnlyFor []string
	emptiedFor      []string
}

func (r *recordedLiveness) joining(string, bool, time.Duration, string) error { return nil }
func (r *recordedLiveness) emptied(group string, _ bool, _ groupKind) {
	r.emptiedFor = append(r.emptiedFor, group)
}
func (r *recordedLiveness) consumerOnly(group string, _ time.Duration) {
	r.consumerOnlyFor = append(r.consumerOnlyFor, group)
}

func removalGroup(m *cg848Manager, classic []string, consumers []string) *cg848Group {
	g := &cg848Group{id: "g", members: map[string]*cg848Member{}}
	for _, id := range classic {
		g.members[id] = &cg848Member{id: id, classic: &classicSeat{}, target: tpSet{}, assigned: tpSet{}}
	}
	for _, id := range consumers {
		g.members[id] = &cg848Member{id: id, target: tpSet{}, assigned: tpSet{}}
	}
	m.groups["g"] = g
	return g
}

// The offsets retention is told when the last classic member of a consumer group goes
// and others remain: and only then.
func TestKIP848RemovingTheLastClassicMemberTellsTheRetention(t *testing.T) {
	for _, tc := range []struct {
		what      string
		classic   []string
		consumers []string
		remove    string
		told      bool
		emptied   bool
	}{
		{"the last classic member, a consumer member remains", []string{"c"}, []string{"a"}, "c", true, false},
		{"one of two classic members", []string{"c", "d"}, []string{"a"}, "c", false, false},
		{"a consumer member", []string{"c"}, []string{"a", "b"}, "a", false, false},
		{"the last member of all, a classic one", []string{"c"}, nil, "c", false, true},
		{"an id that is not a member", []string{"c"}, []string{"a"}, "nobody", false, false},
	} {
		m := new848Manager(t)
		live := &recordedLiveness{}
		m.liveness = live
		g := removalGroup(m, tc.classic, tc.consumers)
		m.mu.Lock()
		m.removeMember(g, tc.remove)
		m.mu.Unlock()
		if told := len(live.consumerOnlyFor) == 1; told != tc.told {
			t.Errorf("%s: told the retention the classic members are gone: %v, want %v", tc.what, live.consumerOnlyFor, tc.told)
		}
		if emptied := len(live.emptiedFor) == 1; emptied != tc.emptied {
			t.Errorf("%s: told the retention the group is empty: %v, want %v", tc.what, live.emptiedFor, tc.emptied)
		}
	}
	// With nobody keeping count the same removal is just a removal.
	m := new848Manager(t)
	g := removalGroup(m, []string{"c"}, []string{"a"})
	m.mu.Lock()
	m.removeMember(g, "c")
	m.mu.Unlock()
	if g.members["c"] != nil || g.members["a"] == nil {
		t.Errorf("members after the removal: %v", g.members)
	}
}

func TestKIP848HasClassicSeesTheClassicMembersOnly(t *testing.T) {
	m := new848Manager(t)
	if removalGroup(m, nil, []string{"a", "b"}).hasClassic() {
		t.Error("a group of consumer-protocol members has a classic member")
	}
	if !removalGroup(m, []string{"c"}, []string{"a"}).hasClassic() {
		t.Error("a group with a classic member has none")
	}
}

// A classic member that heartbeats through the consumer API is kept alive and changes nothing,
// at the epoch it has (recorded: cm-classic-member-sends-consumer-heartbeat); at another it is fenced.
func TestKIP848AClassicMemberHeartbeatingByTheConsumerAPI(t *testing.T) {
	m := new848Manager(t)
	g := removalGroup(m, []string{"c"}, nil)
	g.members["c"].epoch = 4
	res := hb848(m, "g", "c", 4, nil)
	if res.errorCode != 0 || res.memberID != "c" || res.memberEpoch != 4 || res.assignment != nil {
		t.Errorf("at its epoch: %+v, want a keepalive", res)
	}
	res = hb848(m, "g", "c", 3, nil)
	if res.errorCode != kerr.FencedMemberEpoch.Code {
		t.Errorf("at another epoch: %+v, want FENCED_MEMBER_EPOCH", res)
	}
}

// A classic member whose JoinGroup named no session is given the coordinator's.
func TestKIP848ASessionIsTheClassicSeatsOnlyWhenItHasOne(t *testing.T) {
	m := new848Manager(t)
	g := removalGroup(m, []string{"c"}, nil)
	mem := g.members["c"]
	m.mu.Lock()
	m.resetSession(g, mem)
	m.mu.Unlock()
	time.Sleep(100 * time.Millisecond)
	m.mu.Lock()
	still := g.members["c"] != nil
	mem.session.stop()
	m.mu.Unlock()
	if !still {
		t.Error("a classic member with no session of its own was expired at once")
	}
}

// The names a member brings are bounded: a member id of the longest length is taken, one byte more is not.
func TestKIP848AMemberIDOfTheLongestLengthIsTaken(t *testing.T) {
	m := new848Manager(t)
	with848Topic(t, m, "t", 1)
	ok := hb848(m, "g", strings.Repeat("m", maxMemberIDBytes), 0, nil)
	if ok.errorCode != 0 {
		t.Errorf("a member id of %d bytes: %+v", maxMemberIDBytes, ok)
	}
	long := hb848(m, "g", strings.Repeat("m", maxMemberIDBytes+1), 0, nil)
	if long.errorCode != kerr.InvalidRequest.Code {
		t.Errorf("a member id of %d bytes: %+v, want INVALID_REQUEST", maxMemberIDBytes+1, long)
	}
}

// A set of partitions is listed by topic id, partitions ascending.
func TestKIP848PartitionSetsAreListedByTopicIDThenPartition(t *testing.T) {
	s := tpSet{}
	for _, p := range []tp{{meta.TopicID{2}, 5}, {meta.TopicID{1}, 3}, {meta.TopicID{2}, 1}, {meta.TopicID{1}, 0}} {
		s[p] = struct{}{}
	}
	got := s.byTopic()
	if len(got) != 2 || got[0].topic != (meta.TopicID{1}) || got[1].topic != (meta.TopicID{2}) {
		t.Fatalf("topics out of order: %+v", got)
	}
	if got[0].parts[0] != 0 || got[0].parts[1] != 3 || got[1].parts[0] != 1 || got[1].parts[1] != 5 {
		t.Errorf("partitions out of order: %+v", got)
	}
}
