package broker

import (
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// FuzzGroupSequence drives the KIP-848 coordinator with a byte string as a
// sequence of calls (join, poll, report, leave, a stale epoch, a topic change)
// from four members, and checks after every call the one property a consumer
// group exists for: a partition is never owned by two members at once.
//
// The coordinator parses nothing itself, so this is its fuzz target: the bytes are
// the schedule, not a wire message.
func FuzzGroupSequence(f *testing.F) {
	st, err := meta.Open(filepath.Join(f.TempDir(), "meta.db"))
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(func() { st.Close() })
	ta, err := st.CreateTopic("a", 3, nil)
	if err != nil {
		f.Fatal(err)
	}
	tb, err := st.CreateTopic("b", 2, nil)
	if err != nil {
		f.Fatal(err)
	}
	topics := map[string]meta.TopicID{"a": ta.ID, "b": tb.ID}

	// Two members join and converge; one leaves; a stale heartbeat; a topic change.
	f.Add([]byte{0, 1, 4, 3, 1, 0, 5, 0, 1, 0, 1, 0})
	f.Add([]byte{0, 3, 4, 3, 8, 3, 12, 3, 1, 0, 5, 0, 16, 0, 4, 0})
	f.Add([]byte{0, 1, 2, 0, 3, 0, 20, 0, 0, 1})

	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 400 {
			ops = ops[:400]
		}
		m := newCG848Manager(st, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Hour, 0, nil)
		defer m.stop()

		type who struct {
			id    string
			epoch int32
			owned map[tp]struct{} // what the member last received and applied.
		}
		members := make([]*who, 4)
		for i := range members {
			members[i] = &who{id: string(rune('A' + i)), owned: map[tp]struct{}{}}
		}
		request := func(w *who, epoch int32) *kmsg.ConsumerGroupHeartbeatRequest {
			r := kmsg.NewPtrConsumerGroupHeartbeatRequest()
			r.SetVersion(1)
			r.Group, r.MemberID, r.MemberEpoch = "g", w.id, epoch
			r.RebalanceTimeoutMillis = 60_000
			return r
		}
		owned := func(w *who) []kmsg.ConsumerGroupHeartbeatRequestTopic {
			by := map[meta.TopicID][]int32{}
			for p := range w.owned {
				by[p.topic] = append(by[p.topic], p.part)
			}
			out := []kmsg.ConsumerGroupHeartbeatRequestTopic{}
			for id, ps := range by {
				rt := kmsg.NewConsumerGroupHeartbeatRequestTopic()
				rt.TopicID, rt.Partitions = id, ps
				out = append(out, rt)
			}
			return out
		}
		absorb := func(w *who, res cg848Result) {
			if res.errorCode != 0 {
				return
			}
			w.epoch = res.memberEpoch
			if res.assignment != nil {
				w.owned = map[tp]struct{}{}
				for _, at := range res.assignment.Topics {
					for _, p := range at.Partitions {
						w.owned[tp{meta.TopicID(at.TopicID), p}] = struct{}{}
					}
				}
			}
		}

		for i := 0; i+1 < len(ops); i += 2 {
			w := members[ops[i]%4]
			arg := ops[i+1]
			var res cg848Result
			switch (ops[i] >> 2) % 7 {
			case 0: // join
				r := request(w, 0)
				r.SubscribedTopicNames = []string{}
				if arg&1 != 0 {
					r.SubscribedTopicNames = append(r.SubscribedTopicNames, "a")
				}
				if arg&2 != 0 {
					r.SubscribedTopicNames = append(r.SubscribedTopicNames, "b")
				}
				if arg&4 != 0 {
					r.SubscribedTopicNames = append(r.SubscribedTopicNames, "missing")
				}
				if arg&8 != 0 {
					r.ServerAssignor = kmsg.StringPtr("range")
				}
				w.owned = map[tp]struct{}{}
				res = m.heartbeat(r, clientRef{clientID: "fuzz"})
				absorb(w, res)
			case 1: // poll, reporting what it owns
				r := request(w, w.epoch)
				r.Topics = owned(w)
				res = m.heartbeat(r, clientRef{clientID: "fuzz"})
				absorb(w, res)
			case 2: // poll, no report
				res = m.heartbeat(request(w, w.epoch), clientRef{clientID: "fuzz"})
				absorb(w, res)
			case 3: // poll, claiming to own nothing
				r := request(w, w.epoch)
				r.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
				w.owned = map[tp]struct{}{}
				res = m.heartbeat(r, clientRef{clientID: "fuzz"})
				absorb(w, res)
			case 4: // leave
				res = m.heartbeat(request(w, -1), clientRef{clientID: "fuzz"})
				w.epoch, w.owned = 0, map[tp]struct{}{}
			case 5: // a stale or invented epoch
				res = m.heartbeat(request(w, w.epoch+int32(arg%5)-2), clientRef{clientID: "fuzz"})
			case 6: // a topic changed
				m.topicChanged([]string{"a", "b", "missing"}[arg%3])
			}

			// The property, under the lock the timers would take.
			m.mu.Lock()
			holder := map[tp]string{}
			for _, g := range m.groups {
				for id, mem := range g.members {
					if mem.epoch > g.epoch {
						m.mu.Unlock()
						t.Fatalf("after op %d: member %s is at epoch %d, ahead of its group's %d", i/2, id, mem.epoch, g.epoch)
					}
					for p := range mem.assigned {
						if other, dup := holder[p]; dup {
							m.mu.Unlock()
							t.Fatalf("after op %d (%v): partition %v of %v is assigned to both %s and %s", i/2, ops[:i+2], p.part, p.topic, other, id)
						}
						holder[p] = id
						if p.topic != topics["a"] && p.topic != topics["b"] {
							m.mu.Unlock()
							t.Fatalf("after op %d: %s was assigned a partition of a topic that does not exist", i/2, id)
						}
					}
				}
			}
			m.mu.Unlock()
		}
	})
}
