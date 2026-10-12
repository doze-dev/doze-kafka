package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// What is left of a group's committed offsets, and of when it became empty, once the
// group is deleted or has nothing, and its id is used again (GRP-19).

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "offsets-retention-recreate", Run: offsetsRetentionRecreate,
			KafkaEnv: []string{"KAFKA_OFFSETS_RETENTION_MINUTES=1", "KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=1000"},
			Speed:    10,
			Replay:   func(cfg *config.Config) { cfg.OffsetsRetention = time.Minute / 10 },
			Known:    offsetsRecreateKnown(),
		},
	)
}

func offsetsRecreateKnown() []Known {
	return knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "consumer-join-g-c-del", "consumer-join-g-c-late", "consumer-join-g-c-none")
}

func deleteGroupRequest(group string) *kmsg.DeleteGroupsRequest {
	r := kmsg.NewPtrDeleteGroupsRequest()
	r.Groups = []string{group}
	return r
}

func offsetDeleteRequest(group, topic string) *kmsg.OffsetDeleteRequest {
	r := kmsg.NewPtrOffsetDeleteRequest()
	r.Group = group
	t := kmsg.NewOffsetDeleteRequestTopic()
	t.Topic = topic
	p := kmsg.NewOffsetDeleteRequestTopicPartition()
	p.Partition = 0
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	return r
}

// offsetsRetentionRecreate is what is left of "when this group became empty" once the
// group is gone and its id is used again. Every group has a member that commits ret-a at
// 0 and leaves at 0, so each is empty since 0 and would lose its offsets at 60. At 40:
//
//	g-del           DeleteGroups, then a commit with no member.
//	g-c-del         the same, of a consumer-protocol group.
//	g-offdel        OffsetDelete of its only offset, then a commit with no member at once.
//	g-offdel-late   OffsetDelete of its only offset, and the commit five seconds later.
//	g-rejoin        a member joins again and leaves again, committing nothing.
//
// And two groups that show which clock an offset is on when nobody is left:
//
//	g-c-late        consumer protocol; its member commits at 0 and leaves at 40.
//	g-two           never had a member: a commit of ret-a at 0 and of ret-b at 40.
//
// And two that never commit anything, for when a group with no members and no offsets
// stops being listed (the broker checks every second):
//
//	g-none          classic; its member joins and leaves at 0.
//	g-c-none        consumer protocol; the same.
//
// Read at 75 and at 115; listed at 45 as well.
func offsetsRetentionRecreate(e *Env) {
	e.Do("create", createTopics(false, newTopic("ret-a", 1, 1), newTopic("ret-b", 1, 1)), 7)
	groups := []string{"g-del", "g-c-del", "g-offdel", "g-offdel-late", "g-rejoin", "g-c-late", "g-two"}
	e.findCoordinators(append([]string{"g-none", "g-c-none"}, groups...)...)

	classic := func(label, group string) (string, int32) {
		spec := joinSpec{group: group, metadata: consumerSubscription("ret-a")}
		first := e.join(label+"join-needs-id-"+group, spec)
		spec.member = first.MemberID
		j := e.join(label+"join-"+group, spec)
		e.Do(label+"sync-"+group, classicSyncRequest(group, spec.member, "", j.Generation, "range", consumerAssignment("ret-a", 0)), 5)
		return spec.member, j.Generation
	}
	// The clock starts here, and what must be gone by 75 because of a commit at 0 is
	// committed first: a classic group's first join waits for its initial rebalance
	// delay, which the recorded broker has at zero and a replayed one may not.
	start := time.Now()
	h := e.consumerHeartbeat("consumer-join-g-c-del", "g-c-del", "", 0, "ret-a")
	e.consumerCommit("commit-g-c-del", "g-c-del", deref(h.MemberID), h.MemberEpoch, "ret-a", 50)
	e.consumerHeartbeat("leave-g-c-del", "g-c-del", deref(h.MemberID), -1, "ret-a")
	late := e.consumerHeartbeat("consumer-join-g-c-late", "g-c-late", "", 0, "ret-a")
	lateID := deref(late.MemberID)
	e.consumerCommit("commit-g-c-late", "g-c-late", lateID, late.MemberEpoch, "ret-a", 60)
	e.commitOne("commit-g-two-a", "g-two", "", -1, "ret-a", 70)
	cnone := e.consumerHeartbeat("consumer-join-g-c-none", "g-c-none", "", 0, "ret-a")
	e.consumerHeartbeat("leave-g-c-none", "g-c-none", deref(cnone.MemberID), -1, "ret-a")
	for i, g := range []string{"g-del", "g-offdel", "g-offdel-late", "g-rejoin"} {
		m, gen := classic("", g)
		e.commitOne("commit-"+g, g, m, gen, "ret-a", int64(10*(i+1)))
		e.classicLeave("leave-"+g, g, m)
	}
	none, _ := classic("", "g-none")
	e.classicLeave("leave-g-none", "g-none", none)

	// The member that stays until 40 heartbeats while the script waits.
	until := func(at time.Duration) {
		for time.Duration(float64(time.Since(start))*e.speed) < at {
			if lateID != "" {
				hb := kmsg.NewPtrConsumerGroupHeartbeatRequest()
				hb.Group, hb.MemberID, hb.MemberEpoch = "g-c-late", lateID, late.MemberEpoch
				e.Quiet(hb, 0)
			}
			e.Wait(time.Second)
		}
	}
	checkpoint := func(label string) {
		for _, g := range groups {
			e.fetchTopics(label+"-fetch-"+g, g, "ret-a", "ret-b")
		}
		e.listGroups(label + "-list")
	}

	until(40 * time.Second)
	e.consumerHeartbeat("leave-g-c-late-at-40", "g-c-late", lateID, -1, "ret-a")
	lateID = ""
	e.commitOne("commit-g-two-b-at-40", "g-two", "", -1, "ret-b", 71)
	e.Do("delete-g-del", deleteGroupRequest("g-del"), 2)
	e.commitOne("commit-g-del-at-40", "g-del", "", -1, "ret-a", 11)
	e.Do("delete-g-c-del", deleteGroupRequest("g-c-del"), 2)
	e.commitOne("commit-g-c-del-at-40", "g-c-del", "", -1, "ret-a", 51)
	e.Do("offset-delete-g-offdel", offsetDeleteRequest("g-offdel", "ret-a"), 0)
	e.commitOne("commit-g-offdel-at-40", "g-offdel", "", -1, "ret-a", 21)
	e.Do("offset-delete-g-offdel-late", offsetDeleteRequest("g-offdel-late", "ret-a"), 0)
	m, _ := classic("again-", "g-rejoin")
	e.classicLeave("again-leave-g-rejoin", "g-rejoin", m)

	until(45 * time.Second)
	e.listGroups("t45-list")
	e.describeClassic("t45-describe-g-offdel-late", "g-offdel-late")
	e.describeClassic("t45-describe-g-none", "g-none")
	e.describeConsumer("t45-describe-g-c-none", "g-c-none")
	e.commitOne("commit-g-offdel-late-at-45", "g-offdel-late", "", -1, "ret-a", 31)

	until(75 * time.Second)
	checkpoint("t75")
	until(115 * time.Second)
	checkpoint("t115")
}
