package conformance

import (
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Four answers about group requests: DeleteGroups of a group with a member and of one
// that does not exist, OffsetDelete after the topic is gone, a JoinGroup with no member id
// at each version, and a member's JoinGroup sent again while the first is parked.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "group-requests-answers", Run: groupRequestsAnswers},
	)
}

func groupRequestsAnswers(e *Env) {
	e.Do("create", createTopics(false, newTopic("ga", 1, 1), newTopic("ga-gone", 1, 1)), 7)
	e.findCoordinators("ga-del", "ga-off", "ga-j0", "ga-j1", "ga-j2", "ga-j3", "ga-j4", "ga-park")
	sub := consumerSubscription("ga")

	// --- G1: DeleteGroups of a group that has a member and of one never used.
	m := e.join("del-join-needs-id", joinSpec{group: "ga-del", metadata: sub}).MemberID
	e.join("del-join", joinSpec{group: "ga-del", member: m, metadata: sub})
	for _, v := range []int16{0, 1, 2} {
		r := kmsg.NewPtrDeleteGroupsRequest()
		r.Groups = []string{"ga-del", "ga-never"}
		e.Do("del-groups-v"+string(rune('0'+v)), r, v)
	}

	// --- G2: an offset of a topic that is deleted.
	e.commitOne("off-commit", "ga-off", "", -1, "ga-gone", 5)
	e.commitOne("off-commit-kept", "ga-off", "", -1, "ga", 6) // the group outlives the topic.
	e.fetchTopics("off-fetch", "ga-off", "ga-gone")
	dt := kmsg.NewPtrDeleteTopicsRequest()
	dt.TopicNames = []string{"ga-gone"}
	dt.TimeoutMillis = 15000
	e.Do("off-delete-topic", dt, 3)
	md := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = str("ga-gone")
	md.Topics = append(md.Topics, mt)
	md.AllowAutoTopicCreation = false
	e.Await("the topic is gone", md, 9, func(r kmsg.Response) bool {
		t := r.(*kmsg.MetadataResponse).Topics
		return len(t) == 1 && t[0].ErrorCode != 0
	})
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fg := kmsg.NewOffsetFetchRequestGroup()
	fg.Group = "ga-off"
	ft := kmsg.NewOffsetFetchRequestGroupTopic()
	ft.Topic, ft.Partitions = "ga-gone", []int32{0}
	fg.Topics = append(fg.Topics, ft)
	fetch.Groups = append(fetch.Groups, fg)
	e.Await("the offset of the deleted topic is gone", fetch, 8, func(r kmsg.Response) bool {
		return r.(*kmsg.OffsetFetchResponse).Groups[0].Topics[0].Partitions[0].Offset == -1
	})
	e.Do("off-offset-delete", offsetDeleteRequest("ga-off", "ga-gone"), 0)
	e.fetchTopics("off-fetch-after", "ga-off", "ga-gone")
	e.Do("off-describe", describeGroupsRequest("ga-off"), 5)

	// --- G3: a first JoinGroup with no member id, at each version.
	for v := int16(0); v <= 4; v++ {
		g := "ga-j" + string(rune('0'+v))
		e.Do("join-v"+string(rune('0'+v)), classicJoinRequest(joinSpec{group: g, metadata: sub}), v)
		if v < 4 {
			e.Do("join-v"+string(rune('0'+v))+"-describe", describeGroupsRequest(g), 5)
		}
	}

	// --- G4: a member's JoinGroup sent again while its first is parked.
	e.Open("b", "", "")
	e.Open("c", "", "")
	a := e.join("park-a-join-needs-id", joinSpec{group: "ga-park", metadata: sub}).MemberID
	aj := e.join("park-a-join", joinSpec{group: "ga-park", member: a, metadata: sub})
	e.Do("park-a-sync", classicSyncRequest("ga-park", a, "", aj.Generation, "range", consumerAssignment("ga", 0)), 5)
	e.Use("b")
	b := e.join("park-b-join-needs-id", joinSpec{group: "ga-park", metadata: sub}).MemberID
	first := e.Start("park-b-join-parked", classicJoinRequest(joinSpec{group: "ga-park", member: b, metadata: sub}), 7)
	e.Wait(time.Second)
	e.Use("c")
	second := e.Start("park-b-join-again", classicJoinRequest(joinSpec{group: "ga-park", member: b, metadata: sub}), 7)
	e.Wait(time.Second)
	e.Use("")
	e.join("park-a-rejoin", joinSpec{group: "ga-park", member: a, metadata: sub})
	// Kafka drops the parked request's callback: it is never answered.
	if e.Answered(first, 10*time.Second) {
		e.Finish(first)
	}
	e.Use("c")
	e.Finish(second)
	e.Use("")
}

func describeGroupsRequest(groups ...string) *kmsg.DescribeGroupsRequest {
	d := kmsg.NewPtrDescribeGroupsRequest()
	d.Groups = groups
	return d
}
