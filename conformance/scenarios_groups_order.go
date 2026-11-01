package conformance

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// The order DescribeGroups answers several groups in.

func init() {
	Scenarios = append(Scenarios, Scenario{Name: "describe-groups-order", Run: describeGroupsOrder,
		Known: knownSteps(limHeartbeatInterval, "HeartbeatIntervalMillis", "dgo-n1-join", "dgo-n2-join", "dgo-n3-join")})
}

// describeGroupsOrder describes six groups, four with a member and two Kafka has never
// heard of, in several orders, once with a group named twice and once with an empty
// id; and asks the same of the other requests that name several groups:
// ConsumerGroupDescribe, OffsetFetch v8 and DeleteGroups.
func describeGroupsOrder(e *Env) {
	e.Do("create", createTopics(false, newTopic("dgo", 1, 1)), 7)
	live := []string{"dgo-a", "dgo-b", "dgo-c", "dgo-d"}
	e.findCoordinators(append(live, "dgo-x", "dgo-y")...)
	for _, g := range live {
		first := e.classicJoin(g+"-join-needs-id", g, "", "consumer", "dgo")
		j := e.classicJoin(g+"-join", g, first.MemberID, "consumer", "dgo")
		e.classicSync(g+"-sync", g, first.MemberID, j.Generation, "consumer", "dgo")
	}
	orders := map[string][]string{
		"forward":   {"dgo-a", "dgo-b", "dgo-c", "dgo-d", "dgo-x", "dgo-y"},
		"backward":  {"dgo-y", "dgo-x", "dgo-d", "dgo-c", "dgo-b", "dgo-a"},
		"mixed":     {"dgo-c", "dgo-x", "dgo-a", "dgo-d", "dgo-y", "dgo-b"},
		"twice":     {"dgo-b", "dgo-a", "dgo-b"},
		"two":       {"dgo-d", "dgo-a"},
		"two-again": {"dgo-a", "dgo-d"},
		"empty-id":  {"dgo-c", "", "dgo-x", "dgo-a"},
	}
	names := []string{"forward", "backward", "mixed", "twice", "two", "two-again", "empty-id"}
	for _, name := range names {
		r := kmsg.NewPtrDescribeGroupsRequest()
		r.Groups = orders[name]
		e.Do("describe-"+name, r, 5)
	}
	for _, g := range []string{"dgo-n1", "dgo-n2", "dgo-n3"} {
		e.consumerHeartbeat(g+"-join", g, "", 0, "dgo")
	}
	for _, name := range []string{"forward", "backward", "mixed"} {
		r := kmsg.NewPtrConsumerGroupDescribeRequest()
		r.Groups = append([]string{"dgo-n3", "dgo-n1", "dgo-n2"}, orders[name]...)
		e.Do("consumer-describe-"+name, r, 0)
	}
	for _, name := range []string{"forward", "mixed", "empty-id"} {
		r := kmsg.NewPtrOffsetFetchRequest()
		for _, g := range orders[name] {
			rg := kmsg.NewOffsetFetchRequestGroup()
			rg.Group = g
			r.Groups = append(r.Groups, rg)
		}
		e.Do("offset-fetch-"+name, r, 8)
	}
	for i, q := range [][]string{{"dgo-n3", "dgo-y"}, {"dgo-y", "dgo-n3"}, {"dgo-n3", "dgo-y", "dgo-x"}, {"dgo-n1", "dgo-n3", "dgo-y"}, {"dgo-y", "dgo-n1", "dgo-n3"}} {
		r := kmsg.NewPtrConsumerGroupDescribeRequest()
		r.Groups = q
		e.Do(fmt.Sprintf("consumer-describe-one-bucket-%d", i), r, 0)
		d := kmsg.NewPtrDescribeGroupsRequest()
		d.Groups = q
		e.Do(fmt.Sprintf("describe-one-bucket-%d", i), d, 5)
	}
	var many, back []string
	for i := range 20 {
		many = append(many, fmt.Sprintf("dgo-p%d", i))
		back = append([]string{fmt.Sprintf("dgo-p%d", i)}, back...)
	}
	for i, q := range [][]string{{"dgo-p8", "dgo-x", "dgo-p44"}, {"dgo-p44", "dgo-x", "dgo-p8"}, many, back} {
		d := kmsg.NewPtrDescribeGroupsRequest()
		d.Groups = q
		e.Do(fmt.Sprintf("describe-one-partition-or-many-%d", i), d, 5)
	}
	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{"dgo-y", "dgo-c", "dgo-x", "dgo-a", "dgo-b"}
	e.Do("delete-mixed", del, 2)
}
