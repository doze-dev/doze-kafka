package conformance

import (
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// The first rebalance of an empty classic group (group.initial.rebalance.delay.ms) at
// Kafka's default, and the group coordinator's defaults themselves.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "classic-initial-rebalance-delay", Run: classicInitialRebalanceDelay,
			KafkaEnv: []string{"KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=3000"},
			Doze:     func(c *config.Config) { c.GroupInitialRebalanceDelay = 3 * time.Second }},
		Scenario{Name: "group-coordinator-defaults", Run: groupCoordinatorDefaults,
			NoReplay: "Kafka's own settings and their defaults, which doze-kafka does not list among a broker's configs"},
	)
}

// groupCoordinatorDefaults is what the recorded broker says of the group settings the
// scenarios depend on, with their synonyms: the value each recording ran with
// (STATIC_BROKER_CONFIG where conformance/targets.go sets it) and Kafka's default.
func groupCoordinatorDefaults(e *Env) {
	r := kmsg.NewPtrDescribeConfigsRequest()
	r.IncludeSynonyms = true
	res := kmsg.NewDescribeConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeBroker
	res.ResourceName = "1"
	res.ConfigNames = []string{
		"group.initial.rebalance.delay.ms",
		"group.consumer.assignment.interval.ms",
		"group.consumer.min.assignment.interval.ms",
		"group.consumer.heartbeat.interval.ms",
		"group.consumer.session.timeout.ms",
		"offsets.retention.minutes",
		"offsets.retention.check.interval.ms",
	}
	r.Resources = append(r.Resources, res)
	e.Do("group-configs", r, 4)
}

// classicInitialRebalanceDelay is the first JoinGroup of an empty group at Kafka's
// default delay (3 s). Each group is described on a connection of its own at the times
// given, which tells apart a delay counted from the first join, from the last, and one
// started again for a member that joined during it; the joins are answered at the end.
//
//	ird-g      rebalance timeout 30 s; joins at 0 and 1.5 s.
//	ird-three  30 s; joins at 0, 1.5 and 4.5 s (the second delay).
//	ird-short  4 s; joins at 0 and 1.5 s: the second delay is cut to what is left of it.
//	ird-alone  30 s; one join.
//
// Every time described is half a second or more before, or three quarters after, any
// the rules could give: Kafka was seen to complete up to a quarter of a second late
// (a grid of describes every 250 ms, run twice).
func classicInitialRebalanceDelay(e *Env) {
	e.Do("create", createTopics(false, newTopic("ird", 2, 1)), 7)
	e.findCoordinators("ird-warm", "ird-g", "ird-three", "ird-short", "ird-alone")
	run := func(group string, rebalance int32, joins []int, describes []int) {
		start := time.Now()
		at := func(ms int) {
			if wait := time.Duration(ms)*time.Millisecond - time.Since(start); wait > 0 {
				time.Sleep(wait)
			}
		}
		var pending []*Pending
		next := 0
		for _, ms := range describes {
			for next < len(joins) && joins[next] < ms {
				at(joins[next])
				conn := fmt.Sprintf("%s-%d", group, next)
				e.Open(conn, "", "")
				e.Use(conn)
				s := joinSpec{group: group, metadata: consumerSubscription("ird")}
				label := fmt.Sprintf("%s-join-%d", group, next)
				s.member = e.join(label+"-needs-id", s).MemberID
				r := classicJoinRequest(s)
				r.RebalanceTimeoutMillis = rebalance
				pending = append(pending, e.Start(label, r, 7))
				e.Use("")
				next++
			}
			at(ms)
			e.describeClassic(fmt.Sprintf("%s-describe-%dms", group, ms), group)
		}
		for _, p := range pending {
			e.Finish(p)
		}
	}
	// A broker's first group takes longer (about 4 s, recorded twice): it is not one
	// of those measured.
	run("ird-warm", 30000, []int{0}, []int{6000})
	run("ird-g", 30000, []int{0, 1500}, []int{1000, 2500, 4000, 5500, 6750})
	run("ird-three", 30000, []int{0, 1500, 4500}, []int{5500, 6500, 8500, 9750})
	run("ird-short", 4000, []int{0, 1500}, []int{2500, 3500, 4750})
	run("ird-alone", 30000, []int{0}, []int{2500, 3750})
}
