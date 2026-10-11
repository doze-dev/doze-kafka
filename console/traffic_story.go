package console

// The explainers of the Traffic page: what a refusal means and what to do about it
// (Traf-03), who touched a topic (Traf-02) and the story of a group's rebalance
// (Traf-12). Each is built from what the recorder holds and from the one table of error
// codes (protocol.Explain); nothing here is prose about a code that the table does not
// have.

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// refusalWhy is the usual cause of an error, for a line that names it (the clients view's
// "needs a look" callout); "" for a name the table does not know.
func refusalWhy(name string) string {
	for _, code := range protocol.ErrorCodes() {
		if protocol.ErrorName(code) == name {
			return protocol.Explain(code).Cause
		}
	}
	return ""
}

// nearestTopic is the existing topic whose name is closest to name, when it is close
// enough to be what was meant: a third of the name's length at most, and never the same.
func nearestTopic(name string, topics []TopicSummary) (string, bool) {
	best, bestD := "", 1<<30
	for _, t := range topics {
		if t.Name == name {
			continue
		}
		if d := editDistance(name, t.Name); d < bestD {
			best, bestD = t.Name, d
		}
	}
	if best == "" || bestD > max(len([]rune(name))/3, 2) {
		return "", false
	}
	return best, true
}

// errorSummaryView is the inspector's default on the errors view (Traf-03): the most
// frequent refusal, what it means, what to do, and the other refusals by name.
type errorSummaryView struct {
	Name, Class, ClassWord, Tone  string
	Headline, Meaning, Cause, Fix string
	Said                          string // "checkout-api sent Produce for harbour.checkout.invoices."
	Verdict                       string // "Real Kafka would refuse the same call."
	Seen                          string
	SeenAt                        time.Time
	Client                        string
	Nearest                       string
	NearestURL                    string
	Create                        string // a topic that does not exist, to create.
	CreateURL                     string
	TopicsURL                     string
	TopicCount                    int
	Others                        []errorOther
	RowURL                        string
	HasFix                        bool
}

type errorOther struct{ Name, Clients string }

func (c *Console) errorSummary(ctx context.Context, refusals []TrafficRefusal) *errorSummaryView {
	// Expected flow is not a refusal.
	var rs []TrafficRefusal
	for _, r := range refusals {
		if r.Class != "expected" {
			rs = append(rs, r)
		}
	}
	if len(rs) == 0 {
		return nil
	}
	top := rs[0]
	info := protocol.Explain(top.Code)
	v := &errorSummaryView{Name: top.Name, Class: top.Class, ClassWord: classWord(top.Class), Tone: resultTone(top.Class)}
	v.Meaning, v.Cause, v.Fix = info.Meaning, info.Cause, info.Fix
	v.Headline = top.API + " was refused"
	if top.Name == "CONNECTION_CLOSED" {
		v.Headline, v.Meaning = top.API+" ended the connection", "The broker closed the connection because of this request."
	}
	who := "a client"
	if len(top.Clients) > 0 {
		who = top.Clients[0].Client
		v.Client = who
		n := top.Clients[0].Count
		v.Seen = strconv.Itoa(n) + " time"
		if n != 1 {
			v.Seen += "s"
		}
	}
	v.SeenAt = top.Last
	said := who + " sent " + top.API
	if len(top.Topics) > 0 {
		said += " for " + strings.Join(top.Topics, ", ")
	}
	v.Said = said + "."
	switch top.Class {
	case "refused":
		v.Verdict = "Real Kafka would refuse the same call."
	case "denied":
		v.Verdict = "An ACL or a login decided this: nothing is wrong with the broker."
	case "not-implemented":
		v.Verdict = "This broker does not do this; real Kafka would."
	case "failed":
		v.Verdict = "The broker could not do this. Its log says why."
	}
	v.HasFix = v.Fix != ""
	if top.Latest != "" {
		v.RowURL = c.inspectURL(KindRequest, top.Latest)
	}
	if top.Code == 3 && len(top.Topics) > 0 { // UNKNOWN_TOPIC_OR_PARTITION
		topics, err := c.be.ListTopics(ctx)
		if err == nil {
			v.TopicCount = len(topics)
			v.TopicsURL = c.prefix + "/topics"
			v.Create = top.Topics[0]
			v.CreateURL = c.prefix + "/new/topic?name=" + queryEscape(top.Topics[0])
			if n, ok := nearestTopic(top.Topics[0], topics); ok {
				v.Nearest, v.NearestURL = n, c.topicURL(n)
			}
		}
	}
	for _, o := range rs[1:] {
		if len(v.Others) == 3 {
			break
		}
		who := ""
		if len(o.Clients) > 0 {
			who = o.Clients[0].Client
		}
		v.Others = append(v.Others, errorOther{Name: o.Name, Clients: who})
	}
	return v
}

// touchedView is the inspector's default when a topic is filtered (Traf-02): who
// produced to it and who read from it, from the rows the recorder holds.
type touchedView struct {
	Topic, TopicURL      string
	Producers, Consumers []string
	Produces, Fetches    int
	FetchesFolded        int
	Errors               int
	// Groups says the group requests that name the topic are among the rows.
}

func (c *Console) touched(topic string) *touchedView {
	t := &touchedView{Topic: topic, TopicURL: c.topicURL(topic)}
	seenP, seenC := map[string]bool{}, map[string]bool{}
	for _, e := range c.traffic.Events(TrafficFilter{Topic: topic}, 0) {
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		switch e.Kind {
		case "produce":
			t.Produces += max(e.Count, 1)
			if !seenP[who] {
				seenP[who] = true
				t.Producers = append(t.Producers, who)
			}
		case "fetch":
			t.Fetches++
			t.FetchesFolded += max(e.Count-1, 0)
			if !seenC[who] {
				seenC[who] = true
				t.Consumers = append(t.Consumers, who)
			}
		}
		if e.Error != "" && e.Class != "expected" {
			t.Errors += max(e.Count, 1)
		}
	}
	sort.Strings(t.Producers)
	sort.Strings(t.Consumers)
	return t
}

// storyRow is a row of the story, ready to show.
type storyRow struct {
	trafficRow
	Nested bool
}

// storyBlock is one rebalance with its requests.
type storyBlock struct {
	Start, End time.Time
	Done       bool
	From, To   int32
	Cause      string
	Members    int
	Dur        string
	Rows       []storyRow
	// Epoch marks a KIP-848 reassignment, which is one instant.
	Epoch bool
	Title string
}

// storyView is the table of Traf-12.
type storyView struct {
	Group      string
	Blocks     []storyBlock
	Before     []storyRow
	BeforeGen  string
	Requests   int
	Folded     int
	Rebalances int
	Protocol   string
	// Epochs says the group is a KIP-848 group, whose rebalances are epochs.
	Epochs bool
}

func (c *Console) storyView(st TrafficStory, open map[uint64]bool) *storyView {
	sv := &storyView{Group: st.Group, Requests: st.Requests, Folded: st.Folded, Rebalances: len(st.Rebalances), Protocol: st.Protocol, Epochs: st.Protocol == "848"}
	mk := func(e TrafficEvent, nested bool) storyRow {
		row := c.trafficRow(e)
		if open[e.Conn] {
			row.ConnKey = strconv.FormatUint(e.Conn, 10)
		}
		return storyRow{trafficRow: row, Nested: nested}
	}
	for _, rb := range st.Rebalances {
		b := storyBlock{Start: rb.Start, End: rb.End, Done: rb.Done, From: rb.From, To: rb.To, Cause: rb.Cause, Members: rb.Members, Epoch: st.Protocol == "848"}
		if rb.Done {
			b.Dur = shortDuration(rb.End.Sub(rb.Start))
		}
		cause := rb.Cause
		if cause == "" {
			cause = "the group's membership changed"
		}
		switch {
		case !rb.Done:
			b.Title = cause + " · rebalancing now"
		case b.Epoch:
			b.Title = cause + " · " + pluralOf(rb.Members, "member") + " now"
		default:
			b.Title = cause + " · " + pluralOf(rb.Members, "member") + " now · " + b.Dur
		}
		for _, e := range rb.Rows {
			b.Rows = append(b.Rows, mk(e, true))
		}
		sv.Blocks = append(sv.Blocks, b)
	}
	for _, e := range st.Before {
		sv.Before = append(sv.Before, mk(e, false))
	}
	if len(st.Rebalances) > 0 && st.Rebalances[len(st.Rebalances)-1].From > 0 {
		sv.BeforeGen = "generation " + strconv.Itoa(int(st.Rebalances[len(st.Rebalances)-1].From))
		if st.Protocol == "848" {
			sv.BeforeGen = "epoch " + strconv.Itoa(int(st.Rebalances[len(st.Rebalances)-1].From))
		}
	}
	return sv
}

func pluralOf(n int, one string) string {
	if n == 1 {
		return "1 " + one
	}
	return strconv.Itoa(n) + " " + one + "s"
}

// groupSummaryView is the inspector's default on a group's story (Traf-12).
type groupSummaryView struct {
	Group, GroupURL, TimelineURL string
	State                        string
	Protocol                     string
	Topics                       []string
	Why                          string // "A member joined."
	WhyDetail                    string // "The group had 2 members at generation 6 and 3 at generation 7."
	Generations                  []TrafficGeneration
	Has                          bool
	Epochs                       bool
}

func (c *Console) groupSummary(ctx context.Context, group string, st TrafficStory) *groupSummaryView {
	g := &groupSummaryView{Group: group, GroupURL: c.groupURL(group), TimelineURL: c.groupURL(group) + "?tab=timeline", Topics: st.Topics}
	g.Epochs = st.Protocol == "848"
	g.Protocol = "classic protocol"
	if g.Epochs {
		g.Protocol = "KIP-848 protocol"
	}
	if d, err := c.be.DescribeGroup(ctx, group); err == nil && d != nil {
		g.State = d.State
		if p := d.Protocol; p == "848" {
			g.Protocol = "KIP-848 protocol"
		}
	}
	// Why the newest finished rebalance happened.
	for _, rb := range st.Rebalances {
		if !rb.Done {
			continue
		}
		g.Has = true
		cause := rb.Cause
		if cause == "" {
			cause = "the group's membership changed"
		}
		g.Why = upperFirst(cause) + "."
		unit := "generation"
		if g.Epochs {
			unit = "epoch"
		}
		if rb.MembersBefore > 0 && rb.From > 0 {
			g.WhyDetail = "The group had " + pluralOf(rb.MembersBefore, "member") + " at " + unit + " " + strconv.Itoa(int(rb.From)) +
				" and " + strconv.Itoa(rb.Members) + " at " + unit + " " + strconv.Itoa(int(rb.To)) + "."
		}
		break
	}
	g.Generations = st.Generations
	return g
}
