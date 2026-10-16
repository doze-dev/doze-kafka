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
	"unicode"
	"unicode/utf8"

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

// refusalHeadline is the inspector's heading for the most frequent refusal: what was
// asked, in the board's words where the code says what ("Produce to a topic that does
// not exist", Traf-03), and otherwise what kind of no it was.
func refusalHeadline(api string, code int16, class string) string {
	switch code {
	case 3: // UNKNOWN_TOPIC_OR_PARTITION
		if api == "Produce" {
			return api + " to a topic that does not exist"
		}
		return api + " for a topic that does not exist"
	case 29: // TOPIC_AUTHORIZATION_FAILED
		return api + " on a topic the principal may not use"
	case 30: // GROUP_AUTHORIZATION_FAILED
		return api + " for a group the principal may not use"
	case 90: // PRODUCER_FENCED
		return api + " from a producer a newer one replaced"
	}
	switch class {
	case "denied":
		return api + " was denied"
	case "failed":
		return api + " failed"
	case "not-implemented":
		return api + " is not served here"
	}
	return api + " was refused"
}

// lowerFirst writes a sentence's first letter in lower case, to follow "In plain
// words,".
func lowerFirst(s string) string {
	r, n := utf8.DecodeRuneInString(s)
	if n == 0 {
		return s
	}
	return string(unicode.ToLower(r)) + s[n:]
}

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
	info := protocol.ExplainFor(top.Code, protocol.BrokerFacts{Known: c.facts.KafkaMajor > 0, AutoCreateTopics: c.facts.AutoCreateTopics})
	v := &errorSummaryView{Name: top.Name, Class: top.Class, ClassWord: classWord(top.Class), Tone: resultTone(top.Class)}
	v.Meaning, v.Cause, v.Fix = info.Meaning, lowerFirst(info.Cause), info.Fix
	v.Headline = refusalHeadline(top.API, top.Code, top.Class)
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
	// Traf-03's sentence, where the code says that much: what was asked of a topic that is
	// not here.
	if top.Code == 3 && top.API == "Produce" && len(top.Topics) > 0 {
		said = who + " produced to " + strings.Join(top.Topics, ", ") + ", which is not on this broker"
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
	// Produces and Fetches are requests (a folded row's repeats counted one by one);
	// FetchesFolded how many of the fetches are in folded rows.
	Produces, Fetches int
	FetchesFolded     int
	Errors            int
	// Exists says the topic is one the broker has, so "Open the topic" goes somewhere.
	Exists bool
}

func (c *Console) touched(ctx context.Context, topic string) *touchedView {
	t := &touchedView{Topic: topic, TopicURL: c.topicURL(topic), Exists: c.topicExists(ctx, topic)}
	seenP, seenC := map[string]bool{}, map[string]bool{}
	for _, e := range c.traffic.Events(TrafficFilter{Topic: topic}, 0) {
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		n := max(e.Count, 1)
		switch e.Kind {
		case "produce":
			t.Produces += n
			if !seenP[who] {
				seenP[who] = true
				t.Producers = append(t.Producers, who)
			}
		case "fetch":
			t.Fetches += n
			if n > 1 {
				t.FetchesFolded += n
			}
			if !seenC[who] {
				seenC[who] = true
				t.Consumers = append(t.Consumers, who)
			}
		}
		if e.Error != "" && e.Class != "expected" {
			t.Errors += n
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
	// StoryText is what the "What happened" column says.
	StoryText string
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

// storyText is what a request did in the group's story (Traf-12's "What happened"),
// from what the request and its answer say: the board's sentences where the request
// says that much.
func storyText(e TrafficEvent, row trafficRow, nested bool, before string) string {
	switch {
	case e.API == "Heartbeat" && e.Error == "REBALANCE_IN_PROGRESS":
		return "existing member told to rejoin"
	case e.API == "Heartbeat" && e.Error == "" && nested && strings.HasPrefix(e.Detail, "generation "):
		if row.Folded {
			return e.Detail + " accepted, " + row.Interval
		}
		return e.Detail + " accepted"
	case e.API == "Heartbeat" && e.Error == "" && row.Folded:
		s := joinNonEmpty(", ", e.Detail, row.Interval)
		if before != "" {
			s += ", " + before
		}
		return s
	case e.API == "OffsetCommit" && e.Error == "":
		if e.HasGen {
			return row.TopicParts + ", generation " + strconv.Itoa(int(e.Gen))
		}
		return row.TopicParts
	}
	if row.Rhythm {
		return row.DetailText
	}
	if row.DetailText == "" && e.ViaGroup != "" {
		return "fetch by a member"
	}
	return row.DetailText
}

func (c *Console) storyView(st TrafficStory, open map[uint64]bool) *storyView {
	sv := &storyView{Group: st.Group, Requests: st.Requests, Folded: st.Folded, Rebalances: len(st.Rebalances), Protocol: st.Protocol, Epochs: st.Protocol == "848"}
	before := ""
	if len(st.Rebalances) > 0 {
		before = "before the rebalance"
		if strings.Contains(st.Rebalances[len(st.Rebalances)-1].Cause, "joined") {
			before = "before the join"
		}
	}
	view := trafficView{TrafficFilter: TrafficFilter{Group: st.Group}}
	mk := func(e TrafficEvent, nested bool) storyRow {
		row := c.trafficRow(e, view)
		if open[e.Conn] {
			row.ConnKey = strconv.FormatUint(e.Conn, 10)
		}
		b := ""
		if !nested {
			b = before
		}
		return storyRow{trafficRow: row, Nested: nested, StoryText: storyText(e, row, nested, b)}
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
	State, StateTone             string
	Protocol                     string
	Topics                       []string
	Why                          string // "A member joined."
	WhyDetail                    string // "The group had 2 members at generation 6 and 3 at generation 7."
	// JoinNote is the board's "Real Kafka does the same: any join in the classic protocol
	// makes everyone rejoin", under a rebalance a join began (queue R8, recorded).
	JoinNote    bool
	Generations []TrafficGeneration
	Has         bool
	Epochs      bool
}

// stateTone is the board's pill for a group's state: ok when it is stable.
func stateTone(state string) string {
	switch strings.ToLower(state) {
	case "stable":
		return "ok"
	case "preparingrebalance", "completingrebalance", "reconciling", "assigning":
		return "warn"
	case "dead":
		return "bad"
	}
	return ""
}

func (c *Console) groupSummary(ctx context.Context, group string, st TrafficStory) *groupSummaryView {
	g := &groupSummaryView{Group: group, GroupURL: c.groupURL(group), TimelineURL: c.groupURL(group) + "?tab=timeline", Topics: st.Topics}
	g.Epochs = st.Protocol == "848"
	g.Protocol = "classic protocol"
	if g.Epochs {
		g.Protocol = "KIP-848 protocol"
	}
	if d, err := c.be.DescribeGroup(ctx, group); err == nil && d != nil {
		g.State, g.StateTone = d.State, stateTone(d.State)
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
		g.JoinNote = !g.Epochs && strings.Contains(cause, "joined")
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
