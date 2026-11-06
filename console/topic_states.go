package console

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// The states of a topic's Messages tab that are not rows: a compacted topic's cleaner
// (Tp-21), a transaction holding the last stable offset back (Tp-22), a refused client
// (Tp-25), a topic with nothing in it (Tp-27), a log whose start has moved (Tp-30). Each
// is a card above the tape, made only of what the broker or the recorder holds; where
// the console cannot know, the card says so.

// cleanerView is the cleaner card of a compacted topic.
type cleanerView struct {
	Seen         bool // the console can see the cleaner (the broker is in this process).
	Running      bool // a pass is under way on some partition shown.
	Ran          bool // a pass has cleaned something since the broker opened the partition.
	LastAt       time.Time
	LastRemoved  int64
	TotalRemoved int64
	Passes       int
	CleanedTo    string // "p0 4,102 · p1 3,990": where each partition's log has been cleaned to.
	ConfigURL    string
	Scope        string // "partition 1" or "all partitions".
}

func (c *Console) cleanerCard(topic string, partition int32, partitions int32) *cleanerView {
	v := &cleanerView{ConfigURL: c.topicURL(topic) + "?tab=config&show=compaction", Scope: "all partitions"}
	if partition != allPartitions && partitions > 1 {
		v.Scope = "partition " + strconv.Itoa(int(partition))
	}
	if c.logs == nil {
		return v
	}
	var at []string
	for p := int32(0); p < partitions; p++ {
		if partition != allPartitions && p != partition {
			continue
		}
		st, ok := c.logs.Cleaner(topic, p)
		if !ok {
			continue
		}
		v.Seen = true
		v.Running = v.Running || st.Running
		v.Ran = v.Ran || st.Ran
		if st.LastAt.After(v.LastAt) {
			v.LastAt, v.LastRemoved = st.LastAt, st.LastRemoved
		}
		v.TotalRemoved += st.TotalRemoved
		v.Passes += st.Passes
		at = append(at, "p"+strconv.Itoa(int(p))+" "+groupedInt(st.CleanedTo))
	}
	v.CleanedTo = strings.Join(at, " · ")
	return v
}

func groupedInt(n int64) string {
	s := strconv.FormatInt(n, 10)
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

// txnHoldView says a transaction holds a partition's last stable offset behind its end.
type txnHoldView struct {
	Partition int32
	Stable    int64
	End       int64
	Txns      []openTxn
	CommitURL string // the same partition, committed only.
	Cluster   string // where the open transactions are listed.
}

func (c *Console) txnHold(ctx context.Context, topic string, partition int32, partitions int32) *txnHoldView {
	if partition == allPartitions || partition < 0 {
		return nil
	}
	st, err := c.be.StableOffset(ctx, topic, partition)
	if err != nil || st < 0 {
		return nil
	}
	_, latest, err := c.be.PartitionOffsets(ctx, topic, partition)
	if err != nil || st >= latest {
		return nil
	}
	v := &txnHoldView{Partition: partition, Stable: st, End: latest,
		CommitURL: c.topicURL(topic) + "?partition=" + strconv.Itoa(int(partition)) + "&committed=1", Cluster: c.prefix + "/cluster?view=transactions"}
	all, _ := c.be.TopicOpenTxns(ctx, topic, partitions)
	for _, t := range all {
		if t.Partition == partition {
			v.Txns = append(v.Txns, t)
		}
	}
	return v
}

// topicDeniedView is a client the broker refused on this topic, from the recorder, and the
// rule that decided it by the checker's reading of the stored ACLs.
type topicDeniedView struct {
	When                           time.Time
	Client, Principal, Remote, API string
	Version                        int16
	Parts, Error, Operation        string
	Rules                          bool // the ACLs could be read.
	Steps                          []aclStep
	By                             *aclRule
	Allowed                        bool
	CheckURL                       string
	Count                          int
}

func (c *Console) topicDenied(ctx context.Context, topic string) *topicDeniedView {
	if c.traffic == nil {
		return nil
	}
	var last *TrafficEvent
	n := 0
	for _, e := range c.traffic.Events(TrafficFilter{Topic: topic, ErrorsOnly: true}, 0) {
		if e.Class != "denied" || e.Error != "TOPIC_AUTHORIZATION_FAILED" {
			continue
		}
		n += max(e.Count, 1)
		if last == nil {
			ev := e
			last = &ev
		}
	}
	if last == nil {
		return nil
	}
	op := "describe"
	switch last.API {
	case "Produce":
		op = "write"
	case "Fetch":
		op = "read"
	}
	host := last.Remote
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	v := &topicDeniedView{When: last.Time, Client: last.ClientID, Principal: last.Principal, Remote: last.Remote, API: last.API, Version: last.Version,
		Parts: last.Parts, Error: last.Error, Operation: op, Count: n}
	if v.Client == "" {
		v.Client = last.Remote
	}
	q := aclQuestion{Principal: last.Principal, Operation: op, Resource: "topic", Name: topic, Host: host}
	v.CheckURL = c.prefix + "/access/check?" + url.Values{"principal": {q.Principal}, "operation": {op}, "resource": {"topic"}, "name": {topic}, "host": {host}}.Encode()
	if rules, err := c.aclRules(ctx); err == nil {
		ans := rules.check(q)
		v.Rules, v.Steps, v.By, v.Allowed = true, ans.Steps, ans.By, ans.Allowed
	}
	return v
}

// consoleProducer is the command a person can paste to write to the topic from a client.
func (c *Console) consoleProducer(topic string) string {
	cmd := "kafka-console-producer.sh --bootstrap-server " + c.be.displayAddr() + " --topic " + topic
	if len(c.facts.SASLMechanisms) > 0 {
		cmd += " --producer.config client.properties"
	}
	return cmd
}

// behindRow is a group whose committed offset on a partition is below its log start.
type behindRow struct {
	Group, URL, State string
	Committed, Start  int64
	Behind            int64
}

// behindStart lists the groups that committed on the partition below where its log now
// starts: their next fetch is out of range.
func (c *Console) behindStart(ctx context.Context, topic string, partition int32, start int64) []behindRow {
	groups, err := c.be.ListGroups(ctx)
	if err != nil {
		return nil
	}
	var out []behindRow
	for _, g := range groups {
		offs, err := c.be.GroupOffsets(ctx, g.ID)
		if err != nil {
			continue
		}
		for _, o := range offs {
			if o.Topic == topic && o.Partition == partition && o.Committed < start {
				out = append(out, behindRow{Group: g.ID, URL: c.groupURL(g.ID), State: g.State, Committed: o.Committed, Start: start, Behind: start - o.Committed})
			}
		}
	}
	return out
}

// messageStates adds the cards to a Messages tab's data.
func (c *Console) messageStates(ctx context.Context, name string, detail *TopicDetail, partition int32, data map[string]any) {
	if cv, ok := data["Config"].(configView); ok && cv.Compact {
		data["Cleaner"] = c.cleanerCard(name, partition, detail.Partitions)
	}
	data["TxnHold"] = c.txnHold(ctx, name, partition, detail.Partitions)
	data["Denied"] = c.topicDenied(ctx, name)
	bounds, _ := data["Bounds"].([]PartitionBound)
	empty := len(bounds) > 0
	for _, b := range bounds {
		empty = empty && b.Earliest == 0 && b.Latest == 0
	}
	if empty && data["Search"] == nil {
		data["Empty"] = map[string]any{"Command": c.consoleProducer(name), "SASL": len(c.facts.SASLMechanisms) > 0, "Partitions": partitionRange(detail.Partitions)}
	}
	if partition != allPartitions && int(partition) < len(bounds) && bounds[partition].Earliest > 0 {
		b := bounds[partition]
		data["LogMoved"] = map[string]any{"Partition": partition, "Start": b.Earliest, "Gone": b.Earliest - 1, "End": b.Latest, "Behind": c.behindStart(ctx, name, partition, b.Earliest)}
	}
}

// topicNew answers a paused Messages tab's question: how many records have arrived on
// the partitions since it paused, given the log ends it was showing (ends=0:4815,1:4812).
func (c *Console) topicNew(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	d, err := c.be.DescribeTopic(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	bounds, err := c.be.PartitionBounds(r.Context(), name, d.Partitions)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	ends := map[int]int64{}
	for _, kv := range strings.Split(r.URL.Query().Get("ends"), ",") {
		if p, e, ok := strings.Cut(kv, ":"); ok {
			pi, err1 := strconv.Atoi(p)
			ei, err2 := strconv.ParseInt(e, 10, 64)
			if err1 == nil && err2 == nil {
				ends[pi] = ei
			}
		}
	}
	var n int64
	parts := map[string]int64{}
	for _, b := range bounds {
		if was, ok := ends[int(b.Partition)]; ok && b.Latest > was {
			n += b.Latest - was
			parts[strconv.Itoa(int(b.Partition))] = b.Latest - was
		}
	}
	w.Header().Set("Content-Type", "application/json")
	out := `{"n":` + strconv.FormatInt(n, 10) + `,"parts":{`
	first := true
	for p, k := range parts {
		if !first {
			out += ","
		}
		first = false
		out += `"` + p + `":` + strconv.FormatInt(k, 10)
	}
	_, _ = w.Write([]byte(out + "}}"))
}

// whyRow is a request the recorder holds that named a topic that is not there.
type whyRow struct {
	Client, API, Principal string
	When                   time.Time
}

// topicNotFound adds to the not-found page what the broker and the recorder know
// (Tp-29): the nearest topics with their sizes, a way to create it, and which clients
// were refused for asking for it.
func (c *Console) topicNotFound(ctx context.Context, name string, page *errorPage) {
	page.TopicName = name
	page.CreateURL = c.prefix + "/new/topic?name=" + url.QueryEscape(name)
	if ts, err := c.be.ListTopics(ctx); err == nil {
		page.TopicCount = len(ts)
		by := map[string]TopicSummary{}
		for _, t := range ts {
			by[t.Name] = t
		}
		for i, s := range page.Suggestions {
			if t, ok := by[s.Name]; ok {
				page.Suggestions[i].Detail = nounCount(int(t.Partitions), "partition", "partitions") + " · " + nounCount(int(t.Msgs), "record", "records")
			}
		}
	}
	if c.traffic == nil {
		return
	}
	seen := map[string]bool{}
	for _, e := range c.traffic.Events(TrafficFilter{Topic: name, ErrorsOnly: true}, 0) {
		if e.Error != "UNKNOWN_TOPIC_OR_PARTITION" || e.API == "Metadata" && e.ClientID == ClientID {
			continue
		}
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		if seen[who+e.API] || len(page.Why) >= 5 {
			continue
		}
		seen[who+e.API] = true
		page.Why = append(page.Why, whyRow{Client: who, API: e.API, Principal: e.Principal, When: e.Time})
	}
}
