package console

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// topicRef satisfies templates that only need {{.Topic.Name}} (the tape's
// live poll) without paying for a full DescribeTopic (Metadata+DescribeConfigs)
// on every 2.5s tick.
type topicRef struct{ Name string }

// topics is the Topics home (Tp-Home): every topic, the table of them and what needs a
// look. Nothing is selected.
func (c *Console) topics(w http.ResponseWriter, r *http.Request) {
	data, err := c.topicsHomeData(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data["LiveHash"], _ = c.liveHash("topics_live", data)
	c.render(w, r, "topics_home", data)
}

// topicsLive is Tp-Home's table and what is counted over it, polled: the rail's count
// moves when a topic gains records, and the table beside it moves with it.
func (c *Console) topicsLive(w http.ResponseWriter, r *http.Request) {
	data, err := c.topicsHomeData(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.livePartial(w, r, "topics_live", data)
}

func (c *Console) topicsHomeData(r *http.Request) (map[string]any, error) {
	ctx := r.Context()
	list, err := c.be.ListTopics(ctx)
	if err != nil {
		return nil, err
	}
	groups := c.groupRows(ctx, c.listGroupsQuiet(ctx))
	rows := c.topicRows(ctx, list, groups, true)

	var partitions int
	var messages int64
	var compacted, dead int
	readers := map[string]bool{}
	var deadNames []string
	var deadUnread []string
	var size int64
	hasSize := len(rows) > 0
	for _, t := range rows {
		partitions += int(t.Partitions)
		messages += t.Msgs
		size += t.Size
		hasSize = hasSize && t.HasSize
		if t.Compact {
			compacted++
		}
		if t.DeadLetter {
			dead++
			deadNames = append(deadNames, t.Name)
			if t.ReadByCount == 0 {
				deadUnread = append(deadUnread, t.Name)
			}
		}
		for _, g := range t.ReadBy {
			readers[g] = true
		}
	}
	data := map[string]any{
		"List": list, "Rows": rows, "Page": "topics", "Title": "Topics", "Groups": groups,
		"Partitions": partitions, "Messages": messages, "ReaderCount": len(readers),
		"Size": diskSize(size), "HasSize": hasSize, "CompactedWord": countWord(compacted),
		"Compacted": compacted, "DeadLetters": dead, "DeadNames": deadNames, "DeadUnread": deadUnread,
		"AutoCreate": c.facts.AutoCreateTopics, "AutoParts": c.facts.DefaultPartitions, "KnowsAuto": c.facts.KafkaMajor > 0,
	}
	if c.traffic != nil {
		writers := map[string]bool{}
		for _, e := range c.traffic.Events(TrafficFilter{API: "Produce"}, 0) {
			if e.API == "Produce" && len(e.Topics) > 0 {
				who := e.ClientID
				if who == "" {
					who = e.Remote
				}
				writers[who] = true
			}
		}
		data["Writers"], data["HasWriters"] = len(writers), true
		data["RingText"], data["WrittenFrom"] = c.ringText(time.Now())
	}
	if len(rows) == 0 {
		f := c.connectFacts(r)
		data["Facts"], data["Command"] = f, createCommand(f, "", "", nil)
	} else {
		data["Facts"] = connectFacts{DataDir: c.facts.DataDir}
	}
	return data, nil
}

// listGroupsQuiet is ListGroups for a page that can do without it: a broker that does
// not answer is no groups, and the page's own request has already said so.
func (c *Console) listGroupsQuiet(ctx context.Context) []GroupSummary {
	gs, err := c.be.ListGroups(ctx)
	if err != nil {
		return nil
	}
	return gs
}

func (c *Console) topicCreatePage(w http.ResponseWriter, r *http.Request) {
	list, _ := c.be.ListTopics(r.Context())
	taken := make([]string, 0, len(list))
	for _, t := range list {
		taken = append(taken, t.Name)
	}
	b, _ := json.Marshal(taken)
	prefill := strings.TrimSpace(r.URL.Query().Get("name"))
	pj, _ := json.Marshal(prefill)
	// The page can be opened with what it holds filled in (the "Set…" link carries the
	// name and partitions typed so far, and a link can carry settings): what is in the
	// query is what the form shows, as typed.
	cfg := newConfigView(nil)
	for i := range cfg.Settings {
		cfg.Settings[i].Own = strings.TrimSpace(r.URL.Query().Get(cfg.Settings[i].Key))
	}
	parts := strings.TrimSpace(r.URL.Query().Get("partitions"))
	if parts == "" {
		parts = "1"
	}
	command, _ := c.createShell(r)
	c.render(w, r, "topic_create", map[string]any{
		"List": list, "Page": "topics", "Title": "Create topic", "Config": cfg,
		"Taken": taken, "TakenJSON": string(b), "Prefill": prefill, "PrefillJSON": string(pj), "HasRegistry": c.reg != nil, "MaxParts": c.facts.MaxPartitions,
		"Parts": parts, "Open": r.URL.Query().Get("settings") == "1", "ConfigRows": settingRows(cfg.Settings),
		"SetURL": c.prefix + "/new/topic?settings=1", "Command": command,
	})
}

// settingRows lays the settings out two to a row, as Tp-03 does.
func settingRows(s []settingView) [][]settingView {
	var rows [][]settingView
	for i := 0; i < len(s); i += 2 {
		rows = append(rows, s[i:min(i+2, len(s))])
	}
	return rows
}

// createShell is the kafka-topics.sh call for what the form holds (the form's fields
// or the page's query), and the note that says why a value was left out of it.
func (c *Console) createShell(r *http.Request) (string, string) {
	set := map[string]string{}
	note := ""
	for _, ts := range topicSettings {
		raw := strings.TrimSpace(r.FormValue(ts.Key))
		if raw == "" {
			continue
		}
		v, err := ts.parse(raw)
		if err != nil {
			note = ts.Label + ": " + err.Error()
			continue
		}
		set[ts.Key] = v
	}
	return createCommand(c.connectFacts(r), strings.TrimSpace(r.FormValue("name")), strings.TrimSpace(r.FormValue("partitions")), set), note
}

// topicShell is the shell command under the create form, rebuilt from what has been
// typed so far. A value that does not parse yet leaves its --config out and says why.
func (c *Console) topicShell(w http.ResponseWriter, r *http.Request) {
	command, note := c.createShell(r)
	c.partial(w, "shell_preview", map[string]any{"Command": command, "Note": note})
}

func (c *Console) topicCreate(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	// What was typed is what is created, or it is an error: "abc", 0 and -5 used
	// to make a one-partition topic, and 4294967299 a three-partition one.
	partitions := int64(1)
	if raw := strings.TrimSpace(r.FormValue("partitions")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n < 1 {
			c.fail(w, r, badInput("%q is not a number of partitions; use a whole number, 1 or more", raw))
			return
		}
		partitions = n
	}
	// Settings left blank follow the broker's defaults, now and if those change.
	config, _, err := settingsFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.CreateTopic(r.Context(), name, int32(partitions), config); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.topicURL(name), "Topic created")
}

func (c *Console) topicDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.DeleteTopic(r.Context(), name); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.prefix+"/topics", "Topic deleted")
}

func (c *Console) topicPurge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.PurgeTopic(r.Context(), name); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.topicURL(name), "Topic purged — all messages deleted")
}

// topicTabs are the tabs of a topic's page, in the order Sy-01 draws them. Schema is
// there only for a topic the registry has a subject for.
var topicTabs = []string{"messages", "partitions", "readers", "config", "schema"}

func (c *Console) topicDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	list, _ := c.be.ListTopics(ctx)
	detail, err := c.be.DescribeTopic(ctx, name)
	if err != nil {
		c.failResource(w, r, err, "topic", name)
		return
	}
	partition := partitionParam(r, detail.Partitions)
	schemas := c.topicSchemas(r, name)

	tab := r.URL.Query().Get("tab")
	valid := false
	for _, t := range topicTabs {
		valid = valid || t == tab
	}
	if !valid || (tab == "schema" && len(schemas) == 0) {
		tab = "messages"
	}

	data := map[string]any{
		"Schemas": schemas,
		"List":    list, "Topic": detail, "Page": "topics", "View": "topic", "Tab": tab,
		"Partition": partition, "Title": name,
		"Consumers": c.topicConsumers(ctx, name),
		"Config":    newConfigView(detail.Config),
	}
	data["Confirm"] = c.topicConfirms(ctx, name, int(detail.Partitions))
	maps.Copy(data, c.dockData(DockFilter{Topic: name}))
	data["Neighbours"] = c.neighbourClauses(c.topicGraph(ctx, name, data["Consumers"].([]consumerRow)), NodeTopic, name)
	if c.traffic != nil {
		short, _ := c.ringText(time.Now())
		data["NeighboursNote"] = "Written by is " + short + ". Read by is the groups that have committed offsets here; a consumer that assigns partitions itself has no group and is not counted."
	}
	data["Tabs"] = c.topicTabLinks(name, partition, tab, len(schemas) > 0, int(detail.Partitions), len(data["Consumers"].([]consumerRow)))
	data["ListCount"] = len(list)

	if tab != "messages" {
		bounds, err := c.be.PartitionBounds(ctx, name, detail.Partitions)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		data["Bounds"] = bounds
		data["Facts"] = c.topicSummary(ctx, detail, bounds)
		data["PartitionRange"] = partitionRange(detail.Partitions)
		switch tab {
		case "partitions":
			rows, tot := c.partitionRows(ctx, name, bounds)
			data["PartitionRows"], data["PartitionTotals"] = rows, tot
			data["DataDir"] = c.facts.DataDir
			data["SegmentBytes"] = configShown(detail.Config, "segment.bytes")
			data["AddParts"] = c.addPartsView(r, detail.Partitions, c.topicReaders(ctx, name, bounds))
		case "config":
			q := r.URL.Query()
			t := c.configTable(detail.Config, q.Get("show"), q.Get("edit"))
			if t.EditRow != nil {
				single := c.singleSegments(name, int(detail.Partitions))
				typed := q.Get("value")
				if typed == "" {
					typed = t.EditRow.Value
				}
				t.Before = c.configPreview(detail.Config, t.EditRow.Key, typed, single, int(detail.Partitions))
			}
			data["CfgTable"] = t
		case "readers":
			data["Readers"] = c.topicReaders(ctx, name, bounds)
			data["Writers"] = c.topicWriters(name)
			data["HasTraffic"] = c.traffic != nil
		}
		c.composerSchemas(r, name, data)
		c.render(w, r, "topic_detail", data)
		return
	}

	if q, all, before, asked := searchParams(r); asked {
		// A search replaces the tape and the live table with what it found.
		found, err := c.searchTopic(ctx, name, partition, detail.Partitions, q, all, before)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		data["Search"], data["PartitionRange"], data["Rows"] = found, partitionRange(detail.Partitions), found.Rows
		if bounds, err := c.be.PartitionBounds(ctx, name, detail.Partitions); err == nil {
			data["Facts"] = c.topicSummary(ctx, detail, bounds)
			data["Bounds"] = bounds
		}
		c.messageStates(ctx, name, detail, partition, data)
		c.composerSchemas(r, name, data)
		c.render(w, r, "topic_detail", data)
		return
	}
	if mergedParam(r, detail.Partitions) && r.URL.Query().Get("jump") == "" && r.URL.Query().Get("at") == "" && r.URL.Query().Get("open") == "" && r.URL.Query().Get("hl") == "" && fromParam(r) < 0 && !committedParam(r) {
		partition = allPartitions
		data["Partition"] = partition
	}
	jump := jumpParam(r)
	at, err := c.atResolve(r, name, partition)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	atNote := at.Note
	if at.Asked {
		jump = at.Jump
	}
	tape, err := c.tapeData(ctx, name, detail.ID, partition, detail.Partitions, fromParam(r), hlParam(r), jump, committedParam(r))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	maps.Copy(data, tape)
	if bounds, ok := tape["Bounds"].([]PartitionBound); ok {
		data["Facts"] = c.topicSummary(ctx, detail, bounds)
	}
	c.messageStates(ctx, name, detail, partition, data)
	c.composerSchemas(r, name, data)
	if atNote != "" && tape["JumpNote"] == "" {
		data["JumpNote"] = atNote
		if at.View != nil {
			data["Jump"] = at.View
		}
	}
	c.render(w, r, "topic_detail", data)
}

// topicFactsView is the one sentence under a topic's name (Sy-01 part 4): partitions,
// messages, how long they are kept, the schema, and when the newest was written.
type topicFactsView struct {
	Partitions int32
	Messages   int64
	Retention  string
	Compact    bool
	SchemaType string // "Avro", "JSON Schema" or "Protobuf" for the value subject, or "".
	SchemaVer  int
	Newest     time.Time
	HasNewest  bool
	Subject    string
	SubjectURL string
	Size       string // the topic's segments on disk, when the console was told where they are.
	HasSize    bool
	Policy     string // cleanup.policy as the topic has it.
}

// PolicyText is the cleanup policy as the board's facts row says it.
func (f topicFactsView) PolicyText() string {
	p := f.Policy
	if p == "" {
		p = "delete"
	}
	if f.Compact {
		return p + ", compacted"
	}
	return p + ", not compacted"
}

func (c *Console) topicSummary(ctx context.Context, d *TopicDetail, bounds []PartitionBound) topicFactsView {
	f := c.boundsFacts(ctx, d.Name, bounds)
	f.Partitions = d.Partitions
	policy, retention := "", ""
	for _, e := range d.Config {
		switch e.Name {
		case "cleanup.policy":
			policy = e.Value
		case "retention.ms":
			retention = e.Value
		}
	}
	f.Compact = strings.Contains(policy, "compact")
	f.Policy = policy
	switch {
	case f.Compact && !strings.Contains(policy, "delete"):
		f.Retention = "per key"
	default:
		f.Retention = retentionText(retention)
	}
	return f
}

// boundsFacts is the part of the facts sentence that moves as records arrive: how
// many there are, when the newest was written, and what it is. The tape's poll sends
// it with the tape (topic_facts_live), so the sentence and the tape never disagree.
func (c *Console) boundsFacts(ctx context.Context, topic string, bounds []PartitionBound) topicFactsView {
	var f topicFactsView
	newestIn := -1
	for i, b := range bounds {
		f.Messages += b.Count()
		if b.Count() > 0 && b.Newest.After(f.Newest) {
			f.Newest, f.HasNewest, newestIn = b.Newest, true, i
		}
	}
	if d, ok := c.topicDisk(topic, len(bounds)); ok {
		f.Size, f.HasSize = diskSize(d.LogBytes), true
	}
	// The schema is the newest record's, read from its bytes: a value a registry-aware
	// serializer wrote carries the schema's id. A subject named like the topic says
	// nothing about what was written (a producer need not use it), so it is not asked.
	if newestIn >= 0 && c.reg != nil {
		b := bounds[newestIn]
		if msgs, err := c.be.FetchWindow(ctx, topic, b.Partition, b.Latest-1, 1); err == nil && len(msgs) > 0 {
			if v := c.msgView(ctx, topic, msgs[len(msgs)-1]); v.Value.Schema != nil {
				s := v.Value.Schema
				f.SchemaType, f.SchemaVer, f.Subject, f.SubjectURL = s.TypeLabel, s.Version, s.Subject, s.URL
			}
		}
	}
	return f
}

// withLiveFacts adds the moving part of the facts sentence to a tape answer.
func (c *Console) withLiveFacts(ctx context.Context, topic string, data map[string]any) {
	if bounds, ok := data["Bounds"].([]PartitionBound); ok {
		data["FactsLive"] = c.boundsFacts(ctx, topic, bounds)
	}
}

// tabLink is one tab of a topic's page.
type tabLink struct {
	Key, Label, URL string
	On              bool
	Count           string
}

func (c *Console) topicTabLinks(name string, partition int32, tab string, hasSchema bool, partitions, readers int) []tabLink {
	mk := func(key, label, count string) tabLink {
		u := c.topicURL(name)
		q := url.Values{}
		if key != "messages" {
			q.Set("tab", key)
		} else if partition != 0 {
			q.Set("partition", strconv.Itoa(int(partition)))
		}
		if len(q) > 0 {
			u += "?" + q.Encode()
		}
		return tabLink{Key: key, Label: label, URL: u, On: tab == key, Count: count}
	}
	out := []tabLink{
		mk("messages", "Messages", ""),
		mk("partitions", "Partitions", strconv.Itoa(partitions)),
		mk("readers", "Readers", strconv.Itoa(readers)),
		mk("config", "Config", ""),
	}
	if hasSchema {
		out = append(out, mk("schema", "Schema", ""))
	}
	return out
}

// partitionRow is one partition on the Partitions tab (Tp-16): where its log starts and
// ends, who leads it, how far a read-committed consumer may read, what it takes on
// disk, and the transactions open on it.
type partitionRow struct {
	Partition int32
	Start     int64
	End       int64
	Count     int64
	Stable    int64 // the last stable offset.
	Held      bool  // an open transaction holds it behind the end.
	Newest    time.Time
	HasNewest bool
	Leader    string // "broker 1", or "" when Metadata did not say.
	Txns      []openTxn
	// The files, when the console was told the data directory.
	HasDisk     bool
	Size        string
	Segments    int
	Active      string
	ActiveSize  string
	SizeBytes   int64
	Written     time.Time
	WrittenKnow bool
}

// partitionTotals is the All row.
type partitionTotals struct {
	HasDisk  bool
	Size     string
	Segments int
}

func (c *Console) partitionRows(ctx context.Context, topic string, bounds []PartitionBound) ([]partitionRow, partitionTotals) {
	rows := make([]partitionRow, len(bounds))
	leads, _ := c.be.PartitionLeads(ctx, topic)
	txns, _ := c.be.TopicOpenTxns(ctx, topic, int32(len(bounds)))
	disk, hasDisk := c.topicDisk(topic, len(bounds))
	var tot partitionTotals
	tot.HasDisk = hasDisk
	var total int64
	for i, b := range bounds {
		r := partitionRow{Partition: b.Partition, Start: b.Earliest, End: b.Latest, Count: b.Count(), Stable: b.Latest, Newest: b.Newest, HasNewest: !b.Newest.IsZero()}
		if st, err := c.be.StableOffset(ctx, topic, b.Partition); err == nil && st >= 0 && st < b.Latest {
			r.Stable, r.Held = st, true
		}
		if l, ok := leads[b.Partition]; ok && l.Leader >= 0 {
			r.Leader = "broker " + itoa32(l.Leader)
		}
		for _, t := range txns {
			if t.Partition == b.Partition {
				r.Txns = append(r.Txns, t)
			}
		}
		if hasDisk && int(b.Partition) < len(disk.Parts) {
			d := disk.Parts[b.Partition]
			r.HasDisk, r.Size, r.SizeBytes, r.Segments = true, diskSize(d.Bytes), d.Bytes, d.Segments
			if d.Active != "" {
				r.Active, r.ActiveSize, r.Written, r.WrittenKnow = d.Active, diskSize(d.ActiveBytes), d.Written, true
			}
			total += d.Bytes
			tot.Segments += d.Segments
		}
		rows[i] = r
	}
	tot.Size = diskSize(total)
	return rows, tot
}

func itoa32(n int32) string { return strconv.Itoa(int(n)) }

// readerRow is a group reading a topic, with its lag on each partition.
type readerRow struct {
	Group, URL, State, Protocol string
	Members                     int
	PartLag                     []int64 // by partition; -1 where the group has no offset.
	Total                       int64
}

func (c *Console) topicReaders(ctx context.Context, topic string, bounds []PartitionBound) []readerRow {
	groups, err := c.be.ListGroups(ctx)
	if err != nil {
		return nil
	}
	var out []readerRow
	for _, g := range groups {
		offs, err := c.be.GroupOffsets(ctx, g.ID)
		if err != nil {
			continue
		}
		row := readerRow{Group: g.ID, URL: c.groupURL(g.ID), State: g.State, Protocol: protoLabelOf(g.Protocol), Members: g.Members}
		row.PartLag = make([]int64, len(bounds))
		for i := range row.PartLag {
			row.PartLag[i] = -1
		}
		on := false
		for _, o := range offs {
			if o.Topic != topic || o.Partition < 0 || int(o.Partition) >= len(bounds) {
				continue
			}
			on = true
			lag := max(bounds[o.Partition].Latest-o.Committed, 0)
			row.PartLag[o.Partition] = lag
			row.Total += lag
		}
		if on {
			out = append(out, row)
		}
	}
	return out
}

// writerRow is a client the recorder saw produce to a topic.
type writerRow struct {
	Client, Principal, Detail string
	Last                      time.Time
	Error                     string
	Count                     int
}

// topicWriters are the clients whose Produce requests to this topic are among the
// recorder's last rows. A client that has not produced since then is not listed.
func (c *Console) topicWriters(topic string) []writerRow {
	if c.traffic == nil {
		return nil
	}
	idx := map[string]int{}
	var out []writerRow
	for _, e := range c.traffic.Events(TrafficFilter{Topic: topic, API: "Produce"}, 0) {
		if e.API != "Produce" {
			continue
		}
		who := e.ClientID
		if who == "" {
			who = e.Remote
		}
		i, ok := idx[who]
		if !ok {
			idx[who] = len(out)
			out = append(out, writerRow{Client: who, Principal: e.Principal, Detail: e.Detail, Last: e.Time, Error: e.Error})
			i = len(out) - 1
		}
		out[i].Count += max(e.Count, 1)
	}
	return out
}

// topicTape is the live-polled partial backing the tape + message table.
func (c *Console) topicTape(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	id, count, err := c.be.TopicShape(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	partition := partitionParam(r, count)
	if mergedParam(r, count) && !committedParam(r) {
		partition = allPartitions
	}

	bounds, err := c.be.PartitionBounds(ctx, name, count)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	hash := boundsHash(id, bounds)
	if liveUnchanged(w, r, hash) {
		return
	}

	tape, err := c.tapeData(ctx, name, id, partition, count, -1, -1, -1, committedParam(r))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": partition}
	maps.Copy(data, tape)
	c.withLiveFacts(ctx, name, data)
	c.partial(w, "tape_and_table", data)
}

func (c *Console) topicProduce(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	id, count, err := c.be.TopicShape(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}

	in, err := parseProduceForm(r, count)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if subject := strings.TrimSpace(r.FormValue("schema_subject")); subject != "" && in.Value != nil {
		if c.reg == nil {
			c.fail(w, r, badInput("there is no schema registry to encode the value with"))
			return
		}
		if r.FormValue("value_enc") == "base64" {
			c.fail(w, r, badInput("a base64 value is sent as raw bytes; choose “No schema” to send it, or untick b64 to write JSON for %s", subject))
			return
		}
		encoded, err := c.reg.encode(ctx, subject, string(in.Value))
		if err != nil {
			c.fail(w, r, err)
			return
		}
		in.Value = encoded
	}
	partition := in.Partition
	if in.Auto {
		partition = c.be.PickPartition(name, in.Key, count)
	}

	result, err := c.be.Produce(ctx, name, partition, in.Key, in.Value, in.Headers, in.Acks)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	// The view stays on the partition being looked at. It used to switch to the
	// partition the record landed on, with the address bar, the composer and the
	// page's links all still naming the old one. The toast says where the record
	// went, and the partition tabs' counts show it arriving.
	viewing := result.Partition
	if n, err := strconv.ParseInt(r.FormValue("partition_ctx"), 10, 32); err == nil && n >= 0 && n < int64(count) {
		viewing = int32(n)
	} else if r.FormValue("partition_ctx") == "all" && count > 1 {
		viewing = allPartitions
	}
	switch {
	case !result.Confirmed:
		toast(w, "Sent — no confirmation requested (acks=0)")
	case viewing == allPartitions:
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	case result.Partition != viewing:
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10)+" (you are viewing partition "+strconv.Itoa(int(viewing))+")")
	default:
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	}

	tape, err := c.tapeData(ctx, name, id, viewing, count, -1, -1, -1, committedParam(r))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": viewing}
	maps.Copy(data, tape)
	c.withLiveFacts(ctx, name, data)
	c.partial(w, "tape_and_table", data)
	// The empty topic's first-record form replaced the page's own tape with this one; its
	// empty-state card goes with it (htmx names the element that made the request).
	if r.Header.Get("HX-Trigger") == "first-record" {
		_, _ = io.WriteString(w, `<div id="topic-empty" hx-swap-oob="delete"></div>`)
	}
}

func (c *Console) topicMessage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	partition, offset, err := recordCoordinates(r.URL.Query().Get("partition"), r.URL.Query().Get("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
		// Nothing there is not an error to the broker, but it is to the person who
		// clicked: the record is gone (retention, a purge) or never existed. This
		// used to pass a nil error on and panic reading its message.
		err = fmt.Errorf("%w: no record at offset %d of partition %d — it may have been deleted by retention or a purge", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	m := msgs[0]
	// Neighbouring records, for the inspector's older/newer buttons.
	older, newer := int64(-1), int64(-1)
	if earliest, latest, err := c.be.PartitionOffsets(r.Context(), name, partition); err == nil {
		if offset-1 >= earliest {
			older = offset - 1
		}
		if offset+1 < latest {
			newer = offset + 1
		}
	}
	hdrs := headerViews(m.Headers)
	valueV := c.readThroughRegistry(r.Context(), name, newBytesView(m.Value, m.Value == nil), m.Value)
	keyV := c.readThroughRegistry(r.Context(), name, newBytesView(m.Key, m.Key == nil), m.Key)
	data := map[string]any{
		"Topic": name, "Partition": partition, "Msg": m, "Older": older, "Newer": newer,
		"Key": keyV, "Value": valueV, "Headers": hdrs,
		// "Send again" is done by the server from the stored record, so it is exact
		// for any bytes. These are for "Edit a copy": the composer is prefilled with
		// the record as text (or base64 when it is not text), as stored rather than
		// as displayed, for changing before it is sent.
		"EditKey": editField(keyV), "EditKeyEnc": resendEnc(keyV),
		"EditValue": editField(valueV), "EditValueEnc": resendEnc(valueV),
		"EditHeaders": resendHeaders(hdrs),
		"CanEdit":     (valueV.Null || valueV.Empty || valueV.B64 != "") && (keyV.Null || keyV.Empty || keyV.B64 != ""),
		// Tp-06's fields: how the record was stamped, what its parts weigh, who wrote it
		// (from the recorder, where it holds the request; otherwise it says so) and the
		// Copy menu.
		"TimestampType": timestampTypeText(m.TimestampType),
		"HeadersSize":   humanBytes(headersBytes(m.Headers)),
		"Writer":        c.recordWriter(name, partition, m.Offset),
		"Producer":      producerCommand(c.be.displayAddr(), name, keyV, valueV, m.Headers),
	}
	// Tp-07, Tp-08, Tp-09: what a value that is not text, null, or framed by a registry
	// is, and (on a compacted topic) what the cleaner will do with a deleted key.
	compacted := strings.Contains(c.be.topicConfigValues(r.Context(), name, "cleanup.policy")["cleanup.policy"], "compact")
	data["Compacted"] = compacted
	if lg := c.recordLegend(r.Context(), name, partition, m, keyV, valueV, compacted); lg != nil {
		data["Legend"] = lg
	}
	if valueV.Null && compacted && m.Key != nil {
		if h := c.keyHistory(r.Context(), name, partition, m, keyV); h != nil && len(h.Rows) > 0 {
			data["History"] = h
		}
	}
	if valueV.Schema != nil {
		data["Registry"] = c.registryWhere()
		data["BodySize"] = humanBytes(max(len(m.Value)-5, 0))
		data["FrameID"] = valueV.Schema.ID
		data["FrameBytes"] = fmt.Sprintf("%02x %02x %02x %02x", m.Value[1], m.Value[2], m.Value[3], m.Value[4])
	}
	if ref := traceOf(m.Headers, c.traceHeader); ref != nil {
		data["Trace"], data["TraceURL"] = ref, c.traceURL(ref.ID, m.Timestamp, name, partition, m.Offset, "")
	}
	// A value read through the registry is edited as the JSON it means, and the
	// composer is told which subject to encode it with again. (Send again still
	// carries the stored bytes.)
	if valueV.Schema != nil {
		data["EditValue"], data["EditValueEnc"], data["EditSchema"] = valueV.Text, "text", valueV.Schema.Subject
	}
	// A record found by a search across partitions has no single partition to put
	// in the address; the page says which by whether it carries a message base.
	nav := func(label, glyph, word string, off int64) inspectorNav {
		n := inspectorNav{Label: label, Glyph: glyph, Disabled: off < 0}
		n.Text = map[string]string{"chevron-up": "↑", "chevron-down": "↓"}[glyph]
		if off >= 0 {
			n.Title = fmt.Sprintf("%s (offset %d)", word, off)
			n.URL = fmt.Sprintf("%s/message?partition=%d&offset=%d", c.topicURL(name), partition, off)
		} else {
			n.Title = word + " (offset )"
		}
		return n
	}
	c.inspect(w, inspectorView{
		Kind: KindMessage, Key: strconv.FormatInt(m.Offset, 10),
		Title: fmt.Sprintf("P%d · offset %s", partition, groupDigits(m.Offset)), Sub: name,
		Nav:  []inspectorNav{nav("Older record", "chevron-up", "Older", older), nav("Newer record", "chevron-down", "Newer", newer)},
		Body: data,
	})
}

// topicResend produces an exact copy of a stored record: the same key, value and
// headers, byte for byte, to the same partition. It is done here rather than by
// prefilling a form because a browser cannot carry arbitrary bytes faithfully: a
// text input drops newlines, an editor rewrites line endings, re-indented JSON is
// not the JSON that was stored, and a form has no way to say "an empty key" as
// distinct from "no key".
func (c *Console) topicResend(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()
	partition, offset, err := recordCoordinates(r.FormValue("partition"), r.FormValue("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(ctx, name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
		err = fmt.Errorf("%w: no record at offset %d of partition %d — it may have been deleted by retention or a purge", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	m := msgs[0]
	result, err := c.be.Produce(ctx, name, partition, m.Key, m.Value, m.Headers, "1")
	if err != nil {
		c.fail(w, r, err)
		return
	}
	toast(w, "Sent an exact copy to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	id, count, err := c.be.TopicShape(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	tape, err := c.tapeData(ctx, name, id, partition, count, -1, -1, -1, committedParam(r))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": partition}
	maps.Copy(data, tape)
	data["Copied"] = result.Offset // the copy's row is drawn marked (Tp-13).
	c.withLiveFacts(ctx, name, data)
	// The copy is read back from the broker, so what the page sets beside the original is
	// what is stored.
	copies, err := c.be.FetchWindow(ctx, name, partition, result.Offset, 1)
	if err != nil || len(copies) == 0 || copies[0].Offset != result.Offset {
		c.partial(w, "tape_and_table", data)
		return
	}
	c.resendAnswer(w, c.resendView(ctx, name, partition, m, copies[0]), data)
}

// recordCoordinates parses a partition and an offset that name one record. Both
// must be numbers: a partition that is not one used to be read as partition 0.
func recordCoordinates(rawPartition, rawOffset string) (int32, int64, error) {
	partition := int64(0)
	if rawPartition != "" {
		n, err := strconv.ParseInt(rawPartition, 10, 32)
		if err != nil || n < 0 {
			return 0, 0, fmt.Errorf("%w: partition %q is not a partition number", errBadInput, rawPartition)
		}
		partition = n
	}
	offset, err := strconv.ParseInt(rawOffset, 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, fmt.Errorf("%w: offset %q is not an offset", errBadInput, rawOffset)
	}
	return int32(partition), offset, nil
}

// editField is the text a field is carried as into the composer.
func editField(v bytesView) string {
	switch {
	case v.Null || v.Empty:
		return ""
	case v.Binary, v.Schema != nil:
		return v.B64 // a decoded value is edited as JSON; that is arranged where the inspector is built.
	default:
		return v.Raw
	}
}

func resendEnc(v bytesView) string {
	if v.Binary || v.Schema != nil {
		return "base64"
	}
	return "text"
}

// previewOnly is newBytesView without the hex dump and base64 the table never
// shows: for 60 rows of up-to-megabyte values those are not free.
func previewOnly(b []byte) bytesView {
	v := bytesView{Size: len(b), SizeText: humanBytes(len(b))}
	switch {
	case b == nil:
		v.Null, v.Preview = true, "∅ null"
	case len(b) == 0:
		v.Empty, v.Preview = true, `""`
	case looksBinarySample(b):
		v.Binary, v.Preview = true, binaryPreview(b)
	default:
		v.Preview = oneLine(string(b[:min(len(b), previewRunes*4)]), previewRunes)
	}
	return v
}

// partitionParam reads ?partition=, clamped to [0, count).
// allPartitions is the view that merges every partition's newest records (Tp-05's
// default): the tape's partition when none was asked for and the topic has more than one.
const allPartitions int32 = -1

// mergedParam says the request wants the merged view: a topic with several partitions
// and no partition named (or "all").
func mergedParam(r *http.Request, count int32) bool {
	v := r.URL.Query().Get("partition")
	return count > 1 && (v == "" || v == "all")
}

func partitionParam(r *http.Request, count int32) int32 {
	n, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil || n < 0 || int32(n) >= count {
		return 0
	}
	return int32(n)
}

// fromParam reads ?from= (a window start offset); -1 means "the tail" —
// the newest window, which is also the only one that live-polls.
func fromParam(r *http.Request) int64 {
	n, err := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// jumpMalformed is jumpParam's answer for something typed that is not an offset.
const jumpMalformed = -2

// jumpParam reads ?jump= (the "go to offset" box): the offset, -1 when absent, or
// jumpMalformed when something was typed that is not one. The last used to be
// treated as absent, so "12abc" showed the tail as if nothing had been asked.
func jumpParam(r *http.Request) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get("jump"))
	if raw == "" {
		return -1
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return jumpMalformed
	}
	return n
}

// hlParam reads ?hl= — an offset to highlight and scroll to (the jump-to-
// offset control sets it); -1 when absent.
func hlParam(r *http.Request) int64 {
	n, err := strconv.ParseInt(r.URL.Query().Get("hl"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// msgView is a message dressed up for the template: pre-formatted strings
// and the tape's visual hints, computed once per render rather than in the
// template itself.
type msgView struct {
	Offset     int64
	Partition  int32
	Timestamp  time.Time
	Key        bytesView
	Value      bytesView
	Headers    []MessageHeader
	Hot        bool
	TickHeight int
	// Txn is "open" for a record of a transaction not yet decided and "aborted" for
	// one that aborted; empty for a record a read-committed consumer would see.
	Txn string
	// Marker is a transaction's commit or abort marker. It takes an offset and is not
	// a record: the row says so and is not a thing to open.
	Marker bool
}

const tapeWindow = 60 // how many messages the tape/table show at once

// maxPartitionTabs is how many partitions get a tab each. Past it the tabs would
// overflow the panel (a 1000-partition topic is allowed), so a select replaces them.
const maxPartitionTabs = 24

// boundsHash identifies what the tape shows: every partition's retained range, so
// a produce to ANY partition changes it and the tabs' counts refresh.
func boundsHash(topicID string, bounds []PartitionBound) string {
	var b strings.Builder
	// The topic's id, because a topic purged (deleted and re-created) and written
	// to again can hold different records at exactly the same offsets; without it
	// another open tab was told "nothing changed" and kept showing the old ones.
	b.WriteString(topicID)
	b.WriteByte('|')
	for _, p := range bounds {
		fmt.Fprintf(&b, "%d-%d,", p.Earliest, p.Latest)
	}
	return b.String()
}

// tapeData fetches one tapeWindow-sized window of messages on a partition
// and shapes them for both the tick strip (ascending, oldest→newest) and the
// table (descending, newest first) — one fetch, two view orders. from=-1
// means the tail (newest) window; any other value pages into history. Only
// the tail window live-polls: paging back pins the view, so the 2.5s
// refresher can't yank the reader back to the newest messages. hl, when ≥0,
// marks that offset's row for highlight-and-scroll (jump-to-offset).
func (c *Console) tapeData(ctx context.Context, topic, topicID string, partition, partitionCount int32, from, hl, jump int64, committedOnly bool) (map[string]any, error) {
	bounds, err := c.be.PartitionBounds(ctx, topic, partitionCount)
	if err != nil {
		return nil, err
	}
	if partition == allPartitions {
		return c.tapeAllData(ctx, topic, topicID, partitionCount, bounds)
	}
	earliest, latest := bounds[partition].Earliest, bounds[partition].Latest

	// A jump to an offset the log does not hold says so, rather than quietly
	// landing somewhere else.
	jumpNote := ""
	var jumpV *jumpView
	if jump == jumpMalformed {
		jumpNote = "That is not an offset. Type a whole number, 0 or more."
		jump = -1
	}
	if jump >= 0 {
		switch {
		case latest == earliest:
			jumpNote = "This partition has no messages to jump to."
		case jump < earliest:
			jumpNote = fmt.Sprintf("Offset %d is before the start of the log (the oldest is %d) — showing from there.", jump, earliest)
			from, hl = earliest, -1
		case jump >= latest:
			jumpNote = fmt.Sprintf("Offset %s is past the end (latest is %s)", groupDigits(jump), groupDigits(latest))
			jumpV = &jumpView{Tone: "warn", Title: jumpNote,
				Note:      fmt.Sprintf("p%d holds offsets %s to %s now. Showing the newest records.", partition, groupDigits(earliest), groupDigits(latest-1)),
				LatestURL: c.topicURL(topic) + "?partition=" + strconv.Itoa(int(partition)), LatestLabel: "Jump to latest"}
			from, hl = -1, -1
		default:
			from, hl = max(jump-20, earliest), jump
		}
	}
	atTail := from < 0 || from >= max(latest-tapeWindow, earliest)
	if atTail {
		from = max(latest-tapeWindow, earliest)
	} else {
		from = max(from, earliest)
	}
	to := min(from+tapeWindow, latest) // exclusive
	var msgs []Message
	if to > from {
		msgs, err = c.be.FetchWindow(ctx, topic, partition, from, int(to-from))
		if err != nil {
			return nil, err
		}
	}

	// Transactions. A record is "open" at or above the last stable offset and "aborted"
	// below it when a read-committed consumer would not be given it; the offsets with no
	// record are transaction markers. Both are known only by comparing with what a
	// read-committed read returns, so that read is made only when something in the window
	// calls for it.
	status, markers, hasTxn := c.txnStatus(ctx, topic, partition, from, to, latest, msgs)

	asc := make([]msgView, 0, len(msgs))
	rows := make([]msgView, 0, len(msgs)+len(markers))
	next := 0
	for off := from; off < to; off++ {
		if next < len(msgs) && msgs[next].Offset == off {
			v := c.msgView(ctx, topic, msgs[next])
			v.Txn = status[off]
			next++
			if committedOnly && v.Txn != "" {
				continue
			}
			asc = append(asc, v)
			rows = append(rows, v)
		} else if markers[off] && !committedOnly {
			rows = append(rows, msgView{Offset: off, Partition: partition, Marker: true})
		}
	}
	for i := range asc {
		asc[i].Hot = atTail && i >= len(asc)-3
	}
	desc := make([]msgView, len(rows))
	for i, v := range rows {
		desc[len(rows)-1-i] = v
	}
	return map[string]any{
		"PartitionRange": partitionRange(partitionCount),
		"Bounds":         bounds,
		"PartSelect":     partitionCount > maxPartitionTabs,
		"JumpNote":       jumpNote,
		"Jump":           jumpV,
		"Copied":         int64(-1),
		"Messages":       asc,
		"Rows":           desc,
		"Earliest":       earliest,
		"Latest":         latest,
		"Count":          latest - earliest,
		"Hash":           boundsHash(topicID, bounds),
		"AtTail":         atTail,
		"HasOlder":       from > earliest,
		"HasNewer":       !atTail,
		"OlderFrom":      max(from-tapeWindow, earliest),
		"NewerFrom":      to,
		"WindowFrom":     from,
		"WindowLast":     max(to-1, from), // inclusive end, for display
		"HL":             hl,
		"HasTxn":         hasTxn,
		"CommittedOnly":  committedOnly,
	}, nil
}

// committedParam reads ?committed=1, the tape's "committed only" view.
func committedParam(r *http.Request) bool { return r.URL.Query().Get("committed") == "1" }

// txnStatus works out which records in [from, to) belong to a transaction that has not
// committed, and which offsets are markers. hasTxn says the partition has transactions
// worth a toggle. It costs a request for the last stable offset and a read of the committed view.
func (c *Console) txnStatus(ctx context.Context, topic string, partition int32, from, to, latest int64, msgs []Message) (status map[int64]string, markers map[int64]bool, hasTxn bool) {
	status, markers = map[int64]string{}, map[int64]bool{}
	if len(msgs) == 0 {
		return
	}
	have := make(map[int64]bool, len(msgs))
	for _, m := range msgs {
		have[m.Offset] = true
	}
	// A gap is a marker only where a control batch is (TOP-14); one that compaction
	// left is a gap. When the broker cannot say, nothing is called a marker.
	if spans, err := c.be.ControlSpans(ctx, topic, partition, from, to); err == nil {
		markers = markersIn(from, to, have, spans)
	}
	lso, err := c.be.StableOffset(ctx, topic, partition)
	if err != nil {
		lso = latest // an older broker, or one that cannot say: nothing is open.
	}
	hasTxn = len(markers) > 0 || lso < latest
	// The committed view is read every time: an aborted record whose marker lies past the
	// window shows no sign of it in the window itself.
	committed := map[int64]bool{}
	if cm, err := c.be.FetchWindowCommitted(ctx, topic, partition, from, int(to-from)); err == nil {
		for _, m := range cm {
			committed[m.Offset] = true
		}
	} else {
		committed = nil // cannot tell aborted from committed; mark only what is open.
	}
	if committed != nil {
		for _, m := range msgs {
			if m.Offset < lso && !committed[m.Offset] {
				hasTxn = true
			}
		}
	}
	if !hasTxn {
		return
	}
	for _, m := range msgs {
		switch {
		case m.Offset >= lso:
			status[m.Offset] = "open"
		case committed != nil && !committed[m.Offset]:
			status[m.Offset] = "aborted"
		}
	}
	return
}

func partitionRange(n int32) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(i)
	}
	return out
}

// tickHeight is the height in pixels of a tape mark: how big the record is, on a
// log scale from empty (6 px) to a megabyte and beyond (40 px). It used to be a
// pseudo-random function of the offset, which looked meaningful and was not.
func tickHeight(size int) int {
	if size <= 0 {
		return 6
	}
	h := 6 + int(34*math.Log2(1+float64(size))/math.Log2(1+(1<<20)))
	return min(h, 40)
}

// topicMessageRaw serves a record's key or value as the exact bytes, for a value
// too large to read in the inspector or one that is binary.
func (c *Console) topicMessageRaw(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	partition, offset, err := recordCoordinates(r.URL.Query().Get("partition"), r.URL.Query().Get("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
		err = fmt.Errorf("%w: no record at offset %d of partition %d", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	field, data := "value", msgs[0].Value
	if r.URL.Query().Get("field") == "key" {
		field, data = "key", msgs[0].Key
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%d-%d.%s.bin"`, safeFilename(name), partition, offset, field))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(data)
}

// safeFilename keeps a topic name fit for a Content-Disposition filename.
func safeFilename(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}

// mergedPerPartition and mergedRows bound the merged view: it reads the newest records
// of each partition and shows the newest of all of those. The page says when that is
// less than the topic holds.
const (
	mergedPerPartition = 30
	mergedMaxParts     = 12
)

// tapeAllData is tapeData for the view across every partition: the newest records of
// each, merged by timestamp, newest first. It is bounded (mergedPerPartition records
// from each of the first mergedMaxParts partitions) and says so in MergedNote.
func (c *Console) tapeAllData(ctx context.Context, topic, topicID string, partitionCount int32, bounds []PartitionBound) (map[string]any, error) {
	parts := min(partitionCount, mergedMaxParts)
	var all []Message
	capped := false
	var total, latestSum int64
	for p := int32(0); p < parts; p++ {
		b := bounds[p]
		total += b.Count()
		latestSum += b.Latest
		from := max(b.Earliest, b.Latest-mergedPerPartition)
		capped = capped || from > b.Earliest
		if b.Latest <= from {
			continue
		}
		msgs, err := c.be.FetchWindow(ctx, topic, p, from, int(b.Latest-from))
		if err != nil {
			return nil, err
		}
		all = append(all, msgs...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if !all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].Timestamp.After(all[j].Timestamp)
		}
		if all[i].Partition != all[j].Partition {
			return all[i].Partition < all[j].Partition
		}
		return all[i].Offset > all[j].Offset
	})
	if len(all) > tapeWindow {
		all = all[:tapeWindow]
		capped = true
	}
	rows := make([]msgView, len(all))
	for i, m := range all {
		rows[i] = c.msgView(ctx, topic, m)
	}
	asc := make([]msgView, len(rows))
	for i, v := range rows {
		asc[len(rows)-1-i] = v
	}
	for i := range asc {
		asc[i].Hot = i >= len(asc)-3
	}
	note := fmt.Sprintf("%s shown, newest first across partitions", num(int64(len(rows))))
	if capped {
		note += fmt.Sprintf("; the newest %d of each partition were read, so older records are on the partitions' own pages", mergedPerPartition)
	}
	if partitionCount > parts {
		note += fmt.Sprintf("; the topic has %d partitions and the first %d were read", partitionCount, parts)
	}
	lanes, axis := mergedLanes(partitionCount, bounds, asc)
	var ends []string
	for _, b := range bounds {
		ends = append(ends, fmt.Sprintf("p%d %s", b.Partition, num(b.Latest)))
	}
	return map[string]any{
		"Copied":         int64(-1),
		"PartitionRange": partitionRange(partitionCount),
		"Bounds":         bounds,
		"PartSelect":     partitionCount > maxPartitionTabs,
		"Messages":       asc,
		"Rows":           rows,
		"Earliest":       int64(0),
		"Latest":         latestSum,
		"Count":          total,
		"Hash":           boundsHash(topicID, bounds),
		"AtTail":         true,
		"WindowFrom":     int64(0),
		"WindowLast":     int64(0),
		"HL":             int64(-1),
		"Merged":         true,
		"MergedNote":     note,
		"LogEnds":        strings.Join(ends, " · "),
		"Lanes":          lanes,
		"Axis":           axis,
	}, nil
}

// laneTick is one record on a partition's lane: how far along the time axis, and how
// tall (its size on a log scale).
type laneTick struct {
	Pct float64
	H   int
}

// tapeLane is one partition's row of the merged tape.
type tapeLane struct {
	Partition int32
	Count     int64
	Ticks     []laneTick
}

// mergedLanes lays the records the merged view read on a time axis from the oldest of
// them to now, one lane per partition, as Tp-05 draws the tape. The axis is four
// moments: three evenly spaced from the oldest record, then now.
func mergedLanes(partitionCount int32, bounds []PartitionBound, asc []msgView) ([]tapeLane, []time.Time) {
	parts := min(partitionCount, mergedMaxParts)
	lanes := make([]tapeLane, parts)
	for i := range lanes {
		lanes[i] = tapeLane{Partition: int32(i), Count: bounds[i].Count()}
	}
	if len(asc) == 0 {
		return lanes, nil
	}
	t0 := asc[0].Timestamp
	for _, v := range asc {
		if v.Timestamp.Before(t0) {
			t0 = v.Timestamp
		}
	}
	t1 := time.Now()
	span := t1.Sub(t0)
	if span <= 0 {
		span = time.Millisecond
	}
	for _, v := range asc {
		if int(v.Partition) >= len(lanes) {
			continue
		}
		pct := float64(v.Timestamp.Sub(t0)) / float64(span) * 100
		lanes[v.Partition].Ticks = append(lanes[v.Partition].Ticks, laneTick{Pct: min(pct, 99.5), H: v.TickHeight})
	}
	axis := make([]time.Time, 3)
	for i := range axis {
		axis[i] = t0.Add(time.Duration(int64(span) * int64(i) / 3))
	}
	return lanes, axis
}

// timestampTypeText names how the broker stamped a record (the batch's attributes).
func timestampTypeText(t int8) string {
	switch t {
	case 0:
		return "CreateTime"
	case 1:
		return "LogAppendTime"
	}
	return "none"
}

// headersBytes is the bytes of a record's headers: their keys and values.
func headersBytes(hs []MessageHeader) int {
	n := 0
	for _, h := range hs {
		n += len(h.Key) + len(h.Value)
	}
	return n
}

// writerView is who wrote a record, as far as the recorder knows.
type writerView struct {
	Known  bool
	Client string
	Tx     string
	// Why is what the page says when it is not known.
	Why string
}

// recordWriter finds the Produce request that wrote a record in the traffic ledger:
// the request that named the topic and partition and was answered with a base offset
// at or before the record's, and as many records as reach it. The recorder keeps the
// last requests only, so an older record has no writer here, and the page says so; it
// never guesses one. A transactional id is the request's own.
func (c *Console) recordWriter(topic string, partition int32, offset int64) writerView {
	if c.traffic == nil {
		return writerView{Why: "the traffic recorder is off"}
	}
	for _, e := range c.traffic.Events(TrafficFilter{Topic: topic, API: "Produce"}, 0) {
		if e.API != "Produce" {
			continue
		}
		for _, p := range e.Facts.Partitions {
			if p.Topic != topic || p.Partition != partition || !p.HasOffset || p.ErrorCode != 0 {
				continue
			}
			if offset < p.Offset || offset >= p.Offset+int64(max(p.Records, 1)) {
				continue
			}
			who := e.ClientID
			if who == "" {
				who = e.Remote
			}
			return writerView{Known: true, Client: who, Tx: e.TxnID}
		}
	}
	return writerView{Why: "this record's request is not in the recorder: it is older than the last requests the broker keeps, or it came over the console's own connection"}
}

// producerView is the "Copy as kafka-console-producer" item: the command, or why this
// record cannot be said with that tool.
type producerView struct {
	Cmd string
	Why string
}

// producerCommand writes the command that produces a record like this one with Kafka's
// console producer, when the tool can say it exactly: text without tabs or line breaks
// (its line format uses them), a key that is not empty (the tool cannot tell an empty
// key from a null one), a value that is present, and headers that are plain text. The
// tool chooses the partition itself, by the key. Anything else is refused with the
// reason, never approximated.
func producerCommand(addr, topic string, key, val bytesView, hs []MessageHeader) producerView {
	plain := func(s string, extra string) bool {
		return !strings.ContainsAny(s, "\t\r\n\x00"+extra)
	}
	switch {
	case val.Null:
		return producerView{Why: "a null value (a tombstone) cannot be typed into the console producer"}
	case val.Empty:
		return producerView{Why: "the console producer cannot tell an empty value from none"}
	case val.Binary || val.Truncated:
		return producerView{Why: "the value is binary or too large to type on a command line"}
	case key.Empty:
		return producerView{Why: "the console producer cannot send an empty key (it reads it as none)"}
	case key.Binary || key.Truncated:
		return producerView{Why: "the key is binary or too large to type on a command line"}
	}
	value, k := val.Raw, key.Raw
	if val.Schema != nil {
		return producerView{Why: "the value was read through a schema; the console producer would send its JSON as plain text, not the record's bytes"}
	}
	if val.Raw == "" || (!key.Null && key.Raw == "") {
		return producerView{Why: "the key or value is too large to type on a command line"}
	}
	if !plain(value, "") || !plain(k, "") {
		return producerView{Why: "the key or value holds a tab or a line break, which the console producer's lines use"}
	}
	var hdr []string
	for _, h := range hs {
		hv := newBytesView(h.Value, h.Value == nil)
		if hv.Null || hv.Empty || hv.Binary || !plain(h.Key, ",:") || !plain(hv.Raw, ",") || h.Key == "" {
			return producerView{Why: "a header is empty, binary or holds a comma, colon, tab or line break, which the console producer's header syntax uses"}
		}
		hdr = append(hdr, h.Key+":"+hv.Raw)
	}
	line := k + "\t" + value
	props := " --property parse.key=true"
	if key.Null {
		line, props = value, ""
	}
	if len(hdr) > 0 {
		line = strings.Join(hdr, ",") + "\t" + line
		props += " --property parse.headers=true"
	}
	esc := strings.NewReplacer("\\", "\\\\", "%", "%%", "\t", "\\t")
	quote := func(s string) string { return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'" }
	return producerView{Cmd: "printf " + quote(esc.Replace(line)+`\n`) + " | kafka-console-producer.sh --bootstrap-server " + quote(addr) + " --topic " + quote(topic) + props}
}

// registryWhere says which registry decoded a value: "the built-in registry at X" or
// "the registry at X", as the broker's facts name them.
func (c *Console) registryWhere() string {
	f := c.facts
	where := f.RegistryURL
	if where == "" && c.reg != nil {
		where = c.reg.base
	}
	where = strings.TrimPrefix(strings.TrimPrefix(where, "http://"), "https://")
	if f.RegistryBuiltIn {
		return "the built-in registry at " + where
	}
	return "the registry at " + where
}
