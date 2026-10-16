package console

// Confirms that tell the truth. Every destructive action confirms, names the verb
// and the thing, and says exactly what goes with it. The sentence is not typed into
// a template: confirmTable below lists each destructive route with the words its
// confirm shows, built from numbers the console measured on the broker when it drew
// the page ("the committed offsets of 2 groups" is a count of groups, not a phrase).
// confirms_test.go then performs each action against a real broker and checks that
// what happened is what the sentence said.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// confirmFacts are the measured numbers a sentence is built from. Which are
// filled depends on the action.
type confirmFacts struct {
	Name       string // the topic, group or subject.
	Topic      string
	Partition  int32
	Partitions int // partitions of the topic.
	Records    int64
	// Counts and Ends are each partition's records and log-end offset, by partition.
	Counts, Ends []int64
	Groups       []string // groups with committed offsets on the topic.
	Behind       []string // of those, groups with records still unread.
	Lags         []int64  // the records each of Behind has not read.
	Unread       int64    // records those groups have not read.
	// Disk is the topic's files, when the console knows its data directory (DiskOK);
	// DiskExact says the segments begin at the log start, so their bytes are the
	// records' and no more.
	Disk      topicDisk
	DiskOK    bool
	DiskExact bool
	DataDir   string
	// Subjects are the topic's registry subjects (<topic>-key, <topic>-value).
	Subjects []string
	// Writers are the clients the recorder saw produce to the topic.
	Writers []string
	// AutoKnown says the console was told whether auto-create is on (AutoCreate).
	AutoKnown, AutoCreate bool
	Offsets               int // committed offsets a group holds.
	Topics                int // topics those offsets are on.
	Moves                 int // offsets a reset would change.
	Add                   int
	Versions              int
	Version               int
	NewLatest             int // the version that is latest once Version is deleted; 0 when none is left.
	Committed             int64
	ResetTarget           string // "earliest" or "latest".
}

// confirmSeg is a run of a confirm's text: plain, in the mono face (a name, a
// number, an error code) or bold. The dialog draws each with textContent, so a
// topic's name can never be markup.
type confirmSeg struct {
	T string `json:"t"`
	M bool   `json:"m,omitempty"`
	B bool   `json:"b,omitempty"`
}

// confirmLine is a row of a confirm's table (Tp-24): what goes or stays, and how.
type confirmLine struct {
	Label string       `json:"label"`
	Value []confirmSeg `json:"value"`
}

// confirmSay is everything a confirm shows: its title, its paragraph, its table and
// the words on the button that does it.
type confirmSay struct {
	Title string
	Body  []confirmSeg
	Rows  []confirmLine
	Verb  string
}

// Text is the whole confirm as one string: the dialog's text when the console
// cannot be asked (the button's hx-confirm), and what a screen reader reads.
func (s confirmSay) Text() string {
	var b strings.Builder
	b.WriteString(s.Title)
	b.WriteString(". ")
	b.WriteString(segsText(s.Body))
	for _, r := range s.Rows {
		b.WriteString(" ")
		b.WriteString(r.Label)
		b.WriteString(": ")
		b.WriteString(segsText(r.Value))
		b.WriteString(".")
	}
	return b.String()
}

func segsText(s []confirmSeg) string {
	var b strings.Builder
	for _, x := range s {
		b.WriteString(x.T)
	}
	return b.String()
}

func txt(s string) confirmSeg  { return confirmSeg{T: s} }
func mono(s string) confirmSeg { return confirmSeg{T: s, M: true} }
func bold(s string) confirmSeg { return confirmSeg{T: s, B: true} }

// confirmSpec is one destructive route and what its confirm says.
type confirmSpec struct {
	ID     string
	Method string
	Path   string // as registered, with {name}, {id}, {subject} wildcards.
	// Say is the confirm, from the measured facts.
	Say func(f confirmFacts) confirmSay
}

func nounCount(n int, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

func count64(n int64, one, many string) string {
	if n == 1 {
		return fmt.Sprintf("%d %s", n, one)
	}
	return fmt.Sprintf("%d %s", n, many)
}

// numCount is count64 with the number grouped as the pages write it: "14,429 records".
func numCount(n int64, one, many string) string {
	if n == 1 {
		return "1 " + one
	}
	return num(n) + " " + many
}

func nameList(names []string) string {
	if len(names) == 0 {
		return ""
	}
	shown := names
	if len(shown) > 3 {
		shown = shown[:3]
	}
	s := strings.Join(shown, ", ")
	if len(names) > len(shown) {
		s += fmt.Sprintf(" and %d more", len(names)-len(shown))
	}
	return s
}

// monoNames is a list of names in the mono face, "a and b", "a, b and c", "a, b, c
// and 4 more".
func monoNames(names []string) []confirmSeg {
	shown := names
	if len(shown) > 3 {
		shown = shown[:3]
	}
	var out []confirmSeg
	for i, n := range shown {
		switch {
		case i == 0:
		case i == len(shown)-1 && len(names) == len(shown):
			out = append(out, txt(" and "))
		default:
			out = append(out, txt(", "))
		}
		out = append(out, mono(n))
	}
	if more := len(names) - len(shown); more > 0 {
		out = append(out, txt(fmt.Sprintf(" and %d more", more)))
	}
	return out
}

// sentence is a confirm that is one paragraph under a fixed title.
func sentence(title, verb string, f func(confirmFacts) string) func(confirmFacts) confirmSay {
	return func(x confirmFacts) confirmSay {
		return confirmSay{Title: title, Verb: verb, Body: []confirmSeg{txt(f(x))}}
	}
}

// maxListedPartitions is how many partitions a confirm names one by one; a topic
// with more is described in totals.
const maxListedPartitions = 8

// confirmTable is every destructive route of the console. A route that deletes or
// moves something and is not here fails TestEveryDestructiveRouteHasAConfirm.
var confirmTable = []confirmSpec{
	{ID: "topic.delete", Method: "POST", Path: "/topics/{name}/delete", Say: topicDeleteSay},
	{ID: "topic.purge", Method: "POST", Path: "/topics/{name}/purge", Say: topicPurgeSay},
	{
		ID: "topic.partitions", Method: "POST", Path: "/topics/{name}/partitions",
		Say: func(f confirmFacts) confirmSay {
			say := confirmSay{Title: "Add partitions", Verb: "Add partitions"}
			if f.Add > 0 {
				say.Verb = "Add " + nounCount(f.Add, "partition", "partitions")
				say.Body = []confirmSeg{txt(fmt.Sprintf("Add %s to %q, which has %s now, %d after? ", nounCount(f.Add, "partition", "partitions"), f.Name, nounCount(f.Partitions, "partition", "partitions"), f.Partitions+f.Add))}
			} else {
				say.Body = []confirmSeg{txt(fmt.Sprintf("Add the number of partitions entered to %q, which has %s now? ", f.Name, nounCount(f.Partitions, "partition", "partitions")))}
			}
			say.Body = append(say.Body, txt("They cannot be removed afterwards, and a key may hash to a different partition than before, so records with the same key stop being ordered together across the change."))
			return say
		},
	},
	{
		ID: "group.delete", Method: "POST", Path: "/groups/{id}/delete",
		Say: sentence("Delete group", "Delete group", func(f confirmFacts) string {
			if f.Offsets == 0 {
				return fmt.Sprintf("Delete group %q? It has no committed offsets.", f.Name)
			}
			return fmt.Sprintf("Delete group %q and its %s on %s?", f.Name, nounCount(f.Offsets, "committed offset", "committed offsets"), nounCount(f.Topics, "topic", "topics"))
		}),
	},
	{
		ID: "group.reset.earliest", Method: "POST", Path: "/groups/{id}/reset",
		Say: sentence("Reset to earliest", "Reset offsets", func(f confirmFacts) string {
			return fmt.Sprintf("Reset the committed offsets of %q to the earliest retained offset? This moves %s of its %d%s; its consumers will re-read from the start.",
				f.Name, nounCount(f.Moves, "offset", "offsets"), f.Offsets, scopeNote(f))
		}),
	},
	{
		ID: "group.reset.latest", Method: "POST", Path: "/groups/{id}/reset",
		Say: sentence("Reset to latest", "Reset offsets", func(f confirmFacts) string {
			return fmt.Sprintf("Reset the committed offsets of %q to the latest offset? This moves %s of its %d%s; its consumers will skip everything currently in the topics.",
				f.Name, nounCount(f.Moves, "offset", "offsets"), f.Offsets, scopeNote(f))
		}),
	},
	{
		ID: "group.offset.delete", Method: "POST", Path: "/groups/{id}/offset/delete",
		Say: sentence("Remove offset", "Remove", func(f confirmFacts) string {
			return fmt.Sprintf("Remove the committed offset of %q for %s partition %d (it is at %d)? A consumer will start there from its auto.offset.reset policy.",
				f.Name, f.Topic, f.Partition, f.Committed)
		}),
	},
	{
		ID: "schema.version.delete", Method: "POST", Path: "/schemas/{subject}/delete",
		Say: sentence("Delete version", "Delete version", func(f confirmFacts) string {
			return fmt.Sprintf("Delete version %d of %q? It is one of %d; the other %d stay and version %d becomes the latest.",
				f.Version, f.Name, f.Versions, f.Versions-1, f.NewLatest)
		}),
	},
	{
		ID: "schema.delete", Method: "POST", Path: "/schemas/{subject}/delete",
		Say: sentence("Delete subject", "Delete subject", func(f confirmFacts) string {
			return fmt.Sprintf("Delete subject %q with all %s? Messages already written with them stay on their topics, but nothing can decode them by id afterwards.",
				f.Name, nounCount(f.Versions, "version", "versions"))
		}),
	},
}

// topicPurgeSay is Tp-24's purge: DeleteRecords to the high watermark of every
// partition (backend.PurgeTopic). Each value is the topic's, measured.
func topicPurgeSay(f confirmFacts) confirmSay {
	say := confirmSay{Title: "Purge topic " + f.Name, Verb: "Purge " + numCount(f.Records, "record", "records")}
	body := []confirmSeg{}
	if f.Partitions == 1 {
		body = append(body, txt("Deletes every record in its 1 partition, up to the high watermark: "), mono(num(f.Records)))
	} else {
		body = append(body, txt(fmt.Sprintf("Deletes every record in all %d partitions, up to the high watermark: ", f.Partitions)))
		if len(f.Counts) == f.Partitions && f.Partitions <= maxListedPartitions {
			parts := make([]string, len(f.Counts))
			for i, n := range f.Counts {
				parts[i] = num(n)
			}
			body = append(body, mono(strings.Join(parts, " + ")+" = "+num(f.Records)))
		} else {
			body = append(body, mono(num(f.Records)))
		}
	}
	if f.Records == 1 {
		body = append(body, txt(" record"))
	} else {
		body = append(body, txt(" records"))
	}
	switch {
	case f.DiskOK && f.DiskExact:
		body = append(body, txt(", about "+diskSize(f.Disk.LogBytes)+"."))
	case f.DiskOK:
		// The oldest segments begin before the log start (an earlier purge left them):
		// their bytes include records already deleted.
		body = append(body, txt("; the segments that hold them take "+diskSize(f.Disk.LogBytes)+" on disk."))
	default:
		body = append(body, txt("."))
	}
	body = append(body, txt(" This is DeleteRecords on each partition."))
	say.Body = body

	kept := fmt.Sprintf("the topic and its id, %s, config, ACLs", nounCount(f.Partitions, "partition", "partitions"))
	switch len(f.Subjects) {
	case 0:
		kept = strings.Replace(kept, ", config, ACLs", ", config and ACLs", 1)
	case 1:
		kept += ", and the schema subject"
	default:
		kept += ", and the schema subjects"
	}
	say.Rows = append(say.Rows, confirmLine{Label: "Kept", Value: []confirmSeg{txt(kept)}})
	if len(f.Groups) > 0 {
		say.Rows = append(say.Rows, confirmLine{Label: "Groups", Value: []confirmSeg{txt("keep their committed offsets; a group that had read everything carries on with the next record")}})
	} else {
		say.Rows = append(say.Rows, confirmLine{Label: "Groups", Value: []confirmSeg{txt("no group has committed offsets on it")}})
	}
	if len(f.Ends) == f.Partitions && f.Partitions <= maxListedPartitions {
		ends := make([]string, len(f.Ends))
		for i, e := range f.Ends {
			ends[i] = num(e)
		}
		say.Rows = append(say.Rows, confirmLine{Label: "New log start", Value: []confirmSeg{mono(strings.Join(ends, " / "))}})
	} else {
		say.Rows = append(say.Rows, confirmLine{Label: "New log start", Value: []confirmSeg{txt("each partition's log end")}})
	}
	// After the purge the log starts at its old end, so a group that had not read to
	// the end has committed below the start. Its next fetch there is answered
	// OFFSET_OUT_OF_RANGE (TestTopicConfirmsAreTp24 fetches it), and a consumer then
	// resets by auto.offset.reset: earliest and latest are both the new start, as the
	// log is empty; with none it fails (the Kafka consumer's documentation of
	// auto.offset.reset).
	var behind []confirmSeg
	switch {
	case len(f.Behind) > 0:
		shown := f.Behind
		if len(shown) > 3 {
			shown = shown[:3]
		}
		for i, g := range shown {
			if i > 0 {
				if i == len(shown)-1 && len(shown) == len(f.Behind) {
					behind = append(behind, txt(" and "))
				} else {
					behind = append(behind, txt(", "))
				}
			}
			behind = append(behind, mono(g), txt(" (lag "+num(f.Lags[i])+")"))
		}
		if more := len(f.Behind) - len(shown); more > 0 {
			behind = append(behind, txt(fmt.Sprintf(" and %d more", more)))
		}
		verb := " is moved"
		if len(f.Behind) > 1 {
			verb = " are moved"
		}
		behind = append(behind, txt(verb+" to the new start on its next fetch, with "), mono("OFFSET_OUT_OF_RANGE"), txt(" if it has no reset policy"))
		if len(f.Behind) > 1 {
			behind[len(behind)-3] = txt(verb + " to the new start on their next fetch, with ")
			behind[len(behind)-1] = txt(" if they have no reset policy")
		}
	case len(f.Groups) > 0:
		behind = []confirmSeg{txt("none: every group had read to the end")}
	default:
		behind = []confirmSeg{txt("none: no group has committed offsets on it")}
	}
	say.Rows = append(say.Rows, confirmLine{Label: "Readers behind", Value: behind})
	return say
}

// topicDeleteSay is Tp-24's delete.
func topicDeleteSay(f confirmFacts) confirmSay {
	say := confirmSay{Title: "Delete topic " + f.Name, Verb: "Delete topic",
		Body: []confirmSeg{txt("The topic is removed. A new topic with the same name would get a new id and start at offset 0.")}}
	data := "all " + numCount(f.Records, "record", "records") + " and "
	if f.DataDir != "" {
		data += "the " + nounCount(f.Partitions, "partition log", "partition logs") + " under " + f.DataDir + " are deleted"
	} else {
		data += "its " + nounCount(f.Partitions, "partition log", "partition logs") + " are deleted"
	}
	say.Rows = append(say.Rows, confirmLine{Label: "Data", Value: []confirmSeg{txt(data)}})
	if len(f.Groups) > 0 {
		v := append([]confirmSeg{txt("committed offsets of ")}, monoNames(f.Groups)...)
		say.Rows = append(say.Rows, confirmLine{Label: "Offsets", Value: append(v, txt(" for this topic are deleted with it"))})
	} else {
		say.Rows = append(say.Rows, confirmLine{Label: "Offsets", Value: []confirmSeg{txt("no group has committed offsets on it")}})
	}
	if len(f.Subjects) > 0 {
		v := monoNames(f.Subjects)
		if len(f.Subjects) == 1 {
			v = append(v, txt(" is "), bold("kept"), txt(" in the registry; delete it from Schemas if you want it gone"))
		} else {
			v = append(v, txt(" are "), bold("kept"), txt(" in the registry; delete them from Schemas if you want them gone"))
		}
		say.Rows = append(say.Rows, confirmLine{Label: f.subjectLabel(), Value: v})
	}
	// A producer that names a topic that does not exist is answered
	// UNKNOWN_TOPIC_OR_PARTITION, and a Metadata request that allows auto-creation
	// makes it when auto-create is on (recorded: produce-mixed-partition-errors,
	// metadata-autocreate).
	if len(f.Writers) > 0 {
		v := append(monoNames(f.Writers), txt(" will get "), mono("UNKNOWN_TOPIC_OR_PARTITION"))
		switch {
		case !f.AutoKnown:
			v = append(v, txt(", or recreate the topic if auto-create is on"))
		case f.AutoCreate:
			v = append(v, txt(", or recreate the topic: auto-create is on"))
		default:
			v = append(v, txt(": auto-create is off"))
		}
		say.Rows = append(say.Rows, confirmLine{Label: "Still running", Value: v})
	}
	return say
}

func (f confirmFacts) subjectLabel() string {
	if len(f.Subjects) > 1 {
		return "Schema subjects"
	}
	return "Schema subject"
}

// scopeNote says what the count covers when a group has offsets on several topics:
// the page can reset one topic only, and the count is of all of them.
func scopeNote(f confirmFacts) string {
	if f.Topics > 1 {
		return fmt.Sprintf(" (counted over all %d topics; choosing one topic moves only its offsets)", f.Topics)
	}
	return ""
}

// confirmByID finds a spec.
func confirmByID(id string) confirmSpec {
	for _, s := range confirmTable {
		if s.ID == id {
			return s
		}
	}
	panic("console: no confirm " + id)
}

// confirmView is what a template reads for one confirm: the button carries the
// whole of it as a fallback (Text), with its title and verb.
type confirmView struct {
	Text, Title, Verb string
	Say               confirmSay
}

func viewOf(id string, f confirmFacts) confirmView {
	s := confirmByID(id).Say(f)
	return confirmView{Text: s.Text(), Title: s.Title, Verb: s.Verb, Say: s}
}

// topicFacts measures what deleting, purging or growing a topic touches.
func (c *Console) topicFacts(ctx context.Context, name string, partitions int) confirmFacts {
	f := confirmFacts{Name: name, Partitions: partitions, DataDir: c.facts.DataDir,
		AutoKnown: c.facts.KafkaMajor > 0, AutoCreate: c.facts.AutoCreateTopics}
	if bounds, err := c.be.PartitionBounds(ctx, name, int32(partitions)); err == nil {
		f.DiskExact = true
		f.Disk, f.DiskOK = c.topicDisk(name, partitions)
		for i, b := range bounds {
			f.Records += b.Count()
			f.Counts = append(f.Counts, b.Count())
			f.Ends = append(f.Ends, b.Latest)
			if f.DiskOK && i < len(f.Disk.Bases) && f.Disk.Bases[i] >= 0 && f.Disk.Bases[i] < b.Earliest {
				f.DiskExact = false
			}
		}
	}
	if cons, err := c.be.TopicConsumers(ctx, name); err == nil {
		sort.Slice(cons, func(i, j int) bool { return cons[i].Group < cons[j].Group })
		for _, tc := range cons {
			f.Groups = append(f.Groups, tc.Group)
			if tc.Lag > 0 {
				f.Behind = append(f.Behind, tc.Group)
				f.Lags = append(f.Lags, tc.Lag)
				f.Unread += tc.Lag
			}
		}
	}
	if c.reg != nil {
		for _, s := range c.reg.SubjectNames(ctx) {
			if s == name+"-key" || s == name+"-value" {
				f.Subjects = append(f.Subjects, s)
			}
		}
	}
	for _, w := range c.topicWriters(name) {
		f.Writers = append(f.Writers, w.Client)
	}
	return f
}

// topicConfirms are the confirms of a topic's page.
func (c *Console) topicConfirms(ctx context.Context, name string, partitions int) map[string]confirmView {
	f := c.topicFacts(ctx, name, partitions)
	return map[string]confirmView{
		"topic.delete":     viewOf("topic.delete", f),
		"topic.purge":      viewOf("topic.purge", f),
		"topic.partitions": viewOf("topic.partitions", f),
	}
}

// groupConfirms are the confirms of a group's page, from the offsets the page
// already computed.
func groupConfirms(id string, rows []groupLagRow) map[string]confirmView {
	f := confirmFacts{Name: id}
	topics := map[string]bool{}
	var toEarliest, toLatest int
	for _, r := range rows {
		f.Offsets++
		topics[r.Topic] = true
		if r.Deleted {
			continue // a reset skips an offset whose topic is gone.
		}
		if r.Committed != r.Earliest {
			toEarliest++
		}
		if r.Committed != r.Latest {
			toLatest++
		}
	}
	f.Topics = len(topics)
	e, l := f, f
	e.Moves, l.Moves = toEarliest, toLatest
	return map[string]confirmView{
		"group.delete":         viewOf("group.delete", f),
		"group.reset.earliest": viewOf("group.reset.earliest", e),
		"group.reset.latest":   viewOf("group.reset.latest", l),
	}
}

// offsetDeleteConfirm is the confirm of one row's Remove.
func offsetDeleteConfirm(group, topic string, partition int32, committed int64) confirmView {
	return viewOf("group.offset.delete", confirmFacts{Name: group, Topic: topic, Partition: partition, Committed: committed})
}

// subjectConfirms are the confirms of a subject's page: all of it, and the version
// on show (when there is another to keep).
func subjectConfirms(subject string, versions []int, shown int) map[string]confirmView {
	out := map[string]confirmView{"schema.delete": viewOf("schema.delete", confirmFacts{Name: subject, Versions: len(versions)})}
	newLatest := 0
	for _, v := range versions {
		if v != shown {
			newLatest = max(newLatest, v)
		}
	}
	out["schema.version.delete"] = viewOf("schema.version.delete", confirmFacts{Name: subject, Version: shown, Versions: len(versions), NewLatest: newLatest})
	return out
}

// confirmURL is where a button's confirm fetches its sentence at the moment it is
// pressed: {{confirmURL "topic.purge" "name" .Topic.Name}}. The sentence drawn into
// the page is the fallback; it is as old as the page, and a topic gains records.
func confirmURLFor(prefix, id string, kv ...any) string {
	q := url.Values{}
	for i := 0; i+1 < len(kv); i += 2 {
		q.Set(fmt.Sprint(kv[i]), fmt.Sprint(kv[i+1]))
	}
	return prefix + "/confirm/" + id + "?" + q.Encode()
}

// confirmLive answers GET {prefix}/confirm/{id}: the confirm's title, verb and
// sentence, measured now.
func (c *Console) confirmLive(w http.ResponseWriter, r *http.Request) {
	id, q, ctx := r.PathValue("id"), r.URL.Query(), r.Context()
	var views map[string]confirmView
	switch id {
	case "topic.delete", "topic.purge", "topic.partitions":
		name := q.Get("name")
		d, err := c.be.DescribeTopic(ctx, name)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		views = c.topicConfirms(ctx, name, int(d.Partitions))
		if add, err := strconv.Atoi(q.Get("add")); err == nil && add > 0 && id == "topic.partitions" {
			f := c.topicFacts(ctx, name, int(d.Partitions))
			f.Add = add
			views[id] = viewOf(id, f)
		}
	case "group.delete", "group.reset.earliest", "group.reset.latest":
		data, err := c.groupData(r, q.Get("group"))
		if err != nil {
			c.fail(w, r, err)
			return
		}
		views = data["Confirm"].(map[string]confirmView)
	case "group.offset.delete":
		group, topic := q.Get("group"), q.Get("topic")
		p, perr := strconv.Atoi(q.Get("partition"))
		if perr != nil {
			c.fail(w, r, badInput("partition must be a number"))
			return
		}
		offs, err := c.be.GroupOffsets(ctx, group)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		for _, o := range offs {
			if o.Topic == topic && int(o.Partition) == p {
				views = map[string]confirmView{id: offsetDeleteConfirm(group, topic, o.Partition, o.Committed)}
			}
		}
		if views == nil {
			c.fail(w, r, fmt.Errorf("%w: %s has no committed offset on %s partition %d", errNotFound, group, topic, p))
			return
		}
	case "schema.delete", "schema.version.delete":
		if c.registryOff(w, r) {
			return
		}
		subject := q.Get("subject")
		versions, err := c.reg.Versions(ctx, subject)
		if err != nil {
			c.failSubject(w, r, err, subject)
			return
		}
		shown, _ := strconv.Atoi(q.Get("version"))
		views = subjectConfirms(subject, versions, shown)
	default:
		c.fail(w, r, fmt.Errorf("%w: there is no confirm %q", errNotFound, id))
		return
	}
	v, ok := views[id]
	if !ok {
		c.fail(w, r, fmt.Errorf("%w: there is no confirm %q here", errNotFound, id))
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(map[string]any{"text": v.Text, "title": v.Title, "verb": v.Verb, "body": v.Say.Body, "rows": v.Say.Rows})
}
