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
	"fmt"
	"sort"
	"strings"
)

// confirmFacts are the measured numbers a sentence is built from. Which are
// filled depends on the action.
type confirmFacts struct {
	Name        string // the topic, group or subject.
	Topic       string
	Partition   int32
	Partitions  int // partitions of the topic.
	Records     int64
	Groups      []string // groups with committed offsets on the topic.
	Behind      []string // of those, groups with records still unread.
	Unread      int64    // records those groups have not read.
	Offsets     int      // committed offsets a group holds.
	Topics      int      // topics those offsets are on.
	Moves       int      // offsets a reset would change.
	Add         int
	Versions    int
	Version     int
	NewLatest   int // the version that is latest once Version is deleted; 0 when none is left.
	Committed   int64
	ResetTarget string // "earliest" or "latest".
}

// confirmSpec is one destructive route and what its confirm says.
type confirmSpec struct {
	ID     string
	Method string
	Path   string // as registered, with {name}, {id}, {subject} wildcards.
	Title  string
	Verb   string
	// Sentence is the words of the confirm, from the measured facts.
	Sentence func(f confirmFacts) string
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

// confirmTable is every destructive route of the console. A route that deletes or
// moves something and is not here fails TestEveryDestructiveRouteHasAConfirm.
var confirmTable = []confirmSpec{
	{
		ID: "topic.delete", Method: "POST", Path: "/topics/{name}/delete", Title: "Delete topic", Verb: "Delete topic",
		Sentence: func(f confirmFacts) string {
			s := fmt.Sprintf("Delete topic %q? This removes its %s and the %s in them.", f.Name, nounCount(f.Partitions, "partition", "partitions"), count64(f.Records, "record", "records"))
			if len(f.Groups) > 0 {
				s += fmt.Sprintf(" The committed offsets of %s (%s) on it go with it.", nounCount(len(f.Groups), "group", "groups"), nameList(f.Groups))
			}
			return s
		},
	},
	{
		ID: "topic.purge", Method: "POST", Path: "/topics/{name}/purge", Title: "Purge topic", Verb: "Purge",
		Sentence: func(f confirmFacts) string {
			s := fmt.Sprintf("Purge %q? This deletes its %s across %s. The topic keeps its ID and settings, and consumers keep their committed offsets.",
				f.Name, count64(f.Records, "record", "records"), nounCount(f.Partitions, "partition", "partitions"))
			switch {
			case len(f.Behind) == 0:
				s += " No group is behind, so none of them loses a record it has not read."
			default:
				s += fmt.Sprintf(" %s (%s) %s the new start of the log: %s have not been read.",
					nounCount(len(f.Behind), "group is", "groups are"), nameList(f.Behind), "will be behind", count64(f.Unread, "record", "records"))
			}
			return s
		},
	},
	{
		ID: "topic.partitions", Method: "POST", Path: "/topics/{name}/partitions", Title: "Add partitions", Verb: "Add partitions",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Add the number of partitions entered to %q, which has %s now? They cannot be removed afterwards, and a key may hash to a different partition than before, so records with the same key stop being ordered together across the change.",
				f.Name, nounCount(f.Partitions, "partition", "partitions"))
		},
	},
	{
		ID: "group.delete", Method: "POST", Path: "/groups/{id}/delete", Title: "Delete group", Verb: "Delete group",
		Sentence: func(f confirmFacts) string {
			if f.Offsets == 0 {
				return fmt.Sprintf("Delete group %q? It has no committed offsets.", f.Name)
			}
			return fmt.Sprintf("Delete group %q and its %s on %s?", f.Name, nounCount(f.Offsets, "committed offset", "committed offsets"), nounCount(f.Topics, "topic", "topics"))
		},
	},
	{
		ID: "group.reset.earliest", Method: "POST", Path: "/groups/{id}/reset", Title: "Reset to earliest", Verb: "Reset offsets",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Reset the committed offsets of %q to the earliest retained offset? This moves %s of its %d%s; its consumers will re-read from the start.",
				f.Name, nounCount(f.Moves, "offset", "offsets"), f.Offsets, scopeNote(f))
		},
	},
	{
		ID: "group.reset.latest", Method: "POST", Path: "/groups/{id}/reset", Title: "Reset to latest", Verb: "Reset offsets",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Reset the committed offsets of %q to the latest offset? This moves %s of its %d%s; its consumers will skip everything currently in the topics.",
				f.Name, nounCount(f.Moves, "offset", "offsets"), f.Offsets, scopeNote(f))
		},
	},
	{
		ID: "group.offset.delete", Method: "POST", Path: "/groups/{id}/offset/delete", Title: "Remove offset", Verb: "Remove",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Remove the committed offset of %q for %s partition %d (it is at %d)? A consumer will start there from its auto.offset.reset policy.",
				f.Name, f.Topic, f.Partition, f.Committed)
		},
	},
	{
		ID: "schema.version.delete", Method: "POST", Path: "/schemas/{subject}/delete", Title: "Delete version", Verb: "Delete version",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Delete version %d of %q? It is one of %d; the other %d stay and version %d becomes the latest.",
				f.Version, f.Name, f.Versions, f.Versions-1, f.NewLatest)
		},
	},
	{
		ID: "schema.delete", Method: "POST", Path: "/schemas/{subject}/delete", Title: "Delete subject", Verb: "Delete subject",
		Sentence: func(f confirmFacts) string {
			return fmt.Sprintf("Delete subject %q with all %s? Messages already written with them stay on their topics, but nothing can decode them by id afterwards.",
				f.Name, nounCount(f.Versions, "version", "versions"))
		},
	},
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

// confirmView is what a template reads for one confirm.
type confirmView struct {
	Text, Title, Verb string
}

func viewOf(id string, f confirmFacts) confirmView {
	s := confirmByID(id)
	return confirmView{Text: s.Sentence(f), Title: s.Title, Verb: s.Verb}
}

// topicFacts measures what deleting, purging or growing a topic touches.
func (c *Console) topicFacts(ctx context.Context, name string, partitions int) confirmFacts {
	f := confirmFacts{Name: name, Partitions: partitions}
	if bounds, err := c.be.PartitionBounds(ctx, name, int32(partitions)); err == nil {
		for _, b := range bounds {
			f.Records += b.Count()
		}
	}
	if cons, err := c.be.TopicConsumers(ctx, name); err == nil {
		for _, tc := range cons {
			f.Groups = append(f.Groups, tc.Group)
			if tc.Lag > 0 {
				f.Behind = append(f.Behind, tc.Group)
				f.Unread += tc.Lag
			}
		}
	}
	sort.Strings(f.Groups)
	sort.Strings(f.Behind)
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
