package settings

import (
	"fmt"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// Lessons are what the console says beneath a field, in the design's words. They
// live here, with the record they teach, and not in a template: a template names
// the record and the sentence is read from the table (plan A3).
//
// A setting with no lesson is described from its own fields (see Say), so a setting
// added without one still reads as a sentence and never as "unit: x; range: y".

func fixed(s string) func(config.Config) string { return func(config.Config) string { return s } }

var lessons = map[string]func(config.Config) string{
	key(Topic, "retention.ms"):            fixed("How long a segment is kept. 7 d is 604800000. -1 keeps records for ever."),
	key(Topic, "retention.bytes"):         fixed("Per partition. -1 means no size limit."),
	key(Topic, "segment.bytes"):           fixed("Retention trims whole segments, so smaller acts sooner."),
	key(Topic, "max.message.bytes"):       fixed("A bigger batch is refused with MESSAGE_TOO_LARGE."),
	key(Topic, "cleanup.policy"):          fixed("compact keeps the latest record of each key; records with no key are then refused."),
	key(Topic, "message.timestamp.type"):  fixed("Whose clock stamps a record: the producer's or the broker's."),
	key(Broker, "replication-factor"):     fixed("Always 1. This is one broker with no followers to copy to. A request for more is accepted and the topic is made with its one replica; a single Apache Kafka broker would answer INVALID_REPLICATION_FACTOR."),
	key(Group, "new-offset"):              fixed("An offset below the log start or above the log end is refused before it is sent. The log end itself means \"caught up\"."),
	key(Broker, "max-partitions"): func(c config.Config) string {
		const tail = "Partitions can be added later and never removed; a key may then hash to a different partition."
		if c.MaxPartitions > 0 {
			return fmt.Sprintf("1 to %d. This is the broker's cap. %s", c.MaxPartitions, tail)
		}
		return "1 or more. " + tail
	},
}

// Say is the sentence or two a field's help says: the lesson the table holds for it,
// or, for a record without one, a description built from the record's own meaning,
// range, unit, default and whether the broker acts on it. cfg supplies what only the
// running broker knows (the partition cap).
func (s Setting) Say(cfg config.Config) string {
	if f := lessons[key(s.Kind, s.Name)]; f != nil {
		if t := f(cfg); t != "" {
			return t
		}
	}
	return s.describe()
}

func (s Setting) describe() string {
	var parts []string
	if s.Meaning != "" {
		m := strings.TrimSuffix(s.Meaning, ".")
		parts = append(parts, strings.ToUpper(m[:1])+m[1:]+".")
	}
	switch {
	case len(s.Choices) > 0:
		parts = append(parts, "One of "+strings.Join(s.Choices, ", ")+".")
	case s.Range != "" && s.Unit != "":
		parts = append(parts, "Accepts "+s.Range+", in "+s.Unit+".")
	case s.Range != "":
		parts = append(parts, "Accepts "+s.Range+".")
	case s.Unit != "":
		parts = append(parts, "In "+s.Unit+".")
	}
	if s.DefaultNote != "" {
		parts = append(parts, "Default: "+s.DefaultNote+".")
	}
	if s.KafkaDefault != "" {
		parts = append(parts, "Kafka's own default is "+s.KafkaDefault+".")
	}
	if e := s.Effect.Sentence(); e != "" {
		parts = append(parts, e)
	}
	return strings.Join(parts, " ")
}

// Sentence is the effect as a sentence, for a description.
func (e Effect) Sentence() string {
	switch e {
	case Applied:
		return "The broker acts on it."
	case AppliedCompacted:
		return "The broker acts on it for a compacted topic and only stores it otherwise."
	case Stored:
		return "Stored and reported; it changes nothing here."
	case StartupOnly:
		return "Set at start-up; a restart changes it."
	case NotImplemented:
		return "Not implemented: it is named and refused."
	}
	return ""
}
