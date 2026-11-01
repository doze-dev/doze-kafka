package settings

// (This file is named to run after settings_test.go: TestBuiltLazily needs the table unbuilt.)

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// A field's help is a sentence in the design's words. Every record, with or without a
// lesson, says something and never shows its raw fields.
func TestEveryRecordSaysSomethingInWords(t *testing.T) {
	for _, kind := range []Kind{Topic, Broker, Registry, Console, Group} {
		for _, s := range All(kind) {
			say := s.Say(config.Config{})
			if strings.TrimSpace(say) == "" && s.Effect != Informational {
				t.Errorf("%s/%s says nothing", kind, s.Name)
			}
			for _, raw := range []string{"unit:", "range:", "; applied", "default:"} {
				if strings.Contains(say, raw) {
					t.Errorf("%s/%s leaks %q: %s", kind, s.Name, raw, say)
				}
			}
		}
	}
}

func TestPartitionLessonNamesTheCapOnlyWhenKnown(t *testing.T) {
	s, ok := Lookup(Broker, "max-partitions")
	if !ok {
		t.Fatal("no max-partitions record")
	}
	if got := s.Say(config.Config{MaxPartitions: 200}); !strings.HasPrefix(got, "1 to 200. This is the broker's cap.") {
		t.Errorf("with a cap: %q", got)
	}
	if got := s.Say(config.Config{}); !strings.HasPrefix(got, "1 or more.") {
		t.Errorf("without one: %q", got)
	}
}

func TestDescribeBuildsASentenceFromTheRecord(t *testing.T) {
	s := Setting{Name: "x.y", Kind: Topic, Meaning: "how often it rolls", Unit: "ms", Range: "1 or more", DefaultNote: "none", KafkaDefault: "5", Effect: Stored}
	want := "How often it rolls. Accepts 1 or more, in ms. Default: none. Kafka's own default is 5. Stored and reported; it changes nothing here."
	if got := s.Say(config.Config{}); got != want {
		t.Errorf("got  %q\nwant %q", got, want)
	}
	c := Setting{Name: "c", Kind: Topic, Choices: []string{"a", "b"}, Effect: AppliedCompacted}
	if got := c.Say(config.Config{}); got != "One of a, b. The broker acts on it for a compacted topic and only stores it otherwise." {
		t.Errorf("choices: %q", got)
	}
}
