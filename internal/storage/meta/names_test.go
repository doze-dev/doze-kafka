package meta

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateTopicName(t *testing.T) {
	valid := []string{"orders", "payments.events.v1", "a_b-c", "A", "0", "...", ".hidden", "-x", strings.Repeat("a", 249)}
	for _, n := range valid {
		if err := ValidateTopicName(n); err != nil {
			t.Errorf("ValidateTopicName(%q) = %v, want nil", n, err)
		}
	}
	invalid := []string{
		"", ".", "..", "../victim", "a/b", `a\b`, "bad name!", "x'+(1)+'", "with space",
		"tab\t", "nul\x00", "héllo", "a?b", "a#b", "a%b", "emoji🙂", strings.Repeat("a", 250),
	}
	for _, n := range invalid {
		err := ValidateTopicName(n)
		if !errors.Is(err, ErrInvalidTopicName) {
			t.Errorf("ValidateTopicName(%q) = %v, want ErrInvalidTopicName", n, err)
		}
	}
}

// The registry itself must refuse an invalid name, not just the validator: a
// handler that forgot to call it would otherwise reintroduce the traversal.
func TestCreateTopicRejectsInvalidNames(t *testing.T) {
	s, err := Open(t.TempDir() + "/meta.db")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.CreateTopic("../victim", 1, nil); !errors.Is(err, ErrInvalidTopicName) {
		t.Fatalf("CreateTopic(../victim) = %v, want ErrInvalidTopicName", err)
	}
	if topics, _ := s.ListTopics(); len(topics) != 0 {
		t.Fatalf("an invalid topic was registered: %+v", topics)
	}
	if _, err := s.CreateTopic("fine.topic", 1, nil); err != nil {
		t.Fatalf("CreateTopic(fine.topic): %v", err)
	}
}
