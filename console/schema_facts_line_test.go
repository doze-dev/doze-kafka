package console

import (
	"testing"

	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// A change the registry would refuse names the first reason it gives, when it gives one.
func TestTheFactsLineNamesTheFirstReasonAChangeIsRefused(t *testing.T) {
	why := func(findings ...checkFinding) string {
		return allowedNote(checkAnswer{Level: "BACKWARD", Findings: findings}, nil)
	}
	reason := checkFinding{Reasons: []schemaregistry.CompatReason{{Type: "READER_FIELD_MISSING_DEFAULT_VALUE"}, {Type: "TYPE_MISMATCH"}}}
	if got, want := why(reason), "the change would be refused at BACKWARD: READER_FIELD_MISSING_DEFAULT_VALUE"; got != want {
		t.Errorf("one finding with reasons: %q, want %q", got, want)
	}
	// No findings at all is a refusal with nothing to name.
	if got, want := why(), "the change would be refused at BACKWARD"; got != want {
		t.Errorf("no findings: %q, want %q", got, want)
	}
}

// With no change to read, an allowed change does not claim the new fields have defaults.
func TestTheFactsLineClaimsDefaultsOnlyForChangesThatAddFields(t *testing.T) {
	ok := checkAnswer{Level: "BACKWARD", Compatible: true}
	if got, want := allowedNote(ok, nil), "the change is allowed at BACKWARD"; got != want {
		t.Errorf("no changes: %q, want %q", got, want)
	}
	one := []changePart{{Before: "added ", Name: "email", After: " with default null"}}
	if got, want := allowedNote(ok, one), "the change is allowed at BACKWARD: the new field has a default"; got != want {
		t.Errorf("one field with a default: %q, want %q", got, want)
	}
	mixed := []changePart{{Before: "added ", Name: "email", After: " with default null"}, {Before: "removed ", Name: "x"}}
	if got, want := allowedNote(ok, mixed), "the change is allowed at BACKWARD"; got != want {
		t.Errorf("a removal beside the addition: %q, want %q", got, want)
	}
}
