package console

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Every kind of error becomes a refusal with the status and the words a person reads.
func TestRefusalForSortsErrors(t *testing.T) {
	storage := &kafkaRefusal{Err: kerr.KafkaStorageError}
	for name, tc := range map[string]struct {
		err     error
		kind    RefusalKind
		status  int
		heading string
		retry   bool
		notFnd  bool
		message func(string) bool
	}{
		"nothing":                  {nil, Refused, 404, "Not found", false, true, func(m string) bool { return strings.Contains(m, "nothing to show") }},
		"not found":                {fmt.Errorf("%w: no topic", errNotFound), Refused, 404, "Not found", false, true, func(m string) bool { return strings.Contains(m, "no topic") }},
		"bad input":                {badInput("enter a number"), Refused, 400, "Check the form", false, false, func(m string) bool { return strings.Contains(m, "enter a number") }},
		"deadline":                 {context.DeadlineExceeded, Failed, 502, "Can't reach the broker", true, false, func(m string) bool { return strings.Contains(m, "could not get an answer") }},
		"cancelled":                {fmt.Errorf("x: %w", context.Canceled), Failed, 502, "Can't reach the broker", true, false, nil},
		"unknown":                  {errors.New("boom"), Failed, 500, "Something went wrong", true, false, func(m string) bool { return m == "boom" }},
		"unknown topic":            {kerr.UnknownTopicOrPartition, Refused, 404, "Not found", false, true, nil},
		"unknown topic id":         {kerr.UnknownTopicID, Refused, 404, "Not found", false, true, nil},
		"unknown group":            {kerr.GroupIDNotFound, Refused, 404, "Not found", false, true, nil},
		"sasl":                     {kerr.SaslAuthenticationFailed, Denied, 502, "The broker refused the console's login", false, false, func(m string) bool { return strings.Contains(m, "credentials were rejected") }},
		"denied":                   {kerr.TopicAuthorizationFailed, Denied, 403, "Denied", false, false, nil},
		"not implemented":          {kerr.UnsupportedVersion, NotImplemented, 501, "Not implemented", false, false, nil},
		"failed":                   {kerr.RequestTimedOut, Failed, 503, "The broker could not do that right now", true, false, func(m string) bool { return strings.HasSuffix(m, ".") }},
		"refused":                  {kerr.InvalidPartitions, Refused, 400, "The broker refused that", false, false, nil},
		"storage":                  {storage, Failed, 503, "The broker could not do that right now", true, false, func(m string) bool { return strings.Contains(m, "may already be in the log") }},
		"with the brokers words":   {&kafkaRefusal{Err: kerr.InvalidPartitions, Msg: "partitions must be at least 1"}, Refused, 400, "The broker refused that", false, false, func(m string) bool { return m == "partitions must be at least 1" }},
		"storage with words":       {&kafkaRefusal{Err: kerr.KafkaStorageError, Msg: "disk full"}, Failed, 503, "The broker could not do that right now", true, false, func(m string) bool { return m == "disk full." }},
		"unknown topic with words": {&kafkaRefusal{Err: kerr.UnknownTopicOrPartition, Msg: "no such topic here"}, Refused, 404, "Not found", false, true, func(m string) bool { return m == "no such topic here" }},
	} {
		got := refusalFor(tc.err)
		if got.Kind != tc.kind || got.Status != tc.status || got.Heading != tc.heading || got.Retry != tc.retry || got.NotFound != tc.notFnd || tc.message != nil && !tc.message(got.Message) {
			t.Errorf("%s: %+v", name, got)
		}
	}
	// The broker's own words replace its stock sentence, and the rule with it.
	if r := refusalFor(&kafkaRefusal{Err: kerr.InvalidPartitions, Msg: "x"}); r.Rule != "" {
		t.Errorf("rule kept beside the broker's words: %q", r.Rule)
	}
	if r := refusalFor(kerr.InvalidPartitions); r.Rule == "" || r.Source != SourceKafka || r.Name != "INVALID_PARTITIONS" || r.Code != int(kerr.InvalidPartitions.Code) {
		t.Errorf("%+v", r)
	}
	if r := refusalFor(&kafkaRefusal{Err: kerr.KafkaStorageError, Msg: "x"}); strings.Contains(r.Message, "may already") {
		t.Errorf("the storage warning is added to the broker's own words: %q", r.Message)
	}
}

func TestRegistryRefusals(t *testing.T) {
	for _, tc := range []struct {
		status, code int
		kind         RefusalKind
		wantStatus   int
		heading      string
		retry        bool
		rule         bool
	}{
		{404, 40401, Refused, 404, "Not found", false, false},
		{401, 0, Denied, 403, "Denied", false, false},
		{403, 0, Denied, 403, "Denied", false, false},
		{501, 0, NotImplemented, 501, "Not implemented", false, false},
		{500, 50001, Failed, 502, "The schema registry failed", true, false},
		{499, 0, Refused, 400, "The schema registry refused that", false, false},
		{500, 0, Failed, 502, "The schema registry failed", true, false},
		{409, 409, Refused, 400, "The schema registry refused that", false, true},
		{422, 42201, Refused, 400, "The schema registry refused that", false, true},
		{400, 12345, Refused, 400, "The schema registry refused that", false, false},
	} {
		re := &regError{Status: tc.status, Code: tc.code, Message: "m"}
		got := refusalFor(re)
		if got.Kind != tc.kind || got.Status != tc.wantStatus || got.Heading != tc.heading || got.Retry != tc.retry || (got.Rule != "") != tc.rule || got.Source != SourceRegistry {
			t.Errorf("%d/%d: %+v", tc.status, tc.code, got)
		}
		if tc.code != 0 && got.Name != fmt.Sprint(tc.code) || tc.code == 0 && got.Name != "" {
			t.Errorf("%d/%d: name %q", tc.status, tc.code, got.Name)
		}
	}
	for code, want := range map[int]string{42202: "positive number", 42203: "compatibility level", 42204: "mode the registry knows", 42205: "does not permit", 42206: "references this version", 42207: "malformed", 409: "not compatible"} {
		if got := registryRule(code); !strings.Contains(got, want) {
			t.Errorf("registryRule(%d) = %q", code, got)
		}
	}
	if registryRule(1) != "" {
		t.Error("a rule for a code with none")
	}
}

func TestKafkaRefusalText(t *testing.T) {
	if got := (&kafkaRefusal{Err: kerr.InvalidPartitions, Msg: "why"}).Error(); got != kerr.InvalidPartitions.Message+": why" {
		t.Errorf("%q", got)
	}
	if got := (&kafkaRefusal{Err: kerr.InvalidPartitions}).Error(); got != kerr.InvalidPartitions.Error() {
		t.Errorf("%q", got)
	}
	if !errors.Is(&kafkaRefusal{Err: kerr.InvalidPartitions}, kerr.InvalidPartitions) {
		t.Error("a refusal does not unwrap to its error")
	}
}
