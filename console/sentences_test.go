package console

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Sentences the console printed that were wrong (verification of 82b8752, A10): each
// is rendered the way a page renders it and read as a person would.

// "84 identical fetchs": the folded row's sentence pluralises its unit.
func TestAFoldedRowSaysItsUnitInThePlural(t *testing.T) {
	for unit, want := range map[string]string{"fetch": "fetches", "heartbeat": "heartbeats", "offset fetch": "offset fetches", "report": "reports", "request": "requests"} {
		if got := pluralWord(unit); got != want {
			t.Errorf("pluralWord(%q) = %q, want %q", unit, got, want)
		}
	}
	src := &fakeTraffic{events: []TrafficEvent{{ID: "f1", Seq: 1, First: tt0, Time: tt0.Add(84 * time.Second), Count: 84, ClientID: "reader", Remote: "127.0.0.1:50001",
		Principal: "User:ANONYMOUS", API: "Fetch", Version: 13, Topics: []string{"orders"}, Duration: time.Millisecond}}}
	c, _ := newTestConsoleAddr(t, func(o *Options) { o.Traffic = src })
	page := readable(req(t, c, "GET", "/_console/inspect/request?key=f1", nil).Body.String())
	if !strings.Contains(page, "84 identical fetches are shown as one row") {
		t.Errorf("the folded fetch does not say \"84 identical fetches\":\n%s", page)
	}
}

// A registry code is explained by what that code means: 42201 is a schema the
// registry could not parse, not a compatibility level or a mode saying no.
func TestARegistryRefusalSaysWhatItsCodeMeans(t *testing.T) {
	for code, want := range map[int]string{
		409:   "not compatible",
		42201: "could not read the schema",
		42202: "not a version",
		42203: "not a compatibility level",
		42204: "not a mode",
		42205: "mode does not permit",
		42206: "references",
	} {
		status := 422
		if code == 409 {
			status = 409
		}
		r := registryRefusal(&regError{Status: status, Code: code, Message: "m"}, errors.New("m"))
		if !strings.Contains(r.Rule, want) {
			t.Errorf("%d: rule %q, want it to say %q", code, r.Rule, want)
		}
		if code != 409 && code != 42205 && (strings.Contains(r.Rule, "compatibility level") && code != 42203 || strings.Contains(r.Rule, "mode") && code != 42204) {
			t.Errorf("%d is explained as a compatibility or mode refusal: %q", code, r.Rule)
		}
	}
}

// When the broker says why (its ErrorMessage), Kafka's stock text for the code is not
// added after it as "the rule": INVALID_PARTITIONS for a count above the cap read
// "2004 partitions is above this broker's limit of 1000 … Number of partitions is below 1."
func TestTheBrokersOwnReasonIsNotFollowedByKafkasStockText(t *testing.T) {
	msg := "2004 partitions is above this broker's limit of 1000 (--max-partitions)"
	r := refusalFor(&kafkaRefusal{Err: kerr.InvalidPartitions, Msg: msg})
	if r.Message != msg || strings.Contains(r.Rule, "below 1") {
		t.Errorf("message %q, rule %q", r.Message, r.Rule)
	}
	// With no message of the broker's, Kafka's description is what there is to say.
	if r := refusalFor(kerr.InvalidPartitions); r.Rule == "" {
		t.Error("a bare INVALID_PARTITIONS has no rule")
	}
}

// "from recent requests, the oldest just now old".
func TestWrittenBySaysHowFarBackInWords(t *testing.T) {
	now := time.Now()
	c := &Console{traffic: &fakeTraffic{events: []TrafficEvent{{ID: "a", Seq: 1, First: now, Time: now, Count: 1, API: "Produce"}}}}
	short, long := c.ringText(now)
	if strings.Contains(short+long, "just now old") || strings.Contains(short+long, "back just now") {
		t.Errorf("%q / %q", short, long)
	}
	c = &Console{traffic: &fakeTraffic{events: []TrafficEvent{{ID: "a", Seq: 1, First: now.Add(-43 * time.Second), Time: now.Add(-43 * time.Second), Count: 1, API: "Produce"}}}}
	if short, _ := c.ringText(now); short != "from the last 43s of requests" {
		t.Errorf("short = %q", short)
	}
}

// The facts sentence named a schema for any topic with a subject called <topic>-value
// ("Avro value with schema harbour-alerts-value v1" over plain JSON records). It says
// what the newest record is, read from its bytes, or nothing.
func TestTheFactsSentenceSaysOnlyWhatTheRecordsAre(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "plain-value", userV1)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"plain"}, "partitions": {"1"}})
	produceTo(t, c, "plain", 0, 2) // plain text values, no schema id in them.
	page := readable(req(t, c, "GET", "/_console/topics/plain", nil).Body.String())
	facts := between(page, "1 partition,", ".\n")
	if strings.Contains(facts, "schema") || strings.Contains(facts, "Avro") {
		t.Errorf("the facts name a schema the records do not carry: %q", facts)
	}

	register(t, c, "framed-value", userV1)
	register(t, c, "framed-value", userV2)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"framed"}, "partitions": {"1"}})
	if rec := req(t, c, "POST", "/_console/topics/framed/produce", url.Values{"partition": {"0"}, "partition_ctx": {"0"}, "value": {`{"id": 7, "email": null}`}, "schema_subject": {"framed-value"}}); rec.Code != 200 {
		t.Fatalf("produce: %d %s", rec.Code, rec.Body)
	}
	page = readable(req(t, c, "GET", "/_console/topics/framed", nil).Body.String())
	if !strings.Contains(page, "an Avro value with schema framed-value v2") {
		t.Errorf("the facts do not say what the newest record is: %q", between(page, "1 partition,", ".\n"))
	}
}

// "kept with no  limit": retention.ms -1 is no limit by time (retention.bytes may still
// limit it), and the sentence says that.
func TestUnlimitedRetentionIsSaidOnce(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"forever"}, "partitions": {"1"}, "retention.ms": {"-1"}})
	page := readable(req(t, c, "GET", "/_console/topics/forever", nil).Body.String())
	if !strings.Contains(page, "kept with no time limit") || strings.Contains(page, "no  limit") {
		t.Errorf("facts: %s", between(page, "1 partition", "."))
	}
}
