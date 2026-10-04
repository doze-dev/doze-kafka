package protocol

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// The table is the single statement of what the broker serves, so its own shape
// is worth a test: a typo here is advertised to every client.
func TestAPITableIsWellFormed(t *testing.T) {
	seen := map[APIKey]bool{}
	prev := APIKey(-1)
	for _, a := range APIs {
		name := APIName(a.Key)
		req := kmsg.RequestForKey(int16(a.Key))
		if req == nil {
			t.Errorf("key %d is not a Kafka API the protocol library knows", a.Key)
			continue
		}
		if seen[a.Key] {
			t.Errorf("%s is in the table twice", name)
		}
		seen[a.Key] = true
		if a.Key < prev {
			t.Errorf("%s is out of order: the table is sorted by key", name)
		}
		prev = a.Key
		offered := false
		for i, o := range a.Versions {
			major := i + firstMajor
			if !o.OK {
				if offered {
					t.Errorf("%s is offered by an older Kafka major and then withdrawn at Kafka %d", name, major)
				}
				continue
			}
			offered = true
			if o.Min < 0 || o.Min > o.Max {
				t.Errorf("%s at Kafka %d: range %d–%d is not a range", name, major, o.Min, o.Max)
			}
			if o.Max > req.MaxVersion() {
				t.Errorf("%s at Kafka %d: v%d is beyond what the protocol library can encode (v%d)", name, major, o.Max, req.MaxVersion())
			}
			if i > 0 && a.Versions[i-1].OK && (o.Max < a.Versions[i-1].Max || o.Min < a.Versions[i-1].Min) {
				t.Errorf("%s: Kafka %d offers %d–%d, narrower than Kafka %d", name, major, o.Min, o.Max, major-1)
			}
		}
		if !offered {
			t.Errorf("%s is offered by no profile", name)
		}
	}
	// What the profiles say follows from the table.
	for _, major := range SupportedKafkaMajors() {
		p, ok := ProfileFor(major)
		if !ok {
			t.Fatalf("no profile for Kafka %d", major)
		}
		_, hasConsumer := p.RangeFor(kmsg.ConsumerGroupHeartbeat)
		if (p.GroupProtocol == GroupClassicAndConsumer) != hasConsumer {
			t.Errorf("Kafka %d: the group protocol and the ConsumerGroupHeartbeat row disagree", major)
		}
		plain, withSASL := p.Advertised(false), p.Advertised(true)
		if len(withSASL) != len(plain)+2 {
			t.Errorf("Kafka %d: SASL adds %d APIs, want exactly SaslHandshake and SaslAuthenticate", major, len(withSASL)-len(plain))
		}
		for _, a := range plain {
			if a.Key == kmsg.SASLHandshake || a.Key == kmsg.SASLAuthenticate {
				t.Errorf("Kafka %d advertises %s without SASL configured", major, APIName(a.Key))
			}
		}
	}
	if _, ok := ProfileFor(0); ok {
		t.Error("there is no Kafka 0 profile")
	}
	if _, ok := ProfileFor(lastMajor + 1); ok {
		t.Errorf("there is no Kafka %d profile", lastMajor+1)
	}
}

// docTable renders the version table of docs/api-support.md from the API table.
func docTable() string {
	var b strings.Builder
	b.WriteString("| API | Key | Kafka 1 | Kafka 2 | Kafka 3 | Kafka 4 |\n|---|---|---|---|---|---|\n")
	for _, a := range APIs {
		fmt.Fprintf(&b, "| %s | %d |", APIName(a.Key), a.Key)
		for _, o := range a.Versions {
			if o.OK {
				fmt.Fprintf(&b, " %d–%d |", o.Min, o.Max)
			} else {
				b.WriteString(" — |")
			}
		}
		if a.When == WhenSASL {
			b.WriteString(" only when SASL is configured")
		}
		b.WriteString("\n")
	}
	return b.String()
}

// The document's version table is the API table, printed. This is what stops
// them drifting: a changed range fails here until the document is regenerated
// (UPDATE_API_DOCS=1 go test ./internal/protocol -run TestAPISupportDoc).
func TestAPISupportDocMatchesTheTable(t *testing.T) {
	path := filepath.Join("..", "..", "docs", "api-support.md")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc := string(raw)
	const begin, end = "<!-- api-table:begin (generated from internal/protocol/versions.go; do not edit by hand) -->\n", "<!-- api-table:end -->"
	i, j := strings.Index(doc, begin), strings.Index(doc, end)
	if i < 0 || j < i {
		t.Fatalf("%s has no generated table between the api-table markers", path)
	}
	want := docTable()
	if got := doc[i+len(begin) : j]; got != want {
		if os.Getenv("UPDATE_API_DOCS") != "" {
			if err := os.WriteFile(path, []byte(doc[:i+len(begin)]+want+doc[j:]), 0o644); err != nil {
				t.Fatal(err)
			}
			t.Logf("rewrote the table in %s", path)
			return
		}
		t.Errorf("docs/api-support.md does not match the API table.\nRegenerate it: UPDATE_API_DOCS=1 go test ./internal/protocol -run TestAPISupportDoc\n\nwant:\n%s\ngot:\n%s", want, got)
	}
	// Every API also has a line in the "what it is for" table above.
	for _, a := range APIs {
		if !strings.Contains(doc, fmt.Sprintf(" %d ", a.Key)) && !strings.Contains(doc, fmt.Sprintf("/ %d", a.Key)) && !strings.Contains(doc, fmt.Sprintf("%d /", a.Key)) {
			t.Errorf("%s (key %d) is not described in docs/api-support.md", APIName(a.Key), a.Key)
		}
	}
}
