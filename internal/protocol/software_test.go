package protocol

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// What a connection keeps of its client's software is bounded where it is kept, by
// the setter, whatever it is handed: a megabyte of name leaves 128 bytes and no
// reference to the megabyte.
func TestAConnectionKeepsAShortClientSoftware(t *testing.T) {
	c := &Conn{}
	for _, tc := range []struct {
		name, version, want string
	}{
		{"apache-kafka-java", "3.7.0", "apache-kafka-java 3.7.0"},
		{"librdkafka", "", "librdkafka"},
		{strings.Repeat("a", 127) + "é", "", strings.Repeat("a", 125) + "…"}, // 129 bytes, the last character across the bound.
		{"", "", ""},
		{strings.Repeat("a", 127), "1", strings.Repeat("a", 125) + "…"}, // 129 bytes with the space: cut.
		{strings.Repeat("a", 126), "1", strings.Repeat("a", 126) + " 1"},
		{strings.Repeat("a", 1<<20), strings.Repeat("9", 1<<20), strings.Repeat("a", 125) + "…"},
		{"short", strings.Repeat("9", 1<<20), "short " + strings.Repeat("9", 119) + "…"},
		{strings.Repeat("é", 100), "1", strings.Repeat("é", 62) + "…"}, // cut between characters, never inside one.
	} {
		c.SetClientSoftware(tc.name, tc.version)
		got := c.ClientSoftware()
		if got != tc.want {
			t.Errorf("%.12q… %.12q…: kept %d bytes %.24q…, want %d bytes %.24q…", tc.name, tc.version, len(got), got, len(tc.want), tc.want)
		}
		if len(got) > MaxClientSoftware || !utf8.ValidString(got) {
			t.Errorf("kept %d bytes (valid UTF-8: %v), want at most %d", len(got), utf8.ValidString(got), MaxClientSoftware)
		}
	}
	var nobody *Conn
	nobody.SetClientSoftware("x", "1") // a handler test may have no connection.
	if nobody.ClientSoftware() != "" {
		t.Error("a nil connection has software")
	}
}

// ValidClientSoftware is Kafka's rule, and the recording of Apache Kafka 4.3.1 is the
// table: every name and version it refused (INVALID_REQUEST) is refused here, and every
// one it took is taken.
func TestValidClientSoftwareIsWhatKafkaTook(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "conformance", "recordings", "apiversions-client-software.json"))
	if err != nil {
		t.Fatal(err)
	}
	var rec struct {
		Steps []struct {
			Label   string
			API     string
			Version int16
			Request struct {
				ClientSoftwareName, ClientSoftwareVersion string
			}
			Response struct{ ErrorCode int16 }
		}
	}
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	taken, refused := 0, 0
	for _, s := range rec.Steps {
		if s.API != "ApiVersions" || s.Version < 3 {
			continue
		}
		valid := ValidClientSoftware(s.Request.ClientSoftwareName) && ValidClientSoftware(s.Request.ClientSoftwareVersion)
		switch s.Response.ErrorCode {
		case 0:
			taken++
			if !valid {
				t.Errorf("%s: Kafka took %.30q %.30q, and it is refused here", s.Label, s.Request.ClientSoftwareName, s.Request.ClientSoftwareVersion)
			}
		case 42:
			refused++
			if valid {
				t.Errorf("%s: Kafka refused %q %q, and it is taken here", s.Label, s.Request.ClientSoftwareName, s.Request.ClientSoftwareVersion)
			}
		default:
			t.Errorf("%s: the recording has error %d", s.Label, s.Response.ErrorCode)
		}
	}
	if taken < 5 || refused < 10 {
		t.Fatalf("the recording has %d taken and %d refused: it is not the one this test was written for", taken, refused)
	}
	// The edges of the rule, one character at a time.
	for s, want := range map[string]bool{
		"a": true, "Z": true, "0": true, "a-b": true, "a.b": true, "a--b": true, "a.-.b": true, "1.0-rc.1": true,
		"": false, "-": false, ".": false, "a-": false, "-a": false, "a.": false, ".a": false,
		"a b": false, "a_b": false, "a/b": false, "a+b": false, "a\x00b": false, "é": false, "a\tb": false, " a": false,
	} {
		if got := ValidClientSoftware(s); got != want {
			t.Errorf("ValidClientSoftware(%q) = %v, want %v", s, got, want)
		}
	}
}

// A refused ApiVersions request says, in its row, what it called itself and why that
// was not taken; it does not name the client's software.
func TestARefusedClientSoftwareIsDescribedAndNotAdopted(t *testing.T) {
	q := kmsg.NewPtrApiVersionsRequest()
	q.SetVersion(3)
	q.ClientSoftwareName, q.ClientSoftwareVersion = "my app", strings.Repeat("9", 5000)
	resp := kmsg.NewPtrApiVersionsResponse()
	resp.ErrorCode = 42
	r := &Request{Header: RequestHeader{APIKey: kmsg.ApiVersions, APIVersion: 3}}
	r.Noted(q, resp)
	s := r.Describe()
	if s.Software != "" || s.ErrorCode != 42 || !s.Class.IsError() {
		t.Errorf("summary = %+v", s)
	}
	if !strings.Contains(s.Detail, `"my app"`) || !strings.Contains(s.Detail, "letters and digits") || len(s.Detail) > 256 {
		t.Errorf("detail (%d bytes) = %q", len(s.Detail), s.Detail)
	}
	// One that is taken names the software, as it is kept.
	q.ClientSoftwareName, q.ClientSoftwareVersion = strings.Repeat("a", 5000), "1"
	resp.ErrorCode = 0
	if s := r.Describe(); len(s.Software) != MaxClientSoftware || s.Detail != s.Software || s.ErrorCode != 0 {
		t.Errorf("a long name that was taken: software of %d bytes, detail of %d", len(s.Software), len(s.Detail))
	}
}
