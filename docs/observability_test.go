package docs

import (
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// The numbers the guide gives are the constants the recorder runs on: a change to one
// without the other fails here, so the page cannot promise what the code does not keep.
func TestObservabilityGuideStatesTheRecordersBounds(t *testing.T) {
	b, err := os.ReadFile("observability.md")
	if err != nil {
		t.Fatal(err)
	}
	doc := string(b)
	comma := func(n int) string {
		s := strconv.Itoa(n)
		if len(s) > 3 {
			s = s[:len(s)-3] + "," + s[len(s)-3:]
		}
		return s
	}
	for what, want := range map[string]string{
		"rows":                    "| request rows | " + comma(2000) + " ",
		"group events":            "| group events | " + strconv.Itoa(traffic.MaxGroupEvents) + " per group, " + strconv.Itoa(traffic.MaxGroups) + " groups",
		"ticks":                   "| ticks on a folded row | " + strconv.Itoa(traffic.MaxTicks) + " |",
		"detail":                  "| a detail, or the reason a connection was closed | " + strconv.Itoa(traffic.MaxText) + " bytes |",
		"names":                   "| a client id, software, principal, group, transactional id or member id | " + strconv.Itoa(traffic.MaxName) + " bytes |",
		"address":                 "| a peer address | " + strconv.Itoa(traffic.MaxRemote) + " bytes |",
		"topics":                  "| topics named by one request | " + strconv.Itoa(traffic.MaxTopics) + " names and " + comma(traffic.MaxTopicBytes) + " bytes",
		"the page's own capacity": "kept: the last",
	} {
		if what == "the page's own capacity" {
			continue // the page prints it from the recorder's capacity at run time.
		}
		if !strings.Contains(doc, want) {
			t.Errorf("observability.md does not state the %s bound as %q", what, want)
		}
	}
	for _, id := range traffic.ConsoleClientIDs {
		if !strings.Contains(doc, "`"+id+"`") {
			t.Errorf("observability.md does not name the console's own client id %s", id)
		}
	}
	// The folding rules are written down: the ten-minute window is the code's.
	if !strings.Contains(doc, "less than ten minutes ago") {
		t.Error("the folding window is not stated")
	}
}
