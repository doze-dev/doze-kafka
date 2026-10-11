package main

import (
	"slices"
	"testing"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

// The recorder leaves out the console's own clients by their exact ids, and those are
// the ids the console connects with (TRF-21): an app named like them is an app.
func TestTheRecordersLeftOutIdsAreTheConsolesOwn(t *testing.T) {
	want := []string{console.ClientID, console.ClientIDFetch, console.ClientIDTrace}
	got := slices.Clone(traffic.ConsoleClientIDs)
	slices.Sort(want)
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Errorf("the recorder leaves out %v, the console connects as %v", got, want)
	}
}

func TestTrafficForNilIsNil(t *testing.T) {
	if trafficFor(nil) != nil {
		t.Error("a server with no recorder offers a traffic source")
	}
	if clientMetricsFor(nil) != nil {
		t.Error("a server with no store offers client metrics")
	}
}
