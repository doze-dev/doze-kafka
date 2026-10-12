package main

import "testing"

func TestTrafficForNilIsNil(t *testing.T) {
	if trafficFor(nil) != nil {
		t.Error("a server with no recorder offers a traffic source")
	}
	if clientMetricsFor(nil) != nil {
		t.Error("a server with no store offers client metrics")
	}
}
