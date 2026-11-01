package main

import (
	"strings"
	"testing"
	"time"
)

// A by-name URL answers where its host answers on its port, or on 80 when it names none;
// one outside the zone, or that does not parse, is not this check's business.
func TestAnswersAtAsksTheHostOnTheURLsPort(t *testing.T) {
	type asked struct {
		host string
		port int
	}
	for _, c := range []struct {
		url    string
		reach  bool
		want   bool
		asked  *asked
		reason string
	}{
		{"http://kafka.x.doze:8081/", true, true, &asked{"kafka.x.doze", 8081}, "a port"},
		{"http://kafka.x.doze:8081/", false, false, &asked{"kafka.x.doze", 8081}, "a port that does not answer"},
		{"http://console.x.doze", true, true, &asked{"console.x.doze", 80}, "no port"},
		{"http://console.x.doze", false, false, &asked{"console.x.doze", 80}, "no port, no answer"},
		{"https://example.com:8443/", false, true, nil, "outside the zone"},
		{"http://%zz", false, true, nil, "not a URL"},
	} {
		var got *asked
		reach := func(host string, port int) bool { got = &asked{host, port}; return c.reach }
		if ok := answersAt(reach, c.url); ok != c.want {
			t.Errorf("%s: %v, want %v", c.reason, ok, c.want)
		}
		switch {
		case c.asked == nil && got != nil:
			t.Errorf("%s: asked %+v", c.reason, *got)
		case c.asked != nil && (got == nil || *got != *c.asked):
			t.Errorf("%s: asked %+v, want %+v", c.reason, got, *c.asked)
		}
	}
}

// With no front door the URL printed is the port-ful one, but only after waiting up to half
// a second for a front door to appear (a peer's, or this process's own, which binds in a
// goroutine that races the line).
func TestTheNameIsGivenHalfASecondToGainAFrontDoor(t *testing.T) {
	for _, what := range []string{"console", "registry"} {
		z := claimed(t, "shop", &probe{answers: true})
		start := time.Now()
		var got string
		if what == "console" {
			got = z.routeConsole(quietLogger(), "127.0.0.9:9093")
		} else {
			got = z.routeRegistry(quietLogger(), "shop", "127.0.0.9:9093")
		}
		took := time.Since(start)
		if took < 450*time.Millisecond || took > 3*time.Second {
			t.Errorf("%s: answered after %v, want about 500ms", what, took)
		}
		if !strings.Contains(got, ".doze") {
			t.Errorf("%s: %q", what, got)
		}
	}
}

func TestRegistryTLSURL(t *testing.T) {
	for _, c := range []struct {
		beside      bool
		host, bound string
		want        string
	}{
		{true, "kafka.x.doze", "127.0.0.9:8081", "https://kafka.x.doze:8081"},
		{false, "kafka.x.doze", "127.0.0.9:8081", "https://127.0.0.9:8081"},
		{true, "", "127.0.0.9:8081", "https://127.0.0.9:8081"},
		{true, "kafka.x.doze", ":8081", "https://kafka.x.doze:8081"},
		{false, "kafka.x.doze", "0.0.0.0:8081", "https://localhost:8081"},
		{true, "kafka.x.doze", "no-port", "https://no-port"},
	} {
		if got := registryTLSURL(c.beside, c.host, c.bound); got != c.want {
			t.Errorf("registryTLSURL(%v, %q, %q) = %q, want %q", c.beside, c.host, c.bound, got, c.want)
		}
	}
}
