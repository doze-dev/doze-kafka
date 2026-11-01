package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestDialableHost(t *testing.T) {
	for in, want := range map[string]string{
		"":                "localhost",
		"0.0.0.0":         "localhost",
		"::":              "localhost",
		"127.0.0.1":       "127.0.0.1",
		"10.1.2.3":        "10.1.2.3",
		"0:0:0:0:0:0:0:1": "::1",
		"devbox.local":    "devbox.local",
		"localhost":       "localhost",
	} {
		if got := dialableHost(in); got != want {
			t.Errorf("dialableHost(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestResolvesHereNeedsTheAddressToBeAmongTheAnswers(t *testing.T) {
	answer := func(addrs []string, err error) lookupFunc {
		return func(context.Context, string) ([]string, error) { return addrs, err }
	}
	for _, c := range []struct {
		name   string
		lookup lookupFunc
		want   bool
	}{
		{"the address among others", answer([]string{"10.0.0.1", "10.0.0.2"}, nil), true},
		{"another address", answer([]string{"10.0.0.9"}, nil), false},
		{"no answer", answer(nil, nil), false},
		{"an error with the address", answer([]string{"10.0.0.2"}, errors.New("boom")), false},
	} {
		if got := resolvesHere(c.lookup, "kafka.x.doze", "10.0.0.2"); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The lookup is given 750 ms and no less (a slow local resolver is not "does not
// resolve"), and no more (a machine where the name is absent is not kept waiting).
func TestResolvesHereWaitsForTheResolverButNotLong(t *testing.T) {
	slow := func(delay time.Duration) lookupFunc {
		return func(ctx context.Context, _ string) ([]string, error) {
			select {
			case <-time.After(delay):
				return []string{"10.0.0.2"}, nil
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
	}
	if !resolvesHere(slow(300*time.Millisecond), "h", "10.0.0.2") {
		t.Error("an answer after 300 ms was not waited for")
	}
	start := time.Now()
	if resolvesHere(slow(5*time.Second), "h", "10.0.0.2") {
		t.Error("an answer after 5 s was waited for")
	}
	if took := time.Since(start); took < 700*time.Millisecond || took > 2*time.Second {
		t.Errorf("gave up after %v, want about 750 ms", took)
	}
}
