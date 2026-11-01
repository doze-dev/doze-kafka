package log

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"
)

// within runs fn and fails the test, without waiting for it, if it has not returned in d:
// a loop that never ends is a failed test here and not a hung run.
func within(t *testing.T, d time.Duration, what string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("%s panicked: %v", what, r)
			}
			close(done)
		}()
		fn()
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatalf("%s did not return within %v", what, d)
	}
}

func TestF_SortRefusalsOrdersByIndexAndEnds(t *testing.T) {
	within(t, 5*time.Second, "sortRefusals", func() {
		rs := []RecordRefusal{{Index: 3}, {Index: 1}, {Index: 4}, {Index: 2}, {Index: 1, Message: "second"}}
		sortRefusals(rs)
		want := []int32{1, 1, 2, 3, 4}
		for i, r := range rs {
			if r.Index != want[i] {
				t.Errorf("order %v", rs)
				break
			}
		}
		if rs[0].Message != "" || rs[1].Message != "second" {
			t.Errorf("equal indexes changed places: %v", rs)
		}
		sortRefusals(nil)
		sortRefusals(rs[:1])
	})
}

func TestF_SkipPastTheEndSetsTheErrorAndReturns(t *testing.T) {
	within(t, 5*time.Second, "skip", func() {
		r := &reader{src: &sliceSource{b: []byte{1, 2, 3}}}
		r.skip(10)
		if r.err == nil {
			t.Error("skipping past the end left no error")
		}
		if r.n != 3 {
			t.Errorf("skipped %d bytes, want the 3 there were", r.n)
		}
	})
}

func TestF_BoundedReaderStopsExactlyAtItsLimit(t *testing.T) {
	read := func(left int64, src string) (string, error) {
		var got []byte
		var err error
		within(t, 5*time.Second, "boundedReader", func() {
			got, err = io.ReadAll(&boundedReader{r: strings.NewReader(src), left: left})
		})
		return string(got), err
	}
	if got, err := read(3, "abc"); err != nil || got != "abc" {
		t.Errorf("data exactly at the limit: %q, %v", got, err)
	}
	if got, err := read(2, "abcdef"); !errors.Is(err, ErrInflatedTooLarge) || got != "ab" {
		t.Errorf("data past the limit: %q, %v, want ab and ErrInflatedTooLarge", got, err)
	}
	if got, err := read(0, "abc"); !errors.Is(err, ErrInflatedTooLarge) || got != "" {
		t.Errorf("a limit of nothing: %q, %v", got, err)
	}
	if got, err := read(0, ""); err != nil || got != "" {
		t.Errorf("nothing within a limit of nothing: %q, %v", got, err)
	}
}

func TestF_XerialStreamReadsEveryBlockAndReportsADamagedOne(t *testing.T) {
	c1, _ := compress(codecSnappy, []byte("alpha-"))
	c2, _ := compress(codecSnappy, []byte("beta-"))
	c3, _ := compress(codecSnappy, []byte("gamma"))
	within(t, 5*time.Second, "xerial decode", func() {
		got, err := Decompress(codecSnappy, xerial(c1, c2, c3), 1<<20)
		if err != nil || string(got) != "alpha-beta-gamma" {
			t.Errorf("three blocks: %q, %v", got, err)
		}
		bad := append([]byte{}, c2...)
		bad[0] = 0xff // a length header that is not a snappy one.
		bad = append(bad, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff)
		if _, err := Decompress(codecSnappy, xerial(c1, bad), 1<<20); !errors.Is(err, errRecords) {
			t.Errorf("a damaged block: %v", err)
		}
	})
}
