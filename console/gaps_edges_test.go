package console

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"
)

func TestNumWritesAnyIntegerKind(t *testing.T) {
	for _, v := range []any{int(12345), int16(12345), int32(12345), int64(12345), uint64(12345)} {
		want := "12,345"
		if got := num(v); got != want {
			t.Errorf("num(%T) = %q, want %q", v, got, want)
		}
	}
	if num(1234567) != "1,234,567" || num(999) != "999" || num(0) != "0" {
		t.Error("num")
	}
}

// An output that stops short, at any point, is an error and never a half-written answer.
func TestIndentToStopsAtEveryWrite(t *testing.T) {
	doc := []byte(`{"a": [1, {"b": "x\"y", "c": []}, {}], "d": {"e": null}}`)
	var full bytes.Buffer
	if err := indentTo(&limitedWriter{w: &full, left: 1 << 20}, doc); err != nil {
		t.Fatal(err)
	}
	n := full.Len()
	for left := 0; left < n; left++ {
		var buf bytes.Buffer
		if err := indentTo(&limitedWriter{w: &buf, left: left}, doc); err == nil {
			t.Errorf("a limit of %d bytes for %d bytes of output did not fail", left, n)
		}
		if buf.Len() > left {
			t.Errorf("wrote %d bytes past a limit of %d", buf.Len(), left)
		}
	}
	var buf bytes.Buffer
	if err := indentTo(&limitedWriter{w: &buf, left: n}, doc); err != nil || buf.String() != full.String() {
		t.Errorf("exactly enough room: %v", err)
	}
	if !strings.Contains(full.String(), `"b": "x\"y"`) {
		t.Errorf("%s", full.String())
	}
	if _, ok := indentJSON(bytes.Repeat([]byte("["), 3000)); ok {
		// not valid JSON: never indented.
		t.Error("an unclosed value was indented")
	}
	deep := append(bytes.Repeat([]byte("["), 600), bytes.Repeat([]byte("]"), 600)...)
	if out, ok := indentJSON(deep); ok && len(out) > prettyLimit {
		t.Errorf("deeply nested output grew to %d", len(out))
	}
}

// Deleting a topic takes its committed offsets with it, so a reset has only the live ones to move.
func TestResetSkipsOffsetsOfDeletedTopics(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	cl := rawClient(t, addr)
	for _, n := range []string{"live", "gone"} {
		if err := c.be.CreateTopic(ctx, n, 1, nil); err != nil {
			t.Fatal(err)
		}
	}
	bulkProduce(t, cl, "live", 0, 6, vals("m"), time.Time{})
	bulkProduce(t, cl, "gone", 0, 6, vals("m"), time.Time{})
	commitFor(t, addr, "g", "live", map[int32]int64{0: 2})
	commitFor(t, addr, "g", "gone", map[int32]int64{0: 2})
	if err := c.be.DeleteTopic(ctx, "gone"); err != nil {
		t.Fatal(err)
	}
	if n, err := c.be.ResetGroupOffsets(ctx, "g", "latest", ""); err != nil || n != 1 {
		t.Errorf("one live offset: %d %v", n, err)
	}
	if got := groupOffsetsText(t, c, "g"); !strings.Contains(got, "live/0=6") {
		t.Errorf("offsets %s", got)
	}
}

func TestIntervalOfAFoldedRow(t *testing.T) {
	if intervalText(TrafficEvent{Count: 1}) != "" || intervalText(TrafficEvent{Count: 0}) != "" || intervalText(TrafficEvent{Count: -2}) != "" {
		t.Error("an interval of fewer than two")
	}
	t0 := time.Date(2026, 10, 10, 12, 0, 0, 0, time.UTC)
	if got := intervalText(TrafficEvent{Count: 2, First: t0, Time: t0.Add(10 * time.Second)}); got != "every 10 s" {
		t.Errorf("%q", got)
	}
	if got := intervalText(TrafficEvent{Count: 11, First: t0, Time: t0.Add(10 * time.Second)}); got != "every 1 s" {
		t.Errorf("%q", got)
	}
}
