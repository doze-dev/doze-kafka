package console

import (
	"net/url"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// The retention.bytes sentence: -1 is its own sentence, any other number is said with the
// size written out (or the raw bytes when there is no human form).
func TestTheRetentionBytesSentenceNamesTheLimit(t *testing.T) {
	var c *Console
	if got, want := c.configAffects("retention.bytes", "-1", "", 0, 0), "No size limit: nothing is deleted for a partition's size."; got != want {
		t.Errorf("-1: %q, want %q", got, want)
	}
	want := "Closed segments are deleted, oldest first, while a partition holds more than 1 GB; the active segment is never deleted. Consumers behind the new log start get OFFSET_OUT_OF_RANGE."
	if got := c.configAffects("retention.bytes", "1073741824", "1 GB", 0, 0); got != want {
		t.Errorf("with a human size:\n%q\nwant\n%q", got, want)
	}
	want = "Closed segments are deleted, oldest first, while a partition holds more than 500 bytes; the active segment is never deleted. Consumers behind the new log start get OFFSET_OUT_OF_RANGE."
	if got := c.configAffects("retention.bytes", "500", "", 0, 0); got != want {
		t.Errorf("with no human size:\n%q\nwant\n%q", got, want)
	}
}

// The sentence for every other key is its meaning, capitalised, then what the broker does
// with it, then the valid range when it has one.
func TestTheSentenceForAKeyIsItsMeaningWhatActsOnItAndItsRange(t *testing.T) {
	var c *Console
	for _, tc := range []struct{ key, want string }{
		{"max.message.bytes", "Refuse a record batch larger than this. The broker acts on it: produce handler (MESSAGE_TOO_LARGE). Valid range: 0 or more."},
		{"delete.retention.ms", "How long a tombstone survives a compaction pass. The broker acts on it when cleanup.policy has compact: log cleaner. Valid range: 0 or more."},
		{"flush.ms", "Force an fsync after this long. The broker stores and returns it and changes nothing. Valid range: 0 or more."},
		// A stored key with no range ends at the stored sentence.
		{"follower.replication.throttled.replicas", "Replicas whose replication is throttled. The broker stores and returns it and changes nothing."},
	} {
		if got := c.configAffects(tc.key, "5", "", 0, 0); got != tc.want {
			t.Errorf("%s:\n%q\nwant\n%q", tc.key, got, tc.want)
		}
	}
	if got := c.configAffects("no.such.key", "5", "", 0, 0); got != "" {
		t.Errorf("a key the table does not hold has a sentence: %q", got)
	}
}

// The retention.ms sentence adds the all-partitions-are-one-segment line only when there are
// partitions and every one of them is a single segment.
func TestTheRetentionSentenceNamesPartitionsThatAreAllOneSegment(t *testing.T) {
	var c *Console
	const all = "All 2 partitions are a single active segment each"
	for _, tc := range []struct {
		single, parts int
		says          bool
	}{
		{2, 2, true},
		{1, 2, false},
		{0, 2, false},
		{0, 0, false},
		{-1, 2, false},
	} {
		got := c.configAffects("retention.ms", "3600000", "1h", tc.single, tc.parts)
		if has := strings.Contains(got, "All "); has != tc.says {
			t.Errorf("single %d of %d: sentence has the all-one-segment line = %v, want %v:\n%s", tc.single, tc.parts, has, tc.says, got)
		}
		if tc.says && !strings.Contains(got, all) {
			t.Errorf("single %d of %d: %q missing from %s", tc.single, tc.parts, all, got)
		}
	}
	if got, want := c.configAffects("retention.ms", "-1", "", 2, 2), "Records are kept for ever: nothing is deleted for its age."; got != want {
		t.Errorf("-1: %q, want %q", got, want)
	}
}

func TestCapitaliseRaisesTheFirstLetterAndLeavesNothingAlone(t *testing.T) {
	for in, want := range map[string]string{"": "", "a": "A", "abc def": "Abc def", "Abc": "Abc"} {
		if got := capitalise(in); got != want {
			t.Errorf("capitalise(%q) = %q, want %q", in, got, want)
		}
	}
}

// The edit row's Before is the value the broker reports for that key, and where it comes from.
func TestThePreviewBeforeIsTheKeysOwnValueAndSource(t *testing.T) {
	var c *Console
	entries := []ConfigEntry{{Name: "flush.ms", Value: "5"}, {Name: "retention.ms", Value: "1000", Own: true}}
	p := c.configPreview(entries, "retention.ms", "2d", 0, 0)
	if p.Before != "1000" || p.BeforeHuman != "1s" || p.BeforeSource != "override on this topic" {
		t.Errorf("retention.ms before: %q (%q) from %q", p.Before, p.BeforeHuman, p.BeforeSource)
	}
	if p.After != "172800000" || p.AfterHuman != "2d" {
		t.Errorf("after: %q (%q)", p.After, p.AfterHuman)
	}
	p = c.configPreview(entries, "flush.ms", "7", 0, 0)
	if p.Before != "5" || p.BeforeSource != "default" {
		t.Errorf("flush.ms before: %q from %q", p.Before, p.BeforeSource)
	}
	p = c.configPreview(entries, "segment.ms", "7", 0, 0)
	if p.Before != "" || p.BeforeSource != "" {
		t.Errorf("a key the broker did not report has a before: %q from %q", p.Before, p.BeforeSource)
	}
}

// The table counts the keys the broker acts on.
func TestTheConfigTableCountsTheKeysTheBrokerActsOn(t *testing.T) {
	var c *Console
	tab := c.configTable([]ConfigEntry{
		{Name: "retention.ms", Value: "1000", Own: true},
		{Name: "delete.retention.ms", Value: "5"},
		{Name: "flush.ms", Value: "5"},
		{Name: "not.a.setting", Value: "x"},
	}, "", "")
	if tab.All != 4 || tab.Overrides != 1 || tab.Compact != 1 || tab.Acting != 2 {
		t.Errorf("all %d overrides %d compact %d acting %d, want 4 1 1 2", tab.All, tab.Overrides, tab.Compact, tab.Acting)
	}
}

func TestAValueIsSaidTheWayAPersonWouldOnlyWhereThatHelps(t *testing.T) {
	for _, tc := range []struct{ key, raw, want string }{
		{"retention.ms", "1000", "1s"},
		{"retention.ms", "999", ""},
		{"retention.ms", "5", ""},
		{"retention.ms", "9000000000000000000", ""},
		{"retention.ms", "8999999999999999999", "8999999999999999999ms"},
		{"retention.ms", "-1", "no limit"},
		{"retention.ms", "1", ""},
		{"retention.ms", "0", ""},
		{"retention.bytes", "-1", "no limit"},
		{"retention.bytes", "1", ""},
		{"retention.bytes", "5", ""},
		{"retention.bytes", "1023", ""},
		{"retention.bytes", "1024", "1 KB"},
		{"segment.bytes", "1024", "1 KB"},
		{"segment.bytes", "1023", ""},
		{"segment.bytes", "-1", ""},
		{"segment.ms", "-1", ""},
		{"flush.messages", "5000", ""},
		{"flush.messages", "-1", ""},
		{"no.such.key", "5000", ""},
		{"retention.ms", "abc", ""},
	} {
		if got := humanValue(tc.key, tc.raw); got != tc.want {
			t.Errorf("humanValue(%s, %s) = %q, want %q", tc.key, tc.raw, got, tc.want)
		}
	}
}

// A refused value gets what the console adds in its own words: the range (with its unit when
// the key has one), the choices, or the nearest keys when the key is not one.
func TestARefusedConfigGetsTheRangeTheChoicesOrTheNearestKeys(t *testing.T) {
	var c *Console
	for _, tc := range []struct{ key, want string }{
		{"segment.bytes", "Valid range: 1024 or more (bytes)."},
		{"cleanup.policy", "Valid range: delete, compact, or both."},
		{"compression.type", "Valid: uncompressed, zstd, lz4, snappy, gzip, producer."},
		{"message.timestamp.type", "Valid: CreateTime, LogAppendTime."},
		{"follower.replication.throttled.replicas", ""},
		{"zzzzzzzzzzzzzzzzzzzz", "Not a topic config this broker knows."},
		{"retention.mss", "Not a topic config this broker knows. Did you mean retention.ms?"},
	} {
		kr := &kafkaRefusal{Err: kerr.InvalidConfig}
		out := c.explainConfigRefusal(nil, "t", tc.key, kr)
		if out != error(kr) {
			t.Errorf("%s: the refusal is not handed back", tc.key)
		}
		if kr.Facts != tc.want {
			t.Errorf("%s: facts %q, want %q", tc.key, kr.Facts, tc.want)
		}
	}
	other := &kafkaRefusal{Err: kerr.TopicAuthorizationFailed}
	if c.explainConfigRefusal(nil, "t", "retention.mss", other); other.Facts != "" {
		t.Errorf("a refusal that is not about the config got facts: %q", other.Facts)
	}
}

// The nearest names are within reach (two edits, or a quarter of the name), nearest first,
// ties by name, and no more than asked for.
func TestTheNearestNamesAreWithinReachNearestFirstAndTiedByName(t *testing.T) {
	names := []string{"xbc", "abcde", "zzzzz", "abd", "abc", "xbd"}
	if got, want := nearestNames("abc", names, 10), []string{"abc", "abd", "xbc", "abcde", "xbd"}; !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got, want := nearestNames("ABC", names, 2), []string{"abc", "abd"}; !reflect.DeepEqual(got, want) {
		t.Errorf("two of them: got %v, want %v", got, want)
	}
	// A long name allows a quarter of its length: 8 edits for a 32-letter name.
	long := strings.Repeat("a", 32)
	if got := nearestNames(strings.Repeat("a", 24), []string{long}, 3); len(got) != 1 {
		t.Errorf("eight edits from a 32-letter name is within reach, got %v", got)
	}
	if got := nearestNames(strings.Repeat("a", 23), []string{long}, 3); len(got) != 0 {
		t.Errorf("nine edits from a 32-letter name is out of reach, got %v", got)
	}
}

// Saving a key says it was saved, resetting says it follows the default again.
func TestSavingAndResettingAKeyEachSayWhatHappened(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"fl"}, "partitions": {"1"}})
	rec := req(t, c, "POST", "/_console/topics/fl/config/retention.ms", url.Values{"value": {"1d"}})
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "tab=config") || !strings.Contains(loc, "flash=Saved+retention.ms") {
		t.Errorf("save: %d %q", rec.Code, loc)
	}
	rec = req(t, c, "POST", "/_console/topics/fl/config/retention.ms", url.Values{"reset": {"1"}})
	if loc := rec.Header().Get("Location"); !strings.Contains(loc, "flash=retention.ms+follows+the+broker%27s+default+again") {
		t.Errorf("reset: %d %q", rec.Code, loc)
	}
}

// singleSegments counts the partitions that hold exactly one segment file, and says -1 when
// the files cannot be read.
func TestSingleSegmentsCountsThePartitionsWithOneSegment(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"sg"}, "partitions": {"3"}})
	cl := rawClient(t, addr)
	for p := int32(0); p < 3; p++ {
		bulkProduce(t, cl, "sg", p, 2, func(i int) string { return "v" }, time.Time{})
	}
	if n := c.singleSegments("sg", 3); n != 3 {
		t.Errorf("a new three-partition topic has %d single-segment partitions, want 3", n)
	}
	if n := c.singleSegments("nope", 3); n != -1 {
		t.Errorf("a topic with no files: %d, want -1", n)
	}
}
