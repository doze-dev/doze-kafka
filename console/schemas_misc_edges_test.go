package console

import (
	"fmt"
	"net/http/httptest"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// A key goes to the partition Kafka's own hash picks for it, and "where would it go" does
// not depend on the rounds of the round robin; one partition is always partition 0.
func TestAKeyLandsOnThePartitionTheKafkaHashPicks(t *testing.T) {
	c := newTestConsole(t)
	moved := false
	for _, k := range []string{"a", "b", "c", "d", "e", "f", "SKU-1", "SKU-2"} {
		if got := c.be.keyPartition("t", []byte(k), 1); got != 0 {
			t.Errorf("key %s on a topic of one partition: %d", k, got)
		}
		got := c.be.keyPartition("t", []byte(k), 8)
		if want := c.be.PickPartition("t", []byte(k), 8); got != want {
			t.Errorf("key %s: keyPartition %d, PickPartition %d", k, got, want)
		}
		if got < 0 || got >= 8 {
			t.Errorf("key %s on a topic of 8 partitions: %d", k, got)
		}
		moved = moved || got != 0
	}
	if !moved {
		t.Error("eight keys all landed on partition 0 of 8")
	}
}

// A setting that is a choice lists the choices; a value the topic has set that the choice
// does not list is added so saving the form does not unset it, and a topic that has set
// nothing adds nothing.
func TestAChoiceSettingAddsAnUnlistedValueOfTheTopicAndNothingElse(t *testing.T) {
	v := newConfigView([]ConfigEntry{
		{Name: "cleanup.policy", Value: "compact,delete", Own: true},
		{Name: "message.timestamp.type", Value: "CreateTime"},
	})
	options := map[string][]string{}
	for _, s := range v.Settings {
		options[s.Key] = s.Options
	}
	if got, want := options["cleanup.policy"], []string{"delete", "compact", "compact,delete"}; !slices.Equal(got, want) {
		t.Errorf("cleanup.policy options %q, want %q", got, want)
	}
	if got, want := options["message.timestamp.type"], []string{"CreateTime", "LogAppendTime"}; !slices.Equal(got, want) {
		t.Errorf("message.timestamp.type options %q, want %q", got, want)
	}
	// A value the choice lists is not added a second time.
	v = newConfigView([]ConfigEntry{{Name: "cleanup.policy", Value: "compact", Own: true}})
	for _, s := range v.Settings {
		if s.Key == "cleanup.policy" && !slices.Equal(s.Options, []string{"delete", "compact"}) {
			t.Errorf("cleanup.policy compact: options %q", s.Options)
		}
	}
	// What a setting offers is not changed by showing the next topic's.
	if got := topicSettings[4].Options; !slices.Equal(got, []string{"delete", "compact"}) {
		t.Errorf("the shared options were changed: %q", got)
	}
}

// A time exactly at the first record is found at that record; only a time before it is
// "nothing that early".
func TestAskingForExactlyTheFirstRecordsTimeFindsIt(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"stamped"}, "partitions": {"1"}})
	first := time.Date(2026, 1, 2, 3, 4, 5, 678_000_000, time.UTC)
	bulkProduce(t, rawClient(t, addr), "stamped", 0, 3, func(i int) string { return "v" + strconv.Itoa(i) }, first)
	at := func(ms int64) atResult {
		t.Helper()
		res, err := c.atResolve(httptest.NewRequest("GET", "/_console/topics/stamped?at="+strconv.FormatInt(ms, 10), nil), "stamped", 0)
		if err != nil {
			t.Fatal(err)
		}
		return res
	}
	if res := at(first.UnixMilli()); res.Jump != 0 || res.View == nil || res.View.Tone != "ok" || !strings.HasPrefix(res.View.Title, "First record at or after 2026-01-02 03:04:05 UTC is offset 0") {
		t.Errorf("exactly at the first record: %+v %+v", res, res.View)
	}
	if res := at(first.UnixMilli() - 1); res.Jump != 0 || res.View == nil || res.View.Tone != "warn" || res.View.Title != "Nothing that early: showing from the start" {
		t.Errorf("a millisecond before the first record: %+v %+v", res, res.View)
	}
	if res := at(first.UnixMilli() + 1); res.Jump != 1 || res.View == nil || res.View.Tone != "ok" {
		t.Errorf("a millisecond after the first record: %+v %+v", res, res.View)
	}
}

// A time later than the last record shows the newest records and says what the partition
// holds, unless it holds nothing.
func TestATimeAfterTheLastRecordSaysWhatThePartitionHolds(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"three"}, "partitions": {"1"}})
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"nothing"}, "partitions": {"1"}})
	bulkProduce(t, rawClient(t, addr), "three", 0, 3, func(i int) string { return fmt.Sprintf("v%d", i) }, time.Time{})
	future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	note := func(topic string) string {
		t.Helper()
		res, err := c.atResolve(httptest.NewRequest("GET", "/_console/topics/"+topic+"?at="+future, nil), topic, 0)
		if err != nil || res.View == nil {
			t.Fatalf("%s: %v %+v", topic, err, res)
		}
		return res.View.Note
	}
	if got := note("three"); !strings.HasSuffix(got, "(offset 3, nothing there yet); p0 holds offsets 0 to 2.") {
		t.Errorf("a partition of three records: %s", got)
	}
	if got := note("nothing"); !strings.HasSuffix(got, "(offset 0, nothing there yet).") || strings.Contains(got, "holds offsets") {
		t.Errorf("an empty partition: %s", got)
	}
}
