package console

import (
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Zero is a time (the epoch, and "0 seconds ago"), not an error; a bare number
// of ten digits is seconds and of eleven is milliseconds.
func TestParseAtZeroAndDigitWidths(t *testing.T) {
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Time
	}{
		{"0", time.Unix(0, 0).UTC()},
		{"0m", now},
		{"0s ago", now},
		{"1234567890", time.Unix(1234567890, 0).UTC()},
		{"12345678901", time.UnixMilli(12345678901).UTC()},
	} {
		got, err := parseAt(tc.in, now)
		if err != nil || !got.Equal(tc.want) {
			t.Errorf("parseAt(%q) = %v, %v; want %v", tc.in, got, err, tc.want)
		}
	}
}

// What the tape is told to do for ?at=: no jump (-1) unless a message is found,
// and what is sent to the broker is a consumer's ListOffsets (replica -1).
func TestAtParamJumpsAndWhatTheBrokerIsAsked(t *testing.T) {
	c, sn := newSniffedConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"atp"}, "partitions": {"1"}})
	req(t, c, "POST", "/_console/topics/atp/produce", url.Values{"partition": {"0"}, "value": {"a"}, "acks": {"1"}})
	at := func(q string) (int64, string, bool, error) {
		r := httptest.NewRequest("GET", "/_console/topics/atp?at="+url.QueryEscape(q), nil)
		return c.atParam(r, "atp", 0)
	}
	if j, note, asked, err := at(""); j != -1 || note != "" || asked || err != nil {
		t.Errorf("no box: %d %q %v %v", j, note, asked, err)
	}
	if j, note, asked, err := at("whenever"); j != -1 || !strings.HasPrefix(note, "That is not a time") || !asked || err != nil {
		t.Errorf("garbage: %d %q %v %v", j, note, asked, err)
	}
	future := strconv.FormatInt(time.Now().Add(time.Hour).UnixMilli(), 10)
	if j, note, asked, err := at(future); j != -1 || !strings.HasPrefix(note, "No message on this partition is at or after ") || !asked || err != nil {
		t.Errorf("future: %d %q %v %v", j, note, asked, err)
	}
	if j, note, asked, err := at("1h"); j != 0 || !strings.Contains(note, "(offset 0)") || !asked || err != nil {
		t.Errorf("an hour ago: %d %q %v %v", j, note, asked, err)
	}
	r := httptest.NewRequest("GET", "/_console/topics/nope?at=1h", nil)
	if j, note, asked, err := c.atParam(r, "nope", 0); j != -1 || note != "" || !asked || err == nil {
		t.Errorf("unknown topic: %d %q %v %v", j, note, asked, err)
	}
	reqs := sn.sent(2)
	if len(reqs) == 0 {
		t.Fatal("no ListOffsets reached the broker")
	}
	for _, q := range reqs {
		if rid := q.(*kmsg.ListOffsetsRequest).ReplicaID; rid != -1 {
			t.Errorf("ListOffsets replica id %d, want -1 (a consumer)", rid)
		}
	}
}
