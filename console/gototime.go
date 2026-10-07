package console

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// "go to time" sits beside "go to offset" on the tape. The question it answers is
// "what was this partition doing at 10:30", and the broker can answer it: the first
// message at or after a time is a ListOffsets by timestamp.

// errNotATime is parseAt's answer for something that is not a time.
var errNotATime = errors.New("that is not a time")

// parseAt reads what was typed in "go to time" relative to now. Three forms, chosen
// because they are what a person types while debugging:
//
//	15m, 2h, 1d, 90s   that long ago ("15m ago" is the same)
//	2026-10-07 10:30   a date and time; UTC unless it carries a zone (RFC 3339)
//	1760000000000      an epoch, in milliseconds (or seconds, if it has ten digits)
//
// The zone is never guessed: the page shows times in the browser's zone and this
// server does not know it, so a bare date and time is read as UTC and the placeholder
// and the note say so.
func parseAt(raw string, now time.Time) (time.Time, error) {
	s := strings.TrimSpace(strings.TrimSuffix(strings.TrimSpace(raw), "ago"))
	if s == "" {
		return time.Time{}, errNotATime
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		switch {
		case n < 0:
			return time.Time{}, errNotATime
		case len(s) <= 10:
			return time.Unix(n, 0).UTC(), nil
		}
		return time.UnixMilli(n).UTC(), nil
	}
	if unit := s[len(s)-1]; strings.IndexByte("smhd", unit) >= 0 {
		if n, err := strconv.ParseInt(strings.TrimSpace(s[:len(s)-1]), 10, 64); err == nil && n >= 0 {
			d := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour}[unit]
			return now.Add(-time.Duration(n) * d).UTC(), nil
		}
	}
	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05", "2006-01-02T15:04", "2006-01-02 15:04:05", "2006-01-02 15:04", "2006-01-02"} {
		if t, err := time.ParseInLocation(layout, s, time.UTC); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, errNotATime
}

// OffsetAtTime is the offset of the first message in a partition at or after ms (a
// Unix time in milliseconds), and whether there is one.
func (b *backend) OffsetAtTime(ctx context.Context, topic string, partition int32, ms int64) (offset int64, found bool, err error) {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition, rp.Timestamp = partition, ms
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return 0, false, err
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return 0, false, errors.New("unexpected ListOffsets response shape")
	}
	p := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return 0, false, err
	}
	return p.Offset, p.Offset >= 0, nil
}

// atParam resolves ?at= into the tape's jump: the offset to go to, and the note to
// show. asked is false when the box was not used.
func (c *Console) atParam(r *http.Request, topic string, partition int32) (jump int64, note string, asked bool, err error) {
	raw := strings.TrimSpace(r.URL.Query().Get("at"))
	if raw == "" {
		return -1, "", false, nil
	}
	t, perr := parseAt(raw, time.Now())
	if perr != nil {
		return -1, "That is not a time. Try 15m, 2h or 1d (that long ago), 2026-10-07 10:30 (UTC), or an epoch in milliseconds.", true, nil
	}
	off, found, err := c.be.OffsetAtTime(r.Context(), topic, partition, t.UnixMilli())
	if err != nil {
		return -1, "", true, err
	}
	stamp := t.Format("2006-01-02 15:04:05 UTC")
	if !found {
		return -1, fmt.Sprintf("No message on this partition is at or after %s — showing the latest.", stamp), true, nil
	}
	return off, fmt.Sprintf("Showing from the first message at or after %s (offset %d).", stamp, off), true, nil
}
