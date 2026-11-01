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
//
//fuzz:parser class=console-forms FuzzConsoleForms
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
// Unix time in milliseconds), the timestamp of that record, and whether there is one.
// A time later than the last record is answered -1 (recorded: listoffsets.json
// #after-everything), which is found == false.
func (b *backend) OffsetAtTime(ctx context.Context, topic string, partition int32, ms int64) (offset int64, ts int64, found bool, err error) {
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
		return 0, 0, false, err
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		return 0, 0, false, errors.New("unexpected ListOffsets response shape")
	}
	p := resp.Topics[0].Partitions[0]
	if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
		return 0, 0, false, err
	}
	return p.Offset, p.Timestamp, p.Offset >= 0, nil
}

// jumpView is what the page says about a jump (Tp-14, Tp-15): a title, a note, and what
// can be done next. Tone is "ok" when the jump landed where it was asked to and "warn"
// when it could not.
type jumpView struct {
	Tone        string
	Title       string
	Note        string
	OpenURL     string // "Open it": the record the jump landed on.
	LatestURL   string // where the newest records are.
	LatestLabel string // "Jump to latest", "Follow live".
}

// atResult is ?at= resolved.
type atResult struct {
	Jump  int64 // the offset to go to, or -1.
	Note  string
	Asked bool
	View  *jumpView
}

// atParam resolves ?at= into the tape's jump: the offset to go to, and the note to
// show. asked is false when the box was not used.
func (c *Console) atParam(r *http.Request, topic string, partition int32) (jump int64, note string, asked bool, err error) {
	res, err := c.atResolve(r, topic, partition)
	return res.Jump, res.Note, res.Asked, err
}

func (c *Console) atResolve(r *http.Request, topic string, partition int32) (atResult, error) {
	raw := strings.TrimSpace(r.URL.Query().Get("at"))
	if raw == "" {
		return atResult{Jump: -1}, nil
	}
	t, perr := parseAt(raw, time.Now())
	if perr != nil {
		return atResult{Jump: -1, Asked: true, Note: "That is not a time. Try 15m, 2h or 1d (that long ago), 2026-10-07 10:30 (UTC), or an epoch in milliseconds."}, nil
	}
	off, ts, found, err := c.be.OffsetAtTime(r.Context(), topic, partition, t.UnixMilli())
	if err != nil {
		return atResult{Jump: -1, Asked: true}, err
	}
	stamp := t.Format("2006-01-02 15:04:05 UTC")
	tail := c.topicURL(topic) + "?partition=" + strconv.Itoa(int(partition))
	if !found {
		earliest, latest, _ := c.be.PartitionOffsets(r.Context(), topic, partition)
		note := fmt.Sprintf("No message on this partition is at or after %s — showing the latest.", stamp)
		return atResult{Jump: -1, Asked: true, Note: note, View: &jumpView{
			Tone: "warn", Title: fmt.Sprintf("No record at or after %s", stamp),
			Note:      fmt.Sprintf("The broker answers -1 for a time later than the last record. The console's rule: it shows the newest records, from the end of the log (offset %s, nothing there yet)%s.", groupDigits(latest), holdNote(partition, earliest, latest)),
			LatestURL: tail, LatestLabel: "Follow live"}}, nil
	}
	earliest, _, _ := c.be.PartitionOffsets(r.Context(), topic, partition)
	note := fmt.Sprintf("Showing from the first message at or after %s (offset %d).", stamp, off)
	if off == earliest && t.UnixMilli() < ts {
		// A time before the first record lands on the first record.
		return atResult{Jump: off, Asked: true, Note: note, View: &jumpView{
			Tone: "warn", Title: "Nothing that early: showing from the start",
			Note:    fmt.Sprintf("Asked for %s; p%d starts at offset %s, stamped %s.", stamp, partition, groupDigits(off), time.UnixMilli(ts).UTC().Format("2006-01-02 15:04:05.000 UTC")),
			OpenURL: fmt.Sprintf("%s&open=%d", tail, off)}}, nil
	}
	return atResult{Jump: off, Asked: true, Note: note, View: &jumpView{
		Tone: "ok", Title: fmt.Sprintf("First record at or after %s is offset %s", stamp, groupDigits(off)),
		Note:    fmt.Sprintf("Timestamp %s on p%d, found by the timestamp lookup (ListOffsets by time). The row is highlighted below.", time.UnixMilli(ts).UTC().Format("2006-01-02 15:04:05.000 UTC"), partition),
		OpenURL: fmt.Sprintf("%s&open=%d", tail, off)}}, nil
}

// holdNote is " p1 holds offsets 0 to 4,789" for a note that says what the partition has.
func holdNote(partition int32, earliest, latest int64) string {
	if latest <= earliest {
		return ""
	}
	return fmt.Sprintf("; p%d holds offsets %s to %s", partition, groupDigits(earliest), groupDigits(latest-1))
}
