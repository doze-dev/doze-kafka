package console

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"strconv"
)

// Tp-13: Send again. A copy of a stored record is written to the same partition, and the
// answer sets the original and the copy side by side, both as the broker holds them: the
// copy is read back, not assumed. What differs is the offset and the timestamp; what the
// copy does not carry is said (TOP-36).

// resendRow is one line of the "Bytes / What / Same" card.
type resendRow struct {
	Label, What string
	Same        bool
}

// readerNote says what a group that reads the partition will do with the copy.
type readerNote struct {
	Group, URL string
	Read       bool // its committed offset is past the original: it has read it.
}

// resendView is the answer to Send again.
type resendView struct {
	Topic       string
	Partition   int32
	Orig, Copy  Message
	Rows        []resendRow
	Headers     []headerView
	Readers     []readerNote
	TraceID     bool
	TraceURL    string
	OrigOpenURL string
}

func identicalBytes(a, b []byte) bool { return (a == nil) == (b == nil) && bytes.Equal(a, b) }

func sameHeaders(a, b []MessageHeader) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Key != b[i].Key || !identicalBytes(a[i].Value, b[i].Value) {
			return false
		}
	}
	return true
}

// bytesWhat is what a key or a value is, in the words of the card: its size and kind.
func bytesWhat(v bytesView, kind string) string {
	switch {
	case v.Null:
		return "null"
	case v.Empty:
		return "empty, zero bytes"
	case v.Binary:
		return v.SizeText + ", binary"
	case kind == "key":
		return v.Preview + " · " + v.SizeText
	case v.Pretty:
		return v.SizeText + " of JSON"
	}
	return v.SizeText + " of text"
}

func (c *Console) resendView(ctx context.Context, topic string, partition int32, orig, cp Message) resendView {
	rv := resendView{Topic: topic, Partition: partition, Orig: orig, Copy: cp}
	kv, vv := newBytesView(orig.Key, orig.Key == nil), newBytesView(orig.Value, orig.Value == nil)
	hdrs := "none"
	if len(orig.Headers) > 0 {
		hdrs = fmt.Sprintf("%d · %s", len(orig.Headers), humanBytes(headersBytes(orig.Headers)))
	}
	rv.Rows = []resendRow{
		{"Key", bytesWhat(kv, "key"), identicalBytes(orig.Key, cp.Key)},
		{"Value", bytesWhat(vv, "value"), identicalBytes(orig.Value, cp.Value)},
		{"Headers", hdrs, sameHeaders(orig.Headers, cp.Headers)},
	}
	rv.Headers = headerViews(cp.Headers)
	if ref := traceOf(cp.Headers, c.traceHeader); ref != nil {
		rv.TraceID, rv.TraceURL = true, c.traceURL(ref.ID, cp.Timestamp, topic, partition, cp.Offset, "")
	}
	// Groups that read this partition: one whose committed offset is past the original has
	// read it, and will read the copy as well; one that is not yet there will read both.
	if groups, err := c.be.ListGroups(ctx); err == nil {
		for _, g := range groups {
			offs, err := c.be.GroupOffsets(ctx, g.ID)
			if err != nil {
				continue
			}
			for _, o := range offs {
				if o.Topic == topic && o.Partition == partition {
					rv.Readers = append(rv.Readers, readerNote{Group: g.ID, URL: c.groupURL(g.ID), Read: o.Committed > orig.Offset})
				}
			}
		}
	}
	return rv
}

// resendAnswer writes the answer: the side-by-side inspector, with the page's tape and a
// note above it carried out of band, so the page and the panel move together.
func (c *Console) resendAnswer(w http.ResponseWriter, rv resendView, tape map[string]any) {
	tape["OOB"] = true
	data := c.inspectData(inspectorView{
		Kind: KindMessage, Key: strconv.FormatInt(rv.Copy.Offset, 10), Cap: "Send again",
		Title: "Original and copy, side by side", Sub: rv.Topic + " · p" + strconv.Itoa(int(rv.Partition)),
		Body: map[string]any{"Topic": rv.Topic, "Partition": rv.Partition, "Resend": rv},
	})
	data["Tape"] = tape
	data["Note"] = fmt.Sprintf("An exact copy of p%d offset %s was written to p%d at offset %s.", rv.Partition, groupDigits(rv.Orig.Offset), rv.Partition, groupDigits(rv.Copy.Offset))
	c.partial(w, "resend_response", data)
}
