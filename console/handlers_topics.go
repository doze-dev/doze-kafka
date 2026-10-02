package console

import (
	"context"
	"fmt"
	"maps"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// topicRef satisfies templates that only need {{.Topic.Name}} (the tape's
// live poll) without paying for a full DescribeTopic (Metadata+DescribeConfigs)
// on every 2.5s tick.
type topicRef struct{ Name string }

// topics is the home surface: every topic, no selection.
func (c *Console) topics(w http.ResponseWriter, r *http.Request) {
	list, err := c.be.ListTopics(r.Context())
	if err != nil {
		c.fail(w, r, err)
		return
	}
	c.render(w, r, "topics_home", map[string]any{"List": list, "Page": "topics"})
}

func (c *Console) topicCreatePage(w http.ResponseWriter, r *http.Request) {
	list, _ := c.be.ListTopics(r.Context())
	c.render(w, r, "topic_create", map[string]any{"List": list, "Page": "topics", "Title": "Create topic"})
}

func (c *Console) topicCreate(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	partitions, err := strconv.Atoi(r.FormValue("partitions"))
	if err != nil || partitions < 1 {
		partitions = 1
	}
	if err := c.be.CreateTopic(r.Context(), name, int32(partitions)); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.topicURL(name), "Topic created")
}

func (c *Console) topicDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.DeleteTopic(r.Context(), name); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.prefix+"/topics", "Topic deleted")
}

func (c *Console) topicPurge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.PurgeTopic(r.Context(), name); err != nil {
		c.fail(w, r, err)
		return
	}
	c.redirect(w, r, c.topicURL(name), "Topic purged — all messages deleted")
}

func (c *Console) topicDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	list, _ := c.be.ListTopics(ctx)
	detail, err := c.be.DescribeTopic(ctx, name)
	if err != nil {
		c.failResource(w, r, err, "topic", name)
		return
	}
	partition := partitionParam(r, detail.Partitions)

	tape, err := c.tapeData(ctx, name, partition, detail.Partitions, fromParam(r), hlParam(r))
	if err != nil {
		c.fail(w, r, err)
		return
	}

	data := map[string]any{
		"List": list, "Topic": detail, "Page": "topics",
		"Partition": partition, "Title": name,
	}
	maps.Copy(data, tape)
	c.render(w, r, "topic_detail", data)
}

// topicTape is the live-polled partial backing the tape + message table.
func (c *Console) topicTape(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	count, err := c.be.PartitionCount(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	partition := partitionParam(r, count)

	_, latest, err := c.be.PartitionOffsets(ctx, name, partition)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	hash := strconv.FormatInt(latest, 10)
	if liveUnchanged(w, r, hash) {
		return
	}

	tape, err := c.tapeData(ctx, name, partition, count, -1, -1)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": partition}
	maps.Copy(data, tape)
	c.partial(w, "tape_and_table", data)
}

func (c *Console) topicProduce(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	count, err := c.be.PartitionCount(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}

	in, err := parseProduceForm(r, count)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	partition := in.Partition
	if in.Auto {
		partition = c.be.PickPartition(name, in.Key, count)
	}

	result, err := c.be.Produce(ctx, name, partition, in.Key, in.Value, in.Headers, in.Acks)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if result.Confirmed {
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	} else {
		toast(w, "Sent — no confirmation requested (acks=0)")
	}

	// Re-render the tape/table for the partition the message actually landed
	// on, so a produce to a different partition than the one being viewed is
	// still honestly reflected (not silently shown against the wrong tape).
	tape, err := c.tapeData(ctx, name, result.Partition, count, -1, -1)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": result.Partition}
	maps.Copy(data, tape)
	c.partial(w, "tape_and_table", data)
}

func (c *Console) topicMessage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	partition := int32(0)
	if n, err := strconv.Atoi(r.URL.Query().Get("partition")); err == nil {
		partition = int32(n)
	}
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err == nil && len(msgs) == 0 {
		// Nothing there is not an error to the broker, but it is to the person who
		// clicked: the record is gone (retention, a purge) or never existed. This
		// used to pass a nil error on and panic reading its message.
		err = fmt.Errorf("%w: no record at offset %d of partition %d — it may have been deleted by retention or a purge", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	m := msgs[0]
	hdrs := headerViews(m.Headers)
	valueV := newBytesView(m.Value, m.Value == nil)
	keyV := newBytesView(m.Key, m.Key == nil)
	c.partial(w, "message_drawer", map[string]any{
		"Topic": name, "Partition": partition, "Msg": m,
		"TimeStr": m.Timestamp.Format(time.RFC3339), "Key": keyV, "Value": valueV, "Headers": hdrs,
		// What "Send again" carries. Text travels as text; anything else as base64
		// with its encoding named, so the resend is byte-identical.
		"ResendKey": resendField(keyV), "ResendKeyEnc": resendEnc(keyV),
		"ResendValue": resendField(valueV), "ResendValueEnc": resendEnc(valueV),
		"ResendHeaders": resendHeaders(hdrs),
		"CanResend":     valueV.Null || valueV.Empty || valueV.B64 != "",
	})
}

// resendField is the text a field is carried as when resent.
func resendField(v bytesView) string {
	switch {
	case v.Null || v.Empty:
		return ""
	case v.Binary:
		return v.B64
	default:
		return v.Text
	}
}

func resendEnc(v bytesView) string {
	if v.Binary {
		return "base64"
	}
	return "text"
}

// previewOnly is newBytesView without the hex dump and base64 the table never
// shows: for 60 rows of up-to-megabyte values those are not free.
func previewOnly(b []byte) bytesView {
	v := bytesView{Size: len(b), SizeText: humanBytes(len(b))}
	switch {
	case b == nil:
		v.Null, v.Preview = true, "∅ null"
	case len(b) == 0:
		v.Empty, v.Preview = true, `""`
	case looksBinary(b[:min(len(b), 4096)]) || !utf8Valid(b[:min(len(b), 4096)]):
		v.Binary, v.Preview = true, binaryPreview(b)
	default:
		v.Preview = oneLine(string(b[:min(len(b), previewRunes*4)]), previewRunes)
	}
	return v
}

// partitionParam reads ?partition=, clamped to [0, count).
func partitionParam(r *http.Request, count int32) int32 {
	n, err := strconv.Atoi(r.URL.Query().Get("partition"))
	if err != nil || n < 0 || int32(n) >= count {
		return 0
	}
	return int32(n)
}

// fromParam reads ?from= (a window start offset); -1 means "the tail" —
// the newest window, which is also the only one that live-polls.
func fromParam(r *http.Request) int64 {
	n, err := strconv.ParseInt(r.URL.Query().Get("from"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// hlParam reads ?hl= — an offset to highlight and scroll to (the jump-to-
// offset control sets it); -1 when absent.
func hlParam(r *http.Request) int64 {
	n, err := strconv.ParseInt(r.URL.Query().Get("hl"), 10, 64)
	if err != nil || n < 0 {
		return -1
	}
	return n
}

// msgView is a message dressed up for the template: pre-formatted strings
// and the tape's visual hints, computed once per render rather than in the
// template itself.
type msgView struct {
	Offset     int64
	Timestamp  time.Time
	TimeStr    string
	TimeFull   string // full RFC 3339 timestamp, for the hover title.
	Key        bytesView
	Value      bytesView
	Headers    []MessageHeader
	Hot        bool
	TickHeight int
}

const tapeWindow = 60 // how many messages the tape/table show at once

// tapeData fetches one tapeWindow-sized window of messages on a partition
// and shapes them for both the tick strip (ascending, oldest→newest) and the
// table (descending, newest first) — one fetch, two view orders. from=-1
// means the tail (newest) window; any other value pages into history. Only
// the tail window live-polls: paging back pins the view, so the 2.5s
// refresher can't yank the reader back to the newest messages. hl, when ≥0,
// marks that offset's row for highlight-and-scroll (jump-to-offset).
func (c *Console) tapeData(ctx context.Context, topic string, partition, partitionCount int32, from, hl int64) (map[string]any, error) {
	earliest, latest, err := c.be.PartitionOffsets(ctx, topic, partition)
	if err != nil {
		return nil, err
	}
	atTail := from < 0 || from >= max(latest-tapeWindow, earliest)
	if atTail {
		from = max(latest-tapeWindow, earliest)
	} else {
		from = max(from, earliest)
	}
	to := min(from+tapeWindow, latest) // exclusive
	var msgs []Message
	if to > from {
		msgs, err = c.be.FetchWindow(ctx, topic, partition, from, int(to-from))
		if err != nil {
			return nil, err
		}
	}

	asc := make([]msgView, len(msgs))
	for i, m := range msgs {
		asc[i] = msgView{
			Offset: m.Offset, Timestamp: m.Timestamp, TimeStr: m.Timestamp.Format("15:04:05"),
			TimeFull:   m.Timestamp.Format(time.RFC3339),
			Key:        newBytesView(m.Key, m.Key == nil),
			Value:      previewOnly(m.Value),
			Headers:    m.Headers,
			Hot:        atTail && i >= len(msgs)-3,
			TickHeight: tickHeight(len(m.Value)),
		}
	}
	desc := make([]msgView, len(asc))
	for i, v := range asc {
		desc[len(asc)-1-i] = v
	}

	return map[string]any{
		"PartitionRange": partitionRange(partitionCount),
		"Messages":       asc,
		"MessagesDesc":   desc,
		"Earliest":       earliest,
		"Latest":         latest,
		"Hash":           strconv.FormatInt(latest, 10),
		"AtTail":         atTail,
		"HasOlder":       from > earliest,
		"HasNewer":       !atTail,
		"OlderFrom":      max(from-tapeWindow, earliest),
		"NewerFrom":      to,
		"WindowFrom":     from,
		"WindowLast":     max(to-1, from), // inclusive end, for display
		"HL":             hl,
	}, nil
}

func partitionRange(n int32) []int32 {
	out := make([]int32, n)
	for i := range out {
		out[i] = int32(i)
	}
	return out
}

// tickHeight is the height in pixels of a tape mark: how big the record is, on a
// log scale from empty (6 px) to a megabyte and beyond (40 px). It used to be a
// pseudo-random function of the offset, which looked meaningful and was not.
func tickHeight(size int) int {
	if size <= 0 {
		return 6
	}
	h := 6 + int(34*math.Log2(1+float64(size))/math.Log2(1+(1<<20)))
	return min(h, 40)
}

// topicMessageRaw serves a record's key or value as the exact bytes, for a value
// too large to read in the drawer or one that is binary.
func (c *Console) topicMessageRaw(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	partition64, _ := strconv.ParseInt(r.URL.Query().Get("partition"), 10, 32)
	offset, err := strconv.ParseInt(r.URL.Query().Get("offset"), 10, 64)
	if err != nil {
		c.fail(w, r, fmt.Errorf("%w: offset is not a number", errBadInput))
		return
	}
	partition := int32(partition64)
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
		err = fmt.Errorf("%w: no record at offset %d of partition %d", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	field, data := "value", msgs[0].Value
	if r.URL.Query().Get("field") == "key" {
		field, data = "key", msgs[0].Key
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s-%d-%d.%s.bin"`, safeFilename(name), partition, offset, field))
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Write(data)
}

// safeFilename keeps a topic name fit for a Content-Disposition filename.
func safeFilename(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, s)
}
