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
	c.render(w, r, "topic_create", map[string]any{"List": list, "Page": "topics", "Title": "Create topic", "Config": newConfigView(nil)})
}

func (c *Console) topicCreate(w http.ResponseWriter, r *http.Request) {
	name := r.FormValue("name")
	// What was typed is what is created, or it is an error: "abc", 0 and -5 used
	// to make a one-partition topic, and 4294967299 a three-partition one.
	partitions := int64(1)
	if raw := strings.TrimSpace(r.FormValue("partitions")); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 32)
		if err != nil || n < 1 {
			c.fail(w, r, badInput("%q is not a number of partitions; use a whole number, 1 or more", raw))
			return
		}
		partitions = n
	}
	// Settings left blank follow the broker's defaults, now and if those change.
	config, _, err := settingsFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.CreateTopic(r.Context(), name, int32(partitions), config); err != nil {
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

	data := map[string]any{
		"List": list, "Topic": detail, "Page": "topics",
		"Partition": partition, "Title": name,
		"Consumers": c.topicConsumers(ctx, name),
		"Config":    newConfigView(detail.Config),
	}
	if q, all, before, asked := searchParams(r); asked {
		// A search replaces the tape and the live table with what it found.
		found, err := c.searchTopic(ctx, name, partition, detail.Partitions, q, all, before)
		if err != nil {
			c.fail(w, r, err)
			return
		}
		data["Search"], data["PartitionRange"], data["Rows"] = found, partitionRange(detail.Partitions), found.Rows
		c.render(w, r, "topic_detail", data)
		return
	}
	jump := jumpParam(r)
	atJump, atNote, atAsked, err := c.atParam(r, name, partition)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if atAsked {
		jump = atJump
	}
	tape, err := c.tapeData(ctx, name, detail.ID, partition, detail.Partitions, fromParam(r), hlParam(r), jump)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	maps.Copy(data, tape)
	if atNote != "" && tape["JumpNote"] == "" {
		data["JumpNote"] = atNote
	}
	c.render(w, r, "topic_detail", data)
}

// topicTape is the live-polled partial backing the tape + message table.
func (c *Console) topicTape(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	id, count, err := c.be.TopicShape(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	partition := partitionParam(r, count)

	bounds, err := c.be.PartitionBounds(ctx, name, count)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	hash := boundsHash(id, bounds)
	if liveUnchanged(w, r, hash) {
		return
	}

	tape, err := c.tapeData(ctx, name, id, partition, count, -1, -1, -1)
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

	id, count, err := c.be.TopicShape(ctx, name)
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
	// The view stays on the partition being looked at. It used to switch to the
	// partition the record landed on, with the address bar, the composer and the
	// page's links all still naming the old one. The toast says where the record
	// went, and the partition tabs' counts show it arriving.
	viewing := result.Partition
	if n, err := strconv.ParseInt(r.FormValue("partition_ctx"), 10, 32); err == nil && n >= 0 && n < int64(count) {
		viewing = int32(n)
	}
	switch {
	case !result.Confirmed:
		toast(w, "Sent — no confirmation requested (acks=0)")
	case result.Partition != viewing:
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10)+" (you are viewing partition "+strconv.Itoa(int(viewing))+")")
	default:
		toast(w, "Sent to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	}

	tape, err := c.tapeData(ctx, name, id, viewing, count, -1, -1, -1)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": viewing}
	maps.Copy(data, tape)
	c.partial(w, "tape_and_table", data)
}

func (c *Console) topicMessage(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	partition, offset, err := recordCoordinates(r.URL.Query().Get("partition"), r.URL.Query().Get("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
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
	// Neighbouring records, for the drawer's older/newer buttons.
	older, newer := int64(-1), int64(-1)
	if earliest, latest, err := c.be.PartitionOffsets(r.Context(), name, partition); err == nil {
		if offset-1 >= earliest {
			older = offset - 1
		}
		if offset+1 < latest {
			newer = offset + 1
		}
	}
	hdrs := headerViews(m.Headers)
	valueV := newBytesView(m.Value, m.Value == nil)
	keyV := newBytesView(m.Key, m.Key == nil)
	c.partial(w, "message_drawer", map[string]any{
		"Topic": name, "Partition": partition, "Msg": m, "Older": older, "Newer": newer,
		"TimeStr": m.Timestamp.Format(time.RFC3339), "Key": keyV, "Value": valueV, "Headers": hdrs,
		// "Send again" is done by the server from the stored record, so it is exact
		// for any bytes. These are for "Edit a copy": the composer is prefilled with
		// the record as text (or base64 when it is not text), as stored rather than
		// as displayed, for changing before it is sent.
		"EditKey": editField(keyV), "EditKeyEnc": resendEnc(keyV),
		"EditValue": editField(valueV), "EditValueEnc": resendEnc(valueV),
		"EditHeaders": resendHeaders(hdrs),
		"CanEdit":     (valueV.Null || valueV.Empty || valueV.B64 != "") && (keyV.Null || keyV.Empty || keyV.B64 != ""),
	})
}

// topicResend produces an exact copy of a stored record: the same key, value and
// headers, byte for byte, to the same partition. It is done here rather than by
// prefilling a form because a browser cannot carry arbitrary bytes faithfully: a
// text input drops newlines, an editor rewrites line endings, re-indented JSON is
// not the JSON that was stored, and a form has no way to say "an empty key" as
// distinct from "no key".
func (c *Console) topicResend(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()
	partition, offset, err := recordCoordinates(r.FormValue("partition"), r.FormValue("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
	msgs, err := c.be.FetchWindow(ctx, name, partition, offset, 1)
	if err == nil && (len(msgs) == 0 || msgs[0].Offset != offset) {
		err = fmt.Errorf("%w: no record at offset %d of partition %d — it may have been deleted by retention or a purge", errNotFound, offset, partition)
	}
	if err != nil {
		c.fail(w, r, err)
		return
	}
	m := msgs[0]
	result, err := c.be.Produce(ctx, name, partition, m.Key, m.Value, m.Headers, "1")
	if err != nil {
		c.fail(w, r, err)
		return
	}
	toast(w, "Sent an exact copy to partition "+strconv.Itoa(int(result.Partition))+" · offset "+strconv.FormatInt(result.Offset, 10))
	id, count, err := c.be.TopicShape(ctx, name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	tape, err := c.tapeData(ctx, name, id, partition, count, -1, -1, -1)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	data := map[string]any{"Topic": topicRef{Name: name}, "Partition": partition}
	maps.Copy(data, tape)
	c.partial(w, "tape_and_table", data)
}

// recordCoordinates parses a partition and an offset that name one record. Both
// must be numbers: a partition that is not one used to be read as partition 0.
func recordCoordinates(rawPartition, rawOffset string) (int32, int64, error) {
	partition := int64(0)
	if rawPartition != "" {
		n, err := strconv.ParseInt(rawPartition, 10, 32)
		if err != nil || n < 0 {
			return 0, 0, fmt.Errorf("%w: partition %q is not a partition number", errBadInput, rawPartition)
		}
		partition = n
	}
	offset, err := strconv.ParseInt(rawOffset, 10, 64)
	if err != nil || offset < 0 {
		return 0, 0, fmt.Errorf("%w: offset %q is not an offset", errBadInput, rawOffset)
	}
	return int32(partition), offset, nil
}

// editField is the text a field is carried as into the composer.
func editField(v bytesView) string {
	switch {
	case v.Null || v.Empty:
		return ""
	case v.Binary:
		return v.B64
	default:
		return v.Raw
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
	case looksBinarySample(b):
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

// jumpMalformed is jumpParam's answer for something typed that is not an offset.
const jumpMalformed = -2

// jumpParam reads ?jump= (the "go to offset" box): the offset, -1 when absent, or
// jumpMalformed when something was typed that is not one. The last used to be
// treated as absent, so "12abc" showed the tail as if nothing had been asked.
func jumpParam(r *http.Request) int64 {
	raw := strings.TrimSpace(r.URL.Query().Get("jump"))
	if raw == "" {
		return -1
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || n < 0 {
		return jumpMalformed
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
	Partition  int32
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

// maxPartitionTabs is how many partitions get a tab each. Past it the tabs would
// overflow the panel (a 1000-partition topic is allowed), so a select replaces them.
const maxPartitionTabs = 24

// boundsHash identifies what the tape shows: every partition's retained range, so
// a produce to ANY partition changes it and the tabs' counts refresh.
func boundsHash(topicID string, bounds []PartitionBound) string {
	var b strings.Builder
	// The topic's id, because a topic purged (deleted and re-created) and written
	// to again can hold different records at exactly the same offsets; without it
	// another open tab was told "nothing changed" and kept showing the old ones.
	b.WriteString(topicID)
	b.WriteByte('|')
	for _, p := range bounds {
		fmt.Fprintf(&b, "%d-%d,", p.Earliest, p.Latest)
	}
	return b.String()
}

// tapeData fetches one tapeWindow-sized window of messages on a partition
// and shapes them for both the tick strip (ascending, oldest→newest) and the
// table (descending, newest first) — one fetch, two view orders. from=-1
// means the tail (newest) window; any other value pages into history. Only
// the tail window live-polls: paging back pins the view, so the 2.5s
// refresher can't yank the reader back to the newest messages. hl, when ≥0,
// marks that offset's row for highlight-and-scroll (jump-to-offset).
func (c *Console) tapeData(ctx context.Context, topic, topicID string, partition, partitionCount int32, from, hl, jump int64) (map[string]any, error) {
	bounds, err := c.be.PartitionBounds(ctx, topic, partitionCount)
	if err != nil {
		return nil, err
	}
	earliest, latest := bounds[partition].Earliest, bounds[partition].Latest

	// A jump to an offset the log does not hold says so, rather than quietly
	// landing somewhere else.
	jumpNote := ""
	if jump == jumpMalformed {
		jumpNote = "That is not an offset. Type a whole number, 0 or more."
		jump = -1
	}
	if jump >= 0 {
		switch {
		case latest == earliest:
			jumpNote = "This partition has no messages to jump to."
		case jump < earliest:
			jumpNote = fmt.Sprintf("Offset %d is before the start of the log (the oldest is %d) — showing from there.", jump, earliest)
			from, hl = earliest, -1
		case jump >= latest:
			jumpNote = fmt.Sprintf("Offset %d has not been written yet (the newest is %d) — showing the latest messages.", jump, latest-1)
			from, hl = -1, -1
		default:
			from, hl = max(jump-20, earliest), jump
		}
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
		asc[i] = newMsgView(m)
		asc[i].Hot = atTail && i >= len(msgs)-3
	}
	desc := make([]msgView, len(asc))
	for i, v := range asc {
		desc[len(asc)-1-i] = v
	}

	return map[string]any{
		"PartitionRange": partitionRange(partitionCount),
		"Bounds":         bounds,
		"PartSelect":     partitionCount > maxPartitionTabs,
		"JumpNote":       jumpNote,
		"Messages":       asc,
		"Rows":           desc,
		"Earliest":       earliest,
		"Latest":         latest,
		"Count":          latest - earliest,
		"Hash":           boundsHash(topicID, bounds),
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
	partition, offset, err := recordCoordinates(r.URL.Query().Get("partition"), r.URL.Query().Get("offset"))
	if err != nil {
		c.fail(w, r, err)
		return
	}
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
