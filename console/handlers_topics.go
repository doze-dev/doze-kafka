package console

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"net/http"
	"strconv"
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
		c.fail(w, err)
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
		c.fail(w, err)
		return
	}
	c.redirect(w, r, c.prefix+"/topics/"+name, "Topic created")
}

func (c *Console) topicDelete(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.DeleteTopic(r.Context(), name); err != nil {
		c.fail(w, err)
		return
	}
	c.redirect(w, r, c.prefix+"/topics", "Topic deleted")
}

func (c *Console) topicPurge(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if err := c.be.PurgeTopic(r.Context(), name); err != nil {
		c.fail(w, err)
		return
	}
	c.redirect(w, r, c.prefix+"/topics/"+name, "Topic purged — all messages deleted")
}

func (c *Console) topicDetail(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	ctx := r.Context()

	list, _ := c.be.ListTopics(ctx)
	detail, err := c.be.DescribeTopic(ctx, name)
	if err != nil {
		c.fail(w, err)
		return
	}
	partition := partitionParam(r, detail.Partitions)

	tape, err := c.tapeData(ctx, name, partition, detail.Partitions, fromParam(r), hlParam(r))
	if err != nil {
		c.fail(w, err)
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
		c.fail(w, err)
		return
	}
	partition := partitionParam(r, count)

	_, latest, err := c.be.PartitionOffsets(ctx, name, partition)
	if err != nil {
		c.fail(w, err)
		return
	}
	hash := strconv.FormatInt(latest, 10)
	if liveUnchanged(w, r, hash) {
		return
	}

	tape, err := c.tapeData(ctx, name, partition, count, -1, -1)
	if err != nil {
		c.fail(w, err)
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
		c.fail(w, err)
		return
	}

	var key, value []byte
	if v := r.FormValue("key"); v != "" {
		key = []byte(v)
	}
	value = []byte(r.FormValue("value"))

	var headers []MessageHeader
	if raw := r.FormValue("headers"); raw != "" {
		var rows []struct{ K, V string }
		if err := json.Unmarshal([]byte(raw), &rows); err == nil {
			for _, row := range rows {
				if row.K == "" {
					continue
				}
				headers = append(headers, MessageHeader{Key: row.K, Value: []byte(row.V)})
			}
		}
	}

	partition := int32(0)
	if p := r.FormValue("partition"); p == "" || p == "auto" {
		partition = c.be.PickPartition(name, key, count)
	} else if n, err := strconv.Atoi(p); err == nil {
		partition = int32(n)
	}

	result, err := c.be.Produce(ctx, name, partition, key, value, headers, r.FormValue("acks"))
	if err != nil {
		c.fail(w, err)
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
		c.fail(w, err)
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
		c.fail(w, err)
		return
	}
	msgs, err := c.be.FetchWindow(r.Context(), name, partition, offset, 1)
	if err != nil || len(msgs) == 0 {
		c.fail(w, err)
		return
	}
	m := msgs[0]
	value := string(m.Value)
	// Pretty-print JSON payloads for reading; RawValue keeps the exact bytes
	// for "Send again" so a resend is byte-identical to the original.
	pretty := value
	if json.Valid(m.Value) {
		var buf bytes.Buffer
		if json.Indent(&buf, m.Value, "", "  ") == nil {
			pretty = buf.String()
		}
	}
	c.partial(w, "message_drawer", map[string]any{
		"Topic": name, "Partition": partition, "Msg": m,
		"TimeStr": m.Timestamp.Format(time.RFC3339), "KeyStr": string(m.Key),
		"ValueStr": pretty, "RawValue": value,
	})
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
	Offset       int64
	Timestamp    time.Time
	TimeStr      string
	KeyStr       string
	ValuePreview string
	Headers      []MessageHeader
	Hot          bool
	TickHeight   int
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
			KeyStr: string(m.Key), ValuePreview: preview(string(m.Value)), Headers: m.Headers,
			Hot:        atTail && i >= len(msgs)-3,
			TickHeight: 8 + int((m.Offset*7+int64(len(m.Headers))*6)%40),
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

func preview(s string) string {
	const max = 220 // CSS truncates at the real column width; this just bounds payload size
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
