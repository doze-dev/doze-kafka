package console

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// The Used by tab (Sc-11, and Sc-17 when a message carries an id the registry does
// not have): which of the subject's ids the messages on its topics carry, counted
// from the 5-byte header by the schema scan (bounded; what it leaves unread it says).
// The topics are the one the subject is named for and those named after it (a
// dead-letter topic, "<topic>.dlq"); a subject named for no topic (RecordNameStrategy)
// is looked for on every topic, the scan's budget shared between them.

// usedRow is one id on one topic.
type usedRow struct {
	Topic, TopicURL string
	FirstOfTopic    bool
	Part            string // "value" or "key"
	ID, Version     int
	Share           float64 // of the topic's framed messages, in percent.
	Count           int
	Open, OpenURL   string
	Old             bool // an id of a version before the latest.
}

// undecodedRow is one message whose id the registry does not have (Sc-17).
type undecodedRow struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       string
	ID        int
	URL       string
}

// usedTopics is where the subject's ids are looked for.
func (c *Console) usedTopics(ctx context.Context, subject string) []string {
	topics, err := c.be.ListTopics(ctx)
	if err != nil {
		return nil
	}
	base := subjectTopic(subject)
	var out []string
	for _, t := range topics {
		if strings.HasPrefix(t.Name, "__") {
			continue
		}
		if base == "" || t.Name == base || strings.HasPrefix(t.Name, base+".") || strings.HasPrefix(t.Name, base+"-") {
			out = append(out, t.Name)
		}
	}
	sort.Strings(out)
	if base == "" && len(out) > 8 {
		out = out[:8]
	}
	return out
}

// usedBy fills the Used by tab's data.
func (c *Console) usedBy(ctx context.Context, subject string, versions []int, data map[string]any) {
	ids := map[int]int{} // id -> version
	var idList []int
	for _, v := range versions {
		if sv, err := c.reg.Version(ctx, subject, v); err == nil {
			ids[sv.ID] = v
			idList = append(idList, sv.ID)
		}
	}
	latestID := 0
	if len(idList) > 0 {
		latestID = idList[len(idList)-1]
	}
	part := "value"
	if strings.HasSuffix(subject, "-key") {
		part = "key"
	}
	var rows []usedRow
	var undecoded []undecodedRow
	var unread []string
	unknown := map[int]int{}
	scanned, decoded, total := 0, 0, 0
	var firstTopic string
	for _, topic := range c.usedTopics(ctx, subject) {
		scan, err := c.scanSchemaIDs(ctx, topic)
		if err != nil || scan.GaveUp {
			continue
		}
		scanned += scan.Scanned
		unread = append(unread, scan.Unread...)
		framed := 0
		for _, u := range scan.IDs {
			framed += u.Count
		}
		first := true
		// Newest id first, as the board lists them.
		for i := len(scan.IDs) - 1; i >= 0; i-- {
			u := scan.IDs[i]
			v, mine := ids[u.ID]
			if !mine {
				// An id the registry knows belongs to another subject; one it does not
				// know at all is a message nothing here can decode (Sc-17).
				if _, err := c.reg.IDVersions(ctx, u.ID); err != nil {
					unknown[u.ID] += u.Count
					undecoded = append(undecoded, c.undecodedAt(ctx, u.First, u.ID))
					if u.Count > 1 && u.Last != u.First {
						undecoded = append(undecoded, c.undecodedAt(ctx, u.Last, u.ID))
					}
				}
				continue
			}
			total += u.Count
			decoded += u.Count
			if firstTopic == "" {
				firstTopic = topic
			}
			r := usedRow{Topic: topic, TopicURL: c.topicURL(topic), FirstOfTopic: first, Part: part, ID: u.ID, Version: v, Count: u.Count, Old: u.ID != latestID}
			first = false
			if framed > 0 {
				r.Share = 100 * float64(u.Count) / float64(framed)
			}
			at := u.Last
			r.Open = "newest on this id"
			if u.ID != latestID {
				at, r.Open = u.First, "first on this id"
				if u.First.Partition == u.Last.Partition && u.Count > 1 {
					r.Open = fmt.Sprintf("offsets %s to %s", groupDigits(u.First.Offset), groupDigits(u.Last.Offset))
				}
			}
			r.OpenURL = c.messageURL(at)
			rows = append(rows, r)
		}
	}
	unknownN := 0
	var unknownIDs []int
	for id, n := range unknown {
		unknownN += n
		unknownIDs = append(unknownIDs, id)
	}
	sort.Ints(unknownIDs)
	total += unknownN
	data["UsedRows"], data["Undecoded"], data["Scanned"], data["Unread"] = rows, undecoded, scanned, unread
	if len(undecoded) > 0 {
		data["View"] = "schema_undecoded"
	}
	data["UnknownCount"], data["UnknownIDs"], data["Decoded"], data["Framed"] = unknownN, unknownIDs, decoded, total
	data["IDList"], data["UsedTopic"] = idList, firstTopic
	if len(rows) == 0 && firstTopic == "" {
		if t := subjectTopic(subject); t != "" {
			data["UsedTopic"] = t
		}
	}
	// Why an old id still has messages (Sc-11's "Why 720 are still on v1").
	for _, r := range rows {
		if r.Old && r.Count > 0 {
			data["OldRow"] = r
			break
		}
	}
	if o, ok := c.reg.Overview(ctx); ok {
		data["IDsInUse"] = o.IDsInUse
	}
}

// undecodedAt is Sc-17's row for the message at at.
func (c *Console) undecodedAt(ctx context.Context, at tpOffset, id int) undecodedRow {
	row := undecodedRow{Topic: at.Topic, Partition: at.Partition, Offset: at.Offset, ID: id, URL: c.messageURL(at)}
	if ms, err := c.be.FetchWindow(ctx, at.Topic, at.Partition, at.Offset, 1); err == nil && len(ms) > 0 {
		row.Key = oneLine(string(ms[0].Key), 40)
	}
	return row
}

// messageURL is the page of one record.
func (c *Console) messageURL(at tpOffset) string {
	return c.topicURL(at.Topic) + "/message?partition=" + strconv.Itoa(int(at.Partition)) + "&offset=" + strconv.FormatInt(at.Offset, 10)
}

// schemaOpenID is Sc-11's Try it: the newest message on the subject's topics that
// carries a given schema id.
func (c *Console) schemaOpenID(w http.ResponseWriter, r *http.Request) {
	if c.registryOff(w, r) {
		return
	}
	ctx := r.Context()
	subject := groupFromSegment(r.PathValue("subject"))
	id, err := strconv.Atoi(strings.TrimSpace(r.FormValue("id")))
	if err != nil || id <= 0 {
		c.fail(w, r, badInput("a schema id is a number, such as 7"))
		return
	}
	for _, topic := range c.usedTopics(ctx, subject) {
		scan, err := c.scanSchemaIDs(ctx, topic)
		if err != nil {
			continue
		}
		if u := scan.idUse(id); u != nil {
			c.redirect(w, r, c.messageURL(u.Last), "")
			return
		}
	}
	c.fail(w, r, fmt.Errorf("%w: no message read on this subject's topics carries schema id %d", errNotFound, id))
}

// idRangesText is the ids a registry holds, as Sc-17 says them: "ids 1 to 8 and 10".
func idRangesText(rs [][2]int) string {
	var parts []string
	n := 0
	for _, r := range rs {
		n += r[1] - r[0] + 1
		if r[0] == r[1] {
			parts = append(parts, strconv.Itoa(r[0]))
		} else {
			parts = append(parts, fmt.Sprintf("%d to %d", r[0], r[1]))
		}
	}
	if len(parts) == 0 {
		return "no ids"
	}
	word := "ids "
	if n == 1 {
		word = "id "
	}
	if len(parts) == 1 {
		return word + parts[0]
	}
	return word + strings.Join(parts[:len(parts)-1], ", ") + " and " + parts[len(parts)-1]
}
