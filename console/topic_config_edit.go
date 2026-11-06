package console

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/settings"
	"github.com/twmb/franz-go/pkg/kerr"
)

// The Config tab (Tp-18 to Tp-20): every key the broker reports for the topic, where
// each value comes from, whether the broker acts on it, and one key at a time edited
// with what it was, what it becomes and what that changes. What acts and what is only
// kept is read from the settings table the broker itself reads (internal/settings),
// never typed here.

// cfgRow is one key of the table.
type cfgRow struct {
	Key, Value, Human string
	Own               bool
	Acts              bool   // the broker does something with it.
	Why               string // after "acts" or "kept": what.
	Compaction        bool   // only matters when cleanup.policy has compact.
	Editing           bool
}

// cfgTable is the table and the counts the filters carry.
type cfgTable struct {
	Rows                    []cfgRow
	All, Overrides, Compact int
	Acting                  int
	Show                    string // "", "overrides" or "compaction".
	Edit                    string
	EditRow                 *cfgRow
	Before                  cfgPreview
}

func (c *Console) configTable(entries []ConfigEntry, show, edit string) cfgTable {
	t := cfgTable{Show: show, Edit: edit}
	for _, e := range entries {
		row := cfgRow{Key: e.Name, Value: e.Value, Own: e.Own, Human: humanValue(e.Name, e.Value)}
		if s, ok := settings.Lookup(settings.Topic, e.Name); ok {
			row.Acts = s.Effect == settings.Applied || s.Effect == settings.AppliedCompacted
			row.Compaction = s.Effect == settings.AppliedCompacted
			switch {
			case s.Effect == settings.AppliedCompacted:
				row.Why = "on a compacted topic: " + s.Meaning
			case row.Acts:
				row.Why = s.Meaning
			default:
				row.Why = "it changes nothing here: " + s.Meaning
			}
		}
		t.All++
		if row.Own {
			t.Overrides++
		}
		if row.Compaction {
			t.Compact++
		}
		if row.Acts {
			t.Acting++
		}
		row.Editing = edit == e.Name
		if row.Editing {
			r := row
			t.EditRow = &r
		}
		switch {
		case show == "overrides" && !row.Own, show == "compaction" && !row.Compaction:
			if !row.Editing {
				continue
			}
		}
		t.Rows = append(t.Rows, row)
	}
	slices.SortFunc(t.Rows, func(a, b cfgRow) int { return strings.Compare(a.Key, b.Key) })
	return t
}

// humanValue says a number the way a person would, in brackets after it: 604800000
// is "7d", 1073741824 is "1 GB". A value that is not a plain number, or is small, or
// has no unit, gets nothing.
func humanValue(key, raw string) string {
	s, ok := settings.Lookup(settings.Topic, key)
	n, err := strconv.ParseInt(raw, 10, 64)
	if !ok || err != nil {
		return ""
	}
	switch {
	case s.Unit == "ms" && n >= 1000 && n < 9e18:
		return showDuration(raw)
	case s.Unit == "bytes" && n >= 1024:
		return showSize(raw)
	case n == -1 && (key == "retention.ms" || key == "retention.bytes"):
		return "no limit"
	}
	return ""
}

var plainInt = regexp.MustCompile(`^-?[0-9]+$`)

// cfgValue reads what was typed for a key: a plain number is Kafka's, a length of time
// or a size is written out for a setting that has that unit, anything else is sent as it
// is for the broker to judge.
func cfgValue(key, raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", badInput("%s", "enter a value, or reset the key to the broker's default")
	}
	if plainInt.MatchString(raw) {
		return raw, nil
	}
	if s, ok := settings.Lookup(settings.Topic, key); ok {
		switch s.Unit {
		case "ms":
			return parseDuration(raw)
		case "bytes":
			return parseSize(raw)
		}
	}
	return raw, nil
}

// cfgPreview is the edit row's Before, After and What it affects.
type cfgPreview struct {
	Key                               string
	Before, BeforeHuman, BeforeSource string
	After, AfterHuman                 string
	Error                             string
	Affects                           string
	Call                              string
}

func (c *Console) configPreview(entries []ConfigEntry, key, typed string, singleSegment, partitions int) cfgPreview {
	p := cfgPreview{Key: key}
	for _, e := range entries {
		if e.Name == key {
			p.Before, p.BeforeHuman = e.Value, humanValue(key, e.Value)
			p.BeforeSource = "default"
			if e.Own {
				p.BeforeSource = "override on this topic"
			}
		}
	}
	v, err := cfgValue(key, typed)
	if err != nil {
		p.Error = err.Error()
		return p
	}
	p.After, p.AfterHuman = v, humanValue(key, v)
	p.Call = fmt.Sprintf("sends IncrementalAlterConfigs SET %s=%s", key, v)
	p.Affects = c.configAffects(key, v, p.AfterHuman, singleSegment, partitions)
	return p
}

// configAffects says what a new value changes, from the settings table, and for the
// retention keys from how the sweep works (TOP-3, TOP-18).
func (c *Console) configAffects(key, v, human string, singleSegment, partitions int) string {
	s, ok := settings.Lookup(settings.Topic, key)
	if !ok {
		return ""
	}
	switch key {
	case "retention.ms":
		if v == "-1" {
			return "Records are kept for ever: nothing is deleted for its age."
		}
		out := "Only closed segments whose newest record is older than " + orDefault(human, v+" ms") + " are deleted; a segment's age is the largest timestamp of its records, and one with none is judged by when it was written. Open partitions are checked every second, the rest every minute. The active segment is never deleted."
		if partitions > 0 && singleSegment == partitions {
			out += fmt.Sprintf(" All %d partitions are a single active segment each, so nothing is removed until a segment rolls (segment.bytes).", partitions)
		}
		return out + " Consumers behind the new log start get OFFSET_OUT_OF_RANGE."
	case "retention.bytes":
		if v == "-1" {
			return "No size limit: nothing is deleted for a partition's size."
		}
		return "Closed segments are deleted, oldest first, while a partition holds more than " + orDefault(human, v+" bytes") + "; the active segment is never deleted. Consumers behind the new log start get OFFSET_OUT_OF_RANGE."
	}
	out := capitalise(s.Meaning) + ". "
	switch s.Effect {
	case settings.Applied:
		out += "The broker acts on it: " + s.Enforcer + "."
	case settings.AppliedCompacted:
		out += "The broker acts on it when cleanup.policy has compact: " + s.Enforcer + "."
	default:
		out += "The broker stores and returns it and changes nothing."
	}
	if s.Range != "" {
		out += " Valid range: " + s.Range + "."
	}
	return out
}

func orDefault(s, d string) string {
	if s == "" {
		return d
	}
	return s
}

func capitalise(s string) string {
	if s == "" {
		return s
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// singleSegments counts the partitions that hold one segment, when the files can be read.
func (c *Console) singleSegments(topic string, partitions int) int {
	d, ok := c.topicDisk(topic, partitions)
	if !ok {
		return -1
	}
	n := 0
	for _, p := range d.Parts {
		if p.Segments == 1 {
			n++
		}
	}
	return n
}

// topicConfigPreview answers the edit row's Before, After and What it affects as a
// value is typed. It changes nothing.
func (c *Console) topicConfigPreview(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	d, err := c.be.DescribeTopic(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	prev := c.configPreview(d.Config, key, r.URL.Query().Get("value"), c.singleSegments(name, int(d.Partitions)), int(d.Partitions))
	c.partial(w, "cfg_preview", map[string]any{"Topic": d, "P": prev})
}

// topicConfigKey sets one key of a topic, or with reset=1 removes the topic's own value
// so the broker's default applies again. A key the broker does not know is refused by
// the broker, and the page adds the nearest names it has.
func (c *Console) topicConfigKey(w http.ResponseWriter, r *http.Request) {
	name, key := r.PathValue("name"), r.PathValue("key")
	if k := strings.TrimSpace(r.FormValue("key")); k != "" {
		key = k
	}
	var err error
	if r.FormValue("reset") == "1" {
		err = c.be.AlterTopicConfig(r.Context(), name, nil, []string{key})
	} else {
		var v string
		if v, err = cfgValue(key, r.FormValue("value")); err == nil {
			err = c.be.AlterTopicConfig(r.Context(), name, map[string]string{key: v}, nil)
		}
	}
	if err != nil {
		c.fail(w, r, c.explainConfigRefusal(r, name, key, err))
		return
	}
	msg := "Saved " + key
	if r.FormValue("reset") == "1" {
		msg = key + " follows the broker's default again"
	}
	c.redirect(w, r, c.topicURL(name)+"?tab=config", msg)
}

// explainConfigRefusal adds to the broker's own words what the console knows: the range
// the settings table gives for the key, or the nearest keys when it is not one.
func (c *Console) explainConfigRefusal(r *http.Request, topic, key string, err error) error {
	var kr *kafkaRefusal
	if !errors.As(err, &kr) || kr.Err != kerr.InvalidConfig {
		return err
	}
	if s, ok := settings.Lookup(settings.Topic, key); ok {
		if s.Range != "" {
			kr.Facts = "Valid range: " + s.Range + "."
			if s.Unit != "" {
				kr.Facts = "Valid range: " + s.Range + " (" + s.Unit + ")."
			}
		} else if len(s.Choices) > 0 {
			kr.Facts = "Valid: " + strings.Join(s.Choices, ", ") + "."
		}
		return kr
	}
	var names []string
	for _, s := range settings.All(settings.Topic) {
		names = append(names, s.Name)
	}
	if near := nearestNames(key, names, 3); len(near) > 0 {
		kr.Facts = "Not a topic config this broker knows. Did you mean " + strings.Join(near, ", ") + "?"
	} else {
		kr.Facts = "Not a topic config this broker knows."
	}
	return kr
}

// nearest returns up to n of names closest to key by edit distance, within reach.
func nearestNames(key string, names []string, n int) []string {
	type sc struct {
		name string
		d    int
	}
	var all []sc
	for _, nm := range names {
		if d := editDistance(strings.ToLower(key), nm); d <= max(2, len(nm)/4) {
			all = append(all, sc{nm, d})
		}
	}
	slices.SortFunc(all, func(a, b sc) int {
		if a.d != b.d {
			return a.d - b.d
		}
		return strings.Compare(a.name, b.name)
	})
	var out []string
	for _, s := range all {
		if len(out) == n {
			break
		}
		out = append(out, s.name)
	}
	return out
}
