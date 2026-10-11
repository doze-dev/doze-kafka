package console

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The topic settings the console lets you set: the ones the broker acts on.
// Kafka writes them as bare numbers (milliseconds, bytes); here they are typed
// and shown as a person would say them ("7d", "512 MB"), and sent as Kafka's
// numbers. Every other Kafka topic config is stored and reported by the broker
// but changes nothing, so the console shows those and does not offer to edit them.

// topicSetting is one editable setting: its Kafka key, and how to read and show it.
type topicSetting struct {
	Key   string // the Kafka config name, and the form field's name.
	Label string
	Hint  string
	parse func(string) (string, error) // what was typed -> Kafka's value.
	show  func(string) string          // Kafka's value -> how to show it.
	// Options, when set, are the only values: the field is a choice, not text.
	Options []string
	// Always puts the setting in the one-line summary even when the topic has not
	// set it; the others appear there only when they are the topic's own.
	Always bool
}

var topicSettings = []topicSetting{
	{Key: "retention.ms", Label: "Keep records for", Hint: "7d, 12h, 30m, 90s, or forever", parse: parseDuration, show: showDuration, Always: true},
	{Key: "retention.bytes", Label: "Keep at most", Hint: "per partition: 1GB, 500MB, or unlimited", parse: parseSize, show: showSize, Always: true},
	{Key: "segment.bytes", Label: "Segment size", Hint: "64MB, 1GB; retention removes whole segments", parse: parseSegment, show: showSize, Always: true},
	{Key: "max.message.bytes", Label: "Largest batch", Hint: "refuse a record batch bigger than this: 1MB, 64KB", parse: parseSegment, show: showSize},
	{Key: "message.timestamp.type", Label: "Record time", Hint: "LogAppendTime stamps records with the broker's clock", parse: parseChoice("CreateTime", "LogAppendTime"), show: func(v string) string { return v },
		Options: []string{"CreateTime", "LogAppendTime"}},
}

// parseChoice accepts exactly one of the given values.
func parseChoice(options ...string) func(string) (string, error) {
	return func(raw string) (string, error) {
		for _, o := range options {
			if raw == o {
				return o, nil
			}
		}
		return "", badInput("%q is not one of %s", raw, strings.Join(options, ", "))
	}
}

// durationUnits and sizeUnits are the suffixes understood, largest first so that
// a value is shown in the largest unit that divides it exactly.
var durationUnits = []struct {
	suffix string
	ms     int64
}{{"d", 86_400_000}, {"h", 3_600_000}, {"m", 60_000}, {"s", 1000}, {"ms", 1}}

var sizeUnits = []struct {
	suffix string
	bytes  int64
}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}}

// scaled reads "<number><unit>" with one of the given units (case-insensitive,
// optional space), or a bare number, which is taken in the smallest unit as Kafka
// writes it.
func scaled(raw string, units map[string]int64, what, examples string) (string, error) {
	s := strings.ToLower(strings.ReplaceAll(strings.TrimSpace(raw), " ", ""))
	digits := strings.TrimRight(s, "abcdefghijklmnopqrstuvwxyz")
	unit := s[len(digits):]
	n, err := strconv.ParseInt(digits, 10, 64)
	mult, known := units[unit]
	if err != nil || n < 0 || !known {
		return "", badInput("%q is not %s; write it like %s", raw, what, examples)
	}
	if mult != 0 && n > (1<<62)/mult {
		return "", badInput("%q is too large", raw)
	}
	return strconv.FormatInt(n*mult, 10), nil
}

func parseDuration(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "forever", "-1", "unlimited":
		return "-1", nil
	}
	return scaled(raw, map[string]int64{"": 1, "ms": 1, "s": 1000, "m": 60_000, "h": 3_600_000, "d": 86_400_000}, "a length of time", "7d, 12h, 30m, 90s, or forever")
}

func parseSize(raw string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "unlimited", "-1", "forever":
		return "-1", nil
	}
	return sizeValue(raw)
}

func parseSegment(raw string) (string, error) { return sizeValue(raw) }

func sizeValue(raw string) (string, error) {
	return scaled(raw, map[string]int64{"": 1, "b": 1, "kb": 1 << 10, "k": 1 << 10, "mb": 1 << 20, "m": 1 << 20, "gb": 1 << 30, "g": 1 << 30}, "a size", "1GB, 500MB or 64KB")
}

func showDuration(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err != nil:
		return v
	case n < 0:
		return "forever"
	}
	for _, u := range durationUnits {
		if n >= u.ms && n%u.ms == 0 {
			return strconv.FormatInt(n/u.ms, 10) + u.suffix
		}
	}
	return "0ms"
}

func showSize(v string) string {
	n, err := strconv.ParseInt(v, 10, 64)
	switch {
	case err != nil:
		return v
	case n < 0:
		return "unlimited"
	}
	for _, u := range sizeUnits {
		if n >= u.bytes && n%u.bytes == 0 {
			return strconv.FormatInt(n/u.bytes, 10) + " " + u.suffix
		}
	}
	return "0 B"
}

// settingsFromForm reads the settings from a form. A blank field means "the
// broker's default": it is absent from set, and listed in unset.
func settingsFromForm(r *http.Request) (set map[string]string, unset []string, err error) {
	set = map[string]string{}
	for _, ts := range topicSettings {
		raw := strings.TrimSpace(r.FormValue(ts.Key))
		if raw == "" {
			unset = append(unset, ts.Key)
			continue
		}
		v, err := ts.parse(raw)
		if err != nil {
			return nil, nil, fmt.Errorf("%s: %w", ts.Label, err)
		}
		set[ts.Key] = v
	}
	return set, unset, nil
}

// settingView is one editable setting as the topic page shows it.
type settingView struct {
	Key, Label, Hint string
	Own              string   // the topic's own value as typed ("7d"), or "" when it follows the broker.
	Effective        string   // what is in force, shown: the topic's own or the broker's.
	Options          []string // a choice rather than text.
	Summary          bool     // shown in the panel's one-line summary.
}

// configView is a topic's configuration for its page.
type configView struct {
	Settings []settingView
	Others   []ConfigEntry // everything else the broker reports, for reading.
	Compact  bool          // cleanup.policy asks for compaction: the broker cleans it in the background.
}

func newConfigView(entries []ConfigEntry) configView {
	byKey := map[string]ConfigEntry{}
	for _, e := range entries {
		byKey[e.Name] = e
	}
	var v configView
	editable := map[string]bool{}
	for _, ts := range topicSettings {
		editable[ts.Key] = true
		e, ok := byKey[ts.Key]
		sv := settingView{Key: ts.Key, Label: ts.Label, Hint: ts.Hint, Options: ts.Options}
		if ok {
			sv.Effective = ts.show(e.Value)
			if e.Own {
				sv.Own = strings.ReplaceAll(sv.Effective, " ", "")
			}
		}
		sv.Summary = ok && (ts.Always || e.Own)
		v.Settings = append(v.Settings, sv)
	}
	for _, e := range entries {
		if editable[e.Name] {
			continue
		}
		if e.Name == "cleanup.policy" && strings.Contains(e.Value, "compact") {
			v.Compact = true
		}
		v.Others = append(v.Others, e)
	}
	return v
}

// refusal turns a broker's refusal into an error that keeps its code and the
// broker's own ErrorMessage (E3): the Refusal shown names the Kafka error and says
// what the broker said about this request.
func refusal(code int16, msg *string) error {
	err := kerr.TypedErrorForCode(code)
	if err == nil {
		return nil
	}
	r := &kafkaRefusal{Err: err}
	if msg != nil {
		r.Msg = *msg
	}
	return r
}

// AlterTopicConfig sets and unsets a topic's own settings in one change.
func (b *backend) AlterTopicConfig(ctx context.Context, name string, set map[string]string, unset []string) error {
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeTopic, name
	for _, k := range sortedConfigKeys(set) {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		v := set[k]
		c.Name, c.Op, c.Value = k, kmsg.IncrementalAlterConfigOpSet, &v
		res.Configs = append(res.Configs, c)
	}
	for _, k := range unset {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name, c.Op = k, kmsg.IncrementalAlterConfigOpDelete
		res.Configs = append(res.Configs, c)
	}
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(ctx, b.cl)
	if err != nil {
		return err
	}
	for _, r := range resp.Resources {
		if err := refusal(r.ErrorCode, r.ErrorMessage); err != nil {
			return err
		}
	}
	return nil
}

// topicConfig changes a topic's settings from the form on its page, and answers
// with the panel as it now is.
func (c *Console) topicConfig(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	set, unset, err := settingsFromForm(r)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	if err := c.be.AlterTopicConfig(r.Context(), name, set, unset); err != nil {
		c.fail(w, r, err)
		return
	}
	detail, err := c.be.DescribeTopic(r.Context(), name)
	if err != nil {
		c.fail(w, r, err)
		return
	}
	toast(w, "Topic settings saved; they are in force now")
	c.partial(w, "topic_config", map[string]any{"Topic": detail, "Config": newConfigView(detail.Config), "Open": true})
}
