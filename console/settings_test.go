package console

import (
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/settings"
	"github.com/doze-dev/doze-kafka/schemaregistry"
)

// Every setting the console offers a form field for has a record in the settings
// table (the broker's DescribeConfigs and validation read the same table), and the
// line under the field is generated from it.
func TestTopicFormFieldsAreInTheSettingsTable(t *testing.T) {
	for _, ts := range topicSettings {
		s, ok := settings.TopicConfig(ts.Key)
		if !ok {
			t.Errorf("form field %q has no settings record", ts.Key)
			continue
		}
		hint := settingHint(ts.Key, ts.Hint)
		if !strings.Contains(hint, s.Rule()) {
			t.Errorf("%s: hint %q does not carry the table's rule %q", ts.Key, hint, s.Rule())
		}
		if len(s.Choices) > 0 && strings.Join(s.Choices, ",") != strings.Join(ts.Options, ",") {
			t.Errorf("%s: console options %v differ from the table's %v", ts.Key, ts.Options, s.Choices)
		}
	}
	// The broker's defaults that differ from Kafka's are stated, not implied.
	if h := settingHint("retention.ms", ""); !strings.Contains(h, "604800000") {
		t.Errorf("retention.ms hint does not say Kafka's default differs: %q", h)
	}
}

// The registry's levels the console offers are the table's, and the table's are
// the registry's own.
func TestCompatLevelsMatchTheRegistry(t *testing.T) {
	want := []string{
		schemaregistry.CompatBackward, schemaregistry.CompatBackwardTransitive, schemaregistry.CompatForward,
		schemaregistry.CompatForwardTransitive, schemaregistry.CompatFull, schemaregistry.CompatFullTransitive,
		schemaregistry.CompatNone,
	}
	got := compatLevels()
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("levels = %v, want %v", got, want)
	}
}
