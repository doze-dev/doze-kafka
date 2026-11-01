package console

import (
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/settings"
)

// What the console says about a setting (its unit, range, default, and whether the
// broker acts on it) is read from internal/settings, the one table the broker's
// DescribeConfigs and validation read too. Nothing here is typed per field.

// settingHint is the line under a topic setting's field: what the table teaches
// about it, in the design's words, and how to write the value.
func settingHint(key, syntax string) string {
	s, ok := settings.TopicConfig(key)
	if !ok {
		return syntax
	}
	say := s.Say(config.Config{})
	if syntax == "" {
		return say
	}
	return say + " Write it like " + syntax + "."
}

// compatLevels are the registry's compatibility levels, from the settings table.
func compatLevels() []string { return settings.CompatLevels() }
