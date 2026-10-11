package console

import "github.com/doze-dev/doze-kafka/internal/settings"

// What the console says about a setting (its unit, range, default, and whether the
// broker acts on it) is read from internal/settings, the one table the broker's
// DescribeConfigs and validation read too. Nothing here is typed per field.

// settingHint is the line under a topic setting's field: how to write the value,
// then the unit, range, Kafka's default and whether the broker acts on it.
func settingHint(key, syntax string) string {
	s, ok := settings.TopicConfig(key)
	if !ok {
		return syntax
	}
	rule := s.Rule()
	if syntax == "" {
		return rule
	}
	return syntax + " (" + rule + ")"
}

// compatLevels are the registry's compatibility levels, from the settings table.
func compatLevels() []string { return settings.CompatLevels() }
