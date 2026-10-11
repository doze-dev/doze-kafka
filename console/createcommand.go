package console

import (
	"strconv"
	"strings"
)

// createCommand is the kafka-topics.sh call that makes what the create form describes:
// the same CreateTopics request the console sends. A name left empty is the example
// name; settings are in Kafka's own numbers, in the order the form lists them, and
// only those that were filled in appear.
func createCommand(f connectFacts, name, partitions string, set map[string]string) string {
	if name == "" {
		name = "orders.created"
	}
	n := 1
	if v, err := strconv.Atoi(strings.TrimSpace(partitions)); err == nil && v >= 1 {
		n = v
	}
	lines := []string{"kafka-topics.sh --bootstrap-server " + f.Bootstrap}
	if f.CommandConfig != "" {
		lines = append(lines, "  "+strings.TrimSpace(f.CommandConfig))
	}
	lines = append(lines, "  --create --topic "+shellQuote(name))
	lines = append(lines, "  --partitions "+strconv.Itoa(n)+" --replication-factor 1")
	for _, ts := range topicSettings {
		if v, ok := set[ts.Key]; ok {
			lines = append(lines, "  --config "+ts.Key+"="+shellQuote(v))
		}
	}
	return strings.Join(lines, " \\\n")
}

// shellQuote leaves a word that needs no quoting alone and single-quotes the rest.
func shellQuote(s string) string {
	safe := s != ""
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-:/=,+", r)) {
			safe = false
		}
	}
	if safe {
		return s
	}
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
