package broker_test

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/settings"
)

// The settings table says, for each topic config, whether the broker does anything
// with it; the console's line under a field ("stored only: it changes nothing here")
// is read from that. The record is written by hand and the code moves on: the
// produce handler came to read compression.type (an uncompressed topic holds a
// compressed batch to max.message.bytes as it inflates) and the log cleaner to write
// the codec it names, while the table went on saying "stored only".
//
// So the table is held to the code. A topic config whose name the broker's own
// sources spell (this package, outside its tests) is one the broker reads, and its
// record must say it acts; a record that says it acts must be read somewhere here.
func TestSettingsTable_ATopicConfigTheBrokerReadsIsRecordedAsActing(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	literal := regexp.MustCompile(`"([a-z]+(?:\.[a-z]+)+)"`)
	read := map[string][]string{} // config name -> files that spell it
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range literal.FindAllStringSubmatch(string(src), -1) {
			if _, ok := settings.TopicConfig(m[1]); ok && (len(read[m[1]]) == 0 || read[m[1]][len(read[m[1]])-1] != f) {
				read[m[1]] = append(read[m[1]], f)
			}
		}
	}
	if len(read) < 8 {
		t.Fatalf("found only %d topic configs spelled in the broker's sources (%v): the scan is not reading them", len(read), read)
	}
	for _, s := range settings.All(settings.Topic) {
		where, isRead := read[s.Name]
		switch {
		case isRead && !s.Effect.Acts():
			t.Errorf("%s is read by the broker (%s) and its record says %q (%s): make the record say what the broker does with it",
				s.Name, strings.Join(where, ", "), s.Effect, s.Effect.Phrase())
		case !isRead && s.Effect.Acts():
			t.Errorf("%s is recorded as %q and nothing in internal/broker reads it", s.Name, s.Effect)
		case isRead && s.Enforcer == "":
			t.Errorf("%s acts and its record names nothing that enforces it", s.Name)
		}
	}
}

// compression.type, in particular: what the record says is what the broker does, no
// more (it does not re-compress what a producer sends).
func TestSettingsTable_CompressionTypeSaysWhatTheBrokerDoes(t *testing.T) {
	s, ok := settings.TopicConfig("compression.type")
	if !ok {
		t.Fatal("no record")
	}
	if s.Effect != settings.Applied {
		t.Errorf("effect %q, want applied", s.Effect)
	}
	for _, want := range []string{"log cleaner", "produce handler"} {
		if !strings.Contains(s.Enforcer, want) {
			t.Errorf("the enforcer %q does not name the %s", s.Enforcer, want)
		}
	}
	say := s.Say(config.Config{})
	for _, want := range []string{"cleaner rewrites", "uncompressed", "max.message.bytes", "not re-compressed"} {
		if !strings.Contains(say, want) {
			t.Errorf("the field's line does not say %q: %s", want, say)
		}
	}
	if strings.Contains(say, "changes nothing") || strings.Contains(s.Teach(config.Config{}), "stored only") {
		t.Errorf("the field's line still says the setting does nothing: %s / %s", say, s.Teach(config.Config{}))
	}
	// The levels are still only stored: nothing reads them.
	for _, name := range []string{"compression.gzip.level", "compression.lz4.level", "compression.zstd.level"} {
		if l, _ := settings.TopicConfig(name); l.Effect != settings.Stored {
			t.Errorf("%s is recorded as %q; the cleaner compresses at the library's default level", name, l.Effect)
		}
	}
}
