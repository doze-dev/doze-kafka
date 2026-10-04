package broker

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/protocol"
)

// The API table says what is offered; handlers() says what serves it. They must
// name exactly the same APIs: a row with no handler would be advertised and never
// answered (Register panics on that at start-up), and a handler with no row is
// code no client can reach.
func TestEveryAPIInTheTableHasAHandlerAndTheReverse(t *testing.T) {
	handlers := (&Broker{}).handlers()
	inTable := map[protocol.APIKey]bool{}
	for _, a := range protocol.APIs {
		inTable[a.Key] = true
		if handlers[a.Key] == nil {
			t.Errorf("%s is in protocol.APIs and has no handler", protocol.APIName(a.Key))
		}
	}
	for key := range handlers {
		if !inTable[key] {
			t.Errorf("%s has a handler and no row in protocol.APIs: nothing can call it", protocol.APIName(key))
		}
	}
}

// config states which Kafka majors exist without importing the protocol package
// (so that it depends on nothing). This is what keeps the two statements equal.
func TestConfigAndTheAPITableAgreeOnTheKafkaMajors(t *testing.T) {
	majors := protocol.SupportedKafkaMajors()
	if majors[0] != config.MinKafkaMajor || majors[len(majors)-1] != config.MaxKafkaMajor {
		t.Errorf("the API table has columns for Kafka %v; config accepts %d to %d", majors, config.MinKafkaMajor, config.MaxKafkaMajor)
	}
	if config.Default().Version != protocol.DefaultKafkaMajor {
		t.Errorf("the default Kafka major is %d in config and %d in the API table", config.Default().Version, protocol.DefaultKafkaMajor)
	}
	for v := config.MinKafkaMajor - 1; v <= config.MaxKafkaMajor+1; v++ {
		c := config.Default()
		c.Version = v
		_, hasProfile := protocol.ProfileFor(v)
		if valid := c.Validate() == nil; valid != hasProfile {
			t.Errorf("Kafka %d: config says valid=%v, the API table has a profile=%v", v, valid, hasProfile)
		}
	}
}

// config is a leaf: it imports nothing else from this module, so any package may
// import it without a cycle.
func TestConfigImportsNothingFromTheBroker(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("..", "config", "*.go"))
	if len(files) == 0 {
		t.Fatal("found no files in internal/config")
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		src, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(src), `"github.com/doze-dev/doze-kafka/`) {
			t.Errorf("%s imports another package of this module", f)
		}
	}
}
