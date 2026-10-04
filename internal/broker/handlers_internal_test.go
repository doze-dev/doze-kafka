package broker

import (
	"testing"

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
