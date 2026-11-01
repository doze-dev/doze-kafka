// Command errnames prints every error name a Result column can show, one per line:
// the name of every code Kafka defines (what ErrorName answers for the code a broker
// returns), which the browser suite measures against the Result column.
package main

import (
	"fmt"
	"sort"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/protocol"
)

func main() {
	seen := map[string]bool{"CONNECTION_CLOSED": true}
	for code := int16(1); code < 200; code++ {
		if n := protocol.ErrorName(code); !strings.HasPrefix(n, "ERROR_") {
			seen[n] = true
		}
	}
	for _, c := range protocol.ErrorCodes() {
		seen[protocol.ErrorName(c)] = true
	}
	var names []string
	for n := range seen {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		fmt.Println(n)
	}
}
