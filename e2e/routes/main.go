// Command routes prints the console's route table as JSON, for the browser suite's
// route gate (e2e/route-gate.ts). The table is console/routes.go: the same rows the
// console registers its handlers from, so the gate cannot be held to a stale copy.
package main

import (
	"encoding/json"
	"os"

	"github.com/doze-dev/doze-kafka/console"
)

func main() {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(console.Routes()); err != nil {
		os.Exit(1)
	}
}
