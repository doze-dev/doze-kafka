// Command kafka-plugin runs the kafka engine as an out-of-process doze module.
// Dual-purpose: invoked plainly it speaks the plugin protocol; invoked as
// `kafka-plugin __serve …` (what Driver.Plan spawns) it runs the embedded
// doze-kafka broker on a unix socket. The engine logic lives in the module directory beside the broker
// (this directory) and embeds github.com/doze-dev/doze-kafka.
package main

import (
	"fmt"
	"os"

	kafka "github.com/doze-dev/doze-kafka/module"
	dozeplugin "github.com/doze-dev/doze-sdk/plugin"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "__serve" {
		if err := kafka.ServeFromArgs(os.Args[1:]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	dozeplugin.Main(kafka.New(), &kafka.Config{})
}
