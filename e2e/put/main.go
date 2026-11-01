// Command put writes one record with a timestamp the caller chooses, so a browser
// test can show a moment that is not "now" (the displayed date, in a zone far from
// UTC). Usage: put host:port topic RFC3339-time value.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/doze-dev/doze-kafka/e2e/connopt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: put host:port topic RFC3339-time value")
		os.Exit(2)
	}
	at, err := time.Parse(time.RFC3339, os.Args[3])
	if err != nil {
		fmt.Fprintln(os.Stderr, "put:", err)
		os.Exit(2)
	}
	cl, err := kgo.NewClient(append(connopt.Opts(), kgo.SeedBrokers(os.Args[1]), kgo.ClientID("doze-put"), kgo.RecordPartitioner(kgo.ManualPartitioner()))...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "put:", err)
		os.Exit(1)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: os.Args[2], Partition: 0, Timestamp: at, Value: []byte(os.Args[4])}).FirstErr(); err != nil {
		fmt.Fprintln(os.Stderr, "put:", err)
		os.Exit(1)
	}
}
