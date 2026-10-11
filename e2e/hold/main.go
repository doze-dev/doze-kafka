// Command hold keeps one consumer alive in a group, so a browser test has a live
// connection to look at: a client in Traffic, a member in a group page.
// Usage: hold host:port client-id group topic. It prints "ready" once the group
// has assigned it a partition and runs until it is killed.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/e2e/connopt"
	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: hold host:port client-id group topic")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cl, err := kgo.NewClient(append(connopt.Opts(), kgo.SeedBrokers(os.Args[1]), kgo.ClientID(os.Args[2]), kgo.ConsumerGroup(os.Args[3]), kgo.ConsumeTopics(os.Args[4]),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { fmt.Println("ready") }))...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hold:", err)
		os.Exit(1)
	}
	defer cl.Close()
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		cl.PollFetches(pctx)
		cancel()
	}
}
