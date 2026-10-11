// Command hold keeps one consumer alive in a group, so a browser test has a live
// connection to look at: a client in Traffic, a member in a group page.
// Usage: hold host:port client-id group topic. It prints "ready" once the group
// has assigned it a partition and runs until it is killed.
package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/e2e/connopt"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// report plays what a KIP-714 client does over its own connection: subscribe, then push
// reports of its request latency, its error rate and its poll idle ratio.
func report(ctx context.Context, cl *kgo.Client) error {
	sub := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	subResp, err := sub.RequestWith(ctx, cl)
	if err != nil || subResp.ErrorCode != 0 {
		return fmt.Errorf("subscription: %v", err)
	}
	for i, latency := range []float64{1.2, 1.4, 1.6, 1.4} {
		payload := telemetry.Encode(telemetry.Payload{
			Resource: map[string]string{"client_rack": "hold-rack"},
			Metrics: []telemetry.Metric{
				{Name: "org.apache.kafka.consumer.request.latency.avg", Unit: "ms", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: time.Now(), Value: latency}}},
				{Name: "org.apache.kafka.consumer.request.error.rate", Unit: "1/s", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: time.Now(), Value: 0}}},
				{Name: "org.apache.kafka.consumer.poll.idle.ratio.avg", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: time.Now(), Value: 0.9 + float64(i)/100}}},
			},
		})
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		zw.Write(payload)
		zw.Close()
		push := kmsg.NewPtrPushTelemetryRequest()
		push.ClientInstanceID, push.SubscriptionID, push.CompressionType, push.Metrics = subResp.ClientInstanceID, subResp.SubscriptionID, 1, zipped.Bytes()
		if resp, err := push.RequestWith(ctx, cl); err != nil || resp.ErrorCode != 0 {
			return fmt.Errorf("push %d: %v (error code %d)", i, err, resp.ErrorCode)
		}
	}
	return nil
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: hold host:port client-id group topic")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	opts := connopt.Opts()
	// HOLD_NO_METRICS=1 is a client that does not report (Traf-11's board).
	if os.Getenv("HOLD_NO_METRICS") != "" {
		opts = append(opts, kgo.DisableClientMetrics())
	}
	cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers(os.Args[1]), kgo.ClientID(os.Args[2]), kgo.ConsumerGroup(os.Args[3]), kgo.ConsumeTopics(os.Args[4]),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { fmt.Println("ready") }))...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hold:", err)
		os.Exit(1)
	}
	defer cl.Close()
	// HOLD_TELEMETRY=1 makes the client also report its metrics, once, over a connection
	// of its own.
	if os.Getenv("HOLD_TELEMETRY") != "" {
		if err := report(ctx, cl); err != nil {
			fmt.Fprintln(os.Stderr, "hold:", err)
			os.Exit(1)
		}
	}
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		cl.PollFetches(pctx)
		cancel()
	}
}
