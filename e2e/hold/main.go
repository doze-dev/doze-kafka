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
	"strings"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/e2e/connopt"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// report plays what a KIP-714 client does over its own connection: subscribe, push a
// report of its request latency, its error rate and its poll idle ratio, and push again
// on the interval it was given. Told UNKNOWN_SUBSCRIPTION_ID (a broker that restarted has
// forgotten every subscription), or failing to reach the broker, it subscribes again, as
// the Java client does; so the client keeps reporting across a broker restart.
//
// many adds the metrics of moreMetrics to each report: a real client reports well over
// a hundred, and the console lists a few and offers the rest ("All N metrics", the m
// key), which three metrics never show.
func report(ctx context.Context, cl *kgo.Client, many bool) {
	var sub *kmsg.GetTelemetrySubscriptionsResponse
	var id [16]byte
	var burst int
	wait := func(d time.Duration) bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}
	for i := 0; ; i++ {
		if sub == nil {
			req := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
			req.ClientInstanceID = id
			resp, err := req.RequestWith(ctx, cl)
			if err != nil || resp.ErrorCode != 0 {
				if !wait(time.Second) {
					return
				}
				continue
			}
			sub, id, burst = resp, resp.ClientInstanceID, 0
		}
		now := time.Now()
		metrics := []telemetry.Metric{
			{Name: "org.apache.kafka.consumer.request.latency.avg", Unit: "ms", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: now, Value: 1.2 + float64(i%4)/5}}},
			{Name: "org.apache.kafka.consumer.request.error.rate", Unit: "1/s", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: now, Value: 0}}},
			{Name: "org.apache.kafka.consumer.poll.idle.ratio.avg", Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: now, Value: 0.9 + float64(i%4)/100}}},
		}
		if many {
			for n, name := range moreMetrics {
				metrics = append(metrics, telemetry.Metric{Name: "org.apache.kafka.consumer." + name, Kind: telemetry.Gauge, Points: []telemetry.Point{{Time: now, Value: float64(n + 1)}}})
			}
		}
		payload := telemetry.Encode(telemetry.Payload{Resource: map[string]string{"client_rack": "hold-rack"}, Metrics: metrics})
		var zipped bytes.Buffer
		zw := gzip.NewWriter(&zipped)
		zw.Write(payload)
		zw.Close()
		push := kmsg.NewPtrPushTelemetryRequest()
		push.ClientInstanceID, push.SubscriptionID, push.CompressionType, push.Metrics = id, sub.SubscriptionID, 1, zipped.Bytes()
		resp, err := push.RequestWith(ctx, cl)
		if err != nil || resp.ErrorCode != 0 {
			sub = nil // subscribe again on the next turn, with the id it has.
			if !wait(time.Second) {
				return
			}
			continue
		}
		// A few reports at once after each subscription, so a page that opens just after
		// has a series to draw (a sparkline needs two points); then the interval's pace.
		pause := time.Duration(sub.PushIntervalMillis) * time.Millisecond
		if burst++; burst < 4 {
			pause = 50 * time.Millisecond
		}
		if !wait(pause) {
			return
		}
	}
}

// moreMetrics are names a Java consumer reports, beyond the three every report has.
var moreMetrics = []string{
	"fetch.latency.avg", "request.rate", "incoming.byte.rate", "outgoing.byte.rate",
	"connection.count", "io.wait.ratio", "commit.latency.avg", "records.lag.max",
}

func main() {
	if len(os.Args) != 5 {
		fmt.Fprintln(os.Stderr, "usage: hold host:port client-id group topic")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	opts := connopt.Opts()
	// HOLD_NO_METRICS=1 is a client that does not report (Traf-11's board). HOLD_TELEMETRY=1
	// reports through report() above, as the one instance this client is: the library's own
	// reporting is off then, or the client would be two instances under one client id and
	// the console would show whichever pushed last.
	if os.Getenv("HOLD_NO_METRICS") != "" || os.Getenv("HOLD_TELEMETRY") != "" {
		opts = append(opts, kgo.DisableClientMetrics())
	}
	// HOLD_SOFTWARE="name version" is what the client says it is in ApiVersions, where the
	// library would say "kgo": a test of long names needs a long one.
	if name, version, ok := strings.Cut(os.Getenv("HOLD_SOFTWARE"), " "); ok {
		opts = append(opts, kgo.SoftwareNameAndVersion(name, version))
	}
	// HOLD_TXN=<transactional id> writes one record to the topic in a transaction first,
	// over a connection of its own under the same client id (the topic is made if it is
	// not there): the client is then a producer with a transactional id as well.
	if id := os.Getenv("HOLD_TXN"); id != "" {
		tx, err := kgo.NewClient(append(append([]kgo.Opt{}, opts...), kgo.SeedBrokers(os.Args[1]), kgo.ClientID(os.Args[2]),
			kgo.TransactionalID(id), kgo.AllowAutoTopicCreation())...)
		if err == nil {
			defer tx.Close()
			if err = tx.BeginTransaction(); err == nil {
				if err = tx.ProduceSync(ctx, &kgo.Record{Topic: os.Args[4], Value: []byte("held")}).FirstErr(); err == nil {
					err = tx.EndTransaction(ctx, kgo.TryCommit)
				}
			}
		}
		if err != nil {
			fmt.Fprintln(os.Stderr, "hold: the transaction:", err)
			os.Exit(1)
		}
	}
	// HOLD_KIP848=1 joins with the consumer protocol of KIP-848 (ConsumerGroupHeartbeat,
	// the broker assigns), where the default is the classic one (JoinGroup, SyncGroup):
	// a group's story then counts epochs, not generations.
	if os.Getenv("HOLD_KIP848") != "" {
		opts = append(opts, kgo.ServerSideBalancer())
	}
	cl, err := kgo.NewClient(append(opts, kgo.SeedBrokers(os.Args[1]), kgo.ClientID(os.Args[2]), kgo.ConsumerGroup(os.Args[3]), kgo.ConsumeTopics(os.Args[4]),
		kgo.OnPartitionsAssigned(func(context.Context, *kgo.Client, map[string][]int32) { fmt.Println("ready") }))...)
	if err != nil {
		fmt.Fprintln(os.Stderr, "hold:", err)
		os.Exit(1)
	}
	defer cl.Close()
	// HOLD_TELEMETRY=1 makes the client report its metrics for as long as it runs;
	// HOLD_TELEMETRY=many makes it report more of them than the console lists at once.
	if os.Getenv("HOLD_TELEMETRY") != "" {
		go report(ctx, cl, os.Getenv("HOLD_TELEMETRY") == "many")
	}
	for ctx.Err() == nil {
		pctx, cancel := context.WithTimeout(ctx, time.Second)
		cl.PollFetches(pctx)
		cancel()
	}
}
