// Command larder plays the Kafka side of Larder, the example project in
// doze-examples/larder, against a running broker, for the site's media
// (e2e/site-media.ts). Usage:
//
//	larder topics   host:port   create Larder's four topics, 3 partitions each, as its
//	                            doze.hcl declares them (auto_create_topics = false)
//	larder produce  host:port topic client
//	                            publish each "key<TAB>json" line of stdin to topic as it
//	                            arrives, as the service named client does (platform.Publish:
//	                            acks=all, uncompressed, one record at a time)
//	larder services host:port   run Larder's four consumer groups until killed:
//	                            payments  reads orders.placed and answers each order on
//	                                      payments.settled, or payments.failed for a card
//	                                      ending 0002, as services/payments does
//	                            orders, notifications, fulfilment  read what they read
//	                                      in Larder and commit, so their lag is real
//
// site-media.ts writes the events; the JSON shapes
// are Larder's (platform/events/events.go), and so are the group names and the
// keys: the order ID for orders and payments, the user ID for sign-ups.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var topics = []string{"users.signed-up", "orders.placed", "payments.settled", "payments.failed"}

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintln(os.Stderr, "usage: larder topics|services host:port | larder produce host:port topic client")
		os.Exit(2)
	}
	addr := os.Args[2]
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	switch os.Args[1] {
	case "topics":
		err = createTopics(ctx, addr)
	case "produce":
		if len(os.Args) != 5 {
			err = fmt.Errorf("produce needs a topic and a client id")
			break
		}
		err = produce(ctx, addr, os.Args[3], os.Args[4])
	case "services":
		err = services(ctx, addr)
	default:
		err = fmt.Errorf("unknown command %q", os.Args[1])
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "larder:", err)
		os.Exit(1)
	}
}

func createTopics(ctx context.Context, addr string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("doze"))
	if err != nil {
		return err
	}
	defer cl.Close()
	req := kmsg.NewPtrCreateTopicsRequest()
	req.TimeoutMillis = 10000
	for _, name := range topics {
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic, t.NumPartitions, t.ReplicationFactor = name, 3, 1
		req.Topics = append(req.Topics, t)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		return err
	}
	for _, t := range resp.Topics {
		if t.ErrorCode != 0 && t.ErrorCode != 36 { // 36: TOPIC_ALREADY_EXISTS
			return fmt.Errorf("create %s: error code %d", t.Topic, t.ErrorCode)
		}
	}
	return nil
}

// orderPlaced and paymentResult are Larder's events.OrderPlaced and
// events.PaymentResult.
type orderPlaced struct {
	OrderID    string    `json:"order_id"`
	Email      string    `json:"email"`
	TotalPence int64     `json:"total_pence"`
	CardLast4  string    `json:"card_last4"`
	At         time.Time `json:"at"`
}

type paymentResult struct {
	PaymentID   string    `json:"payment_id"`
	OrderID     string    `json:"order_id"`
	Email       string    `json:"email"`
	AmountPence int64     `json:"amount_pence"`
	Reason      string    `json:"reason,omitempty"`
	At          time.Time `json:"at"`
}

func services(ctx context.Context, addr string) error {
	producer, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("payments"),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return err
	}
	defer producer.Close()

	pay := func(ctx context.Context, rec *kgo.Record) error {
		var o orderPlaced
		if err := json.Unmarshal(rec.Value, &o); err != nil {
			return err
		}
		// A charge takes a moment at a card processor.
		time.Sleep(120 * time.Millisecond)
		topic, r := "payments.settled", paymentResult{PaymentID: uuid(), OrderID: o.OrderID, Email: o.Email, AmountPence: o.TotalPence, At: time.Now().UTC()}
		if o.CardLast4 == "0002" {
			topic, r.Reason = "payments.failed", "card declined by the issuer"
		}
		body, _ := json.Marshal(r)
		return producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(o.OrderID), Value: body}).FirstErr()
	}
	none := func(context.Context, *kgo.Record) error { return nil }

	groups := []struct {
		group  string
		topics []string
		handle func(context.Context, *kgo.Record) error
	}{
		{"payments", []string{"orders.placed"}, pay},
		{"orders", []string{"payments.settled", "payments.failed"}, none},
		{"notifications", []string{"users.signed-up", "orders.placed", "payments.failed"}, none},
		{"fulfilment", []string{"payments.settled"}, none},
	}
	var wg sync.WaitGroup
	for _, g := range groups {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID(g.group), kgo.ConsumerGroup(g.group),
			kgo.ConsumeTopics(g.topics...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DisableAutoCommit())
		if err != nil {
			return err
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer cl.Close()
			for {
				fetches := cl.PollFetches(ctx)
				if ctx.Err() != nil {
					return
				}
				fetches.EachRecord(func(rec *kgo.Record) {
					if err := g.handle(ctx, rec); err != nil && ctx.Err() == nil {
						fmt.Fprintln(os.Stderr, g.group+":", err)
					}
				})
				if err := cl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
					fmt.Fprintln(os.Stderr, g.group+": commit:", err)
				}
			}
		}()
	}
	fmt.Println("larder services running")
	wg.Wait()
	return nil
}

func uuid() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// produce reads lines as they come (a pipe is not buffered here the way kcat's
// stdin is, so each record reaches the broker when it is written).
func produce(ctx context.Context, addr, topic, client string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID(client),
		kgo.RequiredAcks(kgo.AllISRAcks()), kgo.ProducerBatchCompression(kgo.NoCompression()))
	if err != nil {
		return err
	}
	defer cl.Close()
	sc := bufio.NewScanner(os.Stdin)
	sc.Buffer(make([]byte, 64<<10), 1<<20)
	for sc.Scan() {
		key, value, ok := strings.Cut(sc.Text(), "\t")
		if !ok {
			return fmt.Errorf("a line without a tab: %q", sc.Text())
		}
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: []byte(value)}).FirstErr(); err != nil {
			return err
		}
	}
	return sc.Err()
}
