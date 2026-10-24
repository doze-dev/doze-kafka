package console

import (
	"context"
	"net"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestClusterInfoIdentityAndSecurityFacts(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	info, err := c.be.ClusterInfo(ctx)
	if err != nil {
		t.Fatal(err)
	}
	host, port, _ := net.SplitHostPort(addr)
	if p, _ := strconv.Atoi(port); info.Host != host || info.Port != int32(p) {
		t.Errorf("the broker is at %s:%d, want %s", info.Host, info.Port, addr)
	}
	if info.Brokers != 1 || info.ClusterID == "" || info.TLS || info.AuthMech != "" {
		t.Errorf("%+v", info)
	}
	names := make([]string, len(info.SupportedAPIs))
	for i, a := range info.SupportedAPIs {
		names[i] = a.Name
		if a.Max < a.Min || a.Name == "" {
			t.Errorf("api %+v", a)
		}
	}
	if len(names) < 10 || !sort.StringsAreSorted(names) {
		t.Errorf("%d APIs, sorted=%v", len(names), sort.StringsAreSorted(names))
	}
	if len(info.BrokerConfig) == 0 {
		t.Error("no broker configuration")
	}

	// How the console itself signs in, from what it was given.
	c.be.opts.SASLUser = "u"
	if info, err = c.be.ClusterInfo(ctx); err != nil || info.AuthMech != "PLAIN" {
		t.Errorf("a user with no mechanism named: %q %v", info.AuthMech, err)
	}
	c.be.opts.SASLMechanism = "SCRAM-SHA-256"
	if info, err = c.be.ClusterInfo(ctx); err != nil || info.AuthMech != "SCRAM-SHA-256" {
		t.Errorf("SCRAM: %q %v", info.AuthMech, err)
	}
	c.be.opts.SASLUser, c.be.opts.SASLMechanism = "", ""
}

func TestClusterInfoOfATLSConsoleSaysTLS(t *testing.T) {
	addr, cfg := bootTLSBroker(t)
	c, err := New(Options{Addr: addr, TLSConfig: cfg, Facts: BrokerFacts{DataDir: t.TempDir()}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	info, err := c.be.ClusterInfo(context.Background())
	if err != nil || !info.TLS {
		t.Errorf("%+v %v", info, err)
	}
}

// A transaction in progress is listed with its timeout, start and partitions.
func TestClusterInfoListsAnOpenTransaction(t *testing.T) {
	c, addr := newTestConsoleAddr(t)
	ctx := context.Background()
	if err := c.be.CreateTopic(ctx, "tx", 2, nil); err != nil {
		t.Fatal(err)
	}
	p, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.TransactionalID("open-one"), kgo.TransactionTimeout(45*time.Second),
		kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if err := p.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for _, part := range []int32{1, 0} {
		if err := p.ProduceSync(ctx, &kgo.Record{Topic: "tx", Partition: part, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	info, err := c.be.ClusterInfo(ctx)
	if err != nil || len(info.Transactions) != 1 {
		t.Fatalf("%+v %v", info.Transactions, err)
	}
	tx := info.Transactions[0]
	if tx.ID != "open-one" || tx.State != "Ongoing" || tx.Timeout != "45s" || tx.ProducerID < 0 {
		t.Errorf("%+v", tx)
	}
	if d := time.Since(tx.Began); tx.Began.IsZero() || d < 0 || d > time.Minute {
		t.Errorf("began %v (%v ago)", tx.Began, d)
	}
	got := append([]string(nil), tx.Partitions...)
	sort.Strings(got)
	if len(got) != 2 || got[0] != "tx-0" || got[1] != "tx-1" {
		t.Errorf("partitions %v", tx.Partitions)
	}
}
