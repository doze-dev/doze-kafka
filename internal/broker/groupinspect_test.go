package broker_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/internal/broker"
	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
)

// bootInspected is boot that also hands back the group inspector of the server.
func bootInspected(t *testing.T, mod func(*config.Config)) (string, broker.GroupInspector) {
	t.Helper()
	skipShort(t)
	dir := t.TempDir()
	cfg := config.Default()
	cfg.Version = 4
	cfg.ListenAddr = "127.0.0.1:0"
	cfg.DataDir, cfg.MetaDB = filepath.Join(dir, "data"), filepath.Join(dir, "meta.db")
	mod(&cfg)
	srv, err := embed.NewFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { srv.Serve(ctx); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return bound.String(), serverhook.Groups(srv)
}

// dialLog is a kgo dialer that remembers the local address of every connection it made:
// the broker sees these as the remote address of the client.
type dialLog struct {
	mu    sync.Mutex
	addrs []string
}

func (d *dialLog) opt() kgo.Opt {
	return kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
		c, err := (&net.Dialer{}).DialContext(ctx, network, host)
		if err == nil {
			d.mu.Lock()
			d.addrs = append(d.addrs, c.LocalAddr().String())
			d.mu.Unlock()
		}
		return c, err
	})
}

func (d *dialLog) has(a string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Contains(d.addrs, a)
}

func TestGroupSnapshot_ClassicGroupWithTwoMembers(t *testing.T) {
	addr, ins := bootInspected(t, func(c *config.Config) {})
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, newClient(t, addr), "snap", 2)

	type app struct {
		id   string
		dial *dialLog
	}
	apps := []app{{"app-a", &dialLog{}}, {"app-b", &dialLog{}}}
	start := time.Now()
	for _, a := range apps {
		cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID(a.id), a.dial.opt(),
			kgo.ConsumerGroup("snapg"), kgo.ConsumeTopics("snap"),
			kgo.SessionTimeout(21*time.Second), kgo.RebalanceTimeout(43*time.Second),
			kgo.Balancers(kgo.RangeBalancer(), kgo.CooperativeStickyBalancer()),
			kgo.HeartbeatInterval(500*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		go cl.PollFetches(ctx)
	}

	var g broker.GroupSnapshot
	deadline := time.Now().Add(30 * time.Second)
	for {
		var ok bool
		g, ok = ins.InspectGroup("snapg")
		if ok && g.State == "Stable" && len(g.Members) == 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no stable group of two: %+v", g)
		}
		time.Sleep(100 * time.Millisecond)
	}

	if g.Kind != "classic" || g.ProtocolType != "consumer" || g.Generation < 1 || g.Epoch != 0 {
		t.Errorf("group: kind %q type %q generation %d epoch %d", g.Kind, g.ProtocolType, g.Generation, g.Epoch)
	}
	if g.ProtocolName != "range" && g.ProtocolName != "cooperative-sticky" {
		t.Errorf("selected protocol %q is none of those offered", g.ProtocolName)
	}
	leaders := 0
	for _, m := range g.Members {
		byID := map[string]app{"app-a": apps[0], "app-b": apps[1]}
		a, ok := byID[m.ClientID]
		if !ok {
			t.Fatalf("member of client %q", m.ClientID)
		}
		if m.SessionTimeout != 21*time.Second || m.RebalanceTimeout != 43*time.Second {
			t.Errorf("%s: timeouts %v / %v, want 21s / 43s", m.ClientID, m.SessionTimeout, m.RebalanceTimeout)
		}
		if !slices.Equal(m.Protocols, []string{"range", "cooperative-sticky"}) {
			t.Errorf("%s: protocols %v", m.ClientID, m.Protocols)
		}
		if !a.dial.has(m.Addr) {
			t.Errorf("%s: addr %q is none of the connections the client made", m.ClientID, m.Addr)
		}
		if h, _, _ := net.SplitHostPort(m.Addr); m.Host != "/"+h {
			t.Errorf("%s: host %q for addr %q", m.ClientID, m.Host, m.Addr)
		}
		if m.Software == "" || m.Principal == "" {
			t.Errorf("%s: software %q principal %q", m.ClientID, m.Software, m.Principal)
		}
		if m.Joined.Before(start) || m.LastHeartbeat.Before(m.Joined) || m.LastHeartbeat.After(g.Taken) {
			t.Errorf("%s: joined %v heartbeat %v, started %v taken %v", m.ClientID, m.Joined, m.LastHeartbeat, start, g.Taken)
		}
		if m.Leader {
			leaders++
			if m.ID != g.Leader {
				t.Errorf("leader flag on %s, group leader %s", m.ID, g.Leader)
			}
		}
	}
	if leaders != 1 {
		t.Errorf("%d members marked leader, want 1", leaders)
	}

	// Heartbeats move the heartbeat time, not the joined time.
	before := g.Members[0]
	time.Sleep(1500 * time.Millisecond)
	g2, _ := ins.InspectGroup("snapg")
	var after broker.MemberSnapshot
	for _, m := range g2.Members {
		if m.ID == before.ID {
			after = m
		}
	}
	if !after.Joined.Equal(before.Joined) || !after.LastHeartbeat.After(before.LastHeartbeat) {
		t.Errorf("after 1.5 s: joined %v -> %v, heartbeat %v -> %v", before.Joined, after.Joined, before.LastHeartbeat, after.LastHeartbeat)
	}
	if _, ok := ins.InspectGroup("nope"); ok {
		t.Error("a group nobody joined has a snapshot")
	}
}

func TestGroupSnapshot_KIP848GroupLeavesWhatItLacksEmpty(t *testing.T) {
	addr, ins := bootInspected(t, func(c *config.Config) { c.ConsumerAssignmentInterval = 0 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mustCreateTopic(t, ctx, newClient(t, addr), "snap8", 2)

	dial := &dialLog{}
	cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("c848"), dial.opt())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	m := &member848{cl: cl, group: "snap8g", id: "m848", owned: map[ownedPart]bool{}, assign: "uniform", ver: -1}
	start := time.Now()
	m.join(t, ctx, []string{"snap8"}) // the helper states a rebalance timeout of 60 s.

	g, ok := ins.InspectGroup("snap8g")
	if !ok || len(g.Members) != 1 {
		t.Fatalf("snapshot: %v %+v", ok, g)
	}
	mem := g.Members[0]
	if g.Kind != "consumer" || g.ProtocolType != "consumer" || g.Epoch < 1 {
		t.Errorf("group: kind %q type %q epoch %d", g.Kind, g.ProtocolType, g.Epoch)
	}
	if g.Generation != 0 || g.Leader != "" || g.ProtocolName != "" {
		t.Errorf("a consumer group has no generation, leader or protocol: %d %q %q", g.Generation, g.Leader, g.ProtocolName)
	}
	if mem.ID != "m848" || mem.ClientID != "c848" || mem.RebalanceTimeout != 60*time.Second {
		t.Errorf("member: id %q client %q rebalance %v", mem.ID, mem.ClientID, mem.RebalanceTimeout)
	}
	if mem.SessionTimeout != 0 || len(mem.Protocols) != 0 || mem.Leader {
		t.Errorf("member has no session timeout, protocols or leadership of its own: %v %v %v", mem.SessionTimeout, mem.Protocols, mem.Leader)
	}
	if !dial.has(mem.Addr) {
		t.Errorf("addr %q is none of the client's connections %v", mem.Addr, dial.addrs)
	}
	if mem.Software == "" || mem.Principal == "" || mem.Joined.Before(start) || mem.LastHeartbeat.Before(mem.Joined) {
		t.Errorf("member: software %q principal %q joined %v heartbeat %v", mem.Software, mem.Principal, mem.Joined, mem.LastHeartbeat)
	}
}
