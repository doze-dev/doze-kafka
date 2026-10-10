package kafka

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/server"
	"github.com/doze-dev/doze-sdk/engine"
	"github.com/twmb/franz-go/pkg/kadm"
)

// broker starts the embedded broker on a unix socket, as the module does.
func broker(t *testing.T) engine.Endpoint {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "kc")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "k.sock")
	if err := os.MkdirAll(filepath.Join(dir, "data"), 0o700); err != nil {
		t.Fatal(err)
	}
	auto := true
	srv, err := server.New(server.Options{Version: 4, DataDir: filepath.Join(dir, "data"), AutoCreateTopics: &auto})
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = srv.ServeListener(ctx, ln); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	return engine.Endpoint{Backend: sock}
}

func TestTheConsoleDescribesTopicsGroupsAndATail(t *testing.T) {
	spec := Driver{}.DescribeConsole()
	if len(spec.Panels) != 3 {
		t.Fatalf("panels = %+v", spec.Panels)
	}
	byID := map[string]engine.Panel{}
	for _, p := range spec.Panels {
		byID[p.ID] = p
	}
	if p := byID["topics"]; p.Kind != "table" || p.Source != "topic" {
		t.Errorf("topics = %+v", p)
	}
	if p := byID["tail"]; p.Kind != "stream" || p.Select != "topics" {
		t.Errorf("tail = %+v", p)
	}
	// Every table column the console draws is something Resources supplies.
	var rate bool
	for _, c := range byID["topics"].Columns {
		rate = rate || c.Role == "rate"
	}
	if !rate {
		t.Error("topics should have a rate column: the host works out the speed")
	}
}

func TestProduceThenSeeItInTheTableAndTheTail(t *testing.T) {
	ep := broker(t)
	d := Driver{}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl, err := unixClient(ep.Backend)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := kadm.NewClient(cl).CreateTopic(ctx, 1, 1, nil, "orders"); err != nil {
		t.Fatal(err)
	}
	cl.Close()

	out, err := d.Run(ctx, engine.Instance{}, ep, "produce", "orders", `{"order":1}`)
	if err != nil || !strings.Contains(out, "produced to orders") {
		t.Fatalf("produce = %q, %v", out, err)
	}

	res, err := d.Resources(ctx, engine.Instance{}, ep)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, r := range res {
		if r.Kind == "topic" && r.Name == "orders" {
			found = true
			if r.Info["high"] != "1" || r.Info["partitions"] == "" {
				t.Errorf("orders info = %+v", r.Info)
			}
		}
	}
	if !found {
		t.Fatalf("the topic is not in the resources: %+v", res)
	}

	// The tail starts with what is there and then follows new messages.
	rows := make(chan engine.Row, 16)
	tctx, tcancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- d.Tail(tctx, engine.Instance{}, ep, "tail", "orders", rows) }()

	want := func(v string) {
		t.Helper()
		select {
		case r := <-rows:
			if r.Cells["value"] != v {
				t.Fatalf("tail row = %+v, want value %q", r.Cells, v)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("no tail row for %q", v)
		}
	}
	want(`{"order":1}`)
	if _, err := d.Run(ctx, engine.Instance{}, ep, "produce", "orders", `{"order":2}`); err != nil {
		t.Fatal(err)
	}
	want(`{"order":2}`)

	tcancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a tail stopped by the host returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the tail did not stop")
	}
}

func TestUnknownActionIsRefused(t *testing.T) {
	if _, err := (Driver{}).Run(context.Background(), engine.Instance{}, engine.Endpoint{}, "purge", "x", ""); err == nil {
		t.Fatal("an action the module does not have should be refused")
	}
}
