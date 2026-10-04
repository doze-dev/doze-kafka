package server_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-kafka/server"
)

// Tests added by the October audit for the embed API.

func mustNew(t *testing.T, opts server.Options) *server.Server {
	t.Helper()
	if opts.DataDir == "" {
		opts.DataDir = t.TempDir()
	}
	srv, err := server.New(opts)
	if err != nil {
		t.Fatal(err)
	}
	return srv
}

// A TCP listener handed to ServeListener was advertised as port 9092: a client
// bootstrapped on the real port and was then sent to one nothing listened on.
func TestServeListenerAdvertisesATCPListenersPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := mustNew(t, server.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.ServeListener(ctx, ln) }()
	t.Cleanup(func() { cancel(); <-done })
	<-srv.Ready()

	cl, err := kgo.NewClient(kgo.SeedBrokers(ln.Addr().String()), kgo.AllowAutoTopicCreation())
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pcancel()
	// Metadata tells the client where to produce; with the wrong port this times out.
	if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "t", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce through a caller's TCP listener: %v", err)
	}
}

// A Server is served once. A second Serve used to bind and serve again over the
// same storage, and the two raced on every field.
func TestASecondServeIsRefusedAndTheFirstCarriesOn(t *testing.T) {
	srv := mustNew(t, server.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	first := make(chan error, 1)
	go func() { first <- srv.Serve(ctx) }()
	<-srv.Ready()

	second := make(chan error, 1)
	go func() { second <- srv.Serve(ctx) }()
	select {
	case err := <-second:
		if err == nil || !strings.Contains(err.Error(), "already serving") {
			t.Errorf("a second Serve returned %v, want an error saying it is already serving", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a second Serve did not return")
	}
	ln2, _ := net.Listen("tcp", "127.0.0.1:0")
	if err := srv.ServeListener(ctx, ln2); err == nil {
		t.Error("ServeListener on a server that is serving returned no error")
	}
	ln2.Close()

	// The first is undisturbed: storage is open and it still answers.
	c, err := net.DialTimeout("tcp", srv.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("the first Serve stopped answering: %v", err)
	}
	c.Close()
	cancel()
	if err := <-first; err != nil {
		t.Errorf("the first Serve returned %v", err)
	}
}

// AlsoListen after Serve bound a port that nothing ever accepted on (and raced
// with Serve reading the listener list). It is refused, and leaves nothing bound.
func TestAlsoListenAfterServeIsRefused(t *testing.T) {
	srv := mustNew(t, server.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	<-srv.Ready()
	// Serve marks itself as serving just after Ready; give it a moment.
	var err error
	var addr net.Addr
	for i := 0; i < 100; i++ {
		if addr, err = srv.AlsoListen(ctx, "127.0.0.1:0"); err != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err == nil {
		t.Fatalf("AlsoListen after Serve kept succeeding (last bound %v)", addr)
	}
}

// Called from several goroutines at once, the three are safe (run with -race).
func TestBindAlsoListenAndServeDoNotRace(t *testing.T) {
	srv := mustNew(t, server.Options{})
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); srv.AlsoListen(ctx, "127.0.0.1:0") }()
		go func() { defer wg.Done(); srv.Serve(ctx) }()
	}
	<-srv.Ready()
	time.Sleep(50 * time.Millisecond)
	cancel()
	wg.Wait()
}

// Close after Bind used to leave the listener bound for the life of the process:
// the port stayed taken, and connections queued in a backlog nobody served.
func TestCloseReleasesTheListeners(t *testing.T) {
	srv := mustNew(t, server.Options{})
	addr, err := srv.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for _, a := range []net.Addr{addr, extra} {
		if c, err := net.DialTimeout("tcp", a.String(), 500*time.Millisecond); err == nil {
			c.Close()
			t.Errorf("%s still accepts connections after Close", a)
		}
	}
	if err := srv.Serve(context.Background()); err == nil {
		t.Error("Serve after Close returned no error")
	}
	if err := srv.Close(); err != nil {
		t.Errorf("a second Close: %v", err)
	}
}

func TestConfigReportsTLSFromOptions(t *testing.T) {
	plain := mustNew(t, server.Options{})
	defer plain.Close()
	if plain.Config().TLS {
		t.Error("a plaintext server reports TLS")
	}
	withTLS := mustNew(t, server.Options{TLSConfig: &tls.Config{MinVersion: tls.VersionTLS12}})
	defer withTLS.Close()
	if !withTLS.Config().TLS {
		t.Error("a server given Options.TLSConfig reports TLS false")
	}
}

// DataDir is documented as required. Left empty it used to mean ./data in
// whatever directory the embedding program was started from.
func TestDataDirIsRequired(t *testing.T) {
	t.Chdir(t.TempDir())
	srv, err := server.New(server.Options{})
	if err == nil {
		srv.Close()
		t.Fatal("New with no DataDir succeeded")
	}
	if !strings.Contains(err.Error(), "DataDir") {
		t.Errorf("the error should name the option: %v", err)
	}
	if entries, _ := os.ReadDir("."); len(entries) != 0 {
		t.Errorf("New with no DataDir created %d entries in the working directory", len(entries))
	}
}

func TestInvalidOptionsAreRefused(t *testing.T) {
	for name, opts := range map[string]server.Options{
		"advertised port":      {AdvertisedPort: 70000},
		"advertised host":      {AdvertisedHost: "localhost:9092"},
		"session timeout":      {ConsumerSessionTimeout: -time.Second},
		"retention overflow":   {RetentionMs: 1 << 62},
		"sub-millisecond sync": {FsyncInterval: 5},
	} {
		opts.DataDir = t.TempDir()
		if srv, err := server.New(opts); err == nil {
			srv.Close()
			t.Errorf("%s: New accepted %+v", name, opts)
		}
	}
}

// Building, serving, using and stopping a broker over and over, on one data
// directory and on many at once: nothing leaks, nothing deadlocks, the directory's
// lock is released each time, and what was written in one round is there in the
// next.
func TestChurn_ServersComeAndGoOverOneDirectoryAndMany(t *testing.T) {
	dir := t.TempDir()
	for round := 0; round < 12; round++ {
		srv := mustNew(t, server.Options{DataDir: dir})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ctx) }()
		<-srv.Ready()
		cl, err := kgo.NewClient(kgo.SeedBrokers(srv.Addr().String()), kgo.AllowAutoTopicCreation())
		if err != nil {
			t.Fatal(err)
		}
		pctx, pcancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := cl.ProduceSync(pctx, &kgo.Record{Topic: "t", Value: []byte(fmt.Sprint(round))}).FirstErr(); err != nil {
			t.Fatalf("round %d: produce: %v", round, err)
		}
		pcancel()
		cl.Close()
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("round %d: Serve returned %v", round, err)
			}
		case <-time.After(10 * time.Second):
			t.Fatalf("round %d: Serve did not return after its context was cancelled", round)
		}
	}
	// Everything written across the rounds is still there, in order.
	srv := mustNew(t, server.Options{DataDir: dir})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	<-srv.Ready()
	cl, err := kgo.NewClient(kgo.SeedBrokers(srv.Addr().String()), kgo.ConsumeTopics("t"), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	if err != nil {
		t.Fatal(err)
	}
	rctx, rcancel := context.WithTimeout(context.Background(), 10*time.Second)
	var got []string
	for len(got) < 12 && rctx.Err() == nil {
		cl.PollFetches(rctx).EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	rcancel()
	cl.Close()
	cancel()
	<-done
	for i, v := range got {
		if v != fmt.Sprint(i) {
			t.Errorf("record %d is %q, want %q", i, v, fmt.Sprint(i))
		}
	}
	if len(got) != 12 {
		t.Errorf("read %d records back after 12 rounds, want 12", len(got))
	}

	// Many at once, each on its own directory.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 3; j++ {
				s, err := server.New(server.Options{DataDir: t.TempDir()})
				if err != nil {
					t.Error(err)
					return
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- s.Serve(ctx) }()
				<-s.Ready()
				cancel()
				if err := <-done; err != nil {
					t.Error(err)
				}
			}
		}()
	}
	wg.Wait()
}
