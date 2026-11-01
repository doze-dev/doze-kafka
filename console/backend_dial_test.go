package console

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync/atomic"
	"testing"

	"github.com/doze-dev/doze-kafka/server"
)

// bootTLSBroker is a broker whose listener terminates TLS with a certificate for
// 127.0.0.1, and the pool that trusts it.
func bootTLSBroker(t *testing.T) (addr string, cfg *tls.Config) {
	t.Helper()
	cert, pool := selfSigned(t)
	srv, err := server.New(server.Options{
		DataDir: t.TempDir(), ListenAddr: "127.0.0.1:0", AdvertisedHost: "127.0.0.1",
		TLSConfig: &tls.Config{Certificates: []tls.Certificate{cert}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	bound, err := srv.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return bound.String(), &tls.Config{RootCAs: pool}
}

var tlsTopics atomic.Int32

func listsTopics(t *testing.T, o Options) {
	t.Helper()
	o.Facts = BrokerFacts{DataDir: t.TempDir()}
	c, err := New(o)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10e9)
	defer cancel()
	if err := c.be.CreateTopic(ctx, fmt.Sprintf("tls%d", tlsTopics.Add(1)), 1, nil); err != nil {
		t.Fatalf("a request over %+v failed: %v", o.Addr, err)
	}
	if ts, err := c.be.ListTopics(ctx); err != nil || len(ts) == 0 {
		t.Fatalf("topics: %v %v", ts, err)
	}
}

// The console reaches a TLS broker however it dials: through the process's own dialer
// (OnDial) and through franz-go's.
func TestConsoleDialsATLSBroker(t *testing.T) {
	addr, cfg := bootTLSBroker(t)

	var dialed atomic.Int32
	listsTopics(t, Options{Addr: addr, TLSConfig: cfg, OnDial: func(_ context.Context, c net.Conn) error {
		dialed.Add(1)
		return nil
	}})
	if dialed.Load() == 0 {
		t.Error("OnDial was not told of the connection")
	}
	// A name set on the config is the one the certificate is checked against.
	named := cfg.Clone()
	named.ServerName = "127.0.0.1"
	listsTopics(t, Options{Addr: addr, TLSConfig: named, OnDial: func(context.Context, net.Conn) error { return nil }})

	listsTopics(t, Options{Addr: addr, TLSConfig: cfg})

}

// And a plain broker through OnDial stays plain.
func TestOnDialWithoutTLSStaysPlain(t *testing.T) {
	_, addr := newTestConsoleAddr(t)
	var dialed atomic.Int32
	listsTopics(t, Options{Addr: addr, OnDial: func(context.Context, net.Conn) error { dialed.Add(1); return nil }})
	if dialed.Load() == 0 {
		t.Error("OnDial was not told of the connection")
	}
}
