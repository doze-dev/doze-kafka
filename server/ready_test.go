package server_test

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/server"
)

// released reports whether Ready has been closed.
func released(srv *server.Server) bool {
	select {
	case <-srv.Ready():
		return true
	case <-time.After(2 * time.Second):
		return false
	}
}

// Waiting on Ready never hangs, and what it means is read from Addr: not nil, the
// listener is bound; nil, the server ended without ever binding. Close without
// Serve used to leave a waiter blocked for ever.
func TestReadyIsClosedHoweverTheServerEndsAndAddrSaysWhich(t *testing.T) {
	t.Run("Close without Serve", func(t *testing.T) {
		srv := mustNew(t, server.Options{})
		waiter := make(chan struct{})
		go func() { <-srv.Ready(); close(waiter) }()
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
		select {
		case <-waiter:
		case <-time.After(2 * time.Second):
			t.Fatal("a waiter on Ready is still blocked after Close")
		}
		if srv.Addr() != nil {
			t.Errorf("Addr = %v on a server closed without binding; nil is how a waiter tells it never became ready", srv.Addr())
		}
		// And it stays that way: nothing can bind it afterwards.
		if _, err := srv.Bind(context.Background()); err == nil {
			t.Error("Bind after Close returned no error")
		}
		if err := srv.Serve(context.Background()); err == nil {
			t.Error("Serve after Close returned no error")
		}
		if srv.Addr() != nil {
			t.Errorf("Addr = %v after Bind and Serve were refused", srv.Addr())
		}
	})
	t.Run("Bind, then Close", func(t *testing.T) {
		srv := mustNew(t, server.Options{})
		addr, err := srv.Bind(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if !released(srv) || srv.Addr() == nil || srv.Addr().String() != addr.String() {
			t.Fatalf("after Bind: Ready closed %v, Addr %v, bound %v", released(srv), srv.Addr(), addr)
		}
		if err := srv.Close(); err != nil {
			t.Fatal(err)
		}
		// It did become ready, and Addr goes on saying where.
		if srv.Addr() == nil {
			t.Error("Addr went back to nil after Close on a server that had bound")
		}
	})
	t.Run("Serve fails to bind", func(t *testing.T) {
		srv := mustNew(t, server.Options{ListenAddr: taken(t)})
		if err := srv.Serve(context.Background()); err == nil {
			t.Fatal("Serve on a taken port returned no error")
		}
		if !released(srv) || srv.Addr() != nil {
			t.Errorf("Ready closed %v, Addr %v; want closed and nil", released(srv), srv.Addr())
		}
	})
	t.Run("Serve", func(t *testing.T) {
		srv := mustNew(t, server.Options{})
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- srv.Serve(ctx) }()
		if !released(srv) || srv.Addr() == nil {
			t.Fatalf("Ready closed %v, Addr %v; want closed and bound", released(srv), srv.Addr())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
	t.Run("ServeListener", func(t *testing.T) {
		srv := mustNew(t, server.Options{})
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- srv.ServeListener(ctx, ln) }()
		if !released(srv) || srv.Addr() == nil || srv.Addr().String() != ln.Addr().String() {
			t.Fatalf("Ready closed %v, Addr %v; want closed and %v", released(srv), srv.Addr(), ln.Addr())
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !refused(ln.Addr().String()) {
			t.Error("the listener given to ServeListener is still open after it returned")
		}
	})
	t.Run("Close racing Bind", func(t *testing.T) {
		// Whichever wins, the two answers agree: Addr is nil exactly when Bind failed.
		// And what a waiter read from Addr when Ready closed stays true: a server that
		// was "closed without binding" does not turn out bound a moment later.
		for i := 0; i < 100; i++ {
			srv := mustNew(t, server.Options{})
			bound := make(chan error, 1)
			atReady := make(chan net.Addr, 1)
			go func() { <-srv.Ready(); atReady <- srv.Addr() }()
			go func() { _, err := srv.Bind(context.Background()); bound <- err }()
			srv.Close()
			err := <-bound
			if first := <-atReady; first == nil && srv.Addr() != nil {
				t.Fatalf("run %d: Addr was nil when Ready closed and is %v now", i, srv.Addr())
			}
			if !released(srv) {
				t.Fatalf("run %d: Ready is still open after Close (Bind returned %v)", i, err)
			}
			if (err != nil) != (srv.Addr() == nil) {
				t.Fatalf("run %d: Bind returned %v and Addr is %v", i, err, srv.Addr())
			}
			if a := srv.Addr(); a != nil && !refused(a.String()) {
				t.Fatalf("run %d: %v still accepts after Close", i, a)
			}
			srv.Close()
		}
	})
}
