package server_test

import (
	"context"
	"errors"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/server"
)

// openFDs is how many file descriptors this process holds, or -1 where there is no
// cheap way to count them (Windows). Reading the directory costs one itself, the
// same one every time.
func openFDs() int {
	for _, dir := range []string{"/proc/self/fd", "/dev/fd"} {
		if ents, err := os.ReadDir(dir); err == nil {
			return len(ents)
		}
	}
	return -1
}

// settled waits for the process to be back to no more than the goroutines and file
// descriptors it had, and reports what is left if it is not.
func settled(t *testing.T, what string, goroutines, fds int) {
	t.Helper()
	var g, f int
	for deadline := time.Now().Add(5 * time.Second); ; {
		// No runtime.GC here, on purpose: a file nobody closed is closed by its
		// finalizer once collected, which would hide exactly the leak this looks for.
		g, f = runtime.NumGoroutine(), openFDs()
		if g <= goroutines && f <= fds {
			return
		}
		if time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if g > goroutines {
		buf := make([]byte, 1<<20)
		t.Errorf("%s: %d goroutines, %d before\n%s", what, g, goroutines, buf[:runtime.Stack(buf, true)])
	}
	if f > fds {
		t.Errorf("%s: %d open file descriptors, %d before", what, f, fds)
	}
}

// taken is an address something else is listening on.
func taken(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln.Addr().String()
}

// refused reports whether nothing accepts on addr any more.
func refused(addr string) bool {
	c, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return true
	}
	c.Close()
	return false
}

// A server that is serving is stopped by Close as it is by cancelling its context:
// Serve returns, and only then is storage closed. (Close used to close storage
// under the connections still being served.)
func TestCloseStopsAServingServerAndWaitsForIt(t *testing.T) {
	dir := t.TempDir()
	goroutines, fds := 0, 0
	{
		warm := mustNew(t, server.Options{})
		done := make(chan error, 1)
		go func() { done <- warm.Serve(context.Background()) }()
		<-warm.Ready()
		warm.Close()
		<-done
		goroutines, fds = runtime.NumGoroutine(), openFDs()
	}
	srv := mustNew(t, server.Options{DataDir: dir})
	extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(context.Background()) }()
	<-srv.Ready()
	addr := srv.Addr().String()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	if err := srv.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve, stopped by Close, returned %v; want nil as for a cancelled context", err)
		}
	default:
		t.Fatal("Close returned while Serve was still running")
	}
	for _, a := range []string{addr, extra.String()} {
		if !refused(a) {
			t.Errorf("%s still accepts after Close", a)
		}
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, err := conn.Read(make([]byte, 1)); err == nil || errors.Is(err, os.ErrDeadlineExceeded) {
		t.Errorf("a connection open when Close was called was not closed (read: %v)", err)
	}
	conn.Close()
	settled(t, "after Close", goroutines, fds)
	again, err := server.New(server.Options{DataDir: dir})
	if err != nil {
		t.Fatalf("the data directory after Close: %v", err)
	}
	again.Close()
}

// A refused ServeListener (the server is already serving, or closed) has not taken
// the listener: it is still the caller's, open.
func TestARefusedServeListenerLeavesTheListenerToTheCaller(t *testing.T) {
	srv := mustNew(t, server.Options{})
	if err := srv.Close(); err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := srv.ServeListener(context.Background(), ln); err == nil {
		t.Fatal("ServeListener after Close returned no error")
	}
	if refused(ln.Addr().String()) {
		t.Error("a refused ServeListener closed the caller's listener")
	}
}

// However Serve ends, the listeners AlsoListen opened end with it. A Serve that
// could not bind its own address used to close storage and leave them bound and
// accepting, with nothing behind them.
func TestServeThatFailsReleasesTheOtherListeners(t *testing.T) {
	for name, serve := range map[string]func(t *testing.T, srv *server.Server) error{
		"the address is taken": func(t *testing.T, srv *server.Server) error {
			return srv.Serve(context.Background())
		},
		"the context is already cancelled": func(t *testing.T, srv *server.Server) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			srv.Serve(ctx)
			return errors.New("over, with or without an error")
		},
		"the listener given is already closed": func(t *testing.T, srv *server.Server) error {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			ln.Close()
			return srv.ServeListener(context.Background(), ln)
		},
	} {
		t.Run(name, func(t *testing.T) {
			opts := server.Options{}
			if name == "the address is taken" {
				opts.ListenAddr = taken(t)
			}
			dir := t.TempDir()
			opts.DataDir = dir
			goroutines := runtime.NumGoroutine()
			srv := mustNew(t, opts)
			extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			if err := serve(t, srv); err == nil {
				t.Fatal("Serve returned no error")
			}
			if !refused(extra.String()) {
				t.Errorf("the AlsoListen listener %s is still bound and accepting after Serve returned", extra)
			}
			settled(t, "after Serve returned", goroutines, 1<<30)
			again, err := server.New(server.Options{DataDir: dir})
			if err != nil {
				t.Fatalf("the data directory after a failed Serve: %v", err)
			}
			again.Close()
		})
	}
}
