package protocol

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

func selfSigned(t *testing.T) *tls.Config {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}}
	der, err := x509.CreateCertificate(rand.Reader, tpl, tpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
}

func serveInBackground(t *testing.T, srv *Server) (cancel func(), done chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done = make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
	return cancel, done
}

// A second address answers as the same broker, in plain text or in TLS as the first
// does, and a bad one is refused with its error.
func TestAlsoListenServesTheSameBroker(t *testing.T) {
	for _, useTLS := range []bool{false, true} {
		srv := NewServer("127.0.0.1:0", echoRouter(), quietLogger())
		if useTLS {
			srv.UseTLS(selfSigned(t))
		}
		ctx := context.Background()
		if _, err := srv.Bind(ctx); err != nil {
			t.Fatal(err)
		}
		extra, err := srv.AlsoListen(ctx, "127.0.0.1:0")
		if err != nil || extra == nil {
			t.Fatalf("tls %v: AlsoListen: %v %v", useTLS, extra, err)
		}
		if a, err := srv.AlsoListen(ctx, "256.0.0.1:1"); err == nil || a != nil {
			t.Errorf("tls %v: a bad address gave %v, %v", useTLS, a, err)
		}
		serveInBackground(t, srv)

		var c net.Conn
		if useTLS {
			c, err = tls.Dial("tcp", extra.String(), &tls.Config{InsecureSkipVerify: true})
		} else {
			c, err = net.Dial("tcp", extra.String())
		}
		if err != nil {
			t.Fatalf("tls %v: %v", useTLS, err)
		}
		c.SetDeadline(time.Now().Add(3 * time.Second))
		hdr, payload := frameFor(kmsg.ApiVersions, 0, 5)
		c.Write(append(hdr, payload...))
		if resp, err := ReadFrame(c); err != nil || len(resp) != 4 {
			t.Errorf("tls %v: %v %v", useTLS, resp, err)
		}
		c.Close()
	}
}

type closeErrListener struct {
	net.Listener
	err error
}

func (l closeErrListener) Close() error { _ = l.Listener.Close(); return l.err }

func localListener(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	return ln
}

// Close reports the first real error of its listeners, and takes "already closed" for
// success; a server never bound closes without trouble.
func TestCloseReportsTheFirstRealListenerError(t *testing.T) {
	if err := NewServer("127.0.0.1:0", echoRouter(), quietLogger()).Close(); err != nil {
		t.Errorf("a server never bound: %v", err)
	}

	first, second := errors.New("first"), errors.New("second")
	srv := NewServer("", echoRouter(), quietLogger())
	srv.UseListener(closeErrListener{localListener(t), first})
	srv.extra = []net.Listener{closeErrListener{localListener(t), second}}
	if err := srv.Close(); err != first {
		t.Errorf("Close = %v, want the first error", err)
	}

	srv = NewServer("", echoRouter(), quietLogger())
	srv.UseListener(closeErrListener{localListener(t), net.ErrClosed})
	srv.extra = []net.Listener{closeErrListener{localListener(t), nil}}
	if err := srv.Close(); err != nil {
		t.Errorf("Close of listeners already closed = %v", err)
	}

	srv = NewServer("", echoRouter(), quietLogger())
	srv.UseListener(closeErrListener{localListener(t), nil})
	srv.extra = []net.Listener{closeErrListener{localListener(t), second}}
	if err := srv.Close(); err != second {
		t.Errorf("Close = %v, want the extra listener's error", err)
	}
}

func TestServeOnATakenAddressReturnsTheError(t *testing.T) {
	ln := localListener(t)
	srv := NewServer(ln.Addr().String(), echoRouter(), quietLogger())
	if err := srv.Serve(context.Background()); err == nil {
		t.Error("Serve on a taken address returned nil")
	}
}

// A clean shutdown is nil. A main listener that fails takes the extra ones down with it,
// and Serve returns.
func TestServeEndsCleanlyAndAMainListenerFailureTakesTheOthersDown(t *testing.T) {
	srv := NewServer("127.0.0.1:0", echoRouter(), quietLogger())
	if _, err := srv.Bind(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Serve after a cancel = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return")
	}

	srv = NewServer("127.0.0.1:0", echoRouter(), quietLogger())
	if _, err := srv.Bind(context.Background()); err != nil {
		t.Fatal(err)
	}
	extra, err := srv.AlsoListen(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- srv.Serve(context.Background()) }()
	roundTrip(t, extra.String(), 1)
	srv.lnMu.Lock()
	main := srv.ln
	srv.lnMu.Unlock()
	main.Close()
	select {
	case err := <-finished:
		if err == nil {
			t.Error("Serve returned nil although its listener failed")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Serve did not return when its main listener closed: the extra one is still accepting")
	}
	if c, err := net.DialTimeout("tcp", extra.String(), time.Second); err == nil {
		c.Close()
		t.Error("the extra listener still accepts")
	}
}

// An accept that fails is retried after 5 ms, then twice as long each time.
func TestAFailedAcceptWaitsBeforeTryingAgain(t *testing.T) {
	ln := localListener(t)
	srv := NewServer("", echoRouter(), quietLogger())
	srv.UseListener(&flakyListener{Listener: ln, failures: 3})
	start := time.Now()
	serveInBackground(t, srv)
	roundTrip(t, ln.Addr().String(), 1)
	if took := time.Since(start); took < 5*time.Millisecond+10*time.Millisecond+20*time.Millisecond {
		t.Errorf("three failed accepts were retried after %v, want at least 35ms", took)
	}
}

type countingHandler struct {
	mu    sync.Mutex
	warns []string
}

func (h *countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *countingHandler) WithAttrs([]slog.Attr) slog.Handler       { return h }
func (h *countingHandler) WithGroup(string) slog.Handler            { return h }
func (h *countingHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn {
		h.mu.Lock()
		h.warns = append(h.warns, r.Message)
		h.mu.Unlock()
	}
	return nil
}

// Connections refused at the cap are logged for the first and every thousandth.
func TestRefusalsAreLoggedForTheFirstAndEveryThousandth(t *testing.T) {
	h := &countingHandler{}
	srv := NewServer("127.0.0.1:0", echoRouter(), slog.New(h))
	srv.maxConns = 1
	addr, err := srv.Bind(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	serveInBackground(t, srv)
	held := dial(t, addr.String())
	defer held.Close()
	deadline := time.Now().Add(5 * time.Second)
	for srv.connCount() < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	var refused atomic.Int32
	for i := 0; i < 1000; i++ {
		c, err := net.Dial("tcp", addr.String())
		if err != nil {
			t.Fatal(err)
		}
		// The server closes a refused connection at once.
		c.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := c.Read(make([]byte, 1)); err != nil && errors.Is(err, io.EOF) {
			refused.Add(1)
		}
		c.Close()
	}
	if srv.refused.Load() != 1000 {
		t.Fatalf("%d refused, want 1000 (%d saw the close)", srv.refused.Load(), refused.Load())
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	var refusing []string
	for _, m := range h.warns {
		if strings.Contains(m, "refusing connections") {
			refusing = append(refusing, m)
		}
	}
	if len(refusing) != 2 {
		t.Errorf("%d refusal warnings for 1000 refusals, want 2 (the first and the thousandth)", len(refusing))
	}
}
