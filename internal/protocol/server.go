package protocol

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// defaultMaxConns is far above what local development uses (a busy app is tens of
// connections) and far below what exhausts a default descriptor limit.
const defaultMaxConns = 2048

// Server accepts TCP connections and serves each one with the Router.
type Server struct {
	addr   string
	router *Router
	log    *slog.Logger
	ln     net.Listener
	extra  []net.Listener // more addresses answering as the same broker (see AlsoListen).

	observer Observer      // told about every connection and request; nil = nobody is watching.
	connSeq  atomic.Uint64 // connection ids, for the observer.

	tlsConfig    *tls.Config // when set, the listener terminates TLS.
	authRequired bool        // when true, connections must complete SASL before other APIs.

	preAuthTimeout time.Duration // total time a connection has to authenticate.
	idleTimeout    time.Duration // how long a connection may sit between requests.
	frameTimeout   time.Duration // how long a request may take to arrive once begun.
	writeTimeout   time.Duration // how long one response may take to write.
	maxConns       int           // connections served at once; more are refused.
	refused        atomic.Int64  // connections refused at the cap (for the log line and tests).

	// lnMu guards the listeners and whether Serve has started. Bind, AlsoListen,
	// Serve and Close may be called from different goroutines by an embedder.
	lnMu    sync.Mutex
	serving bool
	closed  bool

	mu    sync.Mutex
	conns map[*Conn]struct{} // in-flight connections, for shutdown drain.
	wg    sync.WaitGroup     // tracks conn.serve goroutines.
	loops sync.WaitGroup     // tracks the accept loops of extra listeners.
}

// NewServer returns a Server that will listen on addr.
func NewServer(addr string, router *Router, log *slog.Logger) *Server {
	return &Server{addr: addr, router: router, log: log, conns: make(map[*Conn]struct{}), preAuthTimeout: defaultPreAuthTimeout,
		idleTimeout: defaultIdleTimeout, frameTimeout: defaultFrameTimeout, writeTimeout: defaultWriteTimeout, maxConns: defaultMaxConns}
}

// UseTLS makes the listener terminate TLS with the given config. Call before Bind.
func (s *Server) UseTLS(cfg *tls.Config) { s.tlsConfig = cfg }

// UseListener injects a caller-provided listener instead of binding s.addr —
// used by embedders (e.g. the doze proxy hands us a unix-socket listener; the
// Kafka wire protocol is transport-agnostic). Wraps in TLS when configured.
// Call before Serve.
func (s *Server) UseListener(ln net.Listener) {
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	s.lnMu.Lock()
	s.ln = ln
	s.lnMu.Unlock()
}

// RequireAuth gates every non-SASL API behind a completed SASL handshake. Call
// before Serve.
func (s *Server) RequireAuth() { s.authRequired = true }

// Bind opens the listener without accepting yet, and returns the bound address.
// Splitting bind from accept lets callers (and tests) learn the real port when
// addr uses an ephemeral ":0". Serve binds implicitly if Bind was not called.
func (s *Server) Bind(ctx context.Context) (net.Addr, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.addr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	if s.closed || s.ln != nil {
		_ = ln.Close()
		if s.closed {
			return nil, ErrServerClosed
		}
		return nil, errors.New("protocol: the server already has a listener")
	}
	s.ln = ln
	return ln.Addr(), nil
}

// ErrServerClosed is returned by Bind, AlsoListen and Serve once Close has run.
var ErrServerClosed = errors.New("protocol: the server is closed")

// ErrAlreadyServing is returned by a second Serve, and by AlsoListen once Serve
// has started: the accept loops are started by Serve, so a listener added after
// it would be bound and never served.
var ErrAlreadyServing = errors.New("protocol: the server is already serving")

// Close closes every listener. It is for a server that was bound but will not be
// served (start-up failed after Bind); a serving one is stopped by cancelling the
// context given to Serve. Safe to call more than once.
func (s *Server) Close() error {
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	s.closed = true
	var first error
	for _, ln := range s.listeners() {
		if err := ln.Close(); err != nil && first == nil && !errors.Is(err, net.ErrClosed) {
			first = err
		}
	}
	return first
}

// listeners is every listener the server holds. The caller holds lnMu.
func (s *Server) listeners() []net.Listener {
	var out []net.Listener
	if s.ln != nil {
		out = append(out, s.ln)
	}
	return append(out, s.extra...)
}

// AlsoListen opens one more listener, on another address, that serves the same
// broker. The zone uses it to answer on kafka.doze's address when the main
// listener is narrower than the wildcard. Call after Bind and before Serve.
func (s *Server) AlsoListen(ctx context.Context, addr string) (net.Addr, error) {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}
	if s.tlsConfig != nil {
		ln = tls.NewListener(ln, s.tlsConfig)
	}
	s.lnMu.Lock()
	defer s.lnMu.Unlock()
	switch {
	case s.closed:
		_ = ln.Close()
		return nil, ErrServerClosed
	case s.serving:
		_ = ln.Close()
		return nil, ErrAlreadyServing
	}
	s.extra = append(s.extra, ln)
	return ln.Addr(), nil
}

// Serve accepts connections until ctx is cancelled, then stops accepting and
// returns. In-flight connections observe cancellation on their next read.
func (s *Server) Serve(ctx context.Context) error {
	s.lnMu.Lock()
	unbound := s.ln == nil && !s.closed && !s.serving
	s.lnMu.Unlock()
	if unbound {
		if _, err := s.Bind(ctx); err != nil {
			return err
		}
	}
	s.lnMu.Lock()
	switch {
	case s.closed:
		s.lnMu.Unlock()
		return ErrServerClosed
	case s.serving:
		s.lnMu.Unlock()
		return ErrAlreadyServing
	}
	s.serving = true
	ln, extra := s.ln, append([]net.Listener(nil), s.extra...)
	s.lnMu.Unlock()
	s.log.Info("listening", "addr", ln.Addr().String())

	// When serving ends, for whatever reason, stop accepting everywhere and close
	// in-flight connections so any parked in a blocking read unblock and their
	// serve loops exit promptly. (An accept error on the main listener used to
	// leave the extra ones bound and accepting.)
	stop := make(chan struct{})
	defer close(stop)
	shut := func() {
		_ = ln.Close()
		for _, x := range extra {
			_ = x.Close()
		}
		s.closeConns()
	}
	go func() {
		select {
		case <-ctx.Done():
			shut()
		case <-stop:
		}
	}()

	for _, x := range extra {
		s.log.Info("listening", "addr", x.Addr().String())
		s.loops.Add(1)
		go func(x net.Listener) {
			defer s.loops.Done()
			_ = s.acceptLoop(ctx, x)
		}(x)
	}

	err := s.acceptLoop(ctx, ln)
	if ctx.Err() == nil {
		shut() // the main listener failed: take the others down with it.
	}
	s.loops.Wait() // no accept loop may add a connection once we start waiting for them.
	s.wg.Wait()    // let in-flight connections drain before returning.
	if ctx.Err() != nil {
		return nil // clean shutdown
	}
	s.lnMu.Lock()
	closed := s.closed
	s.lnMu.Unlock()
	if closed {
		return ErrServerClosed
	}
	return err
}

// acceptLoop serves one listener until it is closed or ctx is cancelled.
func (s *Server) acceptLoop(ctx context.Context, ln net.Listener) error {
	var backoff time.Duration
	for {
		raw, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Anything else (EMFILE when descriptors run out, ECONNABORTED, a
			// transient network blip) is a problem with one accept, not with the
			// listener. Ending the broker here turned "too many open files" into
			// a dead process; wait and try again, as net/http does.
			if backoff == 0 {
				backoff = 5 * time.Millisecond
			} else if backoff *= 2; backoff > time.Second {
				backoff = time.Second
			}
			s.log.Warn("accept failed; retrying", "err", err, "in", backoff)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
			}
			continue
		}
		backoff = 0
		if s.connCount() >= s.maxConns {
			// A ceiling, so connections that cost nothing to open cannot exhaust
			// descriptors and memory. Logged once in a while, not per refusal.
			if n := s.refused.Add(1); n == 1 || n%1000 == 0 {
				s.log.Warn("refusing connections: at the connection limit", "limit", s.maxConns, "refused_so_far", n)
			}
			_ = raw.Close()
			continue
		}
		conn := newConn(raw, s.router, s.log, s.authRequired)
		conn.observer = s.observer
		conn.id = s.connSeq.Add(1)
		conn.accept()
		conn.preAuthTimeout, conn.idleTimeout, conn.frameTimeout = s.preAuthTimeout, s.idleTimeout, s.frameTimeout
		s.mu.Lock()
		conn.writeTimeout = s.writeTimeout
		s.mu.Unlock()
		s.track(conn)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer s.untrack(conn)
			conn.serve(ctx)
		}()
	}
}

func (s *Server) track(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[c] = struct{}{}
}

func (s *Server) connCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *Server) untrack(c *Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, c)
}

// closeConns closes every currently tracked connection. Serve loops close their
// own socket on exit too; a double close is harmless.
func (s *Server) closeConns() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.close()
	}
}
