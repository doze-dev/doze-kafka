package protocol

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"net"
	"sync"
	"time"
)

// Server accepts TCP connections and serves each one with the Router.
type Server struct {
	addr   string
	router *Router
	log    *slog.Logger
	ln     net.Listener

	tlsConfig    *tls.Config // when set, the listener terminates TLS.
	authRequired bool        // when true, connections must complete SASL before other APIs.

	preAuthTimeout time.Duration // idle limit for a connection that has yet to authenticate.

	mu    sync.Mutex
	conns map[*Conn]struct{} // in-flight connections, for shutdown drain.
	wg    sync.WaitGroup     // tracks conn.serve goroutines.
}

// NewServer returns a Server that will listen on addr.
func NewServer(addr string, router *Router, log *slog.Logger) *Server {
	return &Server{addr: addr, router: router, log: log, conns: make(map[*Conn]struct{}), preAuthTimeout: defaultPreAuthTimeout}
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
	s.ln = ln
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
	s.ln = ln
	return ln.Addr(), nil
}

// Serve accepts connections until ctx is cancelled, then stops accepting and
// returns. In-flight connections observe cancellation on their next read.
func (s *Server) Serve(ctx context.Context) error {
	if s.ln == nil {
		if _, err := s.Bind(ctx); err != nil {
			return err
		}
	}
	ln := s.ln
	s.log.Info("listening", "addr", ln.Addr().String())

	// On cancellation, stop accepting and close in-flight connections so any parked
	// in a blocking read unblock and their serve loops exit promptly.
	go func() {
		<-ctx.Done()
		_ = ln.Close()
		s.closeConns()
	}()

	var backoff time.Duration
	for {
		raw, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				s.wg.Wait() // let in-flight connections drain before returning.
				return nil  // clean shutdown
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
		conn := newConn(raw, s.router, s.log, s.authRequired)
		conn.preAuthTimeout = s.preAuthTimeout
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
