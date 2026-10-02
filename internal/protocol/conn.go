package protocol

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"time"
)

// defaultPreAuthTimeout is how long a connection that must authenticate may sit
// without sending a complete request before it is dropped. A client that
// connects and then says nothing would otherwise hold a goroutine and a socket
// for as long as it liked. Authenticated connections have no such limit:
// long-polling consumers are idle by design.
const defaultPreAuthTimeout = 30 * time.Second

// Conn is a single client connection. Requests on one connection are processed
// and answered in order; a handler echoes the request's correlation id.
//
// When the broker requires authentication, a connection carries the SASL
// conversation: authRequired gates all non-SASL APIs until authenticated is set,
// and saslMechanism/saslState hold the in-flight handshake (the SASL handlers,
// which live in the broker package, drive these through the exported accessors).
type Conn struct {
	raw    net.Conn
	r      *bufio.Reader
	w      *bufio.Writer
	router *Router
	log    *slog.Logger

	authRequired   bool
	authenticated  bool
	preAuthTimeout time.Duration
	saslMechanism  string
	saslState      any // opaque per-connection state owned by the SASL handler (e.g. SCRAM step).
}

func newConn(raw net.Conn, router *Router, logger *slog.Logger, authRequired bool) *Conn {
	return &Conn{
		raw:            raw,
		r:              bufio.NewReader(raw),
		w:              bufio.NewWriter(raw),
		router:         router,
		log:            logger.With("remote", raw.RemoteAddr().String()),
		authRequired:   authRequired,
		preAuthTimeout: defaultPreAuthTimeout,
	}
}

// RemoteHost is the peer's address without the port, in the form Kafka reports a
// member's host: the IP preceded by a slash ("/127.0.0.1").
func (c *Conn) RemoteHost() string {
	host, _, err := net.SplitHostPort(c.raw.RemoteAddr().String())
	if err != nil {
		host = c.raw.RemoteAddr().String()
	}
	return "/" + host
}

// Authenticated reports whether this connection has completed SASL auth.
func (c *Conn) Authenticated() bool { return c.authenticated }

// SetAuthenticated marks the connection authenticated after a successful SASL
// exchange, lifting the pre-auth gate.
func (c *Conn) SetAuthenticated(v bool) { c.authenticated = v }

// SASLMechanism returns the mechanism negotiated by SaslHandshake, if any.
func (c *Conn) SASLMechanism() string { return c.saslMechanism }

// SetSASLMechanism records the mechanism chosen during SaslHandshake.
func (c *Conn) SetSASLMechanism(m string) { c.saslMechanism = m }

// SASLState returns the SASL handler's opaque per-connection state (nil before the
// first SaslAuthenticate step).
func (c *Conn) SASLState() any { return c.saslState }

// SetSASLState stores the SASL handler's opaque per-connection state between steps.
func (c *Conn) SetSASLState(s any) { c.saslState = s }

// close tears down the underlying socket. The server calls it on shutdown to
// unblock a connection parked in a blocking read so its serve loop can exit.
func (c *Conn) close() error { return c.raw.Close() }

// preAuthAllowed reports whether an API may be sent before authentication: the
// version handshake and the SASL exchange itself.
func preAuthAllowed(key APIKey) bool {
	switch key {
	case APIApiVersions, APISASLHandshake, APISASLAuthenticate:
		return true
	default:
		return false
	}
}

// serve reads, dispatches, and replies until the peer closes the connection or
// ctx is cancelled.
func (c *Conn) serve(ctx context.Context) {
	defer c.raw.Close()
	for {
		if ctx.Err() != nil {
			return
		}

		limit := maxFrameBytes
		if c.authRequired && !c.authenticated {
			limit = maxPreAuthFrameBytes
			_ = c.raw.SetReadDeadline(time.Now().Add(c.preAuthTimeout))
		} else if c.authRequired {
			_ = c.raw.SetReadDeadline(time.Time{}) // authenticated: idle is fine.
		}
		frame, err := ReadFrameLimit(c.r, limit)
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				c.log.Debug("connection closed", "err", err)
			}
			return
		}

		header, bodyOffset, err := ParseRequestHeader(frame)
		if err != nil {
			c.log.Warn("bad request header", "err", err)
			return
		}
		c.log.Info("request",
			"api", header.APIKey.Name(),
			"version", header.APIVersion,
			"correlation_id", header.CorrelationID,
			"client_id", header.ClientID,
		)

		// Enforce the SASL gate: until the connection authenticates, only the
		// version handshake and the SASL exchange are allowed. A violation is a
		// misbehaving or unauthenticated client — drop the connection.
		if c.authRequired && !c.authenticated && !preAuthAllowed(header.APIKey) {
			c.log.Warn("unauthenticated request rejected", "api", header.APIKey.Name())
			return
		}

		req := &Request{Header: header, Body: frame[bodyOffset:], Conn: c}
		resp, err := c.route(ctx, req)
		var pe *handlerPanic
		if errors.As(err, &pe) {
			// The handler's bug must cost this connection, not the broker: every
			// other client keeps being served.
			c.log.Error("handler panicked; closing the connection", "api", header.APIKey.Name(),
				"version", header.APIVersion, "client_id", header.ClientID, "panic", pe.value, "stack", pe.stack)
			return
		}
		if err != nil {
			// An unhandled API (one not advertised in ApiVersions) has no response
			// we can frame, and the client is now blocked waiting for this
			// correlation id. Close the connection so it fails fast instead of
			// hanging until its own timeout. Conformant clients never hit this —
			// they only call APIs we advertise.
			c.log.Warn("closing connection: unhandled request", "api", header.APIKey.Name(), "err", err)
			return
		}
		if resp == nil {
			continue
		}

		if err := WriteFrame(c.w, resp); err != nil {
			c.log.Debug("write failed", "err", err)
			return
		}
		if err := c.w.Flush(); err != nil {
			c.log.Debug("flush failed", "err", err)
			return
		}
	}
}

// handlerPanic is a recovered panic from a request handler.
type handlerPanic struct {
	value any
	stack string
}

func (p *handlerPanic) Error() string { return fmt.Sprintf("handler panic: %v", p.value) }

// route dispatches a request, converting a handler panic into an error so one
// connection's bad request cannot take the process down.
func (c *Conn) route(ctx context.Context, req *Request) (resp []byte, err error) {
	defer func() {
		if v := recover(); v != nil {
			resp, err = nil, &handlerPanic{value: v, stack: string(debug.Stack())}
		}
	}()
	return c.router.Route(ctx, req)
}
