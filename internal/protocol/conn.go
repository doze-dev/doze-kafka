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

	"github.com/doze-dev/doze-kafka/internal/kmsg"
)

// defaultPreAuthTimeout is how long a connection that must authenticate may sit
// without sending a complete request before it is dropped. A client that
// connects and then says nothing would otherwise hold a goroutine and a socket
// for as long as it liked. Authenticated connections have no such limit:
// long-polling consumers are idle by design.
const defaultPreAuthTimeout = 30 * time.Second

const (
	// defaultIdleTimeout closes a connection that has sent nothing for this long
	// (Kafka's connections.max.idle.ms default). Clients reconnect on demand. It
	// is measured between requests: a long-poll fetch or a parked JoinGroup is a
	// request in progress, not idleness.
	defaultIdleTimeout = 10 * time.Minute

	// defaultFrameTimeout is how long a request may take to arrive once its
	// first byte has. A peer that announces a frame and then trickles it would
	// otherwise hold the bytes received so far, and a goroutine, for as long as
	// it liked — with or without authentication.
	defaultFrameTimeout = 30 * time.Second

	// defaultWriteTimeout bounds one response write. A peer that sends requests
	// and never reads fills its socket buffer and parks the goroutine in Write.
	defaultWriteTimeout = 30 * time.Second
)

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
	idleTimeout    time.Duration
	frameTimeout   time.Duration
	writeTimeout   time.Duration
	accepted       time.Time // when the connection was accepted; the pre-auth budget runs from here.
	closeAfter     bool      // close once the current response is written (a failed authentication).
	saslMechanism  string
	saslState      any    // opaque per-connection state owned by the SASL handler (e.g. SCRAM step).
	principal      string // who this connection authenticated as ("User:alice"); empty until it has.

	observer Observer // told about this connection and its requests; nil = nobody is watching.
	id       uint64   // this connection's number, for the observer.
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
		idleTimeout:    defaultIdleTimeout,
		frameTimeout:   defaultFrameTimeout,
		writeTimeout:   defaultWriteTimeout,
		accepted:       time.Now(),
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

// AnonymousPrincipal is who a connection is when nothing authenticated it: every
// connection when SASL is off, so an ACL can still name them.
const AnonymousPrincipal = "User:ANONYMOUS"

// Principal is who the connection authenticated as, in Kafka's "Type:name" form, or
// AnonymousPrincipal if it has not (or the broker does not ask). A nil connection,
// as a handler test may pass, is anonymous.
func (c *Conn) Principal() string {
	if c == nil || c.principal == "" {
		return AnonymousPrincipal
	}
	return c.principal
}

// SetPrincipal records who the SASL exchange authenticated. An empty string clears it.
func (c *Conn) SetPrincipal(p string) { c.principal = p }

// RemoteIP is the peer's address without the port or the leading slash RemoteHost
// adds, as an ACL's host field is written.
func (c *Conn) RemoteIP() string {
	if c == nil {
		return ""
	}
	host, _, err := net.SplitHostPort(c.raw.RemoteAddr().String())
	if err != nil {
		return c.raw.RemoteAddr().String()
	}
	return host
}

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

// CloseAfterResponse makes the connection close once the response to the current
// request has been written. A failed authentication uses it: the client is told
// why, and then the connection is gone, so each guess costs a new connection.
func (c *Conn) CloseAfterResponse() { c.closeAfter = true }

// close tears down the underlying socket. The server calls it on shutdown to
// unblock a connection parked in a blocking read so its serve loop can exit.
func (c *Conn) close() error { return c.raw.Close() }

// preAuthAllowed reports whether an API may be sent before authentication: the
// version handshake and the SASL exchange itself.
func preAuthAllowed(key APIKey) bool {
	switch key {
	case kmsg.ApiVersions, kmsg.SASLHandshake, kmsg.SASLAuthenticate:
		return true
	default:
		return false
	}
}

// serve reads, dispatches, and replies until the peer closes the connection or
// ctx is cancelled.
func (c *Conn) serve(ctx context.Context) {
	defer c.raw.Close()
	if c.observer != nil {
		c.observer.ConnOpened(c.id, c.raw.RemoteAddr().String())
		defer c.observer.ConnClosed(c.id)
	}
	for {
		if ctx.Err() != nil {
			return
		}

		// Wait for the next request to begin. A connection that still has to
		// authenticate has one budget for the whole of that, counted from when it
		// was accepted (resetting it per request let a stranger stay forever by
		// sending ApiVersions now and then). Everyone else may idle, within reason.
		preAuth := c.authRequired && !c.authenticated
		wait := time.Now().Add(c.idleTimeout)
		if preAuth {
			wait = c.accepted.Add(c.preAuthTimeout)
		}
		_ = c.raw.SetReadDeadline(wait)
		if _, err := c.r.Peek(1); err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				c.log.Debug("connection closed", "err", err)
			}
			return
		}
		// It has begun: the rest must arrive promptly.
		deadline := time.Now().Add(c.frameTimeout)
		if preAuth && wait.Before(deadline) {
			deadline = wait
		}
		_ = c.raw.SetReadDeadline(deadline)
		frame, err := readRequestFrame(c.r, func(key APIKey) int {
			switch {
			case preAuth:
				return maxPreAuthFrameBytes
			case key == kmsg.Produce:
				return maxFrameBytes
			default:
				return maxControlFrameBytes
			}
		})
		_ = c.raw.SetReadDeadline(time.Time{}) // a handler may legitimately take long (long-poll, parked joins).
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
		// Debug: one line per request is a firehose at info (a consumer polls many
		// times a second), and drowned the lines that mattered.
		c.log.Debug("request",
			"api", APIName(header.APIKey),
			"version", header.APIVersion,
			"correlation_id", header.CorrelationID,
			"client_id", header.ClientID,
		)

		// Enforce the SASL gate: until the connection authenticates, only the
		// version handshake and the SASL exchange are allowed. A violation is a
		// misbehaving or unauthenticated client — drop the connection.
		if c.authRequired && !c.authenticated && !preAuthAllowed(header.APIKey) {
			c.log.Warn("unauthenticated request rejected", "api", APIName(header.APIKey))
			return
		}

		req := &Request{Header: header, Body: frame[bodyOffset:], Conn: c}
		began := time.Now()
		resp, err := c.route(ctx, req)
		var pe *handlerPanic
		if errors.As(err, &pe) {
			// The handler's bug must cost this connection, not the broker: every
			// other client keeps being served.
			c.log.Error("handler panicked; closing the connection", "api", APIName(header.APIKey),
				"version", header.APIVersion, "client_id", header.ClientID, "panic", pe.value, "stack", pe.stack)
			return
		}
		if err != nil {
			// An unhandled API (one not advertised in ApiVersions) has no response
			// we can frame, and the client is now blocked waiting for this
			// correlation id. Close the connection so it fails fast instead of
			// hanging until its own timeout. Conformant clients never hit this —
			// they only call APIs we advertise.
			c.log.Warn("closing connection: unhandled request", "api", APIName(header.APIKey), "err", err)
			return
		}
		if resp == nil {
			if c.observer != nil {
				c.observe(req, began, len(frame), 0)
			}
			continue
		}

		_ = c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout))
		if err := WriteFrame(c.w, resp); err != nil {
			c.log.Debug("write failed", "err", err)
			return
		}
		if err := c.w.Flush(); err != nil {
			c.log.Debug("flush failed", "err", err)
			return
		}
		if c.observer != nil {
			c.observe(req, began, len(frame), len(resp))
		}
		if c.closeAfter {
			return
		}
	}
}

// observe tells the observer about an answered request.
func (c *Conn) observe(req *Request, began time.Time, reqBytes, respBytes int) {
	c.observer.Request(Observation{
		Conn: c.id, Remote: c.raw.RemoteAddr().String(), Principal: c.Principal(),
		ClientID: req.Header.ClientID, API: req.Header.APIKey, Version: req.Header.APIVersion,
		Start: began, Duration: time.Since(began), ReqBytes: reqBytes, RespBytes: respBytes,
		Summary: req.Describe(),
	})
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
