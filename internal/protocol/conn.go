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
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/doze-dev/doze-kafka/internal/kmsg"
	"github.com/twmb/franz-go/pkg/kerr"
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
	versionsSeen   bool   // an ApiVersions request was answered before the SaslHandshake (one is allowed).
	saslState      any    // opaque per-connection state owned by the SASL handler (e.g. SCRAM step).
	software       string // the client's own name and version, from ApiVersions, e.g. "apache-kafka-java 3.7.0".
	principal      string // who this connection authenticated as ("User:alice"); empty until it has.

	observer Observer // told about this connection and its requests; nil = nobody is watching.
	id       uint64   // this connection's number, for the observer.
	done     atomic.Bool
	// own says this process opened the connection to itself (Adopt): the console's.
	own atomic.Bool
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

// ClientSoftware is what the client said it is in ApiVersions ("apache-kafka-java
// 3.7.0"), or "" if it has not said (or is too old to). A nil connection has none.
func (c *Conn) ClientSoftware() string {
	if c == nil {
		return ""
	}
	return c.software
}

// SetClientSoftware records the client's name and version from its ApiVersions
// request. What is kept is bounded here, where it is kept (ClientSoftware), whatever
// the caller was sent.
func (c *Conn) SetClientSoftware(name, version string) {
	if c != nil {
		c.software = ClientSoftware(name, version)
	}
}

// MaxClientSoftware is how many bytes of a client's software name and version are
// kept. Kafka refuses a name for its characters and never for its length (recorded:
// 20,000 characters are taken; claim: cluster AV3), so a name may fill a frame, and it is kept for as long
// as the connection is open: 300 connections that each sent a 1 MiB name held half a
// gigabyte. It is for people to read, so what is kept is its beginning.
const MaxClientSoftware = 128

// ClientSoftware is "name version" as it is kept and shown: cut to MaxClientSoftware
// bytes, ending in an ellipsis when it was cut, and a copy, so that it does not keep
// the request it came from alive.
func ClientSoftware(name, version string) string {
	const dots = "…"
	// Only the beginning can be kept, so only the beginning is put together: one byte
	// past the bound, so that what was cut here is still seen to be too long below.
	if len(name) > MaxClientSoftware+1 {
		name = name[:MaxClientSoftware+1]
	}
	if len(version) > MaxClientSoftware+1 {
		version = version[:MaxClientSoftware+1]
	}
	s := strings.TrimSpace(name + " " + version)
	if len(s) <= MaxClientSoftware {
		return strings.Clone(s)
	}
	cut := MaxClientSoftware - len(dots)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + dots
}

// ValidClientSoftware reports whether s is a client software name or version that
// Apache Kafka accepts in ApiVersions v3 and later: letters and digits, with dots and
// dashes allowed between them, and at least one character (its
// ApiVersionsRequest.isValid; recorded against 4.3.1 in
// conformance/recordings/apiversions-client-software.json; claim: cluster AV1). Kafka answers anything
// else INVALID_REQUEST.
func ValidClientSoftware(s string) bool {
	alnum := func(c byte) bool {
		return 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || '0' <= c && c <= '9'
	}
	if s == "" || !alnum(s[0]) || !alnum(s[len(s)-1]) {
		return false
	}
	for i := 1; i < len(s)-1; i++ {
		if c := s[i]; !alnum(c) && c != '-' && c != '.' {
			return false
		}
	}
	return true
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

// Authenticated reports whether a SASL exchange has completed on this connection.
// From then on the connection is that principal until it closes: Kafka answers a
// further SaslHandshake or SaslAuthenticate ILLEGAL_SASL_STATE and changes nothing
// (claim: auth A7).
func (c *Conn) Authenticated() bool { return c.authenticated }

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

// What the gate does with a request on a connection that has still to log in.
type preAuthVerdict int

const (
	preAuthServe  preAuthVerdict = iota // hand it to its handler.
	preAuthClose                        // close the connection, with no answer.
	preAuthRefuse                       // answer ILLEGAL_SASL_STATE, then close.
)

// preAuth is the order a login must come in, which is the order Apache Kafka holds
// a connection to (recorded: conformance/recordings/sasl-plain.json and
// sasl-scram.json; claims: auth A8, A9, A10):
//
//   - before the SaslHandshake: one ApiVersions, then the SaslHandshake. A second
//     ApiVersions, a SaslAuthenticate with no handshake, and any other request
//     close the connection with no answer.
//   - after an accepted SaslHandshake, until the exchange is over: SaslAuthenticate
//     only. A second SaslHandshake and an ApiVersions are answered
//     ILLEGAL_SASL_STATE and the connection is closed. Any other request closes it
//     too; Kafka first answers that request's own error shape with
//     ILLEGAL_SASL_STATE, which this broker does not (docs/limitations.md).
//
// It is asked only while the connection is not authenticated.
func (c *Conn) preAuth(key APIKey) preAuthVerdict {
	if c.saslMechanism != "" {
		switch key {
		case kmsg.SASLAuthenticate:
			return preAuthServe
		case kmsg.SASLHandshake, kmsg.ApiVersions:
			return preAuthRefuse
		}
		return preAuthClose
	}
	switch key {
	case kmsg.ApiVersions:
		// One that was served is the one allowed (AnsweredVersions, set by the handler);
		// an answer of "that version is not served, ask again at one of these" is not
		// one served, and the client's second ask is its first (franz-go opens at v5).
		if c.versionsSeen {
			return preAuthClose
		}
		return preAuthServe
	case kmsg.SASLHandshake:
		return preAuthServe
	}
	return preAuthClose
}

// AnsweredVersions is called by the ApiVersions handler when it answered with the
// list (or refused the request itself) and not with "that version is not served":
// from then on a connection that has still to log in may not ask again.
func (c *Conn) AnsweredVersions() { c.versionsSeen = true }

// illegalSASLState is the answer to a SaslHandshake or an ApiVersions that came in
// the middle of a login: the error and nothing else (no mechanisms, no APIs).
func illegalSASLState(h RequestHeader) []byte {
	var resp kmsg.Response
	switch h.APIKey {
	case kmsg.SASLHandshake:
		r := kmsg.NewPtrSASLHandshakeResponse()
		r.ErrorCode = kerr.IllegalSaslState.Code
		resp = r
	case kmsg.ApiVersions:
		r := kmsg.NewPtrApiVersionsResponse()
		r.ErrorCode = kerr.IllegalSaslState.Code
		resp = r
	default:
		return nil
	}
	if h.APIVersion < 0 || h.APIVersion > resp.MaxVersion() {
		return nil // a version there is no shape for: the connection is closed without an answer.
	}
	resp.SetVersion(h.APIVersion)
	return EncodeResponse(h, resp)
}

// serve reads, dispatches, and replies until the peer closes the connection or
// ctx is cancelled.
func (c *Conn) serve(ctx context.Context) {
	defer c.done.Store(true)
	defer c.raw.Close()
	defer c.forget() // before the socket is closed: see own.go.
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
			case key == kmsg.PushTelemetry:
				// A payload up to telemetry.max.bytes (1 MiB, advertised) and its header.
				return maxPushTelemetryFrameBytes
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
			c.observeClosed(&RequestHeader{APIKey: -1}, time.Now(), len(frame), "the request header could not be read: "+err.Error())
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
		if c.authRequired && !c.authenticated {
			switch c.preAuth(header.APIKey) {
			case preAuthClose:
				c.log.Warn("unauthenticated request rejected", "api", APIName(header.APIKey))
				c.observeClosed(&header, time.Now(), len(frame), "the request came before authentication")
				return
			case preAuthRefuse:
				c.log.Warn("request out of order during authentication; closing the connection", "api", APIName(header.APIKey))
				if resp := illegalSASLState(header); resp != nil {
					_ = c.raw.SetWriteDeadline(time.Now().Add(c.writeTimeout))
					if WriteFrame(c.w, resp) == nil {
						_ = c.w.Flush()
					}
				}
				c.observeClosed(&header, time.Now(), len(frame), "the request came in the middle of authentication")
				return
			}
		}

		req := &Request{Header: header, Body: frame[bodyOffset:], Conn: c}
		began := time.Now()
		resp, err := c.route(ctx, req)
		var pe *handlerPanic
		if errors.As(err, &pe) {
			// The handler's bug must cost this connection, not the broker: every
			// other client keeps being served.
			c.log.Error("handler panicked; closing the connection", "api", APIName(header.APIKey),
				"version", header.APIVersion, "client_id", header.ClientID, "panic", pe.logValue(header.APIKey), "stack", pe.stack)
			c.observeClosed(&header, began, len(frame), "the broker's handler panicked")
			return
		}
		var ce *CloseError
		if errors.As(err, &ce) {
			c.log.Warn("closing connection", "api", APIName(header.APIKey), "reason", ce.Reason)
			c.observeClosed(&header, began, len(frame), ce.Reason)
			return
		}
		if err != nil {
			// An unhandled API (one not advertised in ApiVersions) has no response
			// we can frame, and the client is now blocked waiting for this
			// correlation id. Close the connection so it fails fast instead of
			// hanging until its own timeout. Conformant clients never hit this —
			// they only call APIs we advertise.
			c.log.Warn("closing connection: unhandled request", "api", APIName(header.APIKey), "err", err)
			c.observeClosed(&header, began, len(frame), notServed(header))
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
			c.observeClosed(&header, began, len(frame), "the answer could not be written: "+err.Error())
			return
		}
		if err := c.w.Flush(); err != nil {
			c.log.Debug("flush failed", "err", err)
			c.observeClosed(&header, began, len(frame), "the answer could not be written: "+err.Error())
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
	o := Observation{
		Conn: c.id, Remote: c.raw.RemoteAddr().String(), Principal: c.Principal(),
		ClientID: req.Header.ClientID, API: req.Header.APIKey, Version: req.Header.APIVersion,
		Start: began, Duration: time.Since(began), ReqBytes: observedSize(req.Header.APIKey, reqBytes), RespBytes: respBytes,
		Correlation: req.Header.CorrelationID, Own: c.own.Load(),
	}
	// The process's own requests (the console's polling) are told to the observer and
	// not described: nobody lists them, and they are most of the requests on a quiet
	// broker with a page open.
	if !o.Own {
		o.Req, o.Resp, o.Summary = req.decoded, req.answered, req.Describe()
	}
	c.observer.Request(o)
}

// observeClosed tells the observer that this request ended the connection, and why.
// It is the row "why does my app keep disconnecting" needs: the loop returns, and
// before this the only trace was a log line.
func (c *Conn) observeClosed(h *RequestHeader, began time.Time, reqBytes int, reason string) {
	if c.observer == nil {
		return
	}
	class := ClassFailed
	if strings.Contains(reason, "authentication") {
		class = ClassDenied
	}
	c.observer.Request(Observation{
		Conn: c.id, Remote: c.raw.RemoteAddr().String(), Principal: c.Principal(),
		ClientID: h.ClientID, API: h.APIKey, Version: h.APIVersion,
		Start: began, Duration: time.Since(began), ReqBytes: observedSize(h.APIKey, reqBytes),
		Correlation: h.CorrelationID, Closed: reason, Own: c.own.Load(),
		Summary: Summary{Class: class, Kind: Kind(h.APIKey), Detail: "closed the connection: " + reason},
	})
}

// CloseError is what a handler returns to end the connection with no answer, for
// the cases where that is what Apache Kafka does (a SCRAM proof of the wrong
// length, claim: auth A14). Reason is for the log and the console; it must not
// hold anything the client sent.
type CloseError struct{ Reason string }

func (e *CloseError) Error() string { return "closing the connection: " + e.Reason }

// handlerPanic is a recovered panic from a request handler.
type handlerPanic struct {
	value any
	stack string
}

func (p *handlerPanic) Error() string { return fmt.Sprintf("handler panic: %v", p.value) }

// logValue is what the log says the handler panicked with. A panic's value is
// whatever the code that panicked put in it, and a handler reading a SASL token can
// put the token there (a formatted "unexpected ..." with its input, an index that is
// the token's length). So for a request that carries a credential the log names the
// value's type and leaves the stack to say where; the stack holds no values.
func (p *handlerPanic) logValue(api APIKey) any {
	if IsAuthAPI(api) {
		return fmt.Sprintf("a %T (its text is not logged for a request that carries a credential)", p.value)
	}
	return p.value
}

// route dispatches a request, converting a handler panic into an error so one
// connection's bad request cannot take the process down.
func (c *Conn) route(ctx context.Context, req *Request) (resp []byte, err error) {
	defer func() {
		if v := recover(); v != nil {
			resp, err = nil, &handlerPanic{value: v, stack: string(debug.Stack())}
		}
	}()
	if hook := routeHook.Load(); hook != nil {
		(*hook)(req)
	}
	return c.router.Route(ctx, req)
}

// notServed says why a request the router would not take ended its connection: an API
// the broker knows by name but not at the version asked for ("Produce v99 is not served
// by this broker"), or a key it has never heard of ("API key 999 is not served...").
func notServed(h RequestHeader) string {
	if kmsg.RequestForKey(int16(h.APIKey)) != nil {
		return h.APIKey.Name() + " v" + strconv.Itoa(int(h.APIVersion)) + " is not served by this broker"
	}
	return "API key " + strconv.Itoa(int(h.APIKey)) + " is not served by this broker"
}
