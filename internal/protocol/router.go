package protocol

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

// Request is a decoded request handed to a handler. Body holds the bytes after
// the request header (see RequestHeader for the tagged-fields caveat).
type Request struct {
	Header RequestHeader
	Body   []byte
	Conn   *Conn
}

// Handler processes one request and returns the full response payload to be
// length-prefixed onto the wire — the response header (correlation id, plus
// tagged fields for flexible versions) followed by the response body. Returning
// (nil, nil) sends nothing, which is how an API behaves while unimplemented.
type Handler func(ctx context.Context, req *Request) ([]byte, error)

// Router dispatches requests to handlers by API key.
type Router struct {
	handlers map[APIKey]Handler
	ranges   map[APIKey]VersionRange // what ApiVersions advertises; nil = no gate (tests).
	log      *slog.Logger
}

// ErrNotAdvertised is returned for a request the broker never offered: an API
// outside the active profile, or a version outside its advertised range. A real
// broker treats this as an invalid request and closes the connection; answering
// it meant decoding formats the log cannot store (Produce v0 message sets) and
// replying in formats the client cannot read (Fetch v0 with v2 batches).
var ErrNotAdvertised = errors.New("request is outside the advertised API versions")

// Advertise records the version range offered for key. Once any range is set,
// Route refuses everything outside what was advertised.
func (r *Router) Advertise(key APIKey, min, max int16) {
	if r.ranges == nil {
		r.ranges = make(map[APIKey]VersionRange)
	}
	r.ranges[key] = VersionRange{Min: min, Max: max}
}

// NewRouter returns an empty Router.
func NewRouter(log *slog.Logger) *Router {
	return &Router{handlers: make(map[APIKey]Handler), log: log}
}

// Register binds a handler to an API key, replacing any previous binding.
func (r *Router) Register(key APIKey, h Handler) { r.handlers[key] = h }

// Route dispatches req to its handler. An unregistered key is a protocol error.
func (r *Router) Route(ctx context.Context, req *Request) ([]byte, error) {
	if r.ranges != nil {
		rng, ok := r.ranges[req.Header.APIKey]
		v := req.Header.APIVersion
		// ApiVersions is exempt from the version check: a client that asks at a
		// version we do not know must still be told what we support.
		if !ok || (req.Header.APIKey != APIApiVersions && (v < rng.Min || v > rng.Max)) {
			return nil, fmt.Errorf("%w: %s v%d", ErrNotAdvertised, req.Header.APIKey.Name(), v)
		}
	}
	h, ok := r.handlers[req.Header.APIKey]
	if !ok {
		return nil, fmt.Errorf("no handler for API %s v%d", req.Header.APIKey.Name(), req.Header.APIVersion)
	}
	return h(ctx, req)
}
