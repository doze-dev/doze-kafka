package protocol

import (
	"context"
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
	log      *slog.Logger
}

// NewRouter returns an empty Router.
func NewRouter(log *slog.Logger) *Router {
	return &Router{handlers: make(map[APIKey]Handler), log: log}
}

// Register binds a handler to an API key, replacing any previous binding.
func (r *Router) Register(key APIKey, h Handler) { r.handlers[key] = h }

// Route dispatches req to its handler. An unregistered key is a protocol error.
func (r *Router) Route(ctx context.Context, req *Request) ([]byte, error) {
	h, ok := r.handlers[req.Header.APIKey]
	if !ok {
		return nil, fmt.Errorf("no handler for API %s v%d", req.Header.APIKey.Name(), req.Header.APIVersion)
	}
	return h(ctx, req)
}
