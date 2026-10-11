// Package serverhook is how this module's own commands reach what a running
// server.Server knows beyond its public API: the recorder of requests and the
// store of client-reported metrics the console reads.
//
// They used to be exported methods on Server, which made two internal types part
// of the embedding API (a caller outside this module cannot name them, but can call
// their methods). Server registers the accessors here when the package loads, and
// only code inside this module can import them.
package serverhook

import (
	"log/slog"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/telemetry"
	"github.com/doze-dev/doze-kafka/internal/traffic"
)

var (
	// New builds a *server.Server from a resolved config (internal/embed wraps it with
	// the type). It is not part of the embedding API because config.Config is internal.
	New func(cfg config.Config, logger *slog.Logger) (any, error)
	// Traffic returns the server's request recorder, or nil when no console runs.
	// The argument is a *server.Server.
	Traffic func(srv any) *traffic.Recorder
	// ClientMetrics returns the server's client-telemetry store, or nil when client
	// telemetry is off. The argument is a *server.Server.
	ClientMetrics func(srv any) *telemetry.Store
)
