// Package embed is the in-module way to build a server.Server from the internal
// config.Config: the CLI, the broker tests, the conformance targets and the e2e
// seed use it. The public server package takes server.Options instead, because an
// embedder outside this module cannot name config.Config.
package embed

import (
	"log/slog"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// NewWithConsole is server.New for a broker that a console will be attached to in
// this process (the doze module's): it keeps what the console's Traffic page reads
// and offers the client-telemetry APIs, exactly as the command line's broker does
// with its console on. Read the two with serverhook.Traffic and
// serverhook.ClientMetrics, and hand them to the console through internal/consolefeed.
func NewWithConsole(opts server.Options) (*server.Server, error) {
	s, err := serverhook.NewWithConsole(opts)
	if err != nil {
		return nil, err
	}
	return s.(*server.Server), nil
}

// NewFromConfig builds a Server from an already-resolved config.
func NewFromConfig(cfg config.Config, logger *slog.Logger) (*server.Server, error) {
	s, err := serverhook.New(cfg, logger)
	if err != nil {
		return nil, err
	}
	return s.(*server.Server), nil
}
