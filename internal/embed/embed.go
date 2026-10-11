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

// NewFromConfig builds a Server from an already-resolved config.
func NewFromConfig(cfg config.Config, logger *slog.Logger) (*server.Server, error) {
	s, err := serverhook.New(cfg, logger)
	if err != nil {
		return nil, err
	}
	return s.(*server.Server), nil
}
