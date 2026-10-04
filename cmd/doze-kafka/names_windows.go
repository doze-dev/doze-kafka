package main

// The .doze zone, on the one platform that has none.
//
// doze-names is file locks, loopback aliases and a resolver hook, and none of the
// three has a Windows form yet — the package does not compile there. So on Windows
// an instance cannot be its name: config.Default listens on 127.0.0.1:9092 instead,
// which is what --listen would say, and everything else behaves as it does elsewhere.

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// zone is doze-kafka's participation in .doze. On Windows there is none.
type zone struct{}

func prepareZone(context.Context, config.Config, *slog.Logger) (*zone, error) { return nil, nil }

func (z *zone) host() string                             { return "" }
func (z *zone) ip() string                               { return "" }
func (z *zone) heldBy() *heldBy                          { return nil }
func (z *zone) routeConsole(*slog.Logger, string) string { return "" }
func (z *zone) close()                                   {}

// qualifiedHost is the name an instance would claim, for the commands that print it.
func qualifiedHost(instance string) string { return "kafka." + instance + ".doze" }

func registered(string) (target, consoleURL string) { return "", "" }

const windowsNames = "dns-setup: .doze names are not available on Windows.\n" +
	"Nothing is lost that doze-kafka needs: the address you pass to --listen works as it does everywhere."

// runDNSSetup is `doze-kafka dns-setup`, which has nothing to set up here. It says
// so and fails, rather than reporting a success that changed nothing.
func runDNSSetup([]string) int {
	fmt.Fprintln(os.Stderr, windowsNames)
	return 1
}

func runDoctor(_ []string, stdout, _ io.Writer) int {
	fmt.Fprintln(stdout, "doze-kafka "+version)
	fmt.Fprintln(stdout, "\n.doze names are not available on Windows; doze-kafka serves on the address it listens on.")
	return 0
}
