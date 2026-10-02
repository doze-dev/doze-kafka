package main

// The .doze zone, on the one platform that has none.
//
// doze-names is file locks, loopback aliases and a resolver hook, and none of
// the three has a Windows form yet — the package does not compile there. That
// used to take the whole binary with it, and the release would have stopped
// building a Windows archive, silently, at the next tag.
//
// Names are additive. The configured listen address is the contract and it is
// all of doze-kafka that Windows gets, which is everything except a prettier
// name. So the zone here is the one main.go already tolerates: absent.

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
)

// zone is doze-kafka's participation in .doze. On Windows there is none.
type zone struct{}

func joinZone(context.Context, *slog.Logger) *zone { return nil }

func (z *zone) brokerURL(*slog.Logger, net.Addr) string { return "" }

func (z *zone) routeConsole(*slog.Logger, string) string { return "" }

func (z *zone) close() {}

// runDNSSetup is `doze-kafka dns-setup`, which has nothing to set up here. It
// says so and fails, rather than reporting a success that changed nothing.
func runDNSSetup([]string) int {
	fmt.Fprintln(os.Stderr, "dns-setup: .doze names are not available on Windows.\n"+
		"Nothing is lost that doze-kafka needs: the broker address you pass to --listen works as it does everywhere.")
	return 1
}
