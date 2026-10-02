//go:build !windows

package main

// Adoption of the shared .doze zone.
//
// doze-kafka claims kafka.doze and joins the zone as a peer: if no other doze
// binary is serving DNS, this process serves it, answering for every peer's
// names and not only its own. That is what lets a machine with nothing but
// doze-kafka installed still resolve .doze — and what lets doze-aws's names
// work when doze-kafka is the one that happens to be running.
//
// Names are additive. The configured listen address is the contract and never
// moves; everything here degrades to a log line.
//
// # Why this is shorter than doze-aws's version
//
// doze-aws binds one specific address, so a name it claims needs a second
// listener on the apex address. The broker's listen address defaults to the
// WILDCARD (:9092), which already answers on 127.0.0.3 — so kafka.doze:9092
// works from the claim alone, with no extra listener and no second port. What
// takes that away is narrowing --listen to a single address, and that case
// warns rather than fails, because the remedy is the user's to choose.
//
// The broker also cannot use a second listener the way doze-aws can: Kafka's
// protocol.Server accepts exactly one, whereas an http.Server serves many.
//
// # The console is a different transport
//
// The wire protocol is TCP and reaches the name by DNS alone. The console is
// HTTP, so it goes through the shared front door by Host header, which is what
// makes http://kafka.doze port-less — the same shape as http://aws.doze.

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"strconv"
	"time"

	names "github.com/doze-dev/doze-names"
)

// zone is doze-kafka's participation in .doze.
type zone struct {
	lease *names.Lease
	srv   *names.Server
	front *names.Ingress
	reg   *names.Registry
}

// joinZone claims kafka.doze and starts serving the zone if nobody else is.
// It never fails: a machine that has not run dns-setup simply has no names,
// which is a smaller thing than refusing to start the broker.
func joinZone(ctx context.Context, logger *slog.Logger) *zone {
	z := &zone{}
	reg := names.Open(names.Home(), "doze-kafka")

	lease, err := reg.Claim(names.Apex("kafka"))
	switch {
	case err == nil:
		z.lease = lease
	default:
		if held, ok := names.Held(err); ok {
			// First-come, and the holder keeps it. Saying which process has it
			// turns "my name stopped working" into something answerable.
			logger.Info("zone: apex name already claimed",
				"name", held.Host, "held_by_pid", held.PID, "owner", held.Owner)
		} else {
			logger.Debug("zone: could not claim the apex name", "err", err)
		}
	}

	z.reg = reg
	// Info, not Debug: these lines are few, they happen at startup, and they are
	// the only warning that a name is not going to work.
	logf := func(format string, args ...any) { logger.Info(fmt.Sprintf(format, args...)) }
	z.srv = names.Serve(ctx, reg, logf)
	// The shared front door is what makes the console's name port-less. macOS
	// will not let an unprivileged process hold :80 on a specific address, only
	// on the wildcard, so one listener fronts every peer's names by Host header.
	z.front = names.ServeIngress(ctx, reg, logf)
	return z
}

// brokerURL is the address to tell clients to use for the broker, or "" if the
// name will not reach it. bound is the broker's real listening address, which
// is the only reliable source of the port once ":0" is in play.
func (z *zone) brokerURL(logger *slog.Logger, bound net.Addr) string {
	if z == nil || z.lease == nil || bound == nil {
		return ""
	}
	host, port, err := net.SplitHostPort(bound.String())
	if err != nil {
		logger.Debug("zone: could not parse the bound address", "addr", bound.String(), "err", err)
		return ""
	}
	name := z.lease.Name.Host

	// The name resolves to the apex address whether or not anything answers
	// there. Advertising kafka.doze:9092 while the broker listens only on
	// 127.0.0.1 would hand out an address that refuses every connection.
	if !servesIP(host, z.lease.IP) {
		logger.Warn("zone: the name resolves but the broker does not listen there",
			"name", name, "apex_addr", net.JoinHostPort(z.lease.IP.String(), port),
			"listening_on", bound.String(),
			"still_works", bound.String(),
			"hint", "listen on the wildcard (--listen :"+port+") for "+name+" to reach the broker")
		return ""
	}
	return net.JoinHostPort(name, port)
}

// servesIP reports whether a listening host answers on ip. An empty or
// unspecified host is the wildcard, which answers on every address the machine
// has — including the apex alias, once dns-setup has created it.
func servesIP(listenHost string, ip net.IP) bool {
	if listenHost == "" {
		return true
	}
	lip := net.ParseIP(listenHost)
	if lip == nil {
		// A hostname rather than an address. It may well resolve to the apex
		// address, but saying so without checking is the kind of guess this
		// whole zone exists to avoid.
		return false
	}
	return lip.IsUnspecified() || lip.Equal(ip)
}

// routeConsole publishes where the front door should send kafka.doze, so
// http://kafka.doze serves the console. It returns the URL to print, which is
// port-less when a front door is up and port-ful when it is not — so the line
// printed at startup is one that actually works.
func (z *zone) routeConsole(logger *slog.Logger, consoleAddr string) string {
	if z == nil || z.lease == nil {
		return ""
	}
	// The front door dials the console at the address it actually bound. A
	// wildcard console address is not dialable as written, so aim at loopback.
	host, port, err := net.SplitHostPort(consoleAddr)
	if err != nil {
		logger.Debug("zone: could not parse the console address", "addr", consoleAddr, "err", err)
		return ""
	}
	if host == "" || net.ParseIP(host).IsUnspecified() {
		host = "127.0.0.1"
	}
	if err := z.lease.Route(net.JoinHostPort(host, port)); err != nil {
		logger.Debug("zone: could not publish the console route", "err", err)
		return ""
	}

	// Wait briefly for a front door to appear before deciding how to print the
	// URL. Ours binds in a goroutine that races this line, and a peer's entry
	// may land a moment after we start — losing either race advertises a
	// port-ful URL for a name that is in fact port-less.
	//
	// The wait only runs while the answer is still the pessimistic one, so the
	// common case — already fronted — costs nothing.
	name := z.lease.Name.Host
	deadline := time.Now().Add(500 * time.Millisecond)
	for {
		u := z.reg.URLFor(name)
		if u == "http://"+name || time.Now().After(deadline) {
			return u
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// close releases the name and stops serving the zone. The registry would prune
// this process's entries anyway once it exits, so this only makes the name
// available again immediately rather than after the next peer's sweep.
func (z *zone) close() {
	if z == nil {
		return
	}
	if z.lease != nil {
		_ = z.lease.Release()
	}
	if z.srv != nil {
		z.srv.Close()
	}
	z.front.Close()
}

// runDNSSetup is `doze-kafka dns-setup`. The same command exists in doze and
// doze-aws and does the same work, because the machine setup belongs to the
// zone rather than to any one binary.
func runDNSSetup(args []string) int {
	fs := flag.NewFlagSet("dns-setup", flag.ExitOnError)
	check := fs.Bool("check", false, "report what is set up, without sudo")
	uninstall := fs.Bool("uninstall", false, "remove everything dns-setup installed")
	print := fs.Bool("print", false, "print the privileged script instead of running it")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	switch {
	case *check:
		st := names.Check()
		fmt.Printf("%s\n%s", st.Platform, st)
		if !st.OK() {
			fmt.Printf("\nrun `doze-kafka dns-setup` to finish. Without it, %s still works —\n"+
				"names are additive, never a replacement.\n", defaultBrokerAddr())
			return 1
		}
		return 0
	case *uninstall:
		if err := names.Uninstall(names.Options{Print: *print}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	default:
		if err := names.Install(names.Options{Print: *print}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	return 0
}

// defaultBrokerAddr is the address the docs promise, for the one message that
// has to name it before any config has been loaded.
func defaultBrokerAddr() string {
	return net.JoinHostPort("localhost", strconv.Itoa(defaultKafkaPort))
}

const defaultKafkaPort = 9092
