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
// # Reaching the broker by name
//
// kafka.doze resolves to the zone's apex address (127.0.0.3). A broker listening
// on the wildcard (--listen :9092) already answers there. The default listener is
// loopback (127.0.0.1:9092), which does not, so it gets a second listener on the
// apex address: same port, same broker (see reachBroker). Anything narrower — a
// specific LAN address — is the user's choice, and warns rather than fails.
//
// # The console is a different transport
//
// The wire protocol is TCP and reaches the name by DNS alone. The console is
// HTTP, so it goes through the shared front door by Host header, which is what
// makes http://kafka.doze port-less — the same shape as http://aws.doze.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/url"
	"os"
	"strconv"
	"time"

	names "github.com/doze-dev/doze-names"

	"github.com/doze-dev/doze-kafka/server"
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

// reachBroker makes the broker answer on kafka.doze and returns the address to
// advertise ("kafka.doze:9092"), or "" if the name will not reach it. bound is the
// broker's real listening address, the only reliable source of the port once ":0"
// is in play.
//
// The name resolves to the apex address (127.0.0.3). A wildcard listener already
// answers there; a loopback listener does not, so it gets a second listener on the
// apex address, same port, same broker. That is what lets the default
// 127.0.0.1:9092 and kafka.doze:9092 both work. Anything narrower than that (a
// specific LAN address, a host name) is the user's choice, and warns instead.
func (z *zone) reachBroker(ctx context.Context, logger *slog.Logger, srv *server.Server, bound net.Addr) string {
	if z == nil || z.lease == nil || bound == nil {
		return ""
	}
	host, port, err := net.SplitHostPort(bound.String())
	if err != nil {
		logger.Debug("zone: could not parse the bound address", "addr", bound.String(), "err", err)
		return ""
	}
	name := z.lease.Name.Host

	if !servesIP(host, z.lease.IP) {
		ip := net.ParseIP(host)
		if ip == nil || !ip.IsLoopback() {
			logger.Warn("zone: the name resolves but the broker does not listen there",
				"name", name, "apex_addr", net.JoinHostPort(z.lease.IP.String(), port),
				"listening_on", bound.String(),
				"still_works", bound.String(),
				"hint", "listen on the wildcard (--listen :"+port+") or on loopback for "+name+" to reach the broker")
			return ""
		}
		apex := net.JoinHostPort(z.lease.IP.String(), port)
		if _, err := srv.AlsoListen(ctx, apex); err != nil {
			// Usually the loopback alias dns-setup creates is not there yet.
			logger.Debug("zone: could not also listen on the apex address", "addr", apex, "err", err)
			return ""
		}
	}

	addr := net.JoinHostPort(name, port)
	if !reachable(addr) {
		logger.Debug("zone: the name does not reach the broker yet", "addr", addr)
		return ""
	}
	return addr
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
	var u string
	for {
		u = z.reg.URLFor(name)
		if u == "http://"+name || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Print a by-name URL only if it answers: a name that does not resolve (no
	// dns-setup) or a front door that is not up is worth saying nothing about.
	if pu, err := url.Parse(u); err == nil && pu.Host != "" {
		hostport := pu.Host
		if pu.Port() == "" {
			hostport = net.JoinHostPort(pu.Hostname(), "80")
		}
		if !reachable(hostport) {
			logger.Debug("zone: the console name does not answer", "url", u)
			return ""
		}
	}
	return u
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
	mode, print, err := parseDNSSetup(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Println(dnsSetupUsage)
			return 0
		}
		fmt.Fprintf(os.Stderr, "doze-kafka dns-setup: %v\n%s\n", err, dnsSetupUsage)
		return 2
	}
	switch mode {
	case "check":
		st := names.Check()
		fmt.Printf("%s\n%s", st.Platform, st)
		if !st.OK() {
			fmt.Printf("\nrun `doze-kafka dns-setup` to finish. Without it, %s still works —\n"+
				"names are additive, never a replacement.\n", defaultBrokerAddr())
			return 1
		}
		return 0
	case "uninstall":
		if err := names.Uninstall(names.Options{Print: print}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	default:
		if err := names.Install(names.Options{Print: print}); err != nil {
			fmt.Fprintln(os.Stderr, err)
			return 1
		}
	}
	return 0
}

const dnsSetupUsage = `usage: doze-kafka dns-setup [--check | --uninstall] [--print]
  (no flag)     install: the resolver entry and loopback aliases for .doze (asks for sudo)
  --check       report what is set up, changing nothing
  --uninstall   remove everything dns-setup installed
  --print       with install or uninstall: print the privileged script instead of running it`

// parseDNSSetup decides what `dns-setup` was asked to do: "install", "check" or
// "uninstall". It refuses anything it does not understand, because the default
// action changes the machine and asks for sudo: `dns-setup extra --check` used to
// stop reading at "extra", never see --check, and run the installer.
func parseDNSSetup(args []string) (mode string, print bool, err error) {
	fs := flag.NewFlagSet("dns-setup", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	check := fs.Bool("check", false, "")
	uninstall := fs.Bool("uninstall", false, "")
	fs.BoolVar(&print, "print", false, "")
	if err := fs.Parse(args); err != nil {
		return "", false, flagError(fs, err)
	}
	switch {
	case fs.NArg() > 0:
		return "", false, fmt.Errorf("unexpected argument %q", fs.Arg(0))
	case *check && *uninstall:
		return "", false, errors.New("--check and --uninstall cannot be combined")
	case *check && print:
		return "", false, errors.New("--check runs nothing privileged, so there is no script for --print to show")
	case *check:
		return "check", false, nil
	case *uninstall:
		return "uninstall", print, nil
	}
	return "install", print, nil
}

// defaultBrokerAddr is the address the docs promise, for the one message that
// has to name it before any config has been loaded.
func defaultBrokerAddr() string {
	return net.JoinHostPort("localhost", strconv.Itoa(defaultKafkaPort))
}

const defaultKafkaPort = 9092
