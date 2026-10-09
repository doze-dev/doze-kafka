package main

// Adoption of the shared .doze zone.
//
// # One name per instance, and nothing else
//
// Every instance claims kafka.<instance>.doze, where the instance defaults to the
// project directory. That name is what doze-kafka is: it is the address clients
// bootstrap on and the host Metadata sends them back to, so kafka.harbour.doze:9092
// and kafka.atlas.doze:9092 are two different instances that never contend, and can
// both be on the standard port.
//
// kafka.doze, the machine-wide apex, is deliberately NOT claimed. It was a
// shorthand for whichever instance started first, and that is the kind of choice
// that costs more than it gives: an address under it means a different instance
// depending on boot order, so it cannot be written into a config file, and it
// quietly points somewhere else the day a colleague starts their own project. One
// rule is better than two. If you want a short name, name the instance something
// short (--name).
//
// This is the same arrangement doze-aws has, on purpose: the experience of the two
// tools is meant to be one experience. The loopback address a name resolves to is
// doze-names' business: qualified names are hashed into a dynamic range, which is
// what lets two instances bind the same port on different addresses.
//
// It also joins the zone as a peer: if no other doze binary is serving DNS, this
// process serves it, answering for every peer's names and not only its own. That is
// what lets a machine with nothing but doze-kafka installed still resolve .doze, and
// what lets doze-aws's names work when doze-kafka is the one that happens to be
// running.
//
// # The console is a different transport
//
// The wire protocol is TCP and reaches the name by DNS alone. The console is HTTP,
// so it goes through the shared front door by Host header, which is what makes
// http://kafka.<instance>.doze port-less, the same shape as http://aws.<instance>.doze.

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	names "github.com/doze-dev/doze-names"
)

// zone is doze-kafka's participation in .doze.
type zone struct {
	// own is kafka.<instance>.doze: this instance's identity in the zone, and the
	// only name it claims.
	own *names.Lease
	// extra are the other names this instance holds for its companions (the schema
	// registry), released with it.
	extra []*names.Lease
	srv   *names.Server
	front *names.Ingress
	reg   *names.Registry
	// held records who owns this instance's name when the claim lost, so startup
	// can name the process instead of reporting a nameless failure.
	holder *heldBy
}

// joinZone claims this instance's name and starts serving the zone if nobody else
// is. It never fails: a machine that has not run dns-setup simply has no names,
// which is a smaller thing than refusing to start. openListener is what decides
// whether the result is servable.
func joinZone(ctx context.Context, logger *slog.Logger, instance string) *zone {
	z := &zone{}
	reg := names.Open(names.Home(), "doze-kafka")
	z.reg = reg

	// The instance's own name. This is the one that matters: losing it means there
	// is no address that is THIS instance, so the holder is kept for the startup
	// message rather than only logged.
	if lease, err := reg.Claim(names.Qualified("kafka", instance)); err == nil {
		z.own = lease
	} else if held, ok := names.Held(err); ok {
		z.holder = &heldBy{host: held.Host, pid: held.PID, owner: held.Owner}
		logger.Info("zone: this instance's name is already served",
			"name", held.Host, "held_by_pid", held.PID, "owner", held.Owner)
	} else {
		logger.Debug("zone: could not claim the instance name", "err", err)
	}

	// Info, not Debug: these lines are few, they happen at startup, and they are the
	// only warning that a name is not going to work.
	logf := func(format string, args ...any) { logger.Info(fmt.Sprintf(format, args...)) }
	z.srv = names.Serve(ctx, reg, logf)
	// The shared front door is what makes the console's name port-less. macOS will
	// not let an unprivileged process hold :80 on a specific address, only on the
	// wildcard, so one listener fronts every peer's names by Host header.
	z.front = names.ServeIngress(ctx, reg, logf)
	return z
}

// prepareZone is the zone for a configuration: none under --listen (an address
// replaces the name, and nothing is claimed or checked), and under a name, .doze
// made resolvable first (see ensureNames) and then joined.
func prepareZone(ctx context.Context, cfg config.Config, logger *slog.Logger) (*zone, error) {
	if !cfg.ByName() {
		return nil, nil
	}
	if names.Disabled() {
		// One switch for every doze binary. With names off and no address given there
		// is nothing to listen on, and the answer is the one openListener gives.
		return nil, errNothingToListenOn
	}
	if _, err := ensureNames(liveNameEnv()); err != nil {
		return nil, err
	}
	return joinZone(ctx, logger, cfg.InstanceName()), nil
}

// host is the name this instance claimed, or "".
func (z *zone) host() string {
	if z == nil || z.own == nil {
		return ""
	}
	return z.own.Name.Host
}

// ip is the loopback address that name resolves to, or "".
func (z *zone) ip() string {
	if z == nil || z.own == nil {
		return ""
	}
	return z.own.IP.String()
}

// heldBy says who has the name this instance wanted.
func (z *zone) heldBy() *heldBy {
	if z == nil {
		return nil
	}
	return z.holder
}

// routeConsole publishes where the front door should send this instance's name, so
// http://kafka.<instance>.doze serves the console. It returns the URL to print,
// which is port-less when a front door is up and port-ful when it is not, so the
// line printed at startup is one that actually works.
func (z *zone) routeConsole(logger *slog.Logger, consoleAddr string) string {
	if z == nil || z.own == nil {
		return ""
	}
	// The front door dials the console at the address it actually bound. A wildcard
	// console address is not dialable as written, so aim at loopback.
	host, port, err := net.SplitHostPort(consoleAddr)
	if err != nil {
		logger.Debug("zone: could not parse the console address", "addr", consoleAddr, "err", err)
		return ""
	}
	if host == "" || net.ParseIP(host).IsUnspecified() {
		host = "127.0.0.1"
	}
	if err := z.own.Route(net.JoinHostPort(host, port)); err != nil {
		logger.Debug("zone: could not publish the console route", "err", err)
		return ""
	}

	// Wait briefly for a front door to appear before deciding how to print the URL.
	// Ours binds in a goroutine that races this line, and a peer's entry may land a
	// moment after we start; losing either race advertises a port-ful URL for a name
	// that is in fact port-less, which is how this read on Linux while :80 was bound
	// the whole time. The wait only runs while the answer is still the pessimistic
	// one, so the common case, already fronted, costs nothing.
	name := z.own.Name.Host
	deadline := time.Now().Add(500 * time.Millisecond)
	var u string
	for {
		u = z.reg.URLFor(name)
		if u == "http://"+name || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	return u
}

// close releases the name and stops serving the zone. The registry would prune this
// process's entries anyway once it exits, so this only makes the name available
// again immediately rather than after the next peer's sweep.
func (z *zone) close() {
	if z == nil {
		return
	}
	for _, l := range z.extra {
		_ = l.Release()
	}
	if z.own != nil {
		_ = z.own.Release()
	}
	if z.srv != nil {
		z.srv.Close()
	}
	z.front.Close()
}

// qualifiedHost is the name an instance claims: kafka.<instance>.doze.
func qualifiedHost(instance string) string { return names.Qualified("kafka", instance).Host }

// registered asks the registry what a running instance published for host: the
// address it actually bound for the console (which carries the name's own
// loopback address), and the URL a person should open for it. Both are "" when
// nothing is running.
func registered(host string) (target, consoleURL string) {
	reg := names.Open(names.Home(), "doze-kafka")
	consoleURL = reg.URLFor(host)
	if e, ok := reg.Snapshot()[host]; ok {
		target = e.Target
	}
	return target, consoleURL
}

// runDNSSetup is `doze-kafka dns-setup`. The same command exists in doze and
// doze-aws and does the same work, because the machine setup belongs to the zone
// rather than to any one binary.
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
			// This is the DIAGNOSTIC command: it is read by someone already stuck,
			// who must not be told that a name which does not resolve is "still
			// there" or that an address they never started on works.
			fmt.Print("\nrun `doze-kafka dns-setup` to finish, or serve on an address instead:\n" +
				"  doze-kafka --listen 127.0.0.1:9092\n")
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
