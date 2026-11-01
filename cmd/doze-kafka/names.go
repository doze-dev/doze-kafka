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
	"net/url"
	"os"
	"strconv"
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
	// reach stands in for names.Reachable in a test; nil is the real one.
	reach reachFunc
}

// reachFunc is names.Reachable: does something accept a TCP connection at host:port.
type reachFunc func(host string, port int) bool

// nameReachable is the check outside a zone (`env` runs as its own process and has
// none). A variable so a test can stand in a machine of its choosing.
var nameReachable reachFunc = names.Reachable

// machineLookup is how this machine resolves a name, for the same commands.
var machineLookup lookupFunc = net.DefaultResolver.LookupHost

// reachable asks whether host:port answers, through the zone's stand-in if it has one.
func (z *zone) reachable(host string, port int) bool {
	if z != nil && z.reach != nil {
		return z.reach(host, port)
	}
	return nameReachable(host, port)
}

// whyNotByName says why this instance's name must not be told to anyone, or "" when
// it may be. Two questions, in the order that costs least:
//
// Does it resolve HERE, to this instance's address? The setup knows a lot about
// whether names ought to resolve, and every one of those facts is a proxy for the
// thing that matters. Resolving it answers the question directly.
//
// Does it answer? Resolving is necessary and not enough: on a Mac the name's address
// is carried to the listener by the zone's network daemon, and a name can go on
// resolving after the daemon has gone. names.Reachable dials it, which is the whole
// of what a client is about to do. The listener is already bound when this is asked,
// so the dial completes without anything accepting it.
func (z *zone) whyNotByName(lookup lookupFunc, port int) string {
	if !resolvesHere(lookup, z.host(), z.ip()) {
		return "does not resolve here"
	}
	if !z.reachable(z.host(), port) {
		return "resolves here but nothing answers at it"
	}
	return ""
}

// byNameOrAddress is the URL to print for something this instance serves by name:
// byName when something answers there, and otherwise the address it bound, with the
// reason said once. A URL that is not by name (no zone, or an address already) is
// returned as it is.
func (z *zone) byNameOrAddress(logger *slog.Logger, what, byName, addr string) string {
	if answersAt(z.reachable, byName) {
		return byName
	}
	byAddr := httpURL(addr)
	// Info, as the broker's own line is: it happens once at startup, and it is the
	// only explanation of why the summary shows an address where a name was expected.
	logger.Info("the "+what+"'s name does not answer here, so its address is what to open",
		"name", byName, "address", byAddr, "fix", "doze-kafka doctor")
	return byAddr
}

// answersAt reports whether a by-name URL answers: its host on its port, 80 when the
// URL has none (the shared front door). A URL outside the zone is not this check's
// business and is said to answer.
func answersAt(reach reachFunc, rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || !names.InZone(u.Hostname()) {
		return true
	}
	port := 80
	if p := u.Port(); p != "" {
		if port, err = strconv.Atoi(p); err != nil {
			return false
		}
	}
	return reach(u.Hostname(), port)
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
	if !cfg.Zone || names.Disabled() {
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
// which is port-less when a front door is up and port-ful when it is not, and the
// address the console bound when the name does not answer at all (see
// byNameOrAddress), so the line printed at startup is one that actually works.
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
	return z.byNameOrAddress(logger, "console", u, net.JoinHostPort(host, port))
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
// address the name resolves to, the address it actually bound for what the name
// serves over HTTP, and the URL a person should open for it. All are "" when
// nothing is running.
//
// The name's address and the bound one are not always the same address: the schema
// registry binds beside the broker, on the instance's address, and is reached at its
// own name through the front door. So "does the name resolve here" is asked of ip,
// never of the target's host.
func registered(host string) (ip, target, openURL string) {
	reg := names.Open(names.Home(), "doze-kafka")
	openURL = reg.URLFor(host)
	if e, ok := reg.Snapshot()[host]; ok {
		ip, target = e.IP, e.Target
	}
	return ip, target, openURL
}

// publishedURL is the URL to print, from a process that is not the server, for what a
// running instance serves at host: the by-name one when the name resolves to the
// address the zone gave it and answers, and otherwise the address it bound. "" when
// nothing is running.
func publishedURL(host string) string {
	ip, target, u := registered(host)
	if u == "" {
		return ""
	}
	if !resolvesHere(machineLookup, host, ip) || !answersAt(nameReachable, u) {
		return httpURL(target)
	}
	return u
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

// zoneListen opens a TCP listener for the registry or the console. On this
// instance's own name that goes through doze-names, because on macOS a name's
// address is translated by the zone's network daemon and cannot be bound; any
// other address (--console-addr, --schema-registry-addr) is listened on as it is.
func zoneListen(addr string) (net.Listener, error) {
	return names.Open(names.Home(), "doze-kafka").Listen(addr)
}
