package main

// Where doze-kafka answers.
//
// # The name is the instance
//
// An instance is kafka.<instance>.doze, where the instance defaults to the project
// directory, and an address is the opt-in. This is the arrangement doze-aws has, and
// it is the same here on purpose: one experience across the doze tools.
//
// Each instance gets its own loopback address from doze-names and binds the same
// port on it, so "the name is the address" does not mean one broker per machine:
// kafka.harbour.doze:9092 and kafka.atlas.doze:9092 are two brokers on port 9092.
// A name carries what an address cannot (which project this is) and survives the
// port moving, and Metadata can send a client back to it.
//
// # --listen is not a fallback, and not an addition
//
// It is for the case a name genuinely cannot serve: a sibling container reaching
// this one over a compose network, where the address comes from Docker and .doze is
// not in play. It REPLACES the name rather than adding to it. An instance that
// answered on both would hand clients two different addresses depending on which one
// they bootstrapped through, and would have to pick one of them to advertise.
//
// # What this costs
//
// 127.0.0.1:9092 is no longer bound by default. `doze-kafka --listen 127.0.0.1:9092`
// restores it exactly, for anyone who wants it back.

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
)

// kafkaPort is the port the broker binds on the name's own address, and the one
// clients type. Unlike doze-aws's, it has no fallback: a port that moved would
// be a bootstrap address nobody wrote down.
const kafkaPort = 9092

// consolePort is the port the console binds on the name's own address, beside
// the broker's. It is not the port anyone types: the front door serves the name
// port-less.
const consolePort = 9093

// consoleAddrFor is where the console binds in name mode: the name's own address.
func consoleAddrFor(ip string) string { return net.JoinHostPort(ip, strconv.Itoa(consolePort)) }

// heldBy says who has the name this instance wanted.
type heldBy struct {
	host  string
	pid   int
	owner string
}

// lookupFunc is net.Resolver.LookupHost, injected so the comparison below can be
// tested without arranging DNS.
type lookupFunc func(context.Context, string) ([]string, error)

// resolveWait bounds the lookup. Generous for a local resolver and short enough
// that nobody notices it on a machine where the name is simply absent.
const resolveWait = 750 * time.Millisecond

// resolvesHere reports whether host resolves, on this machine, to wantIP.
//
// Both halves matter and the second is the one that is easy to skip. A container
// inherits its host's DNS (Docker Desktop and Colima both forward to it), so an
// answer can come from another machine's registry and name an address that means
// something else here: the name answers, and only works by the coincidence of two
// instances having been handed the same address.
func resolvesHere(lookup lookupFunc, host, wantIP string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), resolveWait)
	defer cancel()
	addrs, err := lookup(ctx, host)
	return err == nil && slices.Contains(addrs, wantIP)
}

// binding is the broker's listener and how it is told to the person.
type binding struct {
	ln net.Listener
	// what is "name" or "address", for the log line.
	what string
	// bootstrap is what to tell a client to connect to: the name, or, where the
	// name does not resolve here, the address it is actually on.
	bootstrap string
	// host is what Metadata sends clients back to (bootstrap without the port).
	host string
}

// openListener binds what the configuration asks for.
//
// Exactly one of the two arms runs. A nil zone with no --listen means names are not
// available (see ensureNames); a configuration with --listen never has a zone, which
// is the caller's way of saying names are not in play at all.
func openListener(cfg config.Config, z *zone, logger interface{ Info(string, ...any) }, lookup lookupFunc) (binding, error) {
	// --listen: this address, and nothing else. The error from net.Listen is
	// returned verbatim rather than reworded: "address already in use" on an
	// address the caller typed needs no interpretation from us.
	if !cfg.ByName() {
		ln, err := net.Listen("tcp", cfg.ListenAddr)
		if err != nil {
			return binding{}, err
		}
		host, port, _ := net.SplitHostPort(ln.Addr().String())
		advertised := cfg.AdvertisedHost
		if advertised == "" {
			advertised = dialableHost(host)
		}
		return binding{ln: ln, what: "address", host: advertised, bootstrap: net.JoinHostPort(advertised, port)}, nil
	}

	if z.host() == "" {
		// Losing the name to another live instance is a different problem from having
		// no names at all, and it has a different answer, so say which one happened.
		if h := z.heldBy(); h != nil {
			return binding{}, fmt.Errorf(
				"doze-kafka: %s is already served by pid %d (%s).\n"+
					"  doze-kafka --name <other>    run this instance under its own name\n"+
					"  doze-kafka --listen host:port serve on an address instead",
				h.host, h.pid, h.owner)
		}
		return binding{}, errNothingToListenOn
	}

	addr := net.JoinHostPort(z.ip(), strconv.Itoa(kafkaPort))
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		// Both causes leave the name resolving to an address that answers nothing, so
		// say which one it is: the remedies are opposites.
		hint := "run `doze-kafka dns-setup` once to alias the loopback pool"
		if errors.Is(err, syscall.EADDRINUSE) {
			hint = "something else already holds " + addr + "; free it, or name this instance differently (--name)"
		}
		return binding{}, fmt.Errorf("doze-kafka: cannot listen on %s for %s: %w\n  %s", addr, z.host(), err, hint)
	}

	// The name is what to tell people, unless it does not resolve HERE: the setup
	// knows a lot about whether names OUGHT to resolve, and every one of those facts is
	// a proxy for the thing that matters. Resolving it answers the question directly.
	b := binding{ln: ln, what: "name", host: z.host(), bootstrap: net.JoinHostPort(z.host(), strconv.Itoa(kafkaPort))}
	if !resolvesHere(lookup, z.host(), z.ip()) {
		b.host, b.bootstrap = z.ip(), addr
		logger.Info("the instance name does not resolve here, so the address is what to use",
			"name", z.host(), "address", addr, "fix", "doze-kafka doctor")
	}
	if cfg.AdvertisedHost != "" {
		b.host, b.bootstrap = cfg.AdvertisedHost, net.JoinHostPort(cfg.AdvertisedHost, strconv.Itoa(kafkaPort))
	}
	return b, nil
}

// errNothingToListenOn is "this machine offered no name and no --listen was set": a
// statement about the machine, not a failure of openListener. A sentinel because a
// test needs to tell it apart from a real error: on a machine without .doze there is
// nothing to assert anything about, and a CI runner is exactly that machine.
var errNothingToListenOn = errors.New(
	"doze-kafka: nothing to listen on: .doze gave no name and no --listen was set.\n" +
		"  doze-kafka doctor             show what is missing\n" +
		"  doze-kafka --listen host:port serve on an address instead")

// dialableHost turns a bind host into one a client can dial: a wildcard or empty
// host becomes loopback, since "0.0.0.0" is an address to listen on rather than one
// to connect to. (An explicitly advertised host, or "localhost" for a wildcard, is
// what Metadata says; this is for the line printed beside it.)
func dialableHost(host string) string {
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		return "localhost"
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.String()
	}
	return host
}

// instanceAddress is where a configuration says this instance answers: the broker's
// bootstrap address and the console's URL.
//
// It exists so the commands that do NOT run the server (env, doctor) give the same
// answer the server does. The registry is asked rather than DNS, and rather than
// assuming the port: a running instance publishes the address it actually bound, and
// URLFor gives the port-less form when the shared front door is up. With nothing
// running there is nothing to ask, so the answer is the name the instance WILL claim,
// which is the useful thing to print.
func instanceAddress(cfg config.Config) (bootstrap, console string) {
	if !cfg.ByName() {
		host, port, err := net.SplitHostPort(cfg.ListenAddr)
		if err != nil {
			return cfg.ListenAddr, ""
		}
		advertised := cfg.AdvertisedHost
		if advertised == "" {
			advertised = dialableHost(host)
		}
		cc := ""
		if cfg.Console && cfg.ConsoleAddr != "" {
			cc = "http://" + cfg.ConsoleAddr
		}
		return net.JoinHostPort(advertised, port), cc
	}
	host := qualifiedHost(cfg.InstanceName())
	bootstrap = net.JoinHostPort(host, strconv.Itoa(kafkaPort))
	target, u := registered(host)
	if u != "" {
		console = u
	}
	if ip, _, err := net.SplitHostPort(target); err == nil && !resolvesHere(net.DefaultResolver.LookupHost, host, ip) {
		// The same question the banner asks, asked by a different process: `env` runs
		// on its own, so it cannot look at the server's listener, but the registry
		// records where the instance is. Without this the banner correctly printed an
		// address while the command it points at exported the NAME, and
		// `eval "$(doze-kafka env)"` in a container pointed every client at a host
		// name that does not resolve.
		bootstrap = net.JoinHostPort(ip, strconv.Itoa(kafkaPort))
		console = "http://" + target
	}
	if cfg.AdvertisedHost != "" {
		bootstrap = net.JoinHostPort(cfg.AdvertisedHost, strconv.Itoa(kafkaPort))
	}
	return bootstrap, console
}
