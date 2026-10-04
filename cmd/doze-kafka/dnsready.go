//go:build !windows

package main

// Making sure .doze resolves before anything depends on it.
//
// # The three paths, and why they are different
//
// Setting up the zone is privileged: it writes /etc/resolver/doze on macOS, or
// a systemd unit and an /etc/hosts block on Linux. So "just do it" is not
// available, and the right move depends on who is asking.
//
//	already root   install directly. This is the ordinary CONTAINER case — a
//	               process running as root inside its own filesystem, where
//	               writing /etc/hosts is unremarkable and there is nobody to
//	               ask. No prompt, no TTY needed.
//
//	a TTY          offer it, and run one sudo. A developer at a terminal can
//	               answer, and names.Install does the whole thing under a
//	               single prompt.
//
//	neither        print the command and exit. This is CI, a systemd unit, a
//	               background `doze-kafka &`.
//
// # Why the last one exits rather than prompting
//
// sudo with no TTY waits for a password that will never arrive. A server that
// HANGS at boot is worse than one that errors: CI sits there until the job
// times out, with no output explaining why. And passwordless sudo is the other
// trap — it would succeed silently, rewriting a build machine's DNS as a side
// effect of starting a test fixture.
//
// An unexpected password prompt from a long-running server is also the shape
// users are rightly trained to distrust. From `doze-kafka dns-setup` it is
// unremarkable; from "start the server" it is not.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	names "github.com/doze-dev/doze-names"
)

// dnsState is what a startup check found, so the caller can report it.
type dnsState int

const (
	dnsReady       dnsState = iota // the zone resolves
	dnsInstalled                   // it did not, and we set it up
	dnsUnavailable                 // it does not, and we could not
)

// ensureNames makes .doze resolvable, or explains why it is not.
//
// in and out are injected so this is testable without a terminal; isTTY and
// isRoot likewise, because the whole point of the function is which of those
// it is looking at.
type nameEnv struct {
	check   func() names.Status
	install func(names.Options) error
	script  func(io.Writer) error // print the privileged script rather than run it
	isRoot  func() bool
	isTTY   func() bool
	in      io.Reader
	out     io.Writer
}

func liveNameEnv() nameEnv {
	return nameEnv{
		check:   names.Check,
		install: names.Install,
		script:  func(w io.Writer) error { return names.Install(names.Options{Out: w, Print: true}) },
		isRoot:  func() bool { return os.Geteuid() == 0 },
		isTTY:   stdinIsTTY,
		in:      os.Stdin,
		out:     os.Stderr,
	}
}

// stdinIsTTY reports whether stdin is a terminal, using the character-device
// bit rather than a terminal library — this is the only thing here that needs
// the answer, and it does not need to be clever about it.
func stdinIsTTY() bool {
	fi, err := os.Stdin.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func ensureNames(e nameEnv) (dnsState, error) {
	if e.check().OK() {
		return dnsReady, nil
	}

	// Root: no one to ask, and nothing to ask for. This is a container.
	if e.isRoot() {
		if err := e.install(names.Options{Out: e.out}); err != nil {
			return dnsUnavailable, fmt.Errorf("setting up .doze: %w", err)
		}
		return dnsInstalled, nil
	}

	// A terminal: offer it once.
	if e.isTTY() {
		fmt.Fprintln(e.out, "doze-kafka addresses itself by name, and .doze does not resolve on this machine yet.")
		fmt.Fprintln(e.out, "Setting it up needs sudo once — per machine, not per project.")
		fmt.Fprint(e.out, "Set it up now? [Y/n] ")
		answer, _ := bufio.NewReader(e.in).ReadString('\n')
		switch strings.ToLower(strings.TrimSpace(answer)) {
		case "", "y", "yes":
			if err := e.install(names.Options{Out: e.out}); err != nil {
				return dnsUnavailable, fmt.Errorf("setting up .doze: %w", err)
			}
			return dnsInstalled, nil
		}
		return dnsUnavailable, errNamesDeclined
	}

	// Neither. Say what to run and stop — never wait on a password nobody can
	// type.
	fmt.Fprintln(e.out, "doze-kafka addresses itself by name, and .doze does not resolve on this machine.")
	fmt.Fprintln(e.out, "There is no terminal to ask on, so nothing has been changed.")
	fmt.Fprintln(e.out)
	fmt.Fprintln(e.out, "  doze-kafka dns-setup          set it up (one sudo, once per machine)")
	fmt.Fprintln(e.out, "  doze-kafka dns-setup --print  print the script instead, to run yourself")
	fmt.Fprintln(e.out, "  doze-kafka --listen host:port serve on an address instead of a name")
	return dnsUnavailable, errNamesUnavailable
}

// Sentinel errors so the caller can tell "the user said no" from "there was
// nobody to ask", and report each in its own words.
var (
	errNamesDeclined    = fmt.Errorf("doze-kafka: .doze setup declined")
	errNamesUnavailable = fmt.Errorf("doze-kafka: .doze does not resolve and cannot be set up without a terminal")
)
