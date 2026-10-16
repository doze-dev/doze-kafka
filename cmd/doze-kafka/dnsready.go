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
//	a terminal     offer it, and run one sudo. A developer at a terminal can
//	               answer, and names.Install does the whole thing under a
//	               single prompt. "A terminal" means stdin AND the stream the
//	               question is written to are both terminals: the question has
//	               to be seen and the answer has to be typed.
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
//
// # Yes is something a person typed
//
// The default answer is yes, and only a pressed Enter selects it. End of input
// and a failed read are not answers: they are what a stdin that nobody is at
// looks like (/dev/null, a closed descriptor, a hung-up terminal), and they
// used to be read as "the default", which ran sudo for a process started by
// systemd, launchd, nohup or `docker run` without -i. With passwordless sudo
// that rewrote the machine's DNS as a side effect of starting a broker.

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	names "github.com/doze-dev/doze-names"
	"golang.org/x/term"
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
		isTTY:   promptIsInteractive,
		in:      os.Stdin,
		out:     os.Stderr,
	}
}

// promptIsInteractive reports whether a question can be asked here: stdin and
// the stream the question is written to (stderr) are both terminals.
func promptIsInteractive() bool { return isTerminal(os.Stdin) && isTerminal(os.Stderr) }

// isTerminal reports whether f is a terminal, by asking the operating system
// (isatty on Unix, the console mode on Windows).
//
// It used to test the character-device bit, and /dev/null is a character
// device: `doze-kafka </dev/null` was taken for a person at a keyboard.
func isTerminal(f *os.File) bool {
	if f == nil {
		return false
	}
	fd := f.Fd()
	if fd == ^uintptr(0) { // a closed file.
		return false
	}
	return term.IsTerminal(int(fd))
}

// answeredYes reads one line from in and reports whether it says yes.
//
// Yes is "y", "yes", or Enter on its own (the default the prompt shows). The
// second result is false when nothing was answered at all: the input ended, or
// could not be read, before anything was typed. That is never a yes, and it is
// not a "no" anyone typed either, so the caller reports it in its own words.
func answeredYes(in io.Reader) (yes, answered bool) {
	line, err := bufio.NewReader(in).ReadString('\n')
	word := strings.ToLower(strings.TrimSpace(line))
	if err != nil && word == "" {
		return false, false
	}
	// An empty word here is a completed line: Enter.
	return word == "" || word == "y" || word == "yes", true
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
		yes, answered := answeredYes(e.in)
		if !answered {
			fmt.Fprintln(e.out) // the cursor is still on the question's line.
			fmt.Fprintln(e.out, "No answer was given, so nothing has been changed.")
			return dnsUnavailable, errNamesUnanswered
		}
		if !yes {
			return dnsUnavailable, errNamesDeclined
		}
		if err := e.install(names.Options{Out: e.out}); err != nil {
			return dnsUnavailable, fmt.Errorf("setting up .doze: %w", err)
		}
		return dnsInstalled, nil
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
	errNamesUnanswered  = fmt.Errorf("doze-kafka: .doze setup was offered and the input ended before an answer")
	errNamesUnavailable = fmt.Errorf("doze-kafka: .doze does not resolve and cannot be set up without a terminal")
)
