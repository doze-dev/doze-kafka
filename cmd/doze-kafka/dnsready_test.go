package main

import (
	"errors"
	"io"
	"strings"
	"testing"

	names "github.com/doze-dev/doze-names"
)

// A stub environment. The whole function is a decision about who is asking, so
// every input that decision reads is injected.
type fakeNames struct {
	installed  int
	installErr error
	root       bool
	tty        bool
	answer     string
	out        strings.Builder
}

func (f *fakeNames) env() nameEnv {
	return nameEnv{
		check:   func() names.Status { return names.Status{} }, // OK() is false for the zero value
		install: func(names.Options) error { f.installed++; return f.installErr },
		script:  func(io.Writer) error { return nil },
		isRoot:  func() bool { return f.root },
		isTTY:   func() bool { return f.tty },
		in:      strings.NewReader(f.answer),
		out:     &f.out,
	}
}

// readyEnv is the same stub with the zone already resolving.
func (f *fakeNames) readyEnv() nameEnv {
	e := f.env()
	e.check = func() names.Status { return okStatus() }
	return e
}

// okStatus is a Status that reports OK: one step, done. It is built rather
// than read from names.Check(), which asks the MACHINE — a test that consults
// whether this laptop happens to have run dns-setup passes or fails for
// reasons that have nothing to do with the code.
//
// The shape it depends on is Status.OK()'s rule, which is "at least one step
// and every one done"; if that changes, this stops compiling or stops meaning
// ready, either of which is a visible failure rather than a silent one.
func okStatus() names.Status {
	return names.Status{Platform: "test", Steps: []names.Step{{Name: "resolver", Done: true}}}
}

// The container case: running as root, nobody to ask, so set it up and carry
// on. No prompt is printed, because there is no one to read it.
func TestRootInstallsWithoutAsking(t *testing.T) {
	f := &fakeNames{root: true}
	state, err := ensureNames(f.env())
	if err != nil {
		t.Fatal(err)
	}
	if state != dnsInstalled {
		t.Errorf("state = %v, want dnsInstalled", state)
	}
	if f.installed != 1 {
		t.Errorf("install called %d times, want 1", f.installed)
	}
	if strings.Contains(f.out.String(), "[Y/n]") {
		t.Errorf("prompted a process with no terminal:\n%s", f.out.String())
	}
}

// A developer at a terminal is asked, and Enter means yes.
func TestTTYOffersAndDefaultsToYes(t *testing.T) {
	for _, answer := range []string{"\n", "y\n", "YES\n"} {
		f := &fakeNames{tty: true, answer: answer}
		state, err := ensureNames(f.env())
		if err != nil {
			t.Fatalf("answer %q: %v", answer, err)
		}
		if state != dnsInstalled || f.installed != 1 {
			t.Errorf("answer %q: state=%v installed=%d", answer, state, f.installed)
		}
	}
}

// And no means no — nothing is changed, and the error says which of the two
// unavailable cases this was.
func TestTTYDeclineChangesNothing(t *testing.T) {
	f := &fakeNames{tty: true, answer: "n\n"}
	state, err := ensureNames(f.env())
	if !errors.Is(err, errNamesDeclined) {
		t.Errorf("err = %v, want errNamesDeclined", err)
	}
	if state != dnsUnavailable {
		t.Errorf("state = %v, want dnsUnavailable", state)
	}
	if f.installed != 0 {
		t.Error("installed despite a decline")
	}
}

// The one that matters most: no root, no terminal — CI, a systemd unit, a
// backgrounded process. It must NOT try to install, because sudo with no TTY
// waits on a password that never comes, and a server that hangs at boot is
// worse than one that errors.
func TestNoTTYNeverInstallsAndNeverHangs(t *testing.T) {
	f := &fakeNames{} // not root, not a terminal
	state, err := ensureNames(f.env())
	if !errors.Is(err, errNamesUnavailable) {
		t.Errorf("err = %v, want errNamesUnavailable", err)
	}
	if state != dnsUnavailable {
		t.Errorf("state = %v, want dnsUnavailable", state)
	}
	if f.installed != 0 {
		t.Fatal("attempted a privileged install with no terminal to prompt on")
	}
	// It has to say what to do, or the operator is left guessing.
	out := f.out.String()
	for _, want := range []string{"dns-setup", "--print", "--listen"} {
		if !strings.Contains(out, want) {
			t.Errorf("the way out does not mention %q:\n%s", want, out)
		}
	}
}

// Already resolving: do nothing at all, whoever is asking.
func TestReadyZoneIsLeftAlone(t *testing.T) {
	for _, f := range []*fakeNames{{}, {root: true}, {tty: true, answer: "n\n"}} {
		state, err := ensureNames(f.readyEnv())
		if err != nil || state != dnsReady {
			t.Errorf("state=%v err=%v, want dnsReady", state, err)
		}
		if f.installed != 0 {
			t.Error("installed over a working zone")
		}
	}
}
