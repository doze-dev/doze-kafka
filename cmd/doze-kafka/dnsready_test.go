package main

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"

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
	in         io.Reader // overrides answer
	out        strings.Builder
}

func (f *fakeNames) env() nameEnv {
	var in io.Reader = strings.NewReader(f.answer)
	if f.in != nil {
		in = f.in
	}
	return nameEnv{
		check:   func() names.Status { return names.Status{} }, // OK() is false for the zero value
		install: func(names.Options) error { f.installed++; return f.installErr },
		script:  func(io.Writer) error { return nil },
		isRoot:  func() bool { return f.root },
		isTTY:   func() bool { return f.tty },
		in:      in,
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

// End of input is not an answer, and neither is a read that failed. Both used to
// be the default, which is yes: a terminal that hung up, or a stdin that only
// looked like one, ran the privileged install with nobody having agreed to it.
func TestNoAnswerIsNeverYes(t *testing.T) {
	for name, in := range map[string]io.Reader{
		"end of input":   strings.NewReader(""),
		"spaces, no end": strings.NewReader("   "),
		"a failed read":  iotest.ErrReader(errors.New("input/output error")),
	} {
		f := &fakeNames{tty: true, in: in}
		state, err := ensureNames(f.env())
		if f.installed != 0 {
			t.Fatalf("%s: installed with no answer given", name)
		}
		if !errors.Is(err, errNamesUnanswered) || state != dnsUnavailable {
			t.Errorf("%s: state=%v err=%v, want dnsUnavailable and errNamesUnanswered", name, state, err)
		}
		if !strings.Contains(f.out.String(), "No answer was given, so nothing has been changed.") {
			t.Errorf("%s: it does not say that nothing happened:\n%s", name, f.out.String())
		}
	}
}

// A typed answer counts whether or not the line was finished; anything that is
// not a yes is a no.
func TestAnswers(t *testing.T) {
	for answer, want := range map[string][2]bool{
		"\n": {true, true}, "y\n": {true, true}, "Yes\n": {true, true}, " y \r\n": {true, true}, "y": {true, true},
		"n\n": {false, true}, "no": {false, true}, "maybe\n": {false, true}, "yy\n": {false, true},
		"": {false, false},
	} {
		yes, answered := answeredYes(strings.NewReader(answer))
		if yes != want[0] || answered != want[1] {
			t.Errorf("answeredYes(%q) = %v, %v; want %v, %v", answer, yes, answered, want[0], want[1])
		}
	}
}

// What a daemon is given for stdin is not a terminal, whatever kind of file it
// is. /dev/null is the one that was got wrong: it is a character device, which
// is all the old check asked.
func TestOnlyATerminalIsATerminal(t *testing.T) {
	null, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	defer null.Close()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	defer w.Close()
	empty, err := os.Create(filepath.Join(t.TempDir(), "empty"))
	if err != nil {
		t.Fatal(err)
	}
	defer empty.Close()
	closed, err := os.Open(os.DevNull)
	if err != nil {
		t.Fatal(err)
	}
	closed.Close()
	for name, f := range map[string]*os.File{
		os.DevNull: null, "a pipe's read end": r, "a pipe's write end": w,
		"an empty file": empty, "a closed file": closed, "no file": nil,
	} {
		if isTerminal(f) {
			t.Errorf("%s was taken for a terminal", name)
		}
	}
}
