package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The mutation gate's own regression test. It runs the engine, as `mutate
// check` does, against the packages of testdata/fixture:
//
//	vacuous     runs every line and asserts nothing: every mutant must be LIVED
//	asserting   pins every operator: every mutant must be KILLED
//	kinds       one mutant each that does not compile, fails before any test
//	            starts, never returns, or is reached by no test: none is a kill
//	failing     a test fails unmutated: the run must stop, not count
//	brokenmain  the test binary exits before any test: the run must stop
//
// The gate this replaced passed `"-cpu 2"` to go test as one argument, so every
// mutant's run failed at once with "[setup failed]" and was counted as killed:
// the vacuous package read 5 of 5 KILLED. That is the fault this test is for.

func fixture(t *testing.T, dir string, whole bool) (*Run, error) {
	t.Helper()
	if testing.Short() {
		t.Skip("builds and runs the fixture's tests once per mutant")
	}
	root, err := filepath.Abs(filepath.Join("testdata", "fixture"))
	if err != nil {
		t.Fatal(err)
	}
	e := &Engine{Root: root, Dir: dir, Workers: 2, Whole: whole, MinTimeout: 3 * time.Second}
	if testing.Verbose() {
		e.Log = os.Stderr
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	return e.Run(ctx)
}

func modes(t *testing.T, f func(t *testing.T, whole bool)) {
	for _, whole := range []bool{false, true} {
		name := "the tests that reach the mutant"
		if whole {
			name = "the whole suite"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			f(t, whole)
		})
	}
}

func TestSelfTest_ASuiteThatAssertsNothingKillsNothing(t *testing.T) {
	modes(t, func(t *testing.T, whole bool) {
		run, err := fixture(t, "vacuous", whole)
		if err != nil {
			t.Fatal(err)
		}
		if len(run.Mutants) != 7 {
			t.Fatalf("the fixture has 7 mutants, found %d", len(run.Mutants))
		}
		for _, m := range run.Mutants {
			if m.Status != Lived {
				t.Errorf("%s (%s became %s) is %s (%s): nothing asserts, so it must be LIVED", m.At(), m.From, m.To, m.Status, m.Detail)
			}
		}
		// And a gate with no baseline for them fails on every one.
		if c := Compare(UnitBase{}, run); len(c.Failures) != 7 {
			t.Errorf("want 7 failures against an empty baseline, got %d: %v", len(c.Failures), c.Failures)
		}
	})
}

func TestSelfTest_ASuiteThatAssertsEverythingKillsEverything(t *testing.T) {
	modes(t, func(t *testing.T, whole bool) {
		run, err := fixture(t, "asserting", whole)
		if err != nil {
			t.Fatal(err)
		}
		if len(run.Mutants) != 7 {
			t.Fatalf("the fixture has 7 mutants, found %d", len(run.Mutants))
		}
		for _, m := range run.Mutants {
			if m.Status != Killed {
				t.Errorf("%s (%s became %s) is %s (%s): want KILLED", m.At(), m.From, m.To, m.Status, m.Detail)
			}
			// Killed means a test that started said so, and it is named.
			if !strings.HasPrefix(m.Detail, "Test") {
				t.Errorf("%s: a kill names the test that failed, got %q", m.At(), m.Detail)
			}
		}
		if c := Compare(UnitBase{Debt: []Debt{}}, run); len(c.Failures) != 0 {
			t.Errorf("all killed passes an empty baseline: %v", c.Failures)
		}
	})
}

func TestSelfTest_NoBuildFailureSetupFailureOrTimeoutIsAKill(t *testing.T) {
	modes(t, func(t *testing.T, whole bool) {
		run, err := fixture(t, "kinds", whole)
		if err != nil {
			t.Fatal(err)
		}
		want := map[string]Status{
			"kinds/kinds.go:Join:ARITHMETIC_BASE#1":        NotViable,   // "a" - "b" does not compile.
			"kinds/kinds.go:Ready:CONDITIONALS_BOUNDARY#1": Killed,      // by TestReady.
			"kinds/kinds.go:Ready:CONDITIONALS_NEGATION#1": SetupFailed, // TestMain exits before any test.
			"kinds/kinds.go:Spin:CONDITIONALS_NEGATION#1":  Killed,      // by TestSpin.
			"kinds/kinds.go:Spin:INCREMENT_DECREMENT#1":    TimedOut,    // never returns.
			"kinds/kinds.go:Unreached:ARITHMETIC_BASE#1":   NotCovered,
		}
		if len(run.Mutants) != len(want) {
			t.Errorf("want %d mutants, found %d", len(want), len(run.Mutants))
		}
		for _, m := range run.Mutants {
			if m.Status != want[m.Key()] {
				t.Errorf("%s is %s (%s), want %s", m.Key(), m.Status, m.Detail, want[m.Key()])
			}
		}
		// The gate: the timeout and the failed setup fail it; the uncovered one too.
		c := Compare(UnitBase{Debt: []Debt{}}, run)
		joined := strings.Join(c.Failures, "\n")
		if len(c.Failures) != 3 || !strings.Contains(joined, "timedOut") || !strings.Contains(joined, "setupFailed") || !strings.Contains(joined, "NEW notCovered") {
			t.Errorf("want the timeout, the failed setup and the uncovered mutant to fail the gate, got %v", c.Failures)
		}
	})
}

func TestSelfTest_TestsThatFailUnmutatedStopTheRun(t *testing.T) {
	modes(t, func(t *testing.T, whole bool) {
		for dir, say := range map[string]string{"failing": "TestFailsUnmutated", "brokenmain": "setupFailed"} {
			run, err := fixture(t, dir, whole)
			if err == nil {
				t.Fatalf("%s: the unmutated tests fail, and the run reported %d mutants (%d killed): it must stop instead", dir, len(run.Mutants), run.Count(Killed))
			}
			if !strings.Contains(err.Error(), "UNMUTATED") || !strings.Contains(err.Error(), say) {
				t.Errorf("%s: the error must say the unmutated tests do not pass, and how (%q): %v", dir, say, err)
			}
		}
	})
}
