package conformance

import "testing"

// A scenario that is evidence only (Scenario.NoReplay) is not compared with doze-kafka's
// answers, because it defines a CLIENT_METRICS resource this broker does not have. It is
// still a script, and a script that was edited after it was recorded, or that no longer
// runs, is not what the recording says it is: so each is run against doze-kafka and its
// steps are held to the recording's, in order, by label, API and version.
func TestEvidenceOnlyScenariosAreTheScriptsThatWereRecorded(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a broker per scenario")
	}
	n := 0
	for _, s := range Scenarios {
		if s.NoReplay == "" {
			continue
		}
		n++
		t.Run(s.Name, func(t *testing.T) {
			t.Parallel()
			rec, err := Load(recordingsDir, s.Name)
			if err != nil {
				t.Fatalf("no recording: %v", err)
			}
			target, err := StartDozeFor(t.TempDir(), s)
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			steps, err := Run(s, target)
			if err != nil {
				t.Fatal(err)
			}
			if len(steps) != len(rec.Steps) {
				t.Fatalf("the script has %d steps, the recording %d: record it again", len(steps), len(rec.Steps))
			}
			for i, want := range rec.Steps {
				got := steps[i]
				if got.Label != want.Label || got.API != want.API || got.Version != want.Version || got.Conn != want.Conn {
					t.Errorf("step %d is %s %s v%d on %q, the recording has %s %s v%d on %q: record it again",
						i, got.Label, got.API, got.Version, got.Conn, want.Label, want.API, want.Version, want.Conn)
				}
			}
		})
	}
	if n == 0 {
		t.Error("no evidence-only scenario: the test is checking nothing")
	}
}

func TestAKnownDifferenceNamesAStepByPrefixWhenItEndsInAStar(t *testing.T) {
	k := Known{Step: "subscribe*"}
	for step, want := range map[string]bool{"subscribe": true, "subscribe-second": true, "unsubscribe": false} {
		if got := k.matches(step); got != want {
			t.Errorf("matches(%q) = %v, want %v", step, got, want)
		}
	}
	if !(Known{Step: "push"}).matches("push") || (Known{Step: "push"}).matches("push-again") {
		t.Error("a name without a star must match exactly")
	}
}
