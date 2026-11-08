package console

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

func TestTheCompatibilityLevelRowsMarkTheOneInForce(t *testing.T) {
	for _, now := range []string{"BACKWARD", "NONE", "FULL_TRANSITIVE"} {
		rows := levelRows(now)
		if len(rows) != len(levelOrder) {
			t.Fatalf("%d rows", len(rows))
		}
		for _, r := range rows {
			if want := r.Level == now; r.Now != want || r.On != want {
				t.Errorf("at %s the row %s says now %v on %v, want both %v", now, r.Level, r.Now, r.On, want)
			}
		}
	}
}

// Removing a level that was never set, or a mode that was never set, is not an
// error: the subject already follows the registry's.
func TestRemovingALevelOrModeThatWasNeverSetIsNotAnError(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "plain-value", userV1)
	if rec := req(t, c, "POST", "/_console/schemas/plain-value/config", url.Values{"level": {"default"}, "scope": {"subject"}}); rec.Code != http.StatusSeeOther {
		t.Errorf("remove a level that was never set: %d %s", rec.Code, rec.Body)
	}
	if rec := req(t, c, "POST", "/_console/schemas/plain-value/config", url.Values{"mode": {"default"}, "scope": {"subject"}}); rec.Code != http.StatusSeeOther {
		t.Errorf("remove a mode that was never set: %d %s", rec.Code, rec.Body)
	}
	if rec := req(t, c, "POST", "/_console/schemas/plain-value/mode", url.Values{"mode": {"default"}}); rec.Code != http.StatusSeeOther {
		t.Errorf("remove a mode that was never set (the mode form): %d %s", rec.Code, rec.Body)
	}
}

// The registry's own mode is changed by the form's global scope, and only when it
// differs from what it is.
func TestTheRegistrysModeIsSetOnlyWhenItDiffers(t *testing.T) {
	c, _, _ := newRegistryConsole(t)
	register(t, c, "plain-value", userV1)
	mode := func() string {
		t.Helper()
		m, err := c.reg.Mode(t.Context())
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	if mode() != "READWRITE" {
		t.Fatalf("the registry starts in %s", mode())
	}
	if rec := req(t, c, "POST", "/_console/schemas/plain-value/mode", url.Values{"mode": {"READONLY"}, "scope": {"global"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("set the registry's mode: %d %s", rec.Code, rec.Body)
	}
	if got := mode(); got != "READONLY" {
		t.Errorf("the registry's mode is %s after asking for READONLY", got)
	}
	if rec := req(t, c, "POST", "/_console/schemas/plain-value/mode", url.Values{"mode": {"READWRITE"}, "scope": {"global"}}); rec.Code != http.StatusSeeOther {
		t.Fatalf("set the registry's mode back: %d %s", rec.Code, rec.Body)
	}
	if got := mode(); got != "READWRITE" {
		t.Errorf("the registry's mode is %s after asking for READWRITE", got)
	}
}

// A registry whose check answers that a version would fail, but with no reasons (or a
// finding with none), still gets an answer: "Would fail", without the why.
func TestALevelCheckThatFailsWithoutReasonsStillAnswers(t *testing.T) {
	for name, answer := range map[string]string{
		"no findings":           `{"compatible":false,"level":"FULL","compared":[1,2]}`,
		"a finding, no reasons": `{"compatible":false,"level":"FULL","compared":[1,2],"findings":[{"version":1,"level":"FULL","reasons":[]}]}`,
	} {
		armed := false
		c, _ := newProxiedRegistryConsole(t, func(w http.ResponseWriter, r *http.Request) bool {
			if armed && strings.HasPrefix(r.URL.Path, "/_doze/subjects/s-value/check") {
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(answer))
				return true
			}
			return false
		})
		register(t, c, "s-value", userV1)
		register(t, c, "s-value", userV2)
		armed = true
		noPanic(t, name, func() {
			rec := reqHX(t, c, "POST", "/_console/schemas/s-value/level-check", url.Values{"level": {"FULL"}})
			body := readable(rec.Body.String())
			if rec.Code != 200 || !strings.Contains(body, "Would fail") || strings.Contains(body, "against v1:") {
				t.Errorf("%s: %d %s", name, rec.Code, body)
			}
		})
	}
}
