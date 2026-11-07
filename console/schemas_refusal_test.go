package console

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// A registry refusal reaches the page as one (A2): its kind, its source and the
// registry's own code as its name, the registry's message, and the rule for that code;
// an incompatible schema is 409 and a read-only subject 42205, not a bare 400. (SCH-15)
func TestRefusalsKeepTheRegistryCode(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	register(t, c, "s", userV1)
	bad := `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":"string"}]}`
	rec := req(t, c, "POST", "/_console/schemas/s/register", url.Values{"schema": {bad}})
	body := rec.Body.String()
	if !strings.Contains(body, `data-refusal-kind="refused" data-refusal-source="registry" data-refusal-name="409"`) ||
		!strings.Contains(body, "READER_FIELD_MISSING_DEFAULT_VALUE") || !strings.Contains(readable(body), "not compatible with the versions") {
		t.Errorf("registering an incompatible schema: %d\n%s", rec.Code, readable(body))
	}

	put, _ := http.NewRequest("PUT", ts.URL+"/mode/s", strings.NewReader(`{"mode":"READONLY"}`))
	put.Header.Set("Content-Type", "application/json")
	if resp, err := http.DefaultClient.Do(put); err != nil || resp.StatusCode != 200 {
		t.Fatalf("set READONLY: %v %v", resp, err)
	}
	rec = req(t, c, "POST", "/_console/schemas/s/register", url.Values{"schema": {userV2}})
	body = rec.Body.String()
	if !strings.Contains(body, `data-refusal-name="42205"`) || !strings.Contains(body, "read-only mode") {
		t.Errorf("registering in a READONLY subject: %d\n%s", rec.Code, readable(body))
	}
}
