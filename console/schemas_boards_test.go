package console

import (
	"net/http"
	"net/url"
	"strings"
	"testing"
)

// The Schemas boards' parts that are data: the wire dock read from the registry's own
// request log, and the empty registry (Sc-01).

// client asks the registry as a client would: no X-Doze-Own, so the request is kept.
func client(t *testing.T, method, url, body string) int {
	t.Helper()
	req, _ := http.NewRequest(method, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

func TestTheSchemasDockIsTheRegistrysOwnLog(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	if s := client(t, "GET", ts.URL+"/subjects", ""); s != 200 {
		t.Fatalf("GET /subjects: %d", s)
	}
	if s := client(t, "POST", ts.URL+"/subjects/orders-value/versions", `{"schema":"\"string\""}`); s != 200 {
		t.Fatalf("register: %d", s)
	}
	if s := client(t, "POST", ts.URL+"/subjects/other-value/versions", `{"schema":"\"long\""}`); s != 200 {
		t.Fatalf("register: %d", s)
	}
	client(t, "GET", ts.URL+"/subjects/orders-value/versions/latest?deleted=true", "")

	home := req(t, c, "GET", "/_console/schemas", nil).Body.String()
	dock := between(home, `id="wire-dock"`, `</section>`)
	for _, want := range []string{"GET", "/subjects", "POST", "/subjects/orders-value/versions", "/subjects/other-value/versions", "200",
		"/subjects/orders-value/versions/latest?deleted=true"} {
		if !strings.Contains(dock, want) {
			t.Errorf("the home's dock does not show %q:\n%s", want, readable(dock))
		}
	}
	// The console's own reads (the page just drawn asked for every subject) are not
	// clients' requests, and the registry does not keep them.
	if n := strings.Count(dock, `data-reg-row`); n != 4 {
		t.Errorf("the home's dock has %d rows, want the 4 the client sent:\n%s", n, readable(dock))
	}
	if strings.Contains(dock, "not recorded") {
		t.Error("the dock still says the registry's requests are not recorded")
	}

	subj := between(req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String(), `id="wire-dock"`, `</section>`)
	if !strings.Contains(subj, "/subjects/orders-value/versions") || strings.Contains(subj, "other-value") {
		t.Errorf("a subject's dock shows other subjects' requests or not its own:\n%s", readable(subj))
	}

	// What a person does through the console is a request like any client's: the
	// register is kept, so the dock shows it (Sc-04 draws the console's own 409).
	r := req(t, c, "POST", "/_console/new/schema", url.Values{"subject": {"orders-value"}, "schema": {`"int"`}, "type": {"AVRO"}})
	if r.Code >= 300 && r.Code != 303 {
		t.Logf("register through the console answered %d", r.Code)
	}
	after := between(req(t, c, "GET", "/_console/schemas/orders-value", nil).Body.String(), `id="wire-dock"`, `</section>`)
	if !strings.Contains(after, "409") {
		t.Errorf("the console's refused register is not in the dock:\n%s", readable(after))
	}
}

func TestSchemasNoneIsSc01(t *testing.T) {
	c, _, ts := newRegistryConsole(t)
	page := readable(req(t, c, "GET", "/_console/schemas", nil).Body.String())
	host := strings.TrimPrefix(ts.URL, "http://")
	for _, want := range []string{
		"No subjects",
		"Nothing is registered yet. Subjects appear here the moment a serializer or you register one.",
		"The built-in registry at " + host + " is running and empty.",
		"Nothing exists yet. This is not an error.",
		"How a stock serializer registers on first produce",
		"schema.registry.url=" + ts.URL,
		"value.serializer=io.confluent.kafka.serializers.KafkaAvroSerializer",
		"# auto.register.schemas=true is the default",
		"(TopicNameStrategy)",
		"Use a different strategy",
		"Or register by hand",
	} {
		if !strings.Contains(page, want) {
			t.Errorf("the empty Schemas page does not say %q", want)
		}
	}
	// No SASL on this broker: the producer needs no credentials, so none are shown.
	if strings.Contains(page, "basic.auth") {
		t.Error("the producer's lines ask for credentials on a broker without SASL")
	}
}
