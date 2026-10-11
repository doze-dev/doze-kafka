package console

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// No identity in the code (plan A8): the console runs under a prefix that is not the
// default, on a listener that picked its own port, over a data directory with a
// space in it (newTestConsoleAddr does the last two for every test). Every address
// a page offers must be under the prefix it is mounted at.
func TestEveryAddressAPageOffersIsUnderThePrefix(t *testing.T) {
	const prefix = "/ops/kafka-ui"
	src := sampleTraffic()
	c, addr, _ := newRegistryConsole(t, func(o *Options) { o.Prefix = prefix; o.Traffic = src })
	post := func(path string, kv ...string) {
		form := map[string][]string{}
		for i := 0; i+1 < len(kv); i += 2 {
			form[kv[i]] = []string{kv[i+1]}
		}
		if rec := serveRaw(t, c, "POST", prefix+path, form); rec >= 400 {
			t.Fatalf("POST %s: %d", path, rec)
		}
	}
	post("/new/topic", "name", "orders", "partitions", "2")
	post("/new/schema", "subject", "orders-value", "schema", userV1)
	commitFor(t, addr, "billing", "orders", map[int32]int64{0: 0})

	attr := regexp.MustCompile(`\s(?:href|action|hx-get|hx-post|src|data-live|data-msg-base)="(/[^"]*)"`)
	pages := []string{"", "/topics", "/topics/orders", "/new/topic", "/groups", "/groups/billing", "/cluster", "/access",
		"/schemas", "/schemas/orders-value", "/new/schema", "/traffic", "/topics/orders/message?partition=0&offset=0"}
	checked := 0
	for _, p := range pages {
		body := serveRawBody(t, c, "GET", prefix+p)
		for _, m := range attr.FindAllStringSubmatch(body, -1) {
			checked++
			if !strings.HasPrefix(m[1], prefix+"/") && m[1] != prefix {
				t.Errorf("%s offers %q, which is not under %s", p, m[1], prefix)
			}
		}
	}
	if checked < 100 {
		t.Errorf("only %d addresses were checked", checked)
	}
	// The default prefix is not there at all.
	if code := serveRaw(t, c, "GET", "/_console/topics", nil); code != 404 {
		t.Errorf("/_console answered %d with the console mounted elsewhere", code)
	}
	// The static files, the palette and the live polls are under it too.
	for _, p := range []string{"/static/app.css", "/api/palette", "/api/counts"} {
		if code := serveRaw(t, c, "GET", prefix+p, nil); code != 200 {
			t.Errorf("%s answered %d", p, code)
		}
	}
}

// Nothing in the console's own text names an address, a port or a host. The few
// places that must (the Host guard's rules, the broker address a person typed) are
// listed with their reason.
func TestTheConsoleNamesNoAddressOfItsOwn(t *testing.T) {
	allowed := map[string]string{
		"hostguard.go":          "the DNS-rebinding guard lists the names it always answers to",
		"hostguard_test.go":     "",
		"console.go":            "doc comments that give an example address",
		"backend.go":            "a placeholder seed the dialer ignores",
		"backend_groups.go":     "a comment about a client host Kafka prints",
		"static/shell.js":       "comments about secure origins",
		"static/alpine.min.js":  "vendored",
		"static/htmx.min.js":    "vendored",
		"static/cm6.min.js":     "vendored",
		"templates/layout.html": "",
	}
	re := regexp.MustCompile(`9092|9093|127\.0\.0\.1|localhost|\.doze\b`)
	var files []string
	for _, glob := range []string{"*.go", "templates/*.html", "static/*.js", "static/*.css"} {
		m, _ := filepath.Glob(glob)
		files = append(files, m...)
	}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		if _, ok := allowed[f]; ok {
			continue
		}
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if loc := re.FindIndex(b); loc != nil {
			line := strings.Count(string(b[:loc[0]]), "\n") + 1
			t.Errorf("%s:%d names an address of its own: %q", f, line, re.Find(b))
		}
	}
	// layout.html is allowed to mention none either; it is listed only to be explicit.
	b, _ := os.ReadFile("templates/layout.html")
	if re.Match(b) {
		t.Error("layout.html names an address: the pill shows the broker's, or nothing")
	}
}
