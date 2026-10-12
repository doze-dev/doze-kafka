package console

import (
	"html"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// The Older, Newer and Latest links of the committed-only view keep the view. The
// "&committed=1" was a string written into the href by the template, which escaped
// it into the offset before it ("from=0%26committed%3d1"): the link lost the view
// and, with it, a readable offset.
func TestPagingLinksKeepTheCommittedOnlyView(t *testing.T) {
	c := newTestConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"big"}, "partitions": {"1"}})
	for i := range 70 { // more than one window
		req(t, c, "POST", "/_console/topics/big/produce", url.Values{"partition": {"0"}, "value": {"m" + itoa(i)}, "acks": {"1"}})
	}
	links := func(body string) map[string]url.Values {
		out := map[string]url.Values{}
		for _, chunk := range strings.Split(body, `<a class="pagelink" href="`)[1:] {
			href, rest, _ := strings.Cut(chunk, `"`)
			label := after(rest, ">", "<")
			u, err := url.Parse(html.UnescapeString(href))
			if err != nil {
				t.Fatalf("%q: %v", href, err)
			}
			out[strings.Trim(label, "←→ ")] = u.Query()
		}
		return out
	}
	tail := links(req(t, c, "GET", "/_console/topics/big?committed=1", nil).Body.String())
	if q := tail["Older"]; q.Get("committed") != "1" || q.Get("from") != "0" || q.Get("partition") != "0" {
		t.Errorf("Older of the committed-only tail is read as %v", q)
	}
	back := links(req(t, c, "GET", "/_console/topics/big?partition=0&from=0&committed=1", nil).Body.String())
	if q := back["Newer"]; q.Get("committed") != "1" || q.Get("from") != "60" {
		t.Errorf("Newer of the committed-only history is read as %v", q)
	}
	if q := back["Latest"]; q.Get("committed") != "1" || q.Get("partition") != "0" {
		t.Errorf("Latest of the committed-only history is read as %v", q)
	}
	// Without the view the links do not gain it.
	if q := links(req(t, c, "GET", "/_console/topics/big", nil).Body.String())["Older"]; q.Has("committed") || q.Get("from") != "0" {
		t.Errorf("Older of the plain tail is read as %v", q)
	}
}

// No template writes a query string it was handed into an address. html/template
// escapes a value by where it stands: after a "?" in href, src, action or an hx-
// address a value is one query VALUE, so a string that is itself "k=v" or "&k=v"
// comes out as "k%3dv" and the server reads no such parameter (the Traffic client
// link, TRF-9; the dock's Traffic link; the committed-only paging links). The rule:
// in those attributes an action after "?" or "&" is preceded by "name=", so it is a
// value; a whole query is built in Go from url.Values and handed over as one
// address.
func TestNoTemplateInterpolatesAQueryStringIntoAnAddress(t *testing.T) {
	files, err := filepath.Glob("templates/*.html")
	if err != nil || len(files) == 0 {
		t.Fatalf("no templates: %v", err)
	}
	attr := regexp.MustCompile(`\b(href|src|action|formaction|hx-get|hx-post|hx-put|hx-delete|hx-push-url|poster)="([^"]*)"`)
	action := regexp.MustCompile(`\{\{[^}]*\}\}`)
	for _, f := range files {
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range attr.FindAllStringSubmatch(string(raw), -1) {
			val := m[2]
			q := strings.IndexByte(val, '?')
			if q < 0 {
				continue
			}
			query := val[q+1:]
			for _, loc := range action.FindAllStringIndex(query, -1) {
				act := query[loc[0]:loc[1]]
				if strings.HasPrefix(act, "{{if ") || strings.HasPrefix(act, "{{else") || act == "{{end}}" || strings.HasPrefix(act, "{{range ") || strings.HasPrefix(act, "{{with ") {
					continue // control flow writes nothing itself.
				}
				if !strings.HasSuffix(query[:loc[0]], "=") {
					t.Errorf("%s: %s=%q writes %s into the query where no \"name=\" precedes it: it is escaped as one value, so a query string put there is lost", f, m[1], val, act)
				}
			}
		}
	}
}
