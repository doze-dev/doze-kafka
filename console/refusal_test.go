package console

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/twmb/franz-go/pkg/kerr"
)

// A Refusal's Kafka name comes from the table the wire protocol answers from. It
// is checked for every code that table knows: the console cannot show a name the
// wire does not send.
func TestRefusalNamesAreTheProtocolsNames(t *testing.T) {
	n := 0
	for code := int16(1); code < 200; code++ {
		ke := kerr.TypedErrorForCode(code)
		if ke == nil || (ke == kerr.UnknownServerError && code != kerr.UnknownServerError.Code) {
			continue
		}
		n++
		got := refusalFor(ke)
		if got.Name != protocol.ErrorName(code) || got.Name == "" || got.Name != ke.Message {
			t.Errorf("code %d: refusal names it %q, the protocol says %q", code, got.Name, protocol.ErrorName(code))
		}
		if got.Source != SourceKafka || got.Code != int(code) {
			t.Errorf("code %d: source %q code %d", code, got.Source, got.Code)
		}
		switch got.Kind {
		case Refused, Denied, NotImplemented, Failed:
		default:
			t.Errorf("code %d: kind %q is not one of the four", code, got.Kind)
		}
	}
	if n < 80 {
		t.Errorf("only %d Kafka errors were checked", n)
	}
	// What the broker said about this request travels with its name.
	r := refusalFor(&kafkaRefusal{Err: kerr.InvalidTopicException, Msg: "Topic name is invalid: 'a b'"})
	if r.Name != "INVALID_TOPIC_EXCEPTION" || !strings.Contains(r.Message, "Topic name is invalid: 'a b'") {
		t.Errorf("the broker's ErrorMessage did not survive: %+v", r)
	}
}

// refusalReq is a request as the browser makes it: a form post from a control that
// shows its error beside itself (an htmx request), or a navigation.
func refusalReq(t *testing.T, c *Console, method, path string, form url.Values, hx bool, hdr map[string]string) *httptest.ResponseRecorder {
	t.Helper()
	var r *http.Request
	if form != nil {
		r = httptest.NewRequest(method, path, strings.NewReader(form.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		r = httptest.NewRequest(method, path, nil)
	}
	r.Host = "127.0.0.1:9093"
	if hx {
		r.Header.Set("HX-Request", "true")
	}
	for k, v := range hdr {
		r.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	serveInto(t, c, rec, r)
	return rec
}

var refusalAttr = regexp.MustCompile(`data-refusal-kind="([a-z-]+)"(?: data-refusal-source="([a-z]+)")?(?: data-refusal-name="([^"]+)")?`)

// Every handler that can fail is walked: each failure comes out as a Refusal (its
// kind, and its name where something has one), never as a string.
func TestEveryFailingHandlerAnswersWithARefusal(t *testing.T) {
	src := sampleTraffic()
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = src })
	topicWith(t, c, "orders", 2, 3)
	register(t, c, "orders-value", userV1)

	type want struct{ kind, name, source string }
	form := func(kv ...string) url.Values {
		v := url.Values{}
		for i := 0; i+1 < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	cases := []struct {
		what         string
		method, path string
		form         url.Values
		hx           bool
		hdr          map[string]string
		code         int
		want         want
	}{
		// Topics.
		{"a topic page for a topic that is not there", "GET", "/_console/topics/ghost", nil, false, nil, 404, want{"refused", "UNKNOWN_TOPIC_OR_PARTITION", "kafka"}},
		{"creating a topic that exists", "POST", "/_console/new/topic", form("name", "orders", "partitions", "1"), true, nil, 400, want{"refused", "TOPIC_ALREADY_EXISTS", "kafka"}},
		{"creating a topic with a name Kafka refuses", "POST", "/_console/new/topic", form("name", "bad name!", "partitions", "1"), true, nil, 400, want{"refused", "", ""}},
		{"deleting a topic that is not there", "POST", "/_console/topics/ghost/delete", nil, true, nil, 404, want{"refused", "UNKNOWN_TOPIC_OR_PARTITION", "kafka"}},
		{"purging a topic that is not there", "POST", "/_console/topics/ghost/purge", nil, true, nil, 404, want{"refused", "", ""}},
		{"growing a topic to fewer partitions", "POST", "/_console/topics/orders/partitions", form("partitions", "1"), true, nil, 400, want{"refused", "", ""}},
		{"a topic setting that is not a number", "POST", "/_console/topics/orders/config", form("retention.ms", "soon"), true, nil, 400, want{"refused", "", ""}},
		{"producing to a topic that is not there", "POST", "/_console/topics/ghost/produce", form("value", "x", "partition", "0"), true, nil, 404, want{"refused", "", ""}},
		{"opening a record that is not there", "GET", "/_console/topics/orders/message?partition=0&offset=999", nil, true, nil, 404, want{"refused", "", "console"}},
		{"opening a record with no coordinates", "GET", "/_console/topics/orders/message?partition=x&offset=1", nil, true, nil, 400, want{"refused", "", "console"}},
		// Groups.
		{"a group page for a group that is not there", "GET", "/_console/groups/ghost", nil, false, nil, 404, want{"refused", "", ""}},
		{"deleting a group that is not there", "POST", "/_console/groups/ghost/delete", nil, true, nil, 404, want{"refused", "", ""}},
		{"resetting a group that is not there", "POST", "/_console/groups/ghost/reset", form("to", "earliest"), true, nil, 404, want{"refused", "", ""}},
		{"an offset that is not a number", "POST", "/_console/groups/ghost/offset", form("topic", "orders", "partition", "0", "offset", "x"), true, nil, 400, want{"refused", "", ""}},
		// Schemas.
		{"a subject that is not there", "GET", "/_console/schemas/ghost", nil, false, nil, 404, want{"refused", "40401", "registry"}},
		{"registering a schema the registry cannot parse", "POST", "/_console/schemas/orders-value/register", form("schema", "{not json", "schemaType", "AVRO"), true, nil, 400, want{"refused", "", "registry"}},
		{"a compatibility level that does not exist", "POST", "/_console/schemas/orders-value/config", form("level", "SIDEWAYS"), true, nil, 400, want{"refused", "", ""}},
		{"deleting a subject that is not there", "POST", "/_console/schemas/ghost/delete", nil, true, nil, 404, want{"refused", "40401", "registry"}},
		// Traffic, trace and the inspector.
		{"a request that is not a number", "GET", "/_console/traffic/request?seq=abc", nil, true, nil, 400, want{"refused", "", "console"}},
		{"a trace window that does not exist", "GET", "/_console/trace?id=4bf92f3577b34da6a3ce929d0e0e4736&window=2d", nil, false, nil, 400, want{"refused", "", "console"}},
		{"an inspector kind that does not exist", "GET", "/_console/inspect/nonsense?key=1", nil, true, nil, 404, want{"refused", "", "console"}},
		// The console's own rules.
		{"a page that is not there", "GET", "/_console/nonsense", nil, false, nil, 404, want{"refused", "", "console"}},
		{"a change from another site", "POST", "/_console/topics/orders/purge", nil, true, map[string]string{"Origin": "http://evil.example"}, 403, want{"denied", "", "console"}},
	}
	for _, tc := range cases {
		rec := refusalReq(t, c, tc.method, tc.path, tc.form, tc.hx, tc.hdr)
		body := rec.Body.String()
		if rec.Code != tc.code {
			t.Errorf("%s: %d, want %d: %.200s", tc.what, rec.Code, tc.code, body)
			continue
		}
		m := refusalAttr.FindStringSubmatch(body)
		if m == nil {
			t.Errorf("%s: answered with a string, not a Refusal: %.300s", tc.what, body)
			continue
		}
		if m[1] != tc.want.kind {
			t.Errorf("%s: kind %q, want %q", tc.what, m[1], tc.want.kind)
		}
		if tc.want.name != "" && m[3] != tc.want.name {
			t.Errorf("%s: name %q, want %q", tc.what, m[3], tc.want.name)
		}
		if tc.want.source != "" && tc.hx && m[2] != tc.want.source {
			t.Errorf("%s: source %q, want %q", tc.what, m[2], tc.want.source)
		}
		if tc.want.name == "" && tc.want.source == "console" && m[3] != "" {
			t.Errorf("%s: a rule of the console's own was given the name %q", tc.what, m[3])
		}
	}
}

// Nothing writes an error as a string behind the Refusal's back. The places that
// still speak plainly are listed, each with its reason.
func TestNoHandlerWritesAnErrorString(t *testing.T) {
	allowed := map[string]string{
		"hostguard.go": "the DNS-rebinding guard answers before a page can be built",
		"console.go":   "a template that fails to execute is a bug, and by then a page is half written",
		"static.go":    "static files are not pages",
	}
	files, _ := filepath.Glob("*.go")
	re := regexp.MustCompile(`http\.Error\(|WriteString\(w, ` + "`" + `<div class="err`)
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		b, _ := os.ReadFile(f)
		if re.Match(b) {
			if _, ok := allowed[f]; !ok {
				t.Errorf("%s writes an error string; use c.fail or c.refuse so it is a Refusal", f)
			}
		}
	}
}
