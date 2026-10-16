package console

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

// A2, held from the route table: every route the console answers (Routes(), the rows
// it registers its handlers from) is either driven to the refusals it gives, each
// with its kind, its source and its name exactly (no name where a rule of the
// console's own decided), or says why it gives none. And each of the four kinds is
// given by a handler, not only by the error table: refused and denied by the broker
// and the console, failed by a broker that is not there and a registry that fails,
// not implemented by a registry that does not implement what it was asked.
func TestEveryRouteRefusesInTheOneVocabulary(t *testing.T) {
	c, _, _ := newRegistryConsole(t, func(o *Options) { o.Traffic = sampleTraffic() })
	topicWith(t, c, "orders", 2, 3)
	register(t, c, "orders-value", userV1)
	// A broker that is not there.
	down, err := New(Options{Addr: "127.0.0.1:1"})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(down.Close)
	// A registry that answers every request with this status (and its own code).
	registryAnswering := func(status int) *Console {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/vnd.schemaregistry.v1+json")
			w.WriteHeader(status)
			fmt.Fprintf(w, `{"error_code":%d,"message":"the registry answered %d"}`, status*100+1, status)
		}))
		t.Cleanup(ts.Close)
		cc, _ := newTestConsoleAddr(t, func(o *Options) { o.RegistryURL = ts.URL })
		return cc
	}
	failing, unimplemented := registryAnswering(http.StatusInternalServerError), registryAnswering(http.StatusNotImplemented)

	form := func(kv ...string) url.Values {
		v := url.Values{}
		for i := 0; i+1 < len(kv); i += 2 {
			v.Set(kv[i], kv[i+1])
		}
		return v
	}
	type answer struct {
		code               int
		kind, source, name string
	}
	type tcase struct {
		route        string // the route table's method and path
		what         string
		c            *Console
		method, path string
		form         url.Values
		hx           bool
		hdr          map[string]string
		want         answer
	}
	const (
		refused = "refused"
		denied  = "denied"
		failed  = "failed"
		notImpl = "not-implemented"
	)
	cases := []tcase{
		{"GET /", "the home when the broker is not there", down, "GET", "/_console/", nil, false, nil, answer{502, failed, "", ""}},
		{"GET /", "a page that is not there", c, "GET", "/_console/nonsense", nil, false, nil, answer{404, refused, "", ""}},
		{"GET /topics", "the topics when the broker is not there", down, "GET", "/_console/topics", nil, false, nil, answer{502, failed, "", ""}},
		{"GET /live/topics", "the topics' poll when the broker is not there", down, "GET", "/_console/live/topics", nil, true, nil, answer{502, failed, "console", ""}},
		{"POST /new/topic", "creating a topic that exists", c, "POST", "/_console/new/topic", form("name", "orders", "partitions", "1"), true, nil, answer{400, refused, "kafka", "TOPIC_ALREADY_EXISTS"}},
		{"POST /new/topic", "a change from another site", c, "POST", "/_console/new/topic", form("name", "x"), true, map[string]string{"Origin": "http://evil.example"}, answer{403, denied, "console", ""}},
		{"GET /topics/{name}", "a topic that is not there", c, "GET", "/_console/topics/ghost", nil, true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"POST /topics/{name}/delete", "deleting a topic that is not there", c, "POST", "/_console/topics/ghost/delete", nil, true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"POST /topics/{name}/purge", "purging a topic that is not there", c, "POST", "/_console/topics/ghost/purge", nil, true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"POST /topics/{name}/partitions", "growing a topic to fewer partitions", c, "POST", "/_console/topics/orders/partitions", form("partitions", "1"), true, nil, answer{400, refused, "console", ""}},
		{"POST /topics/{name}/config", "a topic setting that is not a number", c, "POST", "/_console/topics/orders/config", form("retention.ms", "soon"), true, nil, answer{400, refused, "console", ""}},
		{"GET /topics/{name}/tape", "the tape of a topic that is not there", c, "GET", "/_console/topics/ghost/tape?partition=0", nil, true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"POST /topics/{name}/produce", "producing to a topic that is not there", c, "POST", "/_console/topics/ghost/produce", form("value", "x", "partition", "0"), true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"POST /topics/{name}/resend", "sending again a record that is not there", c, "POST", "/_console/topics/orders/resend", form("partition", "0", "offset", "999"), true, nil, answer{404, refused, "console", ""}},
		{"GET /topics/{name}/message", "a record that is not there", c, "GET", "/_console/topics/orders/message?partition=0&offset=999", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /topics/{name}/message/raw", "the bytes of a record that is not there", c, "GET", "/_console/topics/orders/message/raw?partition=0&offset=999", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /trace", "a trace window that does not exist", c, "GET", "/_console/trace?id=4bf92f3577b34da6a3ce929d0e0e4736&window=2d", nil, true, nil, answer{400, refused, "console", ""}},
		{"GET /trace/live", "a trace id that is not one", c, "GET", "/_console/trace/live?id=zz", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /traffic/request", "a request that is not a number", c, "GET", "/_console/traffic/request?seq=abc", nil, true, nil, answer{400, refused, "console", ""}},
		{"GET /traffic/new", "a point in time that is not one", c, "GET", "/_console/traffic/new?since=abc", nil, true, nil, answer{400, refused, "console", ""}},
		{"GET /schemas", "the subjects when the registry fails", failing, "GET", "/_console/schemas", nil, true, nil, answer{502, failed, "registry", "50001"}},
		{"POST /new/schema", "registering a schema the registry cannot parse", c, "POST", "/_console/new/schema", form("subject", "s", "schema", "{nope"), true, nil, answer{400, refused, "registry", "42201"}},
		{"POST /new/schema", "registering with no subject", c, "POST", "/_console/new/schema", form("subject", "", "schema", userV1), true, nil, answer{400, refused, "console", ""}},
		{"GET /schemas/{subject}", "a subject that is not there", c, "GET", "/_console/schemas/ghost", nil, true, nil, answer{404, refused, "registry", "40401"}},
		{"GET /schemas/{subject}", "a subject the registry cannot show", unimplemented, "GET", "/_console/schemas/orders-value", nil, true, nil, answer{501, notImpl, "registry", "50101"}},
		{"POST /schemas/{subject}/check", "a check the registry does not implement", unimplemented, "POST", "/_console/schemas/orders-value/check", form("schema", userV1), true, nil, answer{501, notImpl, "registry", "50101"}},
		{"POST /schemas/{subject}/register", "registering a schema the registry cannot parse", c, "POST", "/_console/schemas/orders-value/register", form("schema", "{not json", "schemaType", "AVRO"), true, nil, answer{400, refused, "registry", "42201"}},
		{"POST /schemas/{subject}/config", "a compatibility level that does not exist", c, "POST", "/_console/schemas/orders-value/config", form("level", "SIDEWAYS"), true, nil, answer{400, refused, "registry", "42203"}},
		{"POST /schemas/{subject}/config", "a level the registry does not implement", unimplemented, "POST", "/_console/schemas/orders-value/config", form("level", "FULL"), true, nil, answer{501, notImpl, "registry", "50101"}},
		{"POST /schemas/{subject}/delete", "deleting a subject that is not there", c, "POST", "/_console/schemas/ghost/delete", nil, true, nil, answer{404, refused, "registry", "40401"}},
		{"GET /groups", "the groups when the broker is not there", down, "GET", "/_console/groups", nil, false, nil, answer{502, failed, "", ""}},
		{"GET /live/groups", "the groups' poll when the broker is not there", down, "GET", "/_console/live/groups", nil, true, nil, answer{502, failed, "console", ""}},
		// Kafka describes a group it does not know as Dead with no error: that it is not
		// there is the console's reading of the group list, so the console's rule.
		{"GET /groups/{id}", "a group that is not there", c, "GET", "/_console/groups/ghost", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /groups/{id}/live", "the poll of a group that is not there", c, "GET", "/_console/groups/ghost/live", nil, true, nil, answer{404, refused, "console", ""}},
		{"POST /groups/{id}/reset", "resetting a group that is not there", c, "POST", "/_console/groups/ghost/reset", form("to", "earliest"), true, nil, answer{404, refused, "console", ""}},
		{"POST /groups/{id}/offset", "an offset that is not a number", c, "POST", "/_console/groups/ghost/offset", form("topic", "orders", "partition", "0", "offset", "x"), true, nil, answer{400, refused, "console", ""}},
		{"POST /groups/{id}/offset/delete", "deleting an offset of a group that is not there", c, "POST", "/_console/groups/ghost/offset/delete", form("topic", "orders", "partition", "0"), true, nil, answer{404, refused, "kafka", "GROUP_ID_NOT_FOUND"}},
		{"POST /groups/{id}/delete", "deleting a group that is not there", c, "POST", "/_console/groups/ghost/delete", nil, true, nil, answer{404, refused, "kafka", "GROUP_ID_NOT_FOUND"}},
		{"GET /cluster", "the cluster when the broker is not there", down, "GET", "/_console/cluster", nil, false, nil, answer{502, failed, "", ""}},
		{"GET /access", "the ACLs when the broker is not there", down, "GET", "/_console/access", nil, false, nil, answer{502, failed, "", ""}},
		{"GET /access/check", "an operation that does not exist", c, "GET", "/_console/access/check?principal=User:a&operation=fly&resource=topic&name=x", nil, true, nil, answer{400, refused, "console", ""}},
		{"POST /new/acl", "an ACL with nothing but a principal", c, "POST", "/_console/new/acl", form("principal", "alice", "resource", "topic"), true, nil, answer{400, refused, "console", ""}},
		{"GET /inspect/{kind}", "an inspector kind that does not exist", c, "GET", "/_console/inspect/nonsense?key=1", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /dock", "a wire with nothing to filter on", c, "GET", "/_console/dock", nil, true, nil, answer{400, refused, "console", ""}},
		{"GET /confirm/{id}", "a confirm that does not exist", c, "GET", "/_console/confirm/nonsense?name=orders", nil, true, nil, answer{404, refused, "console", ""}},
		{"GET /confirm/{id}", "purging a topic that is not there, measured", c, "GET", "/_console/confirm/topic.purge?name=ghost", nil, true, nil, answer{404, refused, "kafka", "UNKNOWN_TOPIC_OR_PARTITION"}},
		{"GET /api/list/{section}", "a section that does not exist", c, "GET", "/_console/api/list/nonsense", nil, true, nil, answer{404, refused, "console", ""}},
	}
	// The routes that give no refusal, and why.
	never := map[string]string{
		"GET /static/":         "the stylesheet, scripts and fonts: a missing file is the file server's 404, not a page",
		"GET ":                 "the prefix without its slash is the home's own address; the home's refusals are GET /'s",
		"GET /new/topic":       "the form is drawn without asking the broker; what it sends is refused at POST /new/topic",
		"GET /new/topic/shell": "the command as typed: what is wrong with a value is said beside the command it builds",
		"GET /traffic":         "Traffic draws what the recorder holds and asks the broker nothing; a filter it does not know is no filter",
		"GET /traffic/live":    "Traffic's rows, as the page: a filter it does not know is no filter",
		"GET /new/schema":      "the form is drawn without asking the registry; what it sends is refused at POST /new/schema",
		"GET /connect":         "the settings are drawn from the console's own facts and ask the broker nothing",
		"GET /new/acl":         "the form is drawn without asking the broker; what it sends is refused at POST /new/acl",
		"GET /new/acl/preview": "the command as typed: what is wrong with a field is said beside the command it builds",
		"GET /api/palette":     "the palette's catalogue, as JSON: what it cannot read it leaves out, and the browser's recents still work",
		"GET /api/counts":      "the heartbeat, as JSON: it says whether the broker answers (\"broker\": 0), which is how a page learns it is gone",
	}

	covered := map[string]bool{}
	kinds := map[string]bool{}
	for _, tc := range cases {
		covered[tc.route] = true
		rec := refusalReq(t, tc.c, tc.method, tc.path, tc.form, tc.hx, tc.hdr)
		body := rec.Body.String()
		m := refusalAttr.FindStringSubmatch(body)
		if m == nil {
			t.Errorf("%s (%s): %d with no Refusal: %.200s", tc.route, tc.what, rec.Code, body)
			continue
		}
		got := answer{rec.Code, m[1], m[2], m[3]}
		want := tc.want
		if !tc.hx {
			want.source = got.source // a page that is a refusal does not carry the source attribute
		}
		if got != want {
			t.Errorf("%s (%s): %+v, want %+v", tc.route, tc.what, got, want)
		}
		kinds[got.kind] = true
	}
	for _, r := range Routes() {
		key := r.Method + " " + r.Path
		if !covered[key] && never[key] == "" {
			t.Errorf("%s: no refusal is driven, and no reason it gives none", key)
		}
		if covered[key] && never[key] != "" {
			t.Errorf("%s: both driven and said to give no refusal", key)
		}
	}
	for _, k := range []string{refused, denied, notImpl, failed} {
		if !kinds[k] {
			t.Errorf("no handler gave a %s refusal", k)
		}
	}
}
