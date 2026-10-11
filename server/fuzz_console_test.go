package server_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/server"
)

// Foundation B12: the console's form inputs. Every POST route and the GET routes
// that take a query are sent arbitrary text, against a real broker and the real
// console handler, and none may panic, take longer than the console's own
// deadline allows, or leak a stack trace into the page.
//
// This lives beside the embedding tests (and not in console/) because it drives
// the console as a user's browser would: through its handler, with a broker
// behind it. Topic-name and form parsers inside console/ get unit-level targets
// in the console stages.
func FuzzConsoleForms(f *testing.F) {
	srv, err := server.New(server.Options{DataDir: f.TempDir()})
	if err != nil {
		f.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	addr, err := srv.Bind(ctx)
	if err != nil {
		f.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx) }()
	f.Cleanup(func() { cancel(); <-done })
	con, err := console.New(console.Options{Addr: addr.String()})
	if err != nil {
		f.Fatal(err)
	}
	f.Cleanup(con.Close)

	routes := []struct{ method, path string }{
		{"POST", "/_console/new/topic"},
		{"POST", "/_console/topics/{name}/delete"},
		{"POST", "/_console/topics/{name}/purge"},
		{"POST", "/_console/topics/{name}/partitions"},
		{"POST", "/_console/topics/{name}/config"},
		{"POST", "/_console/topics/{name}/produce"},
		{"POST", "/_console/topics/{name}/resend"},
		{"GET", "/_console/topics/{name}"},
		{"GET", "/_console/topics/{name}/tape"},
		{"GET", "/_console/topics/{name}/message"},
		{"GET", "/_console/topics/{name}/message/raw"},
		{"GET", "/_console/groups/{name}"},
		{"POST", "/_console/groups/{name}/reset"},
		{"POST", "/_console/groups/{name}/offset"},
		{"POST", "/_console/groups/{name}/offset/delete"},
		{"POST", "/_console/groups/{name}/delete"},
		{"GET", "/_console/trace"},
		{"GET", "/_console/traffic"},
	}
	f.Add(uint8(0), "orders", "name=orders&partitions=3&retention_ms=60000")
	f.Add(uint8(3), "orders", "partitions=4")
	f.Add(uint8(5), "orders", "key=k&value=v&partition=0&headers=a%3Db")
	f.Add(uint8(9), "orders", "partition=0&offset=0")
	f.Add(uint8(14), "g", "topic=orders&partition=0&offset=-5")
	f.Add(uint8(16), "", "q=abc&header=traceparent")

	f.Fuzz(func(t *testing.T, route uint8, name, form string) {
		if len(name) > 300 || len(form) > 1<<12 {
			t.Skip()
		}
		rt := routes[int(route)%len(routes)]
		path := strings.ReplaceAll(rt.path, "{name}", url.PathEscape(name))
		var req *http.Request
		var err error
		if rt.method == "GET" {
			req, err = http.NewRequest("GET", "http://127.0.0.1"+path+"?"+form, nil)
		} else {
			req, err = http.NewRequest("POST", "http://127.0.0.1"+path, strings.NewReader(form))
			if err == nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		}
		if err != nil {
			t.Skip() // not a request net/http will build.
		}
		req.Host = "127.0.0.1"
		rec := httptest.NewRecorder()
		start := time.Now()
		con.ServeHTTP(rec, req)
		if took := time.Since(start); took > 20*time.Second {
			t.Fatalf("%s %s took %v", rt.method, path, took)
		}
		if body := rec.Body.String(); strings.Contains(body, "goroutine ") && strings.Contains(body, "runtime/") {
			t.Fatalf("%s %s leaked a stack trace:\n%s", rt.method, path, body)
		}
	})
}
