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
	// The fields under the names the forms really send, so that the text parsers
	// behind them are in the corpus the fuzzer starts from (docs/fuzz_test.go holds
	// each of them to a target that reaches it): the topic settings as a person
	// types them, on the create and the settings forms; "go to time"; a produce with
	// base64 fields and header rows.
	f.Add(uint8(0), "", "name=settings&partitions=1&retention.ms=7d&retention.bytes=1GB&segment.bytes=64MB&max.message.bytes=1MB&message.timestamp.type=LogAppendTime")
	f.Add(uint8(4), "orders", "retention.ms=12h&retention.bytes=unlimited&segment.bytes=64+mb&max.message.bytes=64KB&message.timestamp.type=CreateTime")
	f.Add(uint8(4), "orders", "retention.ms=forever&retention.bytes=500MB")
	f.Add(uint8(4), "orders", "retention.ms=9223372036854775807d")
	f.Add(uint8(4), "orders", "segment.bytes=-1")
	f.Add(uint8(4), "orders", "message.timestamp.type=Neither")
	f.Add(uint8(7), "orders", "partition=0&at=15m")
	f.Add(uint8(7), "orders", "partition=0&at=2026-10-07+10%3A30")
	f.Add(uint8(7), "orders", "partition=0&at=1760000000000")
	f.Add(uint8(7), "orders", "partition=0&at=yesterday")
	f.Add(uint8(5), "orders", "key=a2V5&key_enc=base64&value=dmFsdWU%3D&value_enc=base64&partition=0&acks=all&headers=%5B%7B%22k%22%3A%22traceparent%22%2C%22v%22%3A%22MDA%22%2C%22b64%22%3Atrue%7D%5D")
	f.Add(uint8(5), "orders", "tombstone=1&key=k&partition=auto&headers=%5B%7B%22k%22%3A%22%22%2C%22v%22%3A%22x%22%7D%5D")
	f.Add(uint8(7), "orders", "partition=0&q=abc")

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
