package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// The second identity (foundation B11): the listener on 127.0.0.1:0, a console
// prefix, a data directory with a space in its name, SASL on and TLS on, all at
// once. `task check:identity` runs this alone; `task check` runs it with the rest.
// What it looks for is a path typed as "/_console", a directory name that was never
// quoted, or a client that was never made to sign in.
func TestSecondIdentity(t *testing.T) {
	cert, key := selfSigned(t)
	dataDir := filepath.Join(t.TempDir(), "data dir with a space")
	flags := []string{"--data-dir", dataDir, "--console-prefix", "/ops/kafka-ui",
		"--sasl-mechanisms", "PLAIN", "--sasl-user", "second:s3cret", "--tls-cert", cert, "--tls-key", key}

	addrs := func(out string) (kafka, console string) {
		k := regexp.MustCompile(`kafka {5}(127\.0\.0\.1:\d+)`).FindStringSubmatch(out)
		c := regexp.MustCompile(`console {3}(http://127\.0\.0\.1:\d+)`).FindStringSubmatch(out)
		if k == nil || c == nil {
			t.Fatalf("the summary names no addresses:\n%s", out)
		}
		return k[1], c[1]
	}
	client := func(kafka string) *kgo.Client {
		cl, err := kgo.NewClient(kgo.SeedBrokers(kafka), kgo.SASL(plain.Auth{User: "second", Pass: "s3cret"}.AsMechanism()),
			kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true}), kgo.ConsumeTopics("second-identity"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.DialTimeout(3*time.Second), kgo.AllowAutoTopicCreation())
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		return cl
	}
	get := func(url string) (int, string) {
		resp, err := http.Get(url)
		if err != nil {
			t.Fatalf("GET %s: %v", url, err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	for round := 1; round <= 2; round++ { // the second round is a restart over the same directory.
		out, stop := start(t, flags...)
		waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
		text := out.String()
		if !strings.Contains(text, "SASL") || !strings.Contains(text, "TLS") || !strings.Contains(text, dataDir) {
			t.Errorf("round %d: the summary does not say SASL, TLS and the data directory:\n%s", round, text)
		}
		kafka, console := addrs(text)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		cl := client(kafka)
		rec := &kgo.Record{Topic: "second-identity", Value: []byte(fmt.Sprintf("round %d", round))}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("round %d: produce over TLS and SASL: %v", round, err)
		}
		var got []string
		for len(got) < round {
			fs := cl.PollFetches(ctx)
			if err := ctx.Err(); err != nil {
				t.Fatalf("round %d: read back %v: %v", round, got, err)
			}
			fs.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
		}
		cancel()
		if got[len(got)-1] != fmt.Sprintf("round %d", round) || got[0] != "round 1" {
			t.Errorf("round %d: read %v; the first round's record must survive the restart", round, got)
		}

		// The console is under its prefix and nowhere else, and reaches the broker.
		if code, body := get(console + "/ops/kafka-ui/topics"); code != 200 || !strings.Contains(body, "second-identity") {
			t.Errorf("round %d: GET /ops/kafka-ui/topics = %d, want 200 listing second-identity", round, code)
		}
		if code, _ := get(console + "/_console/topics"); code != 404 {
			t.Errorf("round %d: GET /_console/topics = %d under another prefix, want 404", round, code)
		}
		if code, body := get(console + "/ops/kafka-ui/cluster"); code != 200 || !strings.Contains(body, "SASL") {
			t.Errorf("round %d: the cluster page = %d, want 200 saying SASL", round, code)
		}
		if code, _ := get(console + "/healthz"); code != 200 {
			t.Errorf("round %d: /healthz = %d", round, code)
		}
		// The bare root points at the prefix, whatever it is.
		noFollow := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		resp, err := noFollow.Get(console + "/")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if loc := resp.Header.Get("Location"); loc != "/ops/kafka-ui" {
			t.Errorf("round %d: the root redirects to %q, want /ops/kafka-ui", round, loc)
		}
		if err := stop(); err != nil {
			t.Errorf("round %d: shutdown: %v", round, err)
		}
	}
}
