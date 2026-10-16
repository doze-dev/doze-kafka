package main

import (
	"context"
	"crypto/tls"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"

	"github.com/doze-dev/doze-kafka/console"
)

// Both directions, in the real binary, over a plain listener and over SASL and TLS.
//
// The console's own requests are not in Traffic: with nothing but the console talking
// to the broker (pages drawn, a topic made, a message written and its tape read), there
// is not one row and not one client.
//
// And a client that is not the console is in Traffic whatever it calls itself. One
// that connects with the console's own client ids used to be invisible, because the
// client id was how the console was known; now the connection is.
func TestTrafficLeavesOutTheConsoleAndNobodyElse(t *testing.T) {
	cert, key := selfSigned(t)
	for name, tc := range map[string]struct {
		flags []string
		opts  []kgo.Opt
	}{
		"plain": {},
		"sasl and tls": {
			flags: []string{"--sasl-mechanisms", "PLAIN", "--sasl-user", "app:s3cret", "--tls-cert", cert, "--tls-key", key},
			opts: []kgo.Opt{kgo.SASL(plain.Auth{User: "app", Pass: "s3cret"}.AsMechanism()),
				kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true})},
		},
	} {
		t.Run(name, func(t *testing.T) {
			out, _ := start(t, tc.flags...)
			waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
			kafka := regexp.MustCompile(`kafka     (\S+)`).FindStringSubmatch(out.String())[1]
			base := regexp.MustCompile(`console   (http://\S+)`).FindStringSubmatch(out.String())[1]
			rows := func(page string) int { return strings.Count(page, `id="rq-`) }

			// The console at work, through every kind of client it has: the admin client
			// (pages), a producer (the topic's first message, written by a client of our
			// own below so the console's fetch reader has something to read), the tape.
			for _, path := range []string{"/_console/topics", "/_console/groups", "/_console/cluster", "/_console/traffic"} {
				consoleGet(t, base, path)
			}
			quiet := consoleGet(t, base, "/_console/traffic")
			if n := rows(quiet); n != 0 || strings.Contains(quiet, console.ClientID) {
				t.Fatalf("with only the console talking to the broker, Traffic lists %d rows:\n%.600s", n, between(quiet, `id="traffic-table"`, `</table>`))
			}
			if clients := consoleGet(t, base, "/_console/traffic?view=clients"); strings.Contains(clients, console.ClientID) {
				t.Error("the console is listed among the connected clients")
			}

			// A client that is not the console, under each name the console uses.
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			for _, id := range []string{console.ClientID, console.ClientIDFetch, console.ClientIDTrace} {
				cl, err := kgo.NewClient(append([]kgo.Opt{kgo.SeedBrokers(kafka), kgo.ClientID(id), kgo.AllowAutoTopicCreation()}, tc.opts...)...)
				if err != nil {
					t.Fatal(err)
				}
				defer cl.Close()
				if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "named-like-the-console", Value: []byte(id)}).FirstErr(); err != nil {
					t.Fatalf("%s: produce: %v", id, err)
				}
			}
			// The console reads the topic's tape with its own fetch client meanwhile.
			consoleGet(t, base, "/_console/topics/named-like-the-console?tab=messages")

			var page string
			waitFor(t, "the three clients' produces to be listed", func() bool {
				page = consoleGet(t, base, "/_console/traffic?topic=named-like-the-console&kind=produce")
				return rows(page) >= 3
			})
			for _, id := range []string{console.ClientID, console.ClientIDFetch, console.ClientIDTrace} {
				if !strings.Contains(page, ">"+id+"<") && !strings.Contains(page, `"`+id+`"`) {
					t.Errorf("the client that calls itself %s is not on the Traffic page", id)
				}
			}
			clients := consoleGet(t, base, "/_console/traffic?view=clients")
			if !strings.Contains(clients, console.ClientIDTrace) || !strings.Contains(clients, console.ClientIDFetch) {
				t.Error("the clients named like the console are not among the connected clients")
			}
			// And still nothing of the console's own: nobody but the console fetched.
			if fetches := consoleGet(t, base, "/_console/traffic?kind=fetch"); rows(fetches) != 0 {
				t.Errorf("the console's own fetch of the tape is listed: %d rows", rows(fetches))
			}
		})
	}
}

func between(s, a, b string) string {
	i := strings.Index(s, a)
	if i < 0 {
		return ""
	}
	j := strings.Index(s[i:], b)
	if j < 0 {
		return s[i:]
	}
	return s[i : i+j]
}
