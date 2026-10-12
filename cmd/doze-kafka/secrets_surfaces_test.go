package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// The surfaces of a running broker that TestNoSecretReachesAnyOutput reads, and what
// proves each was reached (a surface that was not is a failure):
//
//	the log, text and JSON, at debug, info, warn and error
//	    a line this test caused is found at each level the run shows, and is absent
//	    at each level it hides; a JSON log is JSON on every line
//	a login with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512, good password and wrong
//	    the good one is answered Metadata, the wrong one is not, and the Traffic
//	    inspector holds a SASLAuthenticate row for each of the six clients and a
//	    SASLHandshake row naming each mechanism
//	malformed SASL input (tokens with the password where nothing should be)
//	    the Traffic inspector holds a row for each connection that sent one
//	a request before authentication, and a frame that is no request
//	    their warnings are among the log lines above
//	the handler-panic log line
//	    a seam makes the SASLAuthenticate handler panic with the token in its
//	    value; the seam counts its calls and the line is among the log lines above
//	the schema registry with HTTP Basic credentials, good, wrong and absent
//	    200 for the good ones and not for the others
//	/healthz
//	    200 and "ok"
//	the console's pages, partials, inspectors and JSON
//	    a crawl from every section, a floor on how many pages it read, and the two
//	    JSON documents parse
//	every file in the data directory, its name, content and permissions
//	    the metadata database and a segment of the topic the test created are among
//	    the files read
//	a restart over the same data
//	    the second broker lists the topic the first one created
//
// pprofSurfaces and commandSurfaces read the rest.

var saslMechanisms = []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}

// instance is a started broker and where it answers.
type instance struct {
	out                      *safeBuf
	stop                     func() error
	kafka, console, registry string
}

// startInstance is start, with the addresses read through readyHook: the summary that
// names them is not printed at warn or error.
func startInstance(t *testing.T, flags ...string) instance {
	t.Helper()
	got := make(chan readyInfo, 1)
	readyHook = func(r readyInfo) { got <- r }
	t.Cleanup(func() { readyHook = nil })
	out, stop := start(t, flags...)
	select {
	case r := <-got:
		readyHook = nil
		if r.bootstrap == "" || r.consoleURL == "" || r.registryURL == "" {
			t.Fatalf("the broker is up without one of its addresses: %+v", r)
		}
		return instance{out: out, stop: stop, kafka: r.bootstrap, console: r.consoleURL, registry: r.registryURL}
	case <-time.After(30 * time.Second):
		t.Fatalf("the broker did not come up:\n%s", out.String())
		return instance{}
	}
}

func mechanismFor(t *testing.T, name, user, pass string) sasl.Mechanism {
	t.Helper()
	switch name {
	case "PLAIN":
		return plain.Auth{User: user, Pass: pass}.AsMechanism()
	case "SCRAM-SHA-256":
		return scram.Auth{User: user, Pass: pass}.AsSha256Mechanism()
	case "SCRAM-SHA-512":
		return scram.Auth{User: user, Pass: pass}.AsSha512Mechanism()
	}
	t.Fatalf("no client for the mechanism %s", name)
	return nil
}

func (in instance) client(t *testing.T, clientID, mechanism, pass string) *kgo.Client {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(in.kafka), kgo.DialTLSConfig(&tls.Config{InsecureSkipVerify: true}),
		kgo.SASL(mechanismFor(t, mechanism, "alice", pass)), kgo.ClientID(clientID), kgo.RequestRetries(0),
		kgo.DialTimeout(5*time.Second), kgo.WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// loginID is the client id a login of this test uses, so its rows can be found.
func loginID(mechanism string, good bool) string {
	if good {
		return "guard-" + strings.ToLower(mechanism) + "-good"
	}
	return "guard-" + strings.ToLower(mechanism) + "-wrong"
}

// kafkaFrame is one request on the wire with a version 1 header (no tagged fields).
func kafkaFrame(key, version int16, correlation int32, clientID string, body []byte) []byte {
	b := binary.BigEndian.AppendUint16(nil, uint16(key))
	b = binary.BigEndian.AppendUint16(b, uint16(version))
	b = binary.BigEndian.AppendUint32(b, uint32(correlation))
	b = binary.BigEndian.AppendUint16(b, uint16(len(clientID)))
	b = append(b, clientID...)
	b = append(b, body...)
	return append(binary.BigEndian.AppendUint32(nil, uint32(len(b))), b...)
}

func saslHandshakeFrame(clientID, mechanism string) []byte {
	body := append(binary.BigEndian.AppendUint16(nil, uint16(len(mechanism))), mechanism...)
	return kafkaFrame(17, 1, 1, clientID, body)
}

// saslAuthenticateFrame carries token; declared is the length the frame says the token
// has (len(token) for an honest one).
func saslAuthenticateFrame(clientID string, token []byte, declared int) []byte {
	return kafkaFrame(36, 1, 2, clientID, append(binary.BigEndian.AppendUint32(nil, uint32(declared)), token...))
}

// exchange writes one frame and reads what comes back, if anything does: the answer's
// bytes, or "" for a connection that was closed instead. Either is the broker's to
// choose; both are read.
func exchange(t *testing.T, c net.Conn, frame []byte) string {
	t.Helper()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(frame); err != nil {
		return ""
	}
	var size [4]byte
	if _, err := io.ReadFull(c, size[:]); err != nil {
		return ""
	}
	n := binary.BigEndian.Uint32(size[:])
	if n > 1<<20 {
		t.Fatalf("an answer of %d bytes", n)
	}
	body := make([]byte, n)
	got, _ := io.ReadFull(c, body)
	return string(body[:got])
}

func (in instance) dial(t *testing.T) net.Conn {
	t.Helper()
	c, err := tls.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}, "tcp", in.kafka, &tls.Config{InsecureSkipVerify: true})
	if err != nil {
		t.Fatalf("dial %s: %v", in.kafka, err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// malformedSASL is SASL input no client sends, with the password in each place a
// careless error message would repeat. Each is sent on a connection of its own, under
// a client id of its own.
func malformedSASL() []struct {
	id, mechanism string
	tokens        [][]byte
	declared      int // for the last token; 0 is its real length.
} {
	pw := plantedPassword
	return []struct {
		id, mechanism string
		tokens        [][]byte
		declared      int
	}{
		{id: "guard-bad-plain-bare", mechanism: "PLAIN", tokens: [][]byte{[]byte(pw)}},
		{id: "guard-bad-plain-one-nul", mechanism: "PLAIN", tokens: [][]byte{[]byte("alice\x00" + pw)}},
		{id: "guard-bad-plain-four-parts", mechanism: "PLAIN", tokens: [][]byte{[]byte("\x00alice\x00" + pw + "\x00" + pw)}},
		{id: "guard-bad-plain-no-user", mechanism: "PLAIN", tokens: [][]byte{[]byte("\x00\x00" + pw)}},
		{id: "guard-bad-plain-not-utf8", mechanism: "PLAIN", tokens: [][]byte{[]byte("\xff\xfe\x00alice\x00" + pw)}},
		{id: "guard-bad-plain-short-frame", mechanism: "PLAIN", tokens: [][]byte{[]byte("\x00alice\x00" + pw)}, declared: 4096},
		{id: "guard-bad-plain-twice", mechanism: "PLAIN", tokens: [][]byte{[]byte("\x00alice\x00" + plantedWrong), []byte("\x00alice\x00" + pw)}},
		{id: "guard-bad-scram256-bare", mechanism: "SCRAM-SHA-256", tokens: [][]byte{[]byte(pw)}},
		// (Not the password as the first nonce: SCRAM has the server send that one back.)
		{id: "guard-bad-scram256-other-nonce", mechanism: "SCRAM-SHA-256", tokens: [][]byte{[]byte("n,,n=alice,r=abcdefgh"), []byte("c=biws,r=" + pw + ",p=" + pw)}},
		{id: "guard-bad-scram512-bare", mechanism: "SCRAM-SHA-512", tokens: [][]byte{[]byte(pw)}},
		{id: "guard-bad-scram512-proof", mechanism: "SCRAM-SHA-512", tokens: [][]byte{[]byte("n,,n=alice,r=abcdefgh"), []byte("c=biws,r=abcdefgh,p=" + pw)}},
		{id: "guard-bad-no-handshake", mechanism: "", tokens: [][]byte{[]byte("\x00alice\x00" + pw)}},
	}
}

// logRecord is one line of the log.
type logRecord struct{ level, msg, raw string }

var textLogLine = regexp.MustCompile(`^time=\S+ level=(\w+) msg=("(?:[^"\\]|\\.)*"|\S+)`)

// logRecords reads the log in its format. In JSON every line that is not blank must
// be a JSON object; in text, the lines that are not log lines are the summary.
func logRecords(t *testing.T, format, text string) []logRecord {
	t.Helper()
	var out []logRecord
	for _, line := range strings.Split(text, "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if format == "json" {
			var m map[string]any
			if err := json.Unmarshal([]byte(line), &m); err != nil {
				t.Errorf("a line of the JSON log is not JSON (%v): %q", err, line)
				continue
			}
			level, _ := m["level"].(string)
			msg, _ := m["msg"].(string)
			out = append(out, logRecord{level, msg, line})
			continue
		}
		if m := textLogLine.FindStringSubmatch(line); m != nil {
			out = append(out, logRecord{m[1], strings.Trim(m[2], `"`), line})
		}
	}
	return out
}

var logLevels = []string{"debug", "info", "warn", "error"}

// inspectorKey is the key of a row of the Traffic page, as its inspector is asked for.
var inspectorKey = regexp.MustCompile(`/inspect/request\?key=([^"&]+)`)

// inspectors is the inspector of every row the Traffic page lists under a filter.
func (in instance) inspectors(t *testing.T, g guard, filter string) []string {
	t.Helper()
	list := fetch(t, in.console+"/_console/traffic/live?"+filter)
	g.check(t, "the Traffic rows for "+filter, list)
	var out []string
	seen := map[string]bool{}
	for _, m := range inspectorKey.FindAllStringSubmatch(list, -1) {
		if seen[m[1]] {
			continue
		}
		seen[m[1]] = true
		body := fetch(t, in.console+"/_console/inspect/request?key="+m[1])
		g.check(t, "the Traffic inspector of a "+filter+" request", body)
		out = append(out, body)
	}
	return out
}

func anyContains(texts []string, all ...string) bool {
	return slices.ContainsFunc(texts, func(s string) bool {
		for _, a := range all {
			if !strings.Contains(s, a) {
				return false
			}
		}
		return true
	})
}

// consoleSurfaces reads the console: a crawl from every section, and the two JSON
// documents.
func (in instance) consoleSurfaces(t *testing.T, g guard, floor int) {
	t.Helper()
	pages := crawl(t, in.console, "/_console/", "/_console/cluster", "/_console/traffic", "/_console/traffic?view=clients", "/_console/traffic/live",
		"/_console/traffic?api=SASL", "/_console/traffic?kind=errors", "/_console/connect", "/_console/access", "/_console/topics",
		"/_console/groups", "/_console/schemas", "/_console/trace", "/_console/api/palette", "/_console/api/counts")
	if len(pages) < floor {
		t.Errorf("the crawl of the console reached %d pages, fewer than %d", len(pages), floor)
	}
	for p, body := range pages {
		g.check(t, "the console page "+p, body)
	}
	for _, p := range []string{"/_console/api/palette", "/_console/api/counts"} {
		_, body, _ := strings.Cut(pages[p], "\r\n\r\n")
		if !strings.HasPrefix(pages[p], "HTTP/1.1 200") || !json.Valid([]byte(body)) || len(body) < 3 {
			t.Errorf("%s is not a JSON document: %.200q", p, pages[p])
		}
	}
	status, health := httpGet(t, in.console+"/healthz", "", "")
	g.check(t, "/healthz", health)
	if status != 200 || !strings.HasSuffix(strings.TrimSpace(health), "ok") {
		t.Errorf("/healthz = %d %q", status, health)
	}
}

// dataDirSurfaces reads every file under the data directory: its name, what is in it
// and who may read it.
func dataDirSurfaces(t *testing.T, g guard, dir, topic string) {
	t.Helper()
	var metaDB, segment bool
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(dir, path)
		g.check(t, "the name of the data file "+rel, rel)
		info, err := d.Info()
		if err != nil {
			return err
		}
		perm := info.Mode().Perm()
		if d.IsDir() {
			// A directory is made 0755 at most: never writable by anyone else.
			if runtime.GOOS != "windows" && path != dir && perm&^0o755 != 0 {
				t.Errorf("the data directory %s has mode %v, more than 0755", rel, perm)
			}
			return nil
		}
		if !info.Mode().IsRegular() {
			t.Errorf("%s in the data directory is not a regular file (%v), and was not read", rel, info.Mode())
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		g.check(t, "the data file "+rel, string(b))
		if runtime.GOOS != "windows" {
			// The metadata database is where anything about users would be kept (ACLs are;
			// credentials are not kept at all): it is the owner's alone. The rest is record
			// data, and never writable by anyone else.
			switch {
			case rel == "meta.db" || rel == ".doze-kafka.lock":
				if perm != 0o600 {
					t.Errorf("%s has mode %v, want 0600", rel, perm)
				}
			case perm&^0o644 != 0:
				t.Errorf("the data file %s has mode %v, more than 0644", rel, perm)
			}
		}
		if rel == "meta.db" && len(b) > 0 {
			metaDB = true
		}
		if strings.Contains(rel, topic) && strings.HasSuffix(rel, ".log") {
			segment = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading the data directory: %v", err)
	}
	if !metaDB || !segment {
		t.Errorf("the data directory was read without its metadata database (%v) or a segment of %s (%v)", metaDB, topic, segment)
	}
}

func runningBrokerSurfaces(t *testing.T, g guard, format, level, certFile, keyFile string) {
	data := t.TempDir()
	flags := []string{"--data-dir", data, "--log-format", format, "--log-level", level,
		"--sasl-mechanisms", strings.Join(saslMechanisms, ","), "--sasl-user", "alice:" + plantedPassword,
		"--tls-cert", certFile, "--tls-key", keyFile}
	in := startInstance(t, flags...)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// A handler that panics while it holds the token, and says so with the token.
	var panicked atomic.Int32
	restore := protocol.HookRouteForTest(func(r *protocol.Request) {
		if r.Header.ClientID == "guard-panic" && protocol.APIName(r.Header.APIKey) == "SASLAuthenticate" {
			panicked.Add(1)
			panic(fmt.Sprintf("unexpected SASL token %q", r.Body))
		}
	})
	defer restore()

	// Every mechanism, with the wrong password and with the right one.
	var answers strings.Builder
	for _, mech := range saslMechanisms {
		short, stop := context.WithTimeout(ctx, 10*time.Second)
		if _, err := kmsg.NewPtrMetadataRequest().RequestWith(short, in.client(t, loginID(mech, false), mech, plantedWrong)); err != nil {
			answers.WriteString(err.Error() + "\n")
		} else {
			t.Errorf("%s: a wrong password was accepted", mech)
		}
		stop()
		md, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, in.client(t, loginID(mech, true), mech, plantedPassword))
		if err != nil || len(md.Brokers) == 0 {
			t.Fatalf("%s: the right password was not accepted: %v", mech, err)
		}
		answers.WriteString(fmt.Sprintf("%+v\n", md))
	}

	// What a client is answered about the configuration.
	good := in.client(t, "guard-admin", "PLAIN", plantedPassword)
	ct := kmsg.NewCreateTopicsRequest()
	ctt := kmsg.NewCreateTopicsRequestTopic()
	ctt.Topic, ctt.NumPartitions, ctt.ReplicationFactor = "orders", 1, 1
	ct.Topics = append(ct.Topics, ctt)
	if _, err := ct.RequestWith(ctx, good); err != nil {
		t.Fatal(err)
	}
	if err := good.ProduceSync(ctx, &kgo.Record{Topic: "orders", Value: []byte("an order")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	md, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, good)
	if err != nil || len(md.Brokers) == 0 {
		t.Fatalf("metadata: %v", err)
	}
	dc := kmsg.NewDescribeConfigsRequest()
	for _, r := range []struct {
		typ  kmsg.ConfigResourceType
		name string
	}{{kmsg.ConfigResourceTypeBroker, fmt.Sprint(md.Brokers[0].NodeID)}, {kmsg.ConfigResourceTypeTopic, "orders"}} {
		res := kmsg.NewDescribeConfigsRequestResource()
		res.ResourceType, res.ResourceName = r.typ, r.name
		dc.Resources = append(dc.Resources, res)
	}
	resp, err := dc.RequestWith(ctx, good)
	if err != nil {
		t.Fatal(err)
	}
	answers.WriteString(fmt.Sprintf("%+v", resp))
	configs := 0
	for _, r := range resp.Resources {
		for _, c := range r.Configs {
			configs++
			low := strings.ToLower(c.Name)
			if (strings.Contains(low, "password") || strings.Contains(low, "key") || strings.Contains(low, "jaas")) && c.Value != nil && *c.Value != "" {
				t.Errorf("DescribeConfigs gives a value for %s on %s %q", c.Name, r.ResourceType, r.ResourceName)
			}
		}
	}
	if len(resp.Resources) != 2 || configs == 0 {
		t.Errorf("DescribeConfigs answered %d resources and %d configs: nothing was read", len(resp.Resources), configs)
	}

	// Malformed SASL input, a request before authentication, a frame that is no
	// request, and the handler that panics.
	for _, bad := range malformedSASL() {
		c := in.dial(t)
		if bad.mechanism != "" {
			answers.WriteString(exchange(t, c, saslHandshakeFrame(bad.id, bad.mechanism)))
		}
		for i, token := range bad.tokens {
			declared := len(token)
			if i == len(bad.tokens)-1 && bad.declared != 0 {
				declared = bad.declared
			}
			answers.WriteString(exchange(t, c, saslAuthenticateFrame(bad.id, token, declared)))
		}
		c.Close()
	}
	c := in.dial(t)
	answers.WriteString(exchange(t, c, kafkaFrame(3, 1, 1, "guard-early", []byte{0xff, 0xff, 0xff, 0xff}))) // Metadata, all topics.
	c.Close()
	c = in.dial(t)
	answers.WriteString(exchange(t, c, append([]byte{0, 0, 0, byte(len(plantedPassword))}, plantedPassword...)))
	c.Close()
	c = in.dial(t)
	answers.WriteString(exchange(t, c, saslHandshakeFrame("guard-panic", "PLAIN")))
	answers.WriteString(exchange(t, c, saslAuthenticateFrame("guard-panic", []byte("\x00alice\x00"+plantedPassword), 0)))
	c.Close()
	if panicked.Load() == 0 {
		t.Error("the handler was never made to panic")
	}
	g.check(t, "what a client is answered (logins, DescribeConfigs, malformed SASL)", answers.String())

	// The schema registry, with HTTP Basic credentials.
	for _, try := range []struct {
		what, user, pass string
		ok               bool
	}{{"the right password", "alice", plantedPassword, true}, {"a wrong password", "alice", plantedWrong, false}, {"no credentials", "", "", false}} {
		status, text := httpGet(t, in.registry+"/subjects", try.user, try.pass)
		g.check(t, "the schema registry answering "+try.what, text)
		if (status == 200) != try.ok {
			t.Errorf("the schema registry answered %d to %s", status, try.what)
		}
	}

	// The console. The Traffic inspector first, by the rows this test caused.
	for _, mech := range saslMechanisms {
		rows := in.inspectors(t, g, "client="+loginID(mech, true))
		if !anyContains(rows, "SASLAuthenticate", "AuthBytes") {
			t.Errorf("the inspector shows no SASLAuthenticate request for the %s login (%d rows)", mech, len(rows))
		}
		if !anyContains(rows, "SASLHandshake", mech) {
			t.Errorf("the inspector shows no SASLHandshake naming %s (%d rows)", mech, len(rows))
		}
		if rows := in.inspectors(t, g, "client="+loginID(mech, false)); !anyContains(rows, "SASLAuthenticate") {
			t.Errorf("the inspector shows no SASLAuthenticate request for the %s login with a wrong password (%d rows)", mech, len(rows))
		}
	}
	for _, bad := range malformedSASL() {
		if rows := in.inspectors(t, g, "client="+bad.id); !anyContains(rows, "SASLAuthenticate") {
			t.Errorf("the inspector shows no SASLAuthenticate request for %s (%d rows)", bad.id, len(rows))
		}
	}
	if rows := in.inspectors(t, g, "client=guard-panic"); !anyContains(rows, "SASLAuthenticate", "panicked") {
		t.Errorf("the inspector shows no SASLAuthenticate request that ended in the panic (%d rows)", len(rows))
	}
	in.consoleSurfaces(t, g, 40)

	if err := in.stop(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	log := in.out.String()
	g.check(t, "the "+format+" log at "+level, log)

	// Each level shows the lines of its level and above, and this test caused one of
	// each: so a level that shows nothing was read showing nothing, not skipped.
	records := logRecords(t, format, log)
	has := func(lvl, msg, also string) bool {
		return slices.ContainsFunc(records, func(r logRecord) bool {
			return strings.EqualFold(r.level, lvl) && r.msg == msg && strings.Contains(r.raw, also)
		})
	}
	for _, want := range []struct{ level, msg, also string }{
		{"debug", "request", "SASLAuthenticate"},
		{"info", "shutting down: flushing logs to disk", ""},
		{"warn", "unauthenticated request rejected", "Metadata"},
		{"warn", "bad request header", ""},
		{"error", "handler panicked; closing the connection", "guard-panic"},
	} {
		shown := slices.Index(logLevels, want.level) >= slices.Index(logLevels, level)
		if got := has(want.level, want.msg, want.also); got != shown {
			t.Errorf("the %s log at %s: the %s line %q is there = %v, want %v\n%s", format, level, want.level, want.msg, got, shown, log)
		}
	}
	if !has("error", "handler panicked; closing the connection", "stack") {
		t.Errorf("the panic's log line has no stack:\n%s", log)
	}

	dataDirSurfaces(t, g, data, "orders")

	// The same data, started again.
	again := startInstance(t, flags...)
	md, err = kmsg.NewPtrMetadataRequest().RequestWith(ctx, again.client(t, "guard-restart", "SCRAM-SHA-256", plantedPassword))
	if err != nil {
		t.Fatalf("after the restart: %v", err)
	}
	if !slices.ContainsFunc(md.Topics, func(mt kmsg.MetadataResponseTopic) bool { return mt.Topic != nil && *mt.Topic == "orders" }) {
		t.Errorf("the restarted broker does not have the topic the first one created: it did not start over the same data")
	}
	if rows := again.inspectors(t, g, "client=guard-restart"); !anyContains(rows, "SASLAuthenticate", "AuthBytes") {
		t.Errorf("after the restart the inspector shows no SASLAuthenticate request (%d rows)", len(rows))
	}
	again.consoleSurfaces(t, g, 25)
	if err := again.stop(); err != nil {
		t.Errorf("shutdown after the restart: %v", err)
	}
	g.check(t, "the "+format+" log at "+level+" after a restart", again.out.String())
	logRecords(t, format, again.out.String())
	dataDirSurfaces(t, g, data, "orders")
}

// pprofSurfaces reads every page --pprof-addr serves, from a process whose command
// line holds the password (as `doze-kafka --sasl-user alice:...` does) and whose
// broker has just checked it. Each page must answer; the one that is the command
// line must not exist.
func pprofSurfaces(t *testing.T, g guard, certFile, keyFile string) {
	in := startInstance(t, "--log-level", "debug", "--sasl-mechanisms", strings.Join(saslMechanisms, ","),
		"--sasl-user", "alice:"+plantedPassword, "--tls-cert", certFile, "--tls-key", keyFile)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, mech := range saslMechanisms {
		if _, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, in.client(t, loginID(mech, true), mech, plantedPassword)); err != nil {
			t.Fatalf("%s: %v", mech, err)
		}
	}
	args := os.Args
	os.Args = append(slices.Clone(args), "--sasl-user", "alice:"+plantedPassword)
	t.Cleanup(func() { os.Args = args })

	var log safeBuf
	ln, err := servePprof("127.0.0.1:0", slog.New(slog.NewTextHandler(&log, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	base := "http://" + ln.Addr().String() + "/debug/pprof/"
	for _, page := range []struct {
		path, want string
	}{
		{"", "goroutine"},
		{"goroutine?debug=2", "internal/protocol"}, // the broker's own goroutines: this is its process.
		{"goroutine?debug=1", "goroutine profile"},
		{"goroutine", ""},
		{"heap?debug=1", "heap profile"},
		{"heap", ""},
		{"allocs?debug=1", "heap profile"},
		{"threadcreate?debug=1", "threadcreate profile"},
		{"block?debug=1", ""},
		{"mutex?debug=1", ""},
		{"profile?seconds=1", ""},
		{"trace?seconds=1", ""},
		{"symbol", "num_symbols"},
	} {
		resp, err := http.Get(base + page.path)
		if err != nil {
			t.Fatalf("GET %s: %v", page.path, err)
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("GET %s: %v", page.path, err)
		}
		if resp.StatusCode != 200 || len(body) == 0 {
			t.Errorf("pprof's %q answered %d with %d bytes: it was not read", page.path, resp.StatusCode, len(body))
			continue
		}
		text := string(body)
		if bytes.HasPrefix(body, []byte{0x1f, 0x8b}) { // a profile is gzipped protobuf.
			zr, err := gzip.NewReader(bytes.NewReader(body))
			if err != nil {
				t.Fatalf("pprof's %q: %v", page.path, err)
			}
			plain, err := io.ReadAll(zr)
			if err != nil {
				t.Fatalf("pprof's %q: %v", page.path, err)
			}
			text = string(plain)
		}
		if !strings.Contains(text, page.want) {
			t.Errorf("pprof's %q is not the page expected (no %q in it)", page.path, page.want)
		}
		g.check(t, "pprof's "+page.path, text)
	}

	// /debug/pprof/cmdline is the command line, and the command line has the password.
	status, text := httpGet(t, base+"cmdline", "", "")
	g.check(t, "pprof's cmdline", text)
	if status != 404 {
		t.Errorf("pprof's cmdline answered %d, want 404: the command line is where --sasl-user is", status)
	}
	if err := in.stop(); err != nil {
		t.Errorf("shutdown: %v", err)
	}
	g.check(t, "the log of the pprof server", log.String())
	if !strings.Contains(log.String(), "pprof listening") {
		t.Errorf("the pprof server logged nothing:\n%s", log.String())
	}
}

// commandSurfaces reads what the commands print for a configuration with credentials,
// given as flags, as a named file and as the doze-kafka.toml of the working directory:
// env, doctor, config print, --help, help, version, and each one's refusal of a file
// with a password in the wrong place.
func commandSurfaces(t *testing.T, g guard, certFile, keyFile string) {
	dir := t.TempDir()
	t.Chdir(dir)
	flags := []string{"--listen", "127.0.0.1:9092", "--sasl-mechanisms", strings.Join(saslMechanisms, ","),
		"--sasl-user", "alice:" + plantedPassword, "--tls-cert", certFile, "--tls-key", keyFile}

	// The file to run from is what --show-secrets writes, and it does hold the password.
	code, withSecrets, errOut := run3(append([]string{"config", "print", "--show-secrets"}, flags...)...)
	if code != 0 || !strings.Contains(withSecrets, plantedPassword) {
		t.Fatalf("config print --show-secrets: %d %s %s", code, withSecrets, errOut)
	}
	g.check(t, "the stderr of config print --show-secrets", errOut)
	named := filepath.Join(dir, "named.toml")
	if err := os.WriteFile(named, []byte(withSecrets), 0o600); err != nil {
		t.Fatal(err)
	}
	inCwd := filepath.Join(t.TempDir(), "cwd")
	if err := os.MkdirAll(inCwd, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(inCwd, "doze-kafka.toml"), []byte(withSecrets), 0o600); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(dir, "bad.toml")
	if err := os.WriteFile(bad, []byte("[credentials]\nalice = "+plantedPassword+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, how := range []struct {
		what string
		args []string
		cwd  string
	}{{"flags", flags, dir}, {"a named file", []string{"--config", named}, dir}, {"the working directory's file", nil, inCwd}} {
		t.Chdir(how.cwd)
		for _, cmd := range []struct {
			args []string
			want []string
		}{
			{[]string{"env"}, []string{"export KAFKA_BOOTSTRAP_SERVERS=", "KAFKA_SASL_MECHANISM=PLAIN", "SASL is on"}},
			{[]string{"doctor"}, []string{"this instance", "answers on  127.0.0.1:9092"}},
			{[]string{"config", "print"}, []string{`alice = "<redacted>"`, "sasl-mechanisms"}},
			{[]string{"--help"}, []string{"Usage:", "--sasl-user"}},
			{[]string{"env", "--help"}, []string{"Usage:", "--sasl-user"}},
			{[]string{"doctor", "--help"}, []string{"Usage:", "--sasl-user"}},
			{[]string{"config", "print", "--help"}, []string{"Usage:", "--sasl-user"}},
		} {
			// --help comes last, after the configuration it is asked about.
			args := slices.Clone(cmd.args)
			if n := len(args); args[n-1] == "--help" {
				args = append(append(args[:n-1:n-1], how.args...), "--help")
			} else {
				args = append(args, how.args...)
			}
			name := "`" + strings.Join(cmd.args, " ") + "` with " + how.what
			code, out, errOut := run3(args...)
			g.check(t, name, out+errOut)
			if code != 0 || errOut != "" {
				t.Errorf("%s: exit %d, stderr %q", name, code, errOut)
			}
			for _, want := range cmd.want {
				if !strings.Contains(out, want) {
					t.Errorf("%s does not print %q, so it is not the output expected:\n%s", name, want, out)
				}
			}
		}
	}
	t.Chdir(dir)
	for _, cmd := range [][]string{{"help"}, {"version"}, {"--version"}} {
		code, out, errOut := run3(cmd...)
		g.check(t, "`"+strings.Join(cmd, " ")+"`", out+errOut)
		if code != 0 || out == "" {
			t.Errorf("%v: exit %d, stdout %q", cmd, code, out)
		}
	}

	// A file with the password where it cannot be read: every command that loads it
	// refuses it, and says so without the password.
	for _, cmd := range [][]string{{"env"}, {"doctor"}, {"config", "print"}, {"config", "print", "--show-secrets"}, {}} {
		name := "`" + strings.Join(cmd, " ") + "` refusing a bad file"
		code, out, errOut := run3(append(slices.Clone(cmd), "--config", bad)...)
		g.check(t, name, out+errOut)
		if code != exitUsage || !strings.Contains(errOut, "bad.toml") {
			t.Errorf("%s: exit %d, stderr %q", name, code, errOut)
		}
	}
}
