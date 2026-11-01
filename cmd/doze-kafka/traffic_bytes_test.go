package main

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"html"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// The Traffic page, its inspector and its clients view never hold what a client sent
// as data: not a record's key, its value, a header's name or value, and not the
// password of a SASL/PLAIN login, which travels in a request like any other. The
// recorder is held to this where it describes a request
// (internal/protocol: TestFactsNeverHoldRecordBytesOrSecrets); this is the same
// property where a person would see it broken: the real binary, real clients, every
// page of Traffic that a row or a client can open.
//
// Records are written plain and under each compression codec, and read back by a
// consumer in a group, so the bytes cross the broker in a Produce request and again in
// a Fetch response. Each has a key, a value and a header that are found nowhere else.
func TestInspectorNeverContainsRecordBytes(t *testing.T) {
	const password = "pw-Zq7-never-shown-91c"
	out, _ := start(t, "--sasl-mechanisms", "PLAIN", "--sasl-user", "app:"+password)
	waitFor(t, "the summary", func() bool { return strings.Contains(out.String(), "doze-kafka is up") })
	kafka := regexp.MustCompile(`kafka     (\S+)`).FindStringSubmatch(out.String())[1]
	base := regexp.MustCompile(`console   (http://\S+)`).FindStringSubmatch(out.String())[1]
	login := kgo.SASL(plain.Auth{User: "app", Pass: password}.AsMechanism())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	needles := map[string]string{"the SASL password": password}
	codecs := map[string]kgo.CompressionCodec{
		"none": kgo.NoCompression(), "gzip": kgo.GzipCompression(), "snappy": kgo.SnappyCompression(),
		"lz4": kgo.Lz4Compression(), "zstd": kgo.ZstdCompression(),
	}
	for name, codec := range codecs {
		key, value := "key-"+name+"-4d1f8a", "value-"+name+"-b27e03-"+strings.Repeat(name, 8)
		headerName, headerValue := "hdr-"+name+"-9a6c", "hval-"+name+"-e5507b"
		needles["the key of the "+name+" record"] = key
		needles["the value of the "+name+" record"] = value
		needles["the header name of the "+name+" record"] = headerName
		needles["the header value of the "+name+" record"] = headerValue

		cl, err := kgo.NewClient(kgo.SeedBrokers(kafka), login, kgo.ClientID("writer-"+name),
			kgo.ProducerBatchCompression(codec), kgo.AllowAutoTopicCreation())
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		rec := &kgo.Record{Topic: "bytes-" + name, Key: []byte(key), Value: []byte(value),
			Headers: []kgo.RecordHeader{{Key: headerName, Value: []byte(headerValue)}}}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("%s: produce: %v", name, err)
		}
	}

	// Read them all back through a group, and commit: Fetch responses carry the same
	// bytes, and a commit's metadata is a client's too.
	topics := make([]string, 0, len(codecs))
	for name := range codecs {
		topics = append(topics, "bytes-"+name)
	}
	reader, err := kgo.NewClient(kgo.SeedBrokers(kafka), login, kgo.ClientID("reader"), kgo.ConsumerGroup("bytes-readers"),
		kgo.ConsumeTopics(topics...), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), kgo.SessionTimeout(6*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for read := 0; read < len(codecs); {
		fetches := reader.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("read %d of %d records back", read, len(codecs))
		}
		read += fetches.NumRecords()
	}
	if err := reader.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatal(err)
	}

	// Every page of Traffic, and everything a row or a client on them opens.
	pages := map[string]string{}
	get := func(path string) string {
		if _, ok := pages[path]; !ok {
			pages[path] = consoleGet(t, base, path)
		}
		return pages[path]
	}
	var all string
	waitFor(t, "the produces and fetches to be listed", func() bool {
		delete(pages, "/_console/traffic")
		delete(pages, "/_console/traffic?kind=produce")
		delete(pages, "/_console/traffic?kind=fetch")
		all = get("/_console/traffic")
		rows := func(path string) int { return strings.Count(get(path), `id="rq-`) }
		return rows("/_console/traffic?kind=produce") >= len(codecs) && rows("/_console/traffic?kind=fetch") >= 1
	})
	for _, path := range []string{
		"/_console/traffic/live", "/_console/traffic?view=clients", "/_console/traffic?kind=produce", "/_console/traffic?kind=fetch",
		"/_console/traffic?kind=groups", "/_console/traffic?kind=errors", "/_console/traffic?group=bytes-readers",
		"/_console/traffic?group=bytes-readers&flat=1", "/_console/traffic?topic=bytes-zstd", "/_console/traffic?q=writer",
		"/_console/groups/bytes-readers?tab=timeline",
	} {
		get(path)
	}
	opened := map[string]int{}
	link := regexp.MustCompile(`(/_console/inspect/(request|client)\?key=[^"&\s]+)`)
	for _, page := range []string{all, get("/_console/traffic?view=clients"), get("/_console/traffic?kind=fetch"), get("/_console/traffic?group=bytes-readers")} {
		for _, m := range link.FindAllStringSubmatch(page, -1) {
			path := html.UnescapeString(m[1])
			if _, seen := pages[path]; seen {
				continue
			}
			opened[m[2]]++
			get(path)
			if m[2] == "request" {
				get(path + "&unfold=1")
			} else {
				get(path + "&all=1") // every metric a client reports about itself.
			}
		}
	}
	if opened["request"] < 2*len(codecs) || opened["client"] < len(codecs) {
		t.Fatalf("opened %d requests and %d clients in the inspector: the pages did not list what was sent", opened["request"], opened["client"])
	}

	// The inspector did describe the records: what it says of them is their size.
	described := 0
	for path, body := range pages {
		if strings.Contains(path, "inspect/request") && strings.Contains(body, "1 batch, 1 record") {
			described++
		}
	}
	if described < len(codecs) {
		t.Errorf("%d inspectors describe a batch of one record, want one for each of the %d produces", described, len(codecs))
	}

	for what, needle := range needles {
		forms := map[string]string{
			"as it is": needle, "in base64": base64.StdEncoding.EncodeToString([]byte(needle)),
			"in hex": hex.EncodeToString([]byte(needle)), "escaped for a URL": url.QueryEscape(needle),
		}
		for path, body := range pages {
			for form, s := range forms {
				if strings.Contains(body, s) || strings.Contains(strings.ToLower(body), strings.ToLower(s)) {
					t.Errorf("%s is on %s, %s", what, path, form)
				}
			}
		}
	}
}
