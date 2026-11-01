package console

import (
	"context"
	"encoding/json"
	"html"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"
)

// confirmOn is the text of the confirm a page shows for one of its controls: the
// hx-confirm of the first element whose attributes mention hxPost. It is the
// sentence the person reads, as the browser would hand it to the dialog.
func confirmOn(t *testing.T, page, hxPost string) string {
	t.Helper()
	for _, m := range regexp.MustCompile(`(?s)<(?:button|form)[^>]*>`).FindAllString(page, -1) {
		if !strings.Contains(m, `hx-post="`+hxPost+`"`) {
			continue
		}
		if cm := regexp.MustCompile(`hx-confirm="([^"]*)"`).FindStringSubmatch(m); cm != nil {
			return html.UnescapeString(cm[1])
		}
	}
	t.Fatalf("no confirm on a control that posts to %s", hxPost)
	return ""
}

// numberBefore finds the integer written just before word in s ("3 partitions").
func numberBefore(t *testing.T, s, word string) int {
	t.Helper()
	m := regexp.MustCompile(`(\d+) ` + regexp.QuoteMeta(word)).FindStringSubmatch(s)
	if m == nil {
		t.Fatalf("no number before %q in %q", word, s)
	}
	n, _ := strconv.Atoi(m[1])
	return n
}

func offsetsOf(t *testing.T, c *Console, group string) map[string]int64 {
	t.Helper()
	offs, err := c.be.GroupOffsets(context.Background(), group)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for _, o := range offs {
		out[o.Topic+"/"+itoa(int(o.Partition))] = o.Committed
	}
	return out
}

// Each destructive action is performed against a real broker and what happened is
// held against what the confirm said would. A sentence that is true because it was
// measured, not because it was written carefully.
func TestEveryConfirmIsTrueOfWhatTheActionDoes(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	ctx := context.Background()

	t.Run("topic.delete", func(t *testing.T) {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"doomed"}, "partitions": {"3"}})
		for p := 0; p < 3; p++ {
			produceTo(t, c, "doomed", p, 4)
		}
		commitFor(t, addr, "doomed-a", "doomed", map[int32]int64{0: 1})
		commitFor(t, addr, "doomed-b", "doomed", map[int32]int64{1: 2})
		say := confirmOn(t, req(t, c, "GET", "/_console/topics/doomed", nil).Body.String(), "/_console/topics/doomed/delete")
		if n := numberBefore(t, say, "partition logs"); n != 3 {
			t.Errorf("says %d partitions: %s", n, say)
		}
		if n := numberBefore(t, say, "records"); n != 12 {
			t.Errorf("says %d records: %s", n, say)
		}
		groups := 0
		for _, g := range []string{"doomed-a", "doomed-b"} {
			if strings.Contains(say, "offsets of doomed-a and doomed-b for this topic are deleted") {
				groups++
			} else {
				t.Errorf("does not say %s loses its offsets: %s", g, say)
			}
		}
		// What it says happens: the topic is gone, and the two groups lose their offsets on it.
		held := 0
		for _, g := range []string{"doomed-a", "doomed-b"} {
			for k := range offsetsOf(t, c, g) {
				if strings.HasPrefix(k, "doomed/") {
					held++
				}
			}
		}
		if held != 2 {
			t.Fatalf("before the delete, %d offsets were held, want 2", held)
		}
		if rec := req(t, c, "POST", "/_console/topics/doomed/delete", nil); rec.Code != 303 {
			t.Fatalf("delete: %d %s", rec.Code, rec.Body)
		}
		if _, err := c.be.DescribeTopic(ctx, "doomed"); err == nil {
			t.Error("the topic is still there")
		}
		lost := 0
		for _, g := range []string{"doomed-a", "doomed-b"} {
			gone := true
			for k := range offsetsOf(t, c, g) {
				if strings.HasPrefix(k, "doomed/") {
					gone = false
				}
			}
			if gone {
				lost++
			}
		}
		if lost != groups {
			t.Errorf("%d groups lost their offsets on the topic; the confirm said %d groups would", lost, groups)
		}
	})

	t.Run("topic.purge", func(t *testing.T) {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"purgeme"}, "partitions": {"1"}})
		produceTo(t, c, "purgeme", 0, 12)
		commitFor(t, addr, "purge-behind", "purgeme", map[int32]int64{0: 4})
		commitFor(t, addr, "purge-caught-up", "purgeme", map[int32]int64{0: 12})
		say := confirmOn(t, req(t, c, "GET", "/_console/topics/purgeme", nil).Body.String(), "/_console/topics/purgeme/purge")
		if n := numberBefore(t, say, "records"); n != 12 {
			t.Errorf("says %d records: %s", n, say)
		}
		if !strings.Contains(say, "purge-behind") || strings.Contains(say, "purge-caught-up") {
			t.Errorf("names the wrong groups as behind: %s", say)
		}
		lagM := regexp.MustCompile(`purge-behind \(lag (\d+)\)`).FindStringSubmatch(say)
		if lagM == nil {
			t.Fatalf("does not say purge-behind's lag: %s", say)
		}
		unread, _ := strconv.Atoi(lagM[1])
		behind := strings.Count(say, "(lag ")
		// Measured before: what the behind group has not read; after: where the log now starts.
		before := offsetsOf(t, c, "purge-behind")["purgeme/0"]
		if rec := req(t, c, "POST", "/_console/topics/purgeme/purge", nil); rec.Code != 303 {
			t.Fatalf("purge: %d", rec.Code)
		}
		b, err := c.be.PartitionBounds(ctx, "purgeme", 1)
		if err != nil {
			t.Fatal(err)
		}
		if b[0].Count() != 0 {
			t.Errorf("%d records remain", b[0].Count())
		}
		lost := b[0].Earliest - before // records between its committed offset and the new start.
		if int(lost) != unread || behind != 1 {
			t.Errorf("the confirm said %d group(s) would miss %d records; %d were missed", behind, unread, lost)
		}
		if after := offsetsOf(t, c, "purge-caught-up")["purgeme/0"]; after != 12 {
			t.Errorf("a group the confirm did not name lost its offset: %d", after)
		}
	})

	t.Run("topic.partitions", func(t *testing.T) {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"grow"}, "partitions": {"3"}})
		say := confirmOn(t, req(t, c, "GET", "/_console/topics/grow?tab=partitions", nil).Body.String(), "/_console/topics/grow/partitions")
		now := numberBefore(t, say, "partitions now")
		if now != 3 {
			t.Errorf("says it has %d: %s", now, say)
		}
		if !strings.Contains(say, "cannot be removed") {
			t.Errorf("does not say it is permanent: %s", say)
		}
		if rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {"2"}}); rec.Code >= 400 {
			t.Fatalf("add: %d %s", rec.Code, rec.Body)
		}
		d, err := c.be.DescribeTopic(ctx, "grow")
		if err != nil || int(d.Partitions) != now+2 {
			t.Errorf("partitions after = %v (%v), want %d", d, err, now+2)
		}
		// It cannot be removed: asking for fewer is refused.
		if rec := req(t, c, "POST", "/_console/topics/grow/partitions", url.Values{"add": {"-1"}}); rec.Code < 400 {
			t.Error("a shrink was accepted")
		}
	})

	t.Run("group.delete", func(t *testing.T) {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"gd1"}, "partitions": {"2"}})
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"gd2"}, "partitions": {"1"}})
		commitFor(t, addr, "gd", "gd1", map[int32]int64{0: 0, 1: 0})
		commitFor(t, addr, "gd", "gd2", map[int32]int64{0: 0})
		say := confirmOn(t, req(t, c, "GET", "/_console/groups/gd", nil).Body.String(), "/_console/groups/gd/delete")
		n, topics := numberBefore(t, say, "committed offsets"), numberBefore(t, say, "topics")
		if got := len(offsetsOf(t, c, "gd")); got != n || topics != 2 {
			t.Errorf("says %d offsets on %d topics; the group holds %d: %s", n, topics, got, say)
		}
		if rec := req(t, c, "POST", "/_console/groups/gd/delete", nil); rec.Code != 303 {
			t.Fatalf("delete: %d %s", rec.Code, rec.Body)
		}
		if got := len(offsetsOf(t, c, "gd")); got != 0 {
			t.Errorf("%d offsets survive the deletion", got)
		}
	})

	reset := func(t *testing.T, to string) {
		topic, group := "reset-"+to, "rg-"+to
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {topic}, "partitions": {"3"}})
		for p := 0; p < 3; p++ {
			produceTo(t, c, topic, p, 6)
		}
		// Committed at 2, at the start (0), and at the end (6): moving to earliest changes two, to latest two.
		commitFor(t, addr, group, topic, map[int32]int64{0: 2, 1: 0, 2: 6})
		before := offsetsOf(t, c, group)
		say := confirmOn(t, req(t, c, "GET", "/_console/groups/"+group+"?tab=offsets", nil).Body.String(), "/_console/groups/"+group+"/reset")
		_ = say
		var found string
		for _, m := range regexp.MustCompile(`(?s)<button[^>]*>`).FindAllString(req(t, c, "GET", "/_console/groups/"+group+"?tab=offsets", nil).Body.String(), -1) {
			if strings.Contains(m, `hx-vals='{"to":"`+to+`"`) {
				found = html.UnescapeString(regexp.MustCompile(`hx-confirm="([^"]*)"`).FindStringSubmatch(m)[1])
			}
		}
		if found == "" {
			t.Fatalf("no confirm for reset to %s", to)
		}
		moves, of := numberBefore(t, found, "offsets of its"), 0
		if m := regexp.MustCompile(`of its (\d+)`).FindStringSubmatch(found); m != nil {
			of, _ = strconv.Atoi(m[1])
		}
		rec := req(t, c, "POST", "/_console/groups/"+group+"/reset", url.Values{"to": {to}})
		if rec.Code != 200 {
			t.Fatalf("reset: %d %s", rec.Code, rec.Body)
		}
		after := offsetsOf(t, c, group)
		changed := 0
		for k, v := range after {
			if v != before[k] {
				changed++
			}
		}
		if changed != moves || len(before) != of {
			t.Errorf("reset to %s: the confirm said %d of %d offsets move; %d of %d moved (%s)", to, moves, of, changed, len(before), found)
		}
		want := int64(0)
		if to == "latest" {
			want = 6
		}
		for k, v := range after {
			if v != want {
				t.Errorf("reset to %s: %s is at %d, want %d", to, k, v, want)
			}
		}
	}
	t.Run("group.reset.earliest", func(t *testing.T) { reset(t, "earliest") })
	t.Run("group.reset.latest", func(t *testing.T) { reset(t, "latest") })

	t.Run("group.offset.delete", func(t *testing.T) {
		req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"od"}, "partitions": {"2"}})
		produceTo(t, c, "od", 0, 5)
		commitFor(t, addr, "od-g", "od", map[int32]int64{0: 3, 1: 0})
		var say string
		for _, m := range regexp.MustCompile(`(?s)<button[^>]*>`).FindAllString(req(t, c, "GET", "/_console/groups/od-g?tab=offsets", nil).Body.String(), -1) {
			if strings.Contains(m, `/offset/delete"`) && strings.Contains(m, `"partition":"0"`) {
				say = html.UnescapeString(regexp.MustCompile(`hx-confirm="([^"]*)"`).FindStringSubmatch(m)[1])
			}
		}
		if !strings.Contains(say, "od partition 0") || !strings.Contains(say, "it is at 3") {
			t.Fatalf("the confirm does not name the offset it removes: %q", say)
		}
		if rec := req(t, c, "POST", "/_console/groups/od-g/offset/delete", url.Values{"topic": {"od"}, "partition": {"0"}}); rec.Code != 200 {
			t.Fatalf("remove: %d %s", rec.Code, rec.Body)
		}
		got := offsetsOf(t, c, "od-g")
		if _, there := got["od/0"]; there || len(got) != 1 {
			t.Errorf("offsets after = %v: partition 0 should be gone and only partition 1 kept", got)
		}
	})

	t.Run("schema.delete and schema.version.delete", func(t *testing.T) {
		register(t, c, "evolving", userV1)
		register(t, c, "evolving", userV2)
		register(t, c, "evolving", strings.Replace(userV2, `"email"`, `"phone"`, 1))
		page := req(t, c, "GET", "/_console/schemas/evolving", nil).Body.String()
		verSay := confirmOn(t, page, "/_console/schemas/evolving/delete") // the first control is the subject's.
		if n := numberBefore(t, verSay, "versions"); n != 3 {
			t.Errorf("says %d versions: %s", n, verSay)
		}
		// The version on show (the latest) goes first: the confirm says which becomes the latest.
		var vsay string
		for _, m := range regexp.MustCompile(`(?s)<button[^>]*>`).FindAllString(page, -1) {
			if strings.Contains(m, `hx-vals='{"version":"3"}'`) {
				vsay = html.UnescapeString(regexp.MustCompile(`hx-confirm="([^"]*)"`).FindStringSubmatch(m)[1])
			}
		}
		if !strings.Contains(vsay, "Delete version 3") || !strings.Contains(vsay, "version 2 becomes the latest") || numberBefore(t, vsay, "stay") != 2 {
			t.Fatalf("version confirm: %q", vsay)
		}
		if rec := req(t, c, "POST", "/_console/schemas/evolving/delete", url.Values{"version": {"3"}}); rec.Code != 303 {
			t.Fatalf("delete version: %d %s", rec.Code, rec.Body)
		}
		versions, err := c.reg.Versions(ctx, "evolving")
		if err != nil || len(versions) != 2 || versions[len(versions)-1] != 2 {
			t.Errorf("versions after = %v (%v), want [1 2]", versions, err)
		}
		if rec := req(t, c, "POST", "/_console/schemas/evolving/delete", nil); rec.Code != 303 {
			t.Fatalf("delete subject: %d", rec.Code)
		}
		if _, err := c.reg.Versions(ctx, "evolving"); err == nil {
			t.Error("the subject is still there")
		}
	})
}

// A confirm's words are not typed into a template, and a destructive route that
// is not in the table is not allowed: the table is where the sentences live.
func TestEveryDestructiveRouteHasAConfirm(t *testing.T) {
	inTable := map[string]bool{}
	for _, s := range confirmTable {
		inTable[s.Path] = true
		if s.Say == nil {
			t.Fatalf("confirm %s is incomplete", s.ID)
		}
		// Rendered with nothing measured, a confirm still has a title, a verb on its
		// button, and names the thing it acts on.
		say := s.Say(confirmFacts{Name: "the-thing", Topic: "t"})
		if say.Title == "" || say.Verb == "" || !strings.Contains(say.Text(), "the-thing") {
			t.Errorf("confirm %s does not say what it does to what: %+v", s.ID, say)
		}
	}
	destructive := regexp.MustCompile(`/(?:topics|groups|schemas)/\{\w+\}/[a-z/]*(?:delete|purge|reset|partitions)[a-z/]*`)
	files, _ := filepath.Glob("*.go")
	found := map[string]bool{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") || f == "confirms.go" {
			continue
		}
		b, _ := os.ReadFile(f)
		for _, m := range destructive.FindAllString(string(b), -1) {
			found[m] = true
		}
	}
	if len(found) < 6 {
		t.Fatalf("found only %d destructive routes in the sources: %v", len(found), found)
	}
	for route := range found {
		if !inTable[route] {
			t.Errorf("the destructive route %s has no entry in confirmTable", route)
		}
	}

	// No template carries a confirm sentence of its own.
	tmpls, _ := filepath.Glob("templates/*.html")
	lit := regexp.MustCompile(`hx-confirm="([^"]*)"`)
	for _, f := range tmpls {
		b, _ := os.ReadFile(f)
		for _, m := range lit.FindAllStringSubmatch(string(b), -1) {
			if !strings.HasPrefix(m[1], "{{") {
				t.Errorf("%s has a confirm sentence typed into the template: %q (it comes from confirmTable)", f, m[1])
			}
		}
	}
}

// The sentence a page was drawn with is as old as the page, and a topic gains
// records: so the dialog asks the console for it when the button is pressed, and
// what it is told is true of the broker now.
func TestTheConfirmIsMeasuredWhenTheButtonIsPressed(t *testing.T) {
	c, addr, _ := newRegistryConsole(t)
	req(t, c, "POST", "/_console/new/topic", url.Values{"name": {"fresh"}, "partitions": {"1"}})
	page := req(t, c, "GET", "/_console/topics/fresh", nil).Body.String()
	if drawn := confirmOn(t, page, "/_console/topics/fresh/purge"); numberBefore(t, drawn, "records") != 0 {
		t.Fatalf("drawn: %s", drawn)
	}
	// The button names where its sentence is measured.
	if !strings.Contains(page, `data-confirm-url="/_console/confirm/topic.purge?name=fresh"`) {
		t.Error("the purge button does not say where to ask for its sentence")
	}
	produceTo(t, c, "fresh", 0, 5)
	commitFor(t, addr, "fresh-reader", "fresh", map[int32]int64{0: 2})

	rec := req(t, c, "GET", "/_console/confirm/topic.purge?name=fresh", nil)
	if rec.Code != 200 {
		t.Fatalf("%d %s", rec.Code, rec.Body)
	}
	var got struct{ Text, Title, Verb string }
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if numberBefore(t, got.Text, "records") != 5 || !strings.Contains(got.Text, "fresh-reader (lag 3)") {
		t.Errorf("the sentence is not of the topic as it is now: %s", got.Text)
	}
	if got.Title != "Purge topic fresh" || got.Verb != "Purge 5 records" {
		t.Errorf("title/verb = %q/%q", got.Title, got.Verb)
	}
	// Every confirm of the table can be asked for, and a thing that is not there is refused.
	for path, code := range map[string]int{
		"/_console/confirm/topic.delete?name=fresh":                                        200,
		"/_console/confirm/topic.partitions?name=fresh":                                    200,
		"/_console/confirm/group.delete?group=fresh-reader":                                200,
		"/_console/confirm/group.reset.earliest?group=fresh-reader":                        200,
		"/_console/confirm/group.reset.latest?group=fresh-reader":                          200,
		"/_console/confirm/group.offset.delete?group=fresh-reader&topic=fresh&partition=0": 200,
		"/_console/confirm/topic.purge?name=ghost":                                         404,
		"/_console/confirm/nonsense":                                                       404,
		"/_console/confirm/group.offset.delete?group=fresh-reader&topic=fresh&partition=9": 404,
	} {
		if r := req(t, c, "GET", path, nil); r.Code != code {
			t.Errorf("%s answered %d, want %d", path, r.Code, code)
		}
	}
	register(t, c, "fresh-value", userV1)
	register(t, c, "fresh-value", userV2)
	if r := req(t, c, "GET", "/_console/confirm/schema.version.delete?subject=fresh-value&version=2", nil); r.Code != 200 || !strings.Contains(r.Body.String(), "version 1 becomes the latest") {
		t.Errorf("schema.version.delete: %d %s", r.Code, r.Body)
	}
	if r := req(t, c, "GET", "/_console/confirm/schema.delete?subject=fresh-value", nil); r.Code != 200 || !strings.Contains(r.Body.String(), "all 2 versions") {
		t.Errorf("schema.delete: %d %s", r.Code, r.Body)
	}
}
