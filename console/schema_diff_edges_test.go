package console

import (
	"regexp"
	"strings"
	"testing"
)

// Three versions of a subject, each changing a few lines of the one before.
const (
	diffMixV1 = `{"type":"record","name":"M","fields":[{"name":"id","type":"long"},{"name":"a","type":"string","default":""},{"name":"b","type":"long"}]}`
	diffMixV2 = `{"type":"record","name":"M","fields":[{"name":"id","type":"long"},{"name":"c","type":"string","default":""},{"name":"b","type":"long"},{"name":"d","type":"long","default":0}]}`
	diffMixV3 = `{"type":"record","name":"M","fields":[{"name":"id","type":"long"},{"name":"c","type":"string","default":""},{"name":"b","type":"long"},{"name":"d","type":"long","default":0},{"name":"e","type":"long","default":0}]}`
)

func diffMixConsole(t *testing.T) *Console {
	t.Helper()
	c, _, _ := newRegistryConsole(t)
	register(t, c, "mix-value", diffMixV1)
	register(t, c, "mix-value", diffMixV2)
	register(t, c, "mix-value", diffMixV3)
	return c
}

func diffPage(t *testing.T, c *Console, subject, query string) string {
	t.Helper()
	rec := req(t, c, "GET", "/_console/schemas/"+subject+"/diff"+query, nil)
	if rec.Code != 200 {
		t.Fatalf("diff %s: %d %s", query, rec.Code, rec.Body)
	}
	return rec.Body.String()
}

var (
	diffPairRe       = regexp.MustCompile(`<span class="m">(v\d+ to v\d+)</span>`)
	diffOlderLinkRe  = regexp.MustCompile(`<a href="([^"]*)" data-pair-older hidden>`)
	diffNewerLinkRe  = regexp.MustCompile(`<a href="([^"]*)" data-pair-newer hidden>`)
	diffUnifiedRowRe = regexp.MustCompile(`<div class="r dl-(same|del|add)"><span class="m mut">(-?\d+)</span>`)
)

// The page names the pair it compares, whatever ?from and ?to ask: a pair that is not
// the subject's falls back to the latest and the one before it, and the first version
// as the newer one is compared with the one after it.
func TestTheDiffPageNamesThePairItCompares(t *testing.T) {
	c := diffMixConsole(t)
	for _, tc := range []struct{ query, want string }{
		{"", "v2 to v3"},
		{"?from=1&to=3", "v1 to v3"},
		{"?from=1", "v1 to v3"},
		{"?to=1", "v2 to v1"},
		{"?from=3&to=3", "v2 to v3"},
		{"?from=2&to=2", "v1 to v2"},
		{"?from=1&to=1", "v2 to v1"},
	} {
		m := diffPairRe.FindStringSubmatch(diffPage(t, c, "mix-value", tc.query))
		if m == nil || m[1] != tc.want {
			t.Errorf("diff%s compares %v, want %s", tc.query, m, tc.want)
		}
	}
	// A subject with one version has nothing to compare it with: it is compared with itself.
	register(t, c, "solo-value", diffMixV1)
	for _, q := range []string{"", "?to=1", "?from=1&to=1"} {
		if m := diffPairRe.FindStringSubmatch(diffPage(t, c, "solo-value", q)); m == nil || m[1] != "v1 to v1" {
			t.Errorf("a subject with one version, diff%s: %v", q, m)
		}
	}
}

// The older and newer pair links step along the versions, and keep the layout.
func TestTheDiffPageLinksTheOlderAndNewerPair(t *testing.T) {
	c := diffMixConsole(t)
	const base = "/_console/schemas/mix-value/diff?from="
	link := func(re *regexp.Regexp, body string) string {
		m := re.FindStringSubmatch(body)
		if m == nil {
			return ""
		}
		return strings.ReplaceAll(m[1], "&amp;", "&")
	}
	for _, tc := range []struct{ query, older, newer string }{
		{"?from=1&to=2", "", base + "2&to=3"},
		{"?from=2&to=3", base + "1&to=2", ""},
		{"?from=1&to=3", "", ""},
		{"?from=2&to=1", base + "1&to=2", base + "1&to=2"},
		{"?to=1", base + "1&to=2", base + "1&to=2"},
		{"?from=1&to=2&layout=unified", "", base + "2&to=3&layout=unified"},
		{"?from=2&to=3&layout=unified", base + "1&to=2&layout=unified", ""},
	} {
		body := diffPage(t, c, "mix-value", tc.query)
		if got := link(diffOlderLinkRe, body); got != tc.older {
			t.Errorf("diff%s: older pair %q, want %q", tc.query, got, tc.older)
		}
		if got := link(diffNewerLinkRe, body); got != tc.newer {
			t.Errorf("diff%s: newer pair %q, want %q", tc.query, got, tc.newer)
		}
	}
}

// Side by side is the default; ?layout=unified is one column. The tabs under the facts
// leave the Versions tab as the one that is on, and the others go to their own tab.
func TestTheDiffPageIsSideBySideUnlessUnifiedIsAsked(t *testing.T) {
	c := diffMixConsole(t)
	side := diffPage(t, c, "mix-value", "?from=1&to=2")
	if !strings.Contains(side, `id="diff-table" style="--c: 36px 1fr 36px 1fr"`) || strings.Contains(side, `<input type="hidden" name="layout"`) {
		t.Error("the default layout is not side by side")
	}
	uni := diffPage(t, c, "mix-value", "?from=1&to=2&layout=unified")
	if !strings.Contains(uni, `id="diff-table" style="--c: 36px 1fr"`) || !strings.Contains(uni, `<input type="hidden" name="layout" value="unified">`) {
		t.Error("?layout=unified is not one column")
	}
	if other := diffPage(t, c, "mix-value", "?from=1&to=2&layout=side"); !strings.Contains(other, `style="--c: 36px 1fr 36px 1fr"`) {
		t.Error("a layout that is not unified is side by side")
	}
	for _, body := range []string{side, uni} {
		for _, want := range []string{
			`<a class="tab on" href="/_console/schemas/mix-value" aria-current="page">Versions</a>`,
			`<a class="tab " href="/_console/schemas/mix-value?tab=compat">Compatibility</a>`,
			`<a class="tab " href="/_console/schemas/mix-value?tab=used">Used by</a>`,
			`<a class="tab " href="/_console/schemas/mix-value?tab=raw">Raw</a>`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("the tabs lack %s", want)
			}
		}
	}
}

// The unified column numbers a removed line by the older version and the rest by the
// newer one; each side counts every line it has.
func TestTheUnifiedDiffNumbersLinesByTheirVersion(t *testing.T) {
	c := diffMixConsole(t)
	var got []string
	for _, m := range diffUnifiedRowRe.FindAllStringSubmatch(diffPage(t, c, "mix-value", "?from=1&to=2&layout=unified"), -1) {
		got = append(got, m[1]+m[2])
	}
	if want := "same1 same2 del3 del4 add3 add4 add5 same6"; strings.Join(got, " ") != want {
		t.Errorf("unified rows: %s, want %s", strings.Join(got, " "), want)
	}
}

// Each side's heading says when that version was registered.
func TestTheDiffHeadingSaysWhenEachVersionWasRegistered(t *testing.T) {
	c := diffMixConsole(t)
	head := between(diffPage(t, c, "mix-value", "?from=1&to=2"), `<div class="r h">`, "</div>")
	if n := strings.Count(head, `<time class="when"`); n != 2 {
		t.Errorf("the heading holds %d registration times, want 2: %s", n, head)
	}
}
