package console

import (
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// A3, all of it: every control in the templates is accounted for, named or not, and
// no teaching line under a field is typed into a template.

var controlRe = regexp.MustCompile(`(?s)<(input|select|textarea)\b([^>]*)>`)
var attrRe = func(name string) *regexp.Regexp { return regexp.MustCompile(`\s` + name + `="([^"]*)"`) }

// Every control with no name (it sends nothing under one of its own) says what it is.
func TestEveryUnnamedControlSaysWhatItIs(t *testing.T) {
	files, _ := filepath.Glob("templates/*.html")
	seen := map[string]bool{}
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, m := range controlRe.FindAllStringSubmatch(string(b), -1) {
			attrs := m[2]
			if attrRe("name").MatchString(attrs) {
				continue // a named field: TestEveryFormFieldHasASettingsRecordOrSaysWhyNot.
			}
			key := ""
			if a := attrRe("aria-label").FindStringSubmatch(attrs); a != nil {
				key = a[1]
			} else if cl := attrRe("class").FindStringSubmatch(attrs); cl != nil {
				for _, c := range strings.Fields(cl[1]) {
					if _, ok := unnamedFields[c]; ok {
						key = c
					}
				}
			}
			if unnamedFields[key] == "" {
				t.Errorf("%s: a <%s> with no name (%q) does not say what it is: add it to unnamedFields", f, m[1], strings.TrimSpace(attrs))
				continue
			}
			seen[key] = true
		}
	}
	for k := range unnamedFields {
		if !seen[k] {
			t.Errorf("unnamedFields lists %q and no template has such a control", k)
		}
	}
}

// A line under a field (a .teach, a .hint, the acks caption) is generated: from the
// settings table, or from the rule the broker applies. The few hints that are not
// teaching (they say what the form does, not what a setting or a rule is) are listed,
// each with its reason.
func TestNoTeachingLineIsTypedIntoATemplate(t *testing.T) {
	qualifiers := map[string]string{
		"optional":                       "says the field may be left empty; not a fact about a setting",
		"starts from the latest version": "says what the editor holds when it opens",
	}
	files, _ := filepath.Glob("templates/*.html")
	for _, f := range files {
		b, _ := os.ReadFile(f)
		for _, body := range teachingLines(string(b)) {
			if strings.Contains(body, "{{") {
				continue // generated
			}
			text := strings.TrimSpace(regexp.MustCompile(`<[^>]*>`).ReplaceAllString(body, ""))
			if _, ok := qualifiers[text]; ok {
				continue
			}
			t.Errorf("%s: a teaching line is typed into the template: %q", f, text)
		}
	}
}

// teachingLines is the inside of every element whose class is teach, hint or
// ackscaption, to its own closing tag (elements of the same name inside it counted).
func teachingLines(src string) []string {
	open := regexp.MustCompile(`<(span|p|div)\b[^>]*\sclass="[^"]*\b(teach|hint|ackscaption)\b[^"]*"[^>]*>`)
	var out []string
	for _, loc := range open.FindAllStringSubmatchIndex(src, -1) {
		tag := src[loc[2]:loc[3]]
		depth, i := 1, loc[1]
		tagRe := regexp.MustCompile(`<(/?)` + tag + `\b[^>]*>`)
		for depth > 0 {
			m := tagRe.FindStringSubmatchIndex(src[i:])
			if m == nil {
				break
			}
			if m[3] > m[2] {
				depth--
			} else {
				depth++
			}
			if depth == 0 {
				out = append(out, src[loc[1]:i+m[0]])
			}
			i += m[1]
		}
	}
	return out
}

// The check discriminates: a typed line is caught (nested markup and all), a
// generated one and a qualifier are not.
func TestTheTypedLineCheckCatchesATypedLine(t *testing.T) {
	src := `<p class="teach">{{teach "broker" "x"}}</p><span class="hint">optional</span><p class="teach bad"><span class="m" x-text="v"></span> is taken.</p><span class="ackscaption">Durable.</span>`
	var typed []string
	for _, body := range teachingLines(src) {
		text := strings.TrimSpace(regexp.MustCompile(`<[^>]*>`).ReplaceAllString(body, ""))
		if !strings.Contains(body, "{{") && text != "optional" {
			typed = append(typed, text)
		}
	}
	if strings.Join(typed, "|") != "is taken.|Durable." {
		t.Errorf("typed lines = %q, want the nested one and the caption", typed)
	}
}

// The acknowledgement choices are the values the produce form takes, and nothing else.
func TestAcksChoicesAreWhatTheFormTakes(t *testing.T) {
	seen := map[string]bool{}
	for _, a := range acksChoices() {
		r := httptest.NewRequest("POST", "/", strings.NewReader(url.Values{"acks": {a.Value}, "value": {"x"}}.Encode()))
		r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		in, err := parseProduceForm(r, 1)
		if err != nil || in.Acks != a.Value {
			t.Errorf("acks %q: the form took it as %q (%v)", a.Value, in.Acks, err)
		}
		if a.Say == "" || a.Aria == "" {
			t.Errorf("acks %q says nothing", a.Value)
		}
		seen[a.Value] = true
	}
	for _, v := range []string{"0", "1", "all"} {
		if !seen[v] {
			t.Errorf("the form takes acks %q and the choices do not offer it", v)
		}
	}
}

// The name rule shown under the field is the one the broker applies.
func TestTopicNameRuleIsTheBrokers(t *testing.T) {
	rule := string(topicNameRule())
	if !strings.Contains(rule, "at most 249 characters") || meta.MaxTopicNameLen != 249 {
		t.Errorf("the rule says %q; the broker's limit is %d", rule, meta.MaxTopicNameLen)
	}
	long := strings.Repeat("a", meta.MaxTopicNameLen)
	for name, ok := range map[string]bool{"orders.v1_eu-west": true, long: true, long + "a": false, "a b": false, "ä": false, "a/b": false} {
		if got := meta.ValidateTopicName(name) == nil; got != ok {
			t.Errorf("%q: the broker takes it %v, the rule says %v", name, got, ok)
		}
	}
	for _, c := range []string{".", "_", "-"} {
		if !strings.Contains(rule, `<span class="mono">`+c+`</span>`) {
			t.Errorf("the rule does not name %q, which the broker takes", c)
		}
	}
}
