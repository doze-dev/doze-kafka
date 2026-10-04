package console

import (
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// TestTemplateClassesAreStyled guards against the referenced-but-never-wired
// bug class: a template ships a class that no stylesheet defines, and the
// element silently renders unstyled. It has happened twice — .panel-head (the
// SQS peek header stacked as cramped blocks) and .form-page (create pages had
// a width but no padding, gluing titles to the pane border).
//
// Every class token used in the templates must either appear in a stylesheet
// or be listed in hookClasses with a reason.
func TestTemplateClassesAreStyled(t *testing.T) {
	// Classes that intentionally carry no styles: pure JS/layout hooks.
	hookClasses := map[string]string{
		"ago": "relative-time hook — shell.js rewrites its text as time passes",
	}

	css := readAll(t, "static/app.css") + readAll(t, filepath.Join("static", "cm", "codemirror.min.css"))

	actionRe := regexp.MustCompile(`(?s){{.*?}}`)
	// Whitespace before class= keeps Alpine's bound :class="expr" (a JS
	// expression, not a class list) out of the scan.
	classRe := regexp.MustCompile(`\sclass="([^"]*)"|\sclass='([^']*)'`)

	seen := map[string][]string{} // class -> templates using it
	files, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".html") {
			continue
		}
		// Strip template actions from the WHOLE file first — actions can carry
		// quoted arguments ({{if eq .State "ENABLED"}}) that would otherwise
		// truncate the class-attribute capture at the inner quote. Replacing
		// with a space also keeps tokens on either side of an {{if}}/{{else}}
		// boundary from fusing into one.
		src := actionRe.ReplaceAllString(readAll(t, filepath.Join("templates", f.Name())), " ")
		for _, m := range classRe.FindAllStringSubmatch(src, -1) {
			for _, tok := range strings.Fields(m[1] + m[2]) {
				if len(seen[tok]) == 0 || seen[tok][len(seen[tok])-1] != f.Name() {
					seen[tok] = append(seen[tok], f.Name())
				}
			}
		}
	}

	var missing []string
	for tok, users := range seen {
		if _, ok := hookClasses[tok]; ok {
			continue
		}
		if styled(css, tok) {
			continue
		}
		missing = append(missing, tok+" (used in "+strings.Join(users, ", ")+")")
	}
	sort.Strings(missing)
	for _, m := range missing {
		t.Errorf("class .%s is referenced by a template but styled nowhere — add CSS or allowlist it in hookClasses with a reason", m)
	}
}

// styled reports whether class tok appears as a selector-ish token in css.
// A token ending in "-" is a dynamic prefix (class="fl-k-{{.Kind}}") and
// matches if any selector continues it.
func styled(css, tok string) bool {
	if strings.HasSuffix(tok, "-") {
		return strings.Contains(css, "."+tok)
	}
	re := regexp.MustCompile(`\.` + regexp.QuoteMeta(tok) + `([^a-zA-Z0-9_-]|$)`)
	return re.MatchString(css)
}

func readAll(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(b)
}

// TestTemplateTokensAreDefined guards the other half of the styling contract.
// TestTemplateClassesAreStyled catches a class with no rules; this catches an
// inline style reaching for a custom property that does not exist.
//
// It has already happened twice in one week, both times from renaming a token
// in the stylesheet: --accent-blue became --accent and two templates kept the
// old name, and the hsl()-to-hex change left eleven templates passing hex into
// hsl(). Neither failed a test, neither failed a build, and both rendered as a
// silently inherited colour — the most expensive kind of wrong, because it
// looks deliberate.
func TestTemplateTokensAreDefined(t *testing.T) {
	css := readAll(t, "static/app.css")
	defined := map[string]bool{}
	for _, m := range regexp.MustCompile(`(--[a-z0-9-]+)\s*:`).FindAllStringSubmatch(css, -1) {
		defined[m[1]] = true
	}

	files, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	varRe := regexp.MustCompile(`var\((--[a-z0-9-]+)`)
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".html") {
			continue
		}
		src := readAll(t, filepath.Join("templates", f.Name()))
		for _, m := range varRe.FindAllStringSubmatch(src, -1) {
			tok := m[1]
			if !defined[tok] {
				t.Errorf("%s uses var(%s), which app.css does not define", f.Name(), tok)
			}
		}
	}
}

// The type scale is closed, and this is what keeps it that way.
//
// Tokens in vanilla CSS with no build step buy exactly two things: a name, and
// something a test can enforce. Without this the fifteen font sizes and ten
// weights come back — 12.5px reappears in the first hot-fix and nobody notices,
// because a half-pixel difference is invisible one rule at a time and only
// legible as a census.
func TestTypeScaleIsClosed(t *testing.T) {
	// Strip comments first. The prose in this file talks ABOUT sizes and
	// weights — the @font-face note names "font-weight: 550" — and a census
	// that reads its own documentation reports the documentation.
	css := regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(readAll(t, "static/app.css"), " ")

	// font: shorthand hides sizes from this census and silently resets weight,
	// style and line-height to their initial values. "font: inherit" means
	// something else and is allowed.
	for _, m := range regexp.MustCompile(`(?m)(^|[;{\s])font:\s*([^;}]+)`).FindAllStringSubmatch(css, -1) {
		if strings.TrimSpace(m[2]) != "inherit" {
			t.Errorf("font shorthand %q — use longhands, or this census cannot see the size", strings.TrimSpace(m[2]))
		}
	}

	// Sizes come from the --fs-* tokens. A literal size is allowed only where the
	// tokens themselves are defined.
	for _, m := range regexp.MustCompile(`font-size:\s*([^;}]+)`).FindAllStringSubmatch(css, -1) {
		v := strings.TrimSpace(m[1])
		if strings.HasPrefix(v, "var(--fs-") || v == "inherit" {
			continue
		}
		t.Errorf("font-size: %s is not a --fs-* token (xs 10, sm 11, md 12, base 13, lg 15, xl 20)", v)
	}

	weights := map[string]bool{"400": true, "500": true, "550": true, "600": true, "650": true, "700": true}
	for _, m := range regexp.MustCompile(`font-weight:\s*([^;}]+)`).FindAllStringSubmatch(css, -1) {
		v := strings.TrimSpace(m[1])
		if strings.HasPrefix(v, "var(") || v == "inherit" || v == "bold" || v == "normal" {
			continue
		}
		if !weights[v] {
			t.Errorf("font-weight: %s is off the scale (400/500/550/600/650/700)", v)
		}
	}
}

// inlineBudget is a RATCHET, not a ceiling. Going over fails, and so does going
// under — with the new number to write down. That is what makes it tighten
// instead of rot: a budget that only ever caps is a number nobody revisits.
//
// A style attribute carrying only custom properties is exempt, because that is
// data (a service colour, a depth) rather than layout, and moving the dynamic
// ones to custom properties is the point rather than a workaround.
//
// Deliberately NOT chased to zero: `width` on a table column is a fact about
// that table stated in the most local place available, and converting eighty of
// those to classes would make the markup worse and the stylesheet longer.
var inlineBudget = map[string]int{
	"cluster.html": 4, "groups.html": 5, "layout.html": 2, "panes.html": 1, "topics.html": 15,
}

func TestInlineStyleBudget(t *testing.T) {
	files, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	styleRe := regexp.MustCompile(`style="([^"]*)"`)
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".html") {
			continue
		}
		src := readAll(t, filepath.Join("templates", f.Name()))
		n := 0
		for _, m := range styleRe.FindAllStringSubmatch(src, -1) {
			for _, decl := range strings.Split(m[1], ";") {
				if d := strings.TrimSpace(decl); d != "" && !strings.HasPrefix(d, "--") {
					n++
					break
				}
			}
		}
		want, listed := inlineBudget[f.Name()]
		switch {
		case !listed && n > 0:
			t.Errorf("%s has %d inline styles and no budget — add %q: %d to inlineBudget", f.Name(), n, f.Name(), n)
		case n > want:
			t.Errorf("%s has %d inline styles, budget %d — put it in a class", f.Name(), n, want)
		case listed && n < want:
			t.Errorf("%s is down to %d inline styles; lower its budget to %d so the ratchet holds", f.Name(), n, n)
		}
	}
}

// TestCompoundClassesAreStyled closes the hole TestTemplateClassesAreStyled
// leaves open: a modifier class that only ever exists as part of a compound
// selector.
//
// It shipped a real bug. `class="btn btn-outline danger"` appeared on 13 Delete
// buttons across 12 templates. `danger` passed the single-token check because
// .icon-btn.danger and .menu-item.danger both exist — but .btn-outline.danger
// did not, so every Delete rendered as a plain grey outline with no destructive
// colour at all. Nothing failed; the button just looked like every other button.
//
// The rule: if a class NEVER appears as a bare `.tok` rule in the stylesheet,
// it is a modifier. A modifier is only styled if the element also carries a
// class it is actually compounded with somewhere in the CSS.
func TestCompoundClassesAreStyled(t *testing.T) {
	css := regexp.MustCompile(`(?s)/\*.*?\*/`).ReplaceAllString(readAll(t, "static/app.css"), " ")

	// Bare rule: `.tok` standing on its own — nothing selector-ish glued to
	// EITHER side. Checking only the trailing side is what let this bug ship
	// once already: in `.icon-btn.danger:hover` the `:` after `.danger` looks
	// like a clean break, so `danger` read as bare when it is compounded.
	// Pseudo-classes and descendants still count as bare — .btn:hover and
	// .btn .ic both style a plain .btn element.
	bare := func(tok string) bool {
		return regexp.MustCompile(`(^|[^a-zA-Z0-9_.\-])\.` + regexp.QuoteMeta(tok) + `([^a-zA-Z0-9_.\-]|$)`).MatchString(css)
	}
	// Partners: every class X such that .X.tok or .tok.X appears in the CSS.
	partners := func(tok string) map[string]bool {
		out := map[string]bool{}
		q := regexp.QuoteMeta(tok)
		for _, re := range []string{`\.([a-zA-Z0-9_-]+)\.` + q + `\b`, `\.` + q + `\.([a-zA-Z0-9_-]+)`} {
			for _, m := range regexp.MustCompile(re).FindAllStringSubmatch(css, -1) {
				out[m[1]] = true
			}
		}
		return out
	}

	actionRe := regexp.MustCompile(`(?s){{.*?}}`)
	classRe := regexp.MustCompile(`\sclass="([^"]*)"|\sclass='([^']*)'`)
	files, err := os.ReadDir("templates")
	if err != nil {
		t.Fatal(err)
	}
	type site struct{ file, attr string }
	bad := map[string][]site{} // "tok in file" -> sites, deduped per file
	for _, f := range files {
		if !strings.HasSuffix(f.Name(), ".html") {
			continue
		}
		src := actionRe.ReplaceAllString(readAll(t, filepath.Join("templates", f.Name())), " ")
		for _, m := range classRe.FindAllStringSubmatch(src, -1) {
			toks := strings.Fields(m[1] + m[2])
			if len(toks) < 2 {
				continue
			}
			for _, tok := range toks {
				if bare(tok) {
					continue
				}
				p := partners(tok)
				if len(p) == 0 {
					continue // unstyled outright — the single-token test owns this
				}
				matched := false
				for _, other := range toks {
					if other != tok && p[other] {
						matched = true
						break
					}
				}
				if !matched {
					key := tok
					if len(bad[key]) == 0 || bad[key][len(bad[key])-1].file != f.Name() {
						bad[key] = append(bad[key], site{f.Name(), strings.Join(toks, " ")})
					}
				}
			}
		}
	}

	var keys []string
	for k := range bad {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, tok := range keys {
		var where []string
		for _, s := range bad[tok] {
			where = append(where, s.file+` (class="`+s.attr+`")`)
		}
		t.Errorf("modifier .%s is only ever compounded with %v in the CSS, but is used without any of them in %s — add the missing compound rule",
			tok, sortedKeys(partners(tok)), strings.Join(where, ", "))
	}
}

func sortedKeys(m map[string]bool) []string {
	var out []string
	for k := range m {
		out = append(out, "."+k)
	}
	sort.Strings(out)
	return out
}
