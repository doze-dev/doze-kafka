package docs

import (
	"fmt"
	"path"
	"regexp"
	"strings"
	"testing"
)

var (
	inlineLink = regexp.MustCompile(`\]\(\s*<?([^)\s>]+)>?(?:\s+"[^"]*")?\s*\)`)
	refLink    = regexp.MustCompile(`(?m)^\s{0,3}\[[^\]]+\]:\s*<?(\S+?)>?(?:\s+"[^"]*")?\s*$`)
	htmlHref   = regexp.MustCompile(`(?:href|src)="([^"]+)"`)
)

// stripCode is the prose of a document with its code spans removed: a bracketed word
// followed by a parenthesis inside code is code, not a link.
func stripCode(md string) string {
	prose := blankFences(md)
	var b strings.Builder
	last := 0
	for _, s := range codeSpans(prose) {
		b.WriteString(prose[last:s.start])
		b.WriteString(strings.Repeat(" ", s.end-s.start))
		last = s.end
	}
	b.WriteString(prose[last:])
	return b.String()
}

// links returns every link target in a markdown document.
func links(md string) []string {
	md = stripCode(md)
	var out []string
	for _, re := range []*regexp.Regexp{inlineLink, refLink, htmlHref} {
		for _, m := range re.FindAllStringSubmatch(md, -1) {
			out = append(out, m[1])
		}
	}
	return out
}

var (
	headingMarkup = regexp.MustCompile("[`*_]|\\[([^\\]]*)\\]\\([^)]*\\)")
	notSlug       = regexp.MustCompile(`[^\p{L}\p{N} _-]`)
	htmlAnchor    = regexp.MustCompile(`<a\s+(?:name|id)="([^"]+)"`)
)

// anchors is the set of anchors GitHub makes for a markdown file: one per heading
// (lower case, punctuation gone, spaces as dashes, a repeat numbered) and one per
// <a name> or <a id>.
func anchors(md string) map[string]bool {
	set := map[string]bool{}
	seen := map[string]int{}
	for _, line := range strings.Split(blankFences(md), "\n") {
		for _, m := range htmlAnchor.FindAllStringSubmatch(line, -1) {
			set[strings.ToLower(m[1])] = true
		}
		if !strings.HasPrefix(line, "#") {
			continue
		}
		text := strings.TrimSpace(strings.TrimLeft(line, "#"))
		text = headingMarkup.ReplaceAllStringFunc(text, func(s string) string {
			if m := regexp.MustCompile(`\[([^\]]*)\]`).FindStringSubmatch(s); m != nil {
				return m[1]
			}
			return ""
		})
		slug := strings.ReplaceAll(notSlug.ReplaceAllString(strings.ToLower(text), ""), " ", "-")
		if n := seen[slug]; n > 0 {
			set[fmt.Sprintf("%s-%d", slug, n)] = true
		} else {
			set[slug] = true
		}
		seen[slug]++
	}
	return set
}

// checkLinks reports every relative link in the document at rel that points at
// nothing: a file or directory that is not in the tree (or is only on this machine),
// or a #fragment that is no anchor of the markdown file it names. URLs with a scheme
// are not fetched.
func checkLinks(tr tree, rel, md string) []string {
	var bad []string
	for _, l := range links(md) {
		if regexp.MustCompile(`^[a-zA-Z][a-zA-Z0-9+.-]*:`).MatchString(l) || strings.HasPrefix(l, "//") {
			continue // https:, mailto:, and the like
		}
		file, frag, _ := strings.Cut(l, "#")
		file, _, _ = strings.Cut(file, "?")
		target := rel
		if file != "" {
			if strings.HasPrefix(file, "/") {
				target = path.Clean(strings.TrimPrefix(file, "/"))
			} else {
				target = path.Clean(path.Join(path.Dir(rel), file))
			}
		}
		if target == ".." || strings.HasPrefix(target, "../") {
			bad = append(bad, fmt.Sprintf("%s: link to %q points outside the repository", rel, l))
			continue
		}
		isFile, isDir, why := tr.kind(target)
		if !isFile && !isDir {
			bad = append(bad, fmt.Sprintf("%s: link to %q: %s %s", rel, l, target, why))
			continue
		}
		if frag == "" || isDir || !strings.HasSuffix(target, ".md") {
			continue
		}
		body, err := tr.read(target)
		if err != nil {
			bad = append(bad, fmt.Sprintf("%s: link to %q: %v", rel, l, err))
			continue
		}
		if !anchors(body)[strings.ToLower(frag)] {
			bad = append(bad, fmt.Sprintf("%s: link to %q: no heading #%s in %s", rel, l, frag, target))
		}
	}
	return bad
}

// Every markdown file of the project outside plan/ (the README, CONTRIBUTING,
// CHANGELOG, the guides, compat/ and e2e/ and every other README), every relative
// link and anchor in it.
func TestNoDeadRelativeLinks(t *testing.T) {
	tr, withGit := repoTree(t)
	files := repoMarkdown(t, tr.root, withGit)
	must := []string{"README.md", "CONTRIBUTING.md", "CHANGELOG.md", "docs/getting-started.md", "compat/README.md"}
	have := map[string]bool{}
	for _, f := range files {
		have[f] = true
	}
	for _, m := range must {
		if !have[m] {
			t.Errorf("%s is not among the documents checked (%d found): the finder is broken", m, len(files))
		}
	}
	n := 0
	for _, f := range files {
		md, err := tr.read(f)
		if err != nil {
			t.Fatal(err)
		}
		n += len(links(md))
		for _, b := range checkLinks(tr, f, md) {
			t.Error(b)
		}
	}
	if n < 100 {
		t.Fatalf("found only %d links in %d files: the extractor is broken", n, len(files))
	}
	t.Logf("%d links in %d documents", n, len(files))
}

// The checker's own test: each kind of dead link it exists to find, planted.
func TestCheckLinksFindsEachKindOfDeadLink(t *testing.T) {
	tr := fakeTree{
		"docs/other.md":    "# Other\n\n## A heading, with `code`\n\n## Twice\n\n## Twice\n\n<a name=\"legacy\"></a>\n",
		"CONTRIBUTING.md":  "x",
		"scripts/tool.sh":  "x",
		"compat/README.md": "x",
	}
	md := strings.Join([]string{
		"[ok](other.md) [ok](other.md#a-heading-with-code) [ok](other.md#twice-1) [ok](other.md#legacy)",
		"[ok](../CONTRIBUTING.md) [ok](../scripts/) [ok](../compat/README.md) [ok](#top)",
		"[web](https://example.com/x) [mail](mailto:a@b.c) `[code](notalink.md)`",
		"```", "[fenced](x.md)", "```",
		"[dead file](gone.md)",
		"[dead anchor](other.md#nope)",
		"[dead self anchor](#nowhere)",
		"[outside](../../elsewhere.md)",
		"[ref]: missing/ref.md",
		`<img src="img/none.png">`,
		"",
		"# Top",
	}, "\n")
	tr["docs/main.md"] = md
	got := checkLinks(tr, "docs/main.md", md)
	want := []string{"gone.md", "#nope", "#nowhere", "outside the repository", "missing/ref.md", "img/none.png"}
	if len(got) != len(want) {
		t.Errorf("want %d dead links, got %d: %q", len(want), len(got), got)
	}
	for _, w := range want {
		found := false
		for _, g := range got {
			found = found || strings.Contains(g, w)
		}
		if !found {
			t.Errorf("the dead link %q was not reported: %q", w, got)
		}
	}
}

// A link to a file that exists only on this machine (git ignores it) is dead for
// everyone else.
func TestALinkToAnIgnoredFileIsDead(t *testing.T) {
	tr := &diskTree{root: t.TempDir(), shared: map[string]bool{"README.md": true}, dirs: map[string]bool{}}
	writeFile(t, tr.root, "README.md", "[local](notes.md)")
	writeFile(t, tr.root, "notes.md", "only here")
	got := checkLinks(tr, "README.md", "[local](notes.md)")
	if len(got) != 1 || !strings.Contains(got[0], "git ignores it") {
		t.Errorf("want the ignored file reported, got %q", got)
	}
}
