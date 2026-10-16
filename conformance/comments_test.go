package conformance

import (
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// kafkaBehaviourRE finds a comment that says how Apache Kafka behaves.
var kafkaBehaviourRE = regexp.MustCompile(`(?i)(as kafka does|like kafka|real broker|as on (real )?kafka|where kafka does|(^|[^-\w])kafka (returns|answers|refuses|accepts|sends|closes)|as apache kafka (does|answers))`)

// claimRefRE is how a comment cites a claims row: "claim: groups R11b" or
// "claims: auth A7 to A9" (the file under conformance/claims, without .md, and
// the first row's id).
var claimRefRE = regexp.MustCompile(`claims?: ([a-z-]+) ([A-Z][0-9]+[a-z]?)`)

// commentsOwnedElsewhere are files whose comments other work converts at merge;
// each is listed so that the list can only shrink. A file here is still scanned
// for claim ids that name no row.
var commentsOwnedElsewhere = map[string]bool{
	"internal/broker/produce.go":      true,
	"internal/broker/group.go":        true,
	"internal/broker/groups.go":       true,
	"internal/broker/groupmigrate.go": true,
	"internal/broker/groupdelete.go":  true,
	"internal/telemetry/store.go":     true,
}

// Every comment in the broker's sources that says what Kafka does cites the
// claims row that shows it, and the row exists: a claim about Kafka cannot sit in
// a code comment with no evidence behind it. A comment block is the run of
// comment lines it is in.
func TestKafkaBehaviourCommentsCiteAClaim(t *testing.T) {
	claims, err := ReadClaims("claims")
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]bool{}
	for _, c := range claims {
		rows[strings.TrimSuffix(c.File, ".md")+" "+c.ID] = true
	}
	for _, dir := range []string{"../internal", "../cmd", "../server"} {
		err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() && filepath.Base(path) == "kmsg" {
				return filepath.SkipDir // generated from Kafka's message definitions.
			}
			if d.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
				return nil
			}
			rel := strings.TrimPrefix(filepath.ToSlash(path), "../")
			raw, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			lines := strings.Split(string(raw), "\n")
			for i := 0; i < len(lines); {
				if !isComment(lines[i]) {
					i++
					continue
				}
				j := i
				var block []string
				for j < len(lines) && isComment(lines[j]) {
					block = append(block, lines[j])
					j++
				}
				text := strings.Join(block, " ")
				for _, m := range claimRefRE.FindAllStringSubmatch(text, -1) {
					if !rows[m[1]+" "+m[2]] {
						t.Errorf("%s:%d cites %s %s, which no claims file has", rel, i+1, m[1], m[2])
					}
				}
				if kafkaBehaviourRE.MatchString(text) && !claimRefRE.MatchString(text) && !commentsOwnedElsewhere[rel] {
					t.Errorf("%s:%d says what Kafka does with no claim id (// claim: <file> <id>): %s", rel, i+1, strings.TrimSpace(block[0]))
				}
				i = j
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// isComment reports whether a line is a // comment, or ends in one.
func isComment(line string) bool { return strings.Contains(line, "//") }
