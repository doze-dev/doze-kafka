package meta

import (
	"errors"
	"fmt"
	"math/rand"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// The index answers exactly what the rule spelled out over a list answers, for any
// set of names: whether there is a collision, and of which kind. (Which of several
// colliding names is reported is not part of the rule.)
func TestNameIndexAgreesWithTheRuleOverAList(t *testing.T) {
	rng := rand.New(rand.NewSource(7))
	// Few letters and many separators, so collisions of both kinds are common, and
	// two non-ASCII letters that equal ASCII ones without regard to case (a topic
	// registered before names were validated may hold them).
	alphabet := []rune{'a', 'A', 'b', 'B', 'k', 'K', 's', 'S', '.', '_', '-', 'K', 'ſ'}
	random := func() string {
		n := 1 + rng.Intn(4)
		var sb strings.Builder
		for range n {
			sb.WriteRune(alphabet[rng.Intn(len(alphabet))])
		}
		return sb.String()
	}
	for round := range 200 {
		x := newNameIndex()
		var have []string
		for range 1 + rng.Intn(30) {
			n := random()
			if !slices.Contains(have, n) {
				have = append(have, n)
				x.add(n)
			}
		}
		// Take some away again: the index must forget them.
		for range min(rng.Intn(5), len(have)) {
			i := rng.Intn(len(have))
			x.remove(have[i])
			have = slices.Delete(have, i, i+1)
		}
		for range 50 {
			name := random()
			want, got := FindCollision(name, have), x.collision(name, nil)
			if (want == nil) != (got == nil) {
				t.Fatalf("round %d: %q among %q: the list says %v, the index says %v", round, name, have, want, got)
			}
			if want == nil {
				continue
			}
			// A case-only collision is reported as one whenever there is one.
			caseOnly := slices.ContainsFunc(have, func(e string) bool { return e != name && strings.EqualFold(e, name) })
			if got.CaseOnly != caseOnly {
				t.Fatalf("round %d: %q among %q: CaseOnly = %v, want %v", round, name, have, got.CaseOnly, caseOnly)
			}
			if !slices.Contains(have, got.Existing) || got.Existing == name {
				t.Fatalf("round %d: %q among %q collides with %q, which is not a topic beside it", round, name, have, got.Existing)
			}
			if c := FindCollision(name, []string{got.Existing}); c == nil {
				t.Fatalf("round %d: %q does not collide with %q", round, name, got.Existing)
			}
		}
		if len(have) == 0 && (len(x.byFold) != 0 || len(x.byMetric) != 0) {
			t.Fatalf("round %d: an index of nothing still holds %v %v", round, x.byFold, x.byMetric)
		}
	}
}

// CreateTopicBeside lets a name off the '.' against '_' rule for the names given
// (the topics its own CreateTopics request has created, which is all Kafka lets it
// off: recorded, createtopics-collisions), and for nothing else.
func TestCreateTopicBesideTheNamesOfItsOwnRequest(t *testing.T) {
	path := filepath.Join(t.TempDir(), "meta.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { s.Close() }()
	mustTopics(t, s, "old.name", "Cased")

	request := map[string]struct{}{}
	create := func(name string) error {
		_, err := s.CreateTopicBeside(name, 1, nil, request)
		if err == nil {
			request[name] = struct{}{}
		}
		return err
	}
	if err := create("new.name"); err != nil {
		t.Fatal(err)
	}
	if err := create("new_name"); err != nil {
		t.Errorf("new_name beside new.name of the same request: %v", err)
	}
	var c *CollisionError
	if err := create("old_name"); !errors.As(err, &c) || c.Existing != "old.name" || c.CaseOnly {
		t.Errorf("old_name against old.name, which was there before the request: %v", err)
	}
	if err := create("cased"); !errors.As(err, &c) || !c.CaseOnly {
		t.Errorf("cased against Cased: %v", err)
	}
	if err := create("New.Name"); !errors.As(err, &c) || !c.CaseOnly || c.Existing != "new.name" {
		t.Errorf("New.Name against new.name of the same request (letter case is never let off): %v", err)
	}
	if err := create("new.name"); !errors.Is(err, ErrTopicExists) {
		t.Errorf("the same name twice: %v", err)
	}
	// Outside the request the pair is two topics like any other.
	if c := s.TopicCollision("new-name", nil); c != nil {
		t.Errorf("a hyphen collides: %v", c)
	}
	if c := s.TopicCollision("new_name", nil); c == nil || c.Existing != "new.name" {
		t.Errorf("TopicCollision(new_name) with nothing let off = %v", c)
	}
	if c := s.TopicCollision("new.name", nil); c == nil || c.Existing != "new_name" {
		t.Errorf("TopicCollision(new.name) with nothing let off = %v", c)
	}
	if _, err := s.CreateTopic("old_name", 1, nil); !errors.Is(err, ErrTopicCollides) {
		t.Errorf("CreateTopic lets nothing off: %v", err)
	}

	// The index is rebuilt from the registry on open, and forgets a deleted topic.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s, err = Open(path); err != nil {
		t.Fatal(err)
	}
	if c := s.TopicCollision("OLD.NAME", nil); c == nil || !c.CaseOnly {
		t.Errorf("after reopening, OLD.NAME against old.name: %v", c)
	}
	if c := s.TopicCollision("old_name", nil); c == nil || c.Existing != "old.name" {
		t.Errorf("after reopening, old_name against old.name: %v", c)
	}
	if err := s.DeleteTopic("old.name"); err != nil {
		t.Fatal(err)
	}
	if c := s.TopicCollision("old_name", nil); c != nil {
		t.Errorf("old.name is deleted and old_name still collides: %v", c)
	}
	if _, err := s.CreateTopic("old_name", 1, nil); err != nil {
		t.Errorf("creating old_name once old.name is gone: %v", err)
	}
}

// What it costs to ask whether a name collides does not grow with the number of
// topics (it was a comparison with every one of them).
func BenchmarkTopicCollision(b *testing.B) {
	for _, existing := range []int{1000, 100000} {
		b.Run(fmt.Sprintf("existing=%d", existing), func(b *testing.B) {
			x := newNameIndex()
			for i := range existing {
				x.add(fmt.Sprintf("existing.topic-%d", i))
			}
			b.ResetTimer()
			for i := range b.N {
				if c := x.collision("wanted_topic."+fmt.Sprint(i%1000), nil); c != nil {
					b.Fatal(c)
				}
			}
		})
	}
}
