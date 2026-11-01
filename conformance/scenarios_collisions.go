package conformance

import "github.com/twmb/franz-go/pkg/kmsg"

// What Apache Kafka answers when the names that may not live side by side (they
// differ only in '.' against '_', so their metric names would be one) arrive in the
// same CreateTopics request, with and without validate_only, and against a topic
// that exists.
//
// What it answers is not what one would guess: a name is held against the topics
// that existed before the request, never against the others in it, so "pair.one"
// and "pair_one" sent together are both created.

func init() {
	Scenarios = append(Scenarios, Scenario{Name: "createtopics-collisions", Run: createTopicsCollisions})
}

func createTopicsCollisions(e *Env) {
	const v = 7
	e.Do("two-in-one-request", createTopics(false, newTopic("pair.one", 1, 1), newTopic("pair_one", 1, 1)), v)
	e.Do("two-in-one-request-other-order", createTopics(false, newTopic("pair_two", 1, 1), newTopic("pair.two", 1, 1)), v)
	e.Do("three-in-one-request", createTopics(false, newTopic("tri.a.b", 1, 1), newTopic("tri_a.b", 1, 1), newTopic("tri.a_b", 1, 1)), v)
	e.Do("validate-only-two-in-one-request", createTopics(true, newTopic("dry.one", 1, 1), newTopic("dry_one", 1, 1)), v)
	e.Do("existing", createTopics(false, newTopic("have.it", 1, 1)), v)
	e.Do("one-collides-with-existing-one-does-not", createTopics(false, newTopic("have_it", 1, 1), newTopic("fine", 1, 1)), v)
	e.Do("validate-only-collides-with-existing", createTopics(true, newTopic("have_it", 1, 1)), v)
	e.Do("same-name-is-not-a-collision", createTopics(false, newTopic("have.it", 1, 1)), v)
	// A later request is held against what the earlier one created.
	e.Do("a-third-spelling-in-a-later-request", createTopics(false, newTopic("pair_one", 1, 1), newTopic("tri_a_b", 1, 1)), v)
	// Kafka's metadata follows a create by a moment: wait for the nine there are.
	e.DoUntil("what-exists", metadataAll(), 12, func(r kmsg.Response) bool {
		return len(r.(*kmsg.MetadataResponse).Topics) >= 9
	})
}
