# Claims: the review of 2026-10-12

Claims about Apache Kafka that the fixes of an independent review rest on. Every row
is `recorded` against `apache/kafka:4.3.1` (`go tool task conformance:record`) and
replayed against doze-kafka by `go tool task conformance`.

## CreateTopics: names that collide

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| C1 | A name is held against the topics that existed before the CreateTopics request, not against the others in it: `pair.one` and `pair_one` in one request are both created (code 0), in either order, and so are three spellings in one request | recorded | conformance/recordings/createtopics-collisions.json | createtopics-collisions, steps two-in-one-request, two-in-one-request-other-order, three-in-one-request, what-exists |
| C2 | With validate_only, two such names in one request both validate (code 0), and a name that collides with an existing topic is INVALID_TOPIC_EXCEPTION (17) with the text "Topic 'have_it' collides with existing topic: have.it" | recorded | conformance/recordings/createtopics-collisions.json | createtopics-collisions, steps validate-only-two-in-one-request, validate-only-collides-with-existing |
| C3 | A request holding one name that collides with an existing topic and one that does not refuses the first (17) and creates the second | recorded | conformance/recordings/createtopics-collisions.json | createtopics-collisions, step one-collides-with-existing-one-does-not |
| C4 | A later request is held against what an earlier one created: a fourth spelling of three topics made together is 17, and of the topics it collides with the message names the one created last | recorded | conformance/recordings/createtopics-collisions.json | createtopics-collisions, step a-third-spelling-in-a-later-request |
