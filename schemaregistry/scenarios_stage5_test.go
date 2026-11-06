package schemaregistry

import (
	"fmt"
	"net/url"
	"strings"
)

// The recording queue of plan/stages/schemas.md section 8 (R1 to R4, R6 to R8, R10,
// R12 to R14): the questions the first recording left open, asked of Confluent's
// registry so the answers here are its answers and not a recollection.

const (
	avroUser3 = `{"type":"record","name":"User","fields":[{"name":"id","type":"long"},{"name":"email","type":["null","string"],"default":null},{"name":"age","type":["null","int"],"default":null}]}`
	jsonLeaf  = `{"type":"object","properties":{"v":{"type":"string"}}}`
)

// jsonRefTo is a JSON Schema whose properties a and b are the referenced names.
func jsonRefTo(names ...string) string {
	var props []string
	for i, n := range names {
		props = append(props, fmt.Sprintf(`"p%d":{"$ref":%q}`, i, n))
	}
	return `{"type":"object","properties":{` + strings.Join(props, ",") + `}}`
}

// avroChain is a record named C<n> with one field of the type C<n-1>, which a reference
// supplies.
func avroChain(n int) string {
	return fmt.Sprintf(`{"type":"record","name":"C%d","fields":[{"name":"prev","type":"C%d"}]}`, n, n-1)
}

func chainSteps(depth int) []scriptStep {
	steps := []scriptStep{post("/subjects/chain0/versions", schemaBody(`{"type":"record","name":"C0","fields":[{"name":"v","type":"int"}]}`))}
	for i := 1; i <= depth; i++ {
		steps = append(steps, post(fmt.Sprintf("/subjects/chain%d/versions", i),
			refBody("AVRO", avroChain(i), Reference{Name: fmt.Sprintf("C%d", i-1), Subject: fmt.Sprintf("chain%d", i-1), Version: 1})))
	}
	return steps
}

func chainCleanup(depth int) []scriptStep {
	var names []string
	for i := depth; i >= 0; i-- {
		names = append(names, fmt.Sprintf("chain%d", i))
	}
	return cleanupSubjects(names...)
}

var long4000 = strings.Repeat("s", 4000)

func init() {
	scenarios = append(scenarios,
		// R1: what a permanent delete gives back. Version numbers and ids after the
		// latest version, and then the whole subject, are deleted for good.
		scenario{
			name: "permanent-delete-and-register-again",
			steps: []scriptStep{
				post("/subjects/pd/versions", schemaBody(avroUser1)),
				post("/subjects/pd/versions", schemaBody(avroUser2)),
				del("/subjects/pd/versions/2"),
				del("/subjects/pd/versions/2?permanent=true"),
				get("/subjects/pd/versions"),
				get("/schemas/ids/@2"),
				post("/subjects/pd/versions", schemaBody(avroUser3)), // a new schema: version 2 or 3?
				get("/subjects/pd/versions"),
				post("/subjects/pd/versions", schemaBody(avroUser2)), // the deleted text again: its old id?
				get("/subjects/pd/versions"),
				del("/subjects/pd"),
				del("/subjects/pd?permanent=true"),
				get("/subjects/pd/versions"),
				get("/schemas/ids/@1"),
				post("/subjects/pd/versions", schemaBody(avroUser1)), // version 1 again, id 1 again?
				get("/subjects/pd/versions"),
				get("/subjects/pd/versions/latest"),
			},
			cleanup: cleanupSubjects("pd"),
		},
		// R2: a permanent delete of what was not soft deleted first, and what happens to
		// a subject's level and mode when it is deleted for good.
		scenario{
			name: "permanent-delete-rules",
			steps: []scriptStep{
				post("/subjects/pr/versions", schemaBody(avroUser1)),
				put("/config/pr", `{"compatibility":"FULL"}`),
				put("/mode/pr", `{"mode":"READWRITE"}`),
				del("/subjects/pr?permanent=true"),
				del("/subjects/pr/versions/1?permanent=true"),
				del("/subjects/pr/versions/latest?permanent=true"),
				del("/subjects/pr"),
				get("/config/pr"),
				get("/mode/pr"),
				get("/subjects?deleted=true"),
				del("/subjects/pr?permanent=true"),
				get("/config/pr"),
				get("/mode/pr"),
				del("/subjects/pr?permanent=true"),
				get("/subjects?deleted=true"),
			},
			cleanup: cleanupSubjects("pr"),
		},
		// R3: everything a READONLY subject is asked, and the modes this registry does not
		// implement.
		scenario{
			name: "readonly-subject",
			steps: []scriptStep{
				post("/subjects/ro/versions", schemaBody(avroUser1)),
				put("/mode/ro", `{"mode":"READONLY"}`),
				put("/config/ro", `{"compatibility":"FULL"}`),
				get("/config/ro"),
				del("/config/ro"),
				post("/compatibility/subjects/ro/versions/latest", schemaBody(avroUser2)),
				post("/compatibility/subjects/ro/versions/latest?verbose=true", schemaBody(avroUser2)),
				post("/subjects/ro", schemaBody(avroUser1)),
				post("/subjects/ro/versions", schemaBody(avroUser1)), // already there
				post("/subjects/ro/versions", schemaBody(avroUser2)),
				del("/subjects/ro/versions/1"),
				del("/subjects/ro"),
				put("/mode/ro", `{"mode":"IMPORT"}`),
				put("/mode/ro?force=true", `{"mode":"IMPORT"}`),
				put("/mode/ro", `{"mode":"READONLY_OVERRIDE"}`),
				get("/mode/ro"),
				put("/mode", `{"mode":"IMPORT"}`),
				put("/mode?force=true", `{"mode":"IMPORT"}`),
				get("/mode"),
				put("/mode", `{"mode":"READWRITE"}`),
				put("/mode/ro", `{"mode":"READWRITE"}`),
				get("/mode/ro"),
			},
			cleanup: cleanupSubjects("ro"),
		},
		// R4: the Content-Type a request carries.
		scenario{
			name: "content-types",
			steps: []scriptStep{
				withType(post("/subjects/ct1/versions", schemaBody(avroUser1)), "none"),
				withType(post("/subjects/ct2/versions", schemaBody(avroUser1)), "text/plain"),
				withType(post("/subjects/ct3/versions", schemaBody(avroUser1)), "application/json"),
				withType(post("/subjects/ct4/versions", schemaBody(avroUser1)), "application/vnd.schemaregistry+json"),
				withType(post("/subjects/ct5/versions", schemaBody(avroUser1)), "application/vnd.schemaregistry.v1+json"),
				withType(post("/subjects/ct6/versions", schemaBody(avroUser1)), "application/octet-stream"),
				withType(post("/subjects/ct7/versions", schemaBody(avroUser1)), "application/xml"),
				withType(post("/subjects/ct8/versions", schemaBody(avroUser1)), "application/x-www-form-urlencoded"),
				withType(post("/subjects/ct9/versions", schemaBody(avroUser1)), "multipart/form-data; boundary=x"),
				withType(post("/subjects/ct10/versions", schemaBody(avroUser1)), "application/json; charset=utf-8"),
				withType(post("/compatibility/subjects/ct3/versions/latest", schemaBody(avroUser2)), "text/plain"),
				withType(post("/subjects/ct3", schemaBody(avroUser1)), "text/plain"),
				withType(put("/config/ct3", `{"compatibility":"FULL"}`), "text/plain"),
				withType(put("/mode/ct3", `{"mode":"READONLY"}`), "text/plain"),
				withType(put("/config/ct3", `{"compatibility":"FULL"}`), "none"),
				get("/subjects"),
			},
			cleanup: cleanupSubjects("ct1", "ct2", "ct3", "ct4", "ct5", "ct6", "ct7", "ct8", "ct9", "ct10"),
		},
		// R6: limit and offset on the lists.
		scenario{
			name: "limit-and-offset",
			steps: []scriptStep{
				post("/subjects/lo1/versions", schemaBody(avroUser1)),
				post("/subjects/lo2/versions", schemaBody(avroUser1)),
				post("/subjects/lo3/versions", schemaBody(avroUser1)),
				post("/subjects/lo3/versions", schemaBody(avroUser2)),
				post("/subjects/lo3/versions", schemaBody(avroUser3)),
				get("/subjects"),
				get("/subjects?limit=2"),
				get("/subjects?limit=2&offset=1"),
				get("/subjects?offset=2"),
				get("/subjects?offset=10"),
				get("/subjects?limit=0"),
				get("/subjects?limit=-1"),
				get("/subjects?offset=-1"),
				get("/subjects?limit=abc"),
				get("/subjects/lo3/versions?limit=1"),
				get("/subjects/lo3/versions?limit=2&offset=1"),
				get("/schemas?limit=1"),
				get("/schemas?limit=2&offset=1"),
				get("/schemas?latestOnly=true&limit=2"),
			},
			cleanup: cleanupSubjects("lo1", "lo2", "lo3"),
		},
		// R7: subject names.
		scenario{
			name: "subject-names",
			steps: []scriptStep{
				post("/subjects/%20/versions", schemaBody(avroUser1)),
				post("/subjects/"+strings.Repeat("n", 255)+"/versions", schemaBody(avroUser1)),
				post("/subjects/"+long4000+"/versions", schemaBody(avroUser1)),
				post("/subjects/"+strings.Repeat("m", 9000)+"/versions", schemaBody(avroUser1)),
				post("/subjects/ctl%01x/versions", schemaBody(avroUser1)),
				post("/subjects/nl%0Ax/versions", schemaBody(avroUser1)),
				post("/subjects/tab%09x/versions", schemaBody(avroUser1)),
				post("/subjects/"+url.PathEscape(":.ctx:orders")+"/versions", schemaBody(avroUser1)),
				post("/subjects/"+url.PathEscape(":.:orders")+"/versions", schemaBody(avroUser1)),
				post("/subjects/"+url.PathEscape(":*:")+"/versions", schemaBody(avroUser1)),
				post("/subjects/"+url.PathEscape("__GLOBAL")+"/versions", schemaBody(avroUser1)),
				get("/subjects"),
				get("/contexts"),
			},
			cleanup: cleanupSubjects("%20", strings.Repeat("n", 255), long4000, "ctl%01x", "nl%0Ax", "tab%09x",
				url.PathEscape(":.ctx:orders"), url.PathEscape(":.:orders"), url.PathEscape(":*:"), "__GLOBAL"),
		},
		// R8: bodies past any limit there is.
		scenario{
			name: "large-bodies",
			steps: []scriptStep{
				post("/subjects/big/versions", `{"schema":"{\"type\":\"string\",\"doc\":\"@PAD(1048576)\"}"}`),
				post("/subjects/big/versions", `{"schema":"{\"type\":\"string\",\"doc\":\"@PAD(9437184)\"}"}`),
				post("/subjects/big/versions", `{"schema":"{\"type\":\"string\",\"doc\":\"@PAD(20971520)\"}"}`),
				put("/config/big", `{"compatibility":"FULL","pad":"@PAD(1048576)"}`),
				put("/config/big", `{"compatibility":"FULL","pad":"@PAD(20971520)"}`),
				put("/mode/big", `{"mode":"READWRITE","pad":"@PAD(20971520)"}`),
				get("/subjects/big/versions"),
			},
			cleanup: cleanupSubjects("big"),
		},
		// R10: nested references. The same name at two levels, a diamond, and a chain
		// deeper than this registry's 32.
		scenario{
			name: "nested-references",
			steps: append([]scriptStep{
				post("/subjects/leaf/versions", typedBody("JSON", jsonLeaf)),
				post("/subjects/mid/versions", refBody("JSON", jsonRefTo("x.json"), Reference{Name: "x.json", Subject: "leaf", Version: 1})),
				post("/subjects/top/versions", refBody("JSON", jsonRefTo("x.json"), Reference{Name: "x.json", Subject: "mid", Version: 1})),
				post("/subjects/top2/versions", refBody("JSON", jsonRefTo("y.json"), Reference{Name: "y.json", Subject: "mid", Version: 1})),
				post("/subjects/diamond/versions", refBody("JSON", jsonRefTo("a.json", "b.json"),
					Reference{Name: "a.json", Subject: "mid", Version: 1}, Reference{Name: "b.json", Subject: "top2", Version: 1})),
				get("/subjects/leaf/versions/1/referencedby"),
				get("/subjects/mid/versions/1/referencedby"),
				del("/subjects/top2/versions/1"),
				get("/subjects/mid/versions/1/referencedby"),
			}, chainSteps(40)...),
			cleanup: append(cleanupSubjects("diamond", "top2", "top", "mid", "leaf"), chainCleanup(40)...),
		},
		// R12: the routes this registry does not serve, and parameters it ignores.
		scenario{
			name: "unserved-routes-and-parameters",
			steps: []scriptStep{
				post("/subjects/up/versions", schemaBody(avroUser1)),
				get("/contexts"),
				get("/v1/metadata/id"),
				get("/v1/metadata/version"),
				get("/schemas/guids/00000000-0000-0000-0000-000000000000"),
				get("/schemas/ids/@1?fetchMaxId=true"),
				get("/schemas/ids/@1?subject=up"),
				get("/schemas/ids/@1?subject=nobody"),
				get("/schemas/ids/@1?format=serialized"),
				get("/subjects/up/versions/1?format=serialized"),
				post("/subjects/up?deleted=true", schemaBody(avroUser1)),
				del("/subjects/up"),
				post("/subjects/up", schemaBody(avroUser1)),
				post("/subjects/up?deleted=true", schemaBody(avroUser1)),
				get("/subjects?deletedOnly=true"),
				get("/subjects?deleted=true&deletedOnly=true"),
			},
			cleanup: cleanupSubjects("up"),
		},
		// R13: a compatibility check against what is not there.
		scenario{
			name: "compatibility-of-missing",
			steps: []scriptStep{
				post("/compatibility/subjects/cm/versions", schemaBody(avroUser1)),
				post("/compatibility/subjects/cm/versions/latest", schemaBody(avroUser1)),
				post("/compatibility/subjects/cm/versions/1", schemaBody(avroUser1)),
				post("/compatibility/subjects/cm/versions?verbose=true", schemaBody(avroUser1)),
				post("/subjects/cm/versions", schemaBody(avroUser1)),
				post("/compatibility/subjects/cm/versions/9", schemaBody(avroUser2)),
				post("/compatibility/subjects/cm/versions/abc", schemaBody(avroUser2)),
				post("/compatibility/subjects/cm/versions", schemaBody(avroUser2)),
			},
			cleanup: cleanupSubjects("cm"),
		},
		// What Confluent stores for an unnamed Avro type that carries a doc or a property
		// (found while recording large-bodies: two strings with different docs got one id).
		scenario{
			name: "avro-attributes-on-unnamed-types",
			steps: []scriptStep{
				post("/subjects/un1/versions", schemaBody(`{"type":"string","doc":"d"}`)),
				get("/subjects/un1/versions/latest"),
				post("/subjects/un2/versions", schemaBody(`{"type":"string","foo":"bar"}`)),
				get("/subjects/un2/versions/latest"),
				post("/subjects/un3/versions", schemaBody(`{"type":"array","items":"int","doc":"d"}`)),
				get("/subjects/un3/versions/latest"),
				post("/subjects/un4/versions", schemaBody(`{"type":"map","values":"int","doc":"d","foo":1}`)),
				get("/subjects/un4/versions/latest"),
				post("/subjects/un5/versions", schemaBody(`{"type":"string","logicalType":"uuid","doc":"d"}`)),
				get("/subjects/un5/versions/latest"),
				post("/subjects/un6/versions", schemaBody(`{"type":"long","doc":"d","aliases":["x"],"name":"n","default":1}`)),
				get("/subjects/un6/versions/latest"),
				post("/subjects/un7/versions", schemaBody(`{"type":"record","name":"R","fields":[{"name":"f","type":{"type":"int","doc":"inner"}}]}`)),
				get("/subjects/un7/versions/latest"),
			},
			cleanup: cleanupSubjects("un1", "un2", "un3", "un4", "un5", "un6", "un7"),
		},
		// R14: an id nobody holds.
		scenario{
			name: "unknown-id",
			steps: []scriptStep{
				get("/schemas/ids/@999"),
				get("/schemas/ids/@999/schema"),
				get("/schemas/ids/@999/subjects"),
				get("/schemas/ids/@999/versions"),
				get("/schemas/ids/-1"),
				get("/schemas/ids/abc"),
			},
		},
	)
}
