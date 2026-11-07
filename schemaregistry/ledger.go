package schemaregistry

// The registry's REST routes by tier, as docs/api-support.md lists them (its
// "Schema registry REST" table is generated from this, and a test fails for a
// route served with no row, or a row for a route not served that is not S).
//
//   - F: served, and answers as Confluent's registry does (recorded against
//     cp-schema-registry 7.7.1 where a script covers it; docs/schema-registry.md says
//     where it differs on purpose).
//   - X: served, doze-kafka's own: not a Confluent path (/_doze/).
//   - S: not served; a request for it answers 404 in Confluent's JSON. Every S row
//     says why.

type ledgerRow struct {
	Route, Tier, Note string
}

var routeLedger = []ledgerRow{
	{"GET /{$}", "F", "`{}`, as Confluent answers its root."},
	{"GET /subjects", "F", "`?deleted=true`, `?deletedOnly=true`, `?subjectPrefix=`."},
	{"GET /subjects/{subject}/versions", "F", "`?deleted=true`."},
	{"POST /subjects/{subject}/versions", "F", "`?normalize=true`; READONLY answers a held schema with its id."},
	{"POST /subjects/{subject}", "F", "Lookup: the version a schema is registered as."},
	{"DELETE /subjects/{subject}", "F", "Soft, then `?permanent=true`."},
	{"GET /subjects/{subject}/versions/{version}", "F", "`latest` or a number; `?deleted=true`."},
	{"GET /subjects/{subject}/versions/{version}/schema", "F", ""},
	{"GET /subjects/{subject}/versions/{version}/referencedby", "F", ""},
	{"DELETE /subjects/{subject}/versions/{version}", "F", "Soft, then `?permanent=true`."},
	{"GET /schemas", "F", "`?subjectPrefix=`, `?deleted=true`, `?latestOnly=true`, `?offset=`, `?limit=`."},
	{"GET /schemas/types", "F", "AVRO, JSON, PROTOBUF."},
	{"GET /schemas/ids/{id}", "F", "`?fetchMaxId=true` answers `maxId`."},
	{"GET /schemas/ids/{id}/schema", "F", ""},
	{"GET /schemas/ids/{id}/subjects", "F", "`?deleted=true`."},
	{"GET /schemas/ids/{id}/versions", "F", "`?deleted=true`."},
	{"POST /compatibility/subjects/{subject}/versions/{version}", "F", "`?verbose=true`: Confluent's reasons, word for word."},
	{"POST /compatibility/subjects/{subject}/versions", "F", "Against what the level says; `?verbose=true`."},
	{"GET /config", "F", ""},
	{"GET /config/{subject}", "F", "`?defaultToGlobal=true`."},
	{"PUT /config", "F", ""},
	{"PUT /config/{subject}", "F", "Refused in READONLY, as recorded."},
	{"DELETE /config", "F", ""},
	{"DELETE /config/{subject}", "F", ""},
	{"GET /mode", "F", ""},
	{"GET /mode/{subject}", "F", "`?defaultToGlobal=true`."},
	{"PUT /mode", "F", "READWRITE and READONLY; IMPORT is refused."},
	{"PUT /mode/{subject}", "F", "READWRITE and READONLY; IMPORT is refused."},
	{"DELETE /mode/{subject}", "F", ""},
	{"GET /_doze/overview", "X", "Every subject, level, mode and id in one answer, with the next free id (the console's)."},
	{"GET /_doze/requests", "X", "The newest 2,000 requests, by subject (the console's wire dock)."},
	{"GET /_doze/subjects/{subject}/versions/{version}", "X", "A version with when it was registered and by whom."},
	{"GET /_doze/subjects/{subject}/versions/{version}/resolved", "X", "An Avro schema with its referenced types written out."},
	{"POST /_doze/subjects/{subject}/check", "X", "A check at any level, the subject's left as it is, each reason taken apart."},
	{"GET /contexts", "S", "Contexts (Schema Linking) are not offered: one registry, one context."},
	{"GET /exporters", "S", "Schema Linking's exporters are not offered."},
	{"DELETE /mode", "S", "Not a Confluent route either: the global mode is set with PUT."},
	{"GET /subjects/{subject}/metadata", "S", "Data contracts (metadata, rules, tags) are not offered."},
	{"POST /dek-registry/v1/keks", "S", "Client-side field encryption's key registry is not offered."},
}
