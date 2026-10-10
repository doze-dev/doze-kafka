# Schemas

Stage SCH. Size L. Follows [../01-protocol.md](../01-protocol.md); the common acceptance criteria there apply in full, and this page adds Schemas' own. Foundation items (`A1`, `A4`, `B3` ...) are in [../00-foundation.md](../00-foundation.md).

## 1. Summary

Schemas is the built-in Confluent-compatible registry (Avro, JSON Schema, Protobuf), the console section that drives it, and the console's reading of registry-encoded record values. It is the one surface whose oracle already exists: about 1,460 schema pairs and 236 scripted REST steps were recorded from a real Confluent Schema Registry 7.7, and the Go registry replays them. That makes the compatibility and canonical-form logic the best-proved code in the repository, and it makes the gaps easy to name: everything the recording does not cover is `read` or `memory` (section 8), and everything the design adds on top (timestamps, who registered, request log, topic scan, structured reasons, resolved view) has no data behind it at all.

Size, measured: `schemaregistry/` is 7,900 lines with tests (6,100 without), `console/handlers_schemas.go` 366, `console/registry.go` 430 (the REST client, the codec cache and the line diff), `console/templates/schemas.html` 124, `server/registry.go` 42. The console reaches the registry **over HTTP like any client** (`console/registry.go:23`), even though both live in one process. That is a good property (the console works against any Confluent-compatible registry) and it is the reason every "new" board item below that needs registry-internal data must become either an extension endpoint under a `/_doze/` prefix or an in-process interface. Section 11 picks the extension endpoint.

Peers: [topics](topics.md) (the record table and drawer call `msgView` / `readThroughRegistry`; the composer calls `encode`; the topic page links `<topic>-key` / `<topic>-value`), [traffic](traffic.md) (the "wire, underneath" strip on every board needs registry requests, which the Kafka recorder never sees), [connect](connect.md) (the registry line in the snippets, and the URL the console prints), [groups](groups.md) (lag in neighbours), [map](map.md) (subject edges), [access](access.md) (the registry has no authorisation, only authentication), [trace](trace.md) (its bounded scanner is the model for the topic scan in SCH-25).

Packages owned: `schemaregistry/` (REST handlers, avro/json/protobuf engines, `protofromdesc.go`, `protoprint.go`, compatibility, references, normalize, modes, config levels, soft and permanent delete, id allocation), `server/registry.go`, the registry's storage in `internal/storage/meta/registry.go`, `console/handlers_schemas.go`, `console/registry.go`, `console/templates/schemas.html`, and the console's decoding of registry-encoded payloads.

What the audit found, in one paragraph: the registry is faithful where it was recorded and unprotected where it was not. Two requests can hold the registry-wide mutex for minutes (an exponential reference graph; a deeply nested Avro schema compared with a previous one), a 200 MB `PUT /config` body is accepted, any web page can `POST` a schema to a developer's registry (no `Content-Type` check, no Host guard that the console has), the Avro reasons in a 409 are not in the shape Confluent sends and `docs/schema-registry.md` says they are, and the console's delete hard-deletes in one click with a confirm that says less than it does.

**Estimate:** L, 18 days (36 half-days), see section 11. **Actual:** _fill in when done._

## 2. Contract

### Kafka APIs the surface touches

The registry is not on the Kafka wire (there is no `_schemas` topic; state is in `meta.db`, `docs/schema-registry.md` "Persistence"). The console uses Kafka as a client for the decode and compose paths. Tiers from `internal/protocol/versions.go`; no API changes tier in this stage.

| Key | API | Used by Schemas for | Now | Target | Note |
|---|---|---|---|---|---|
| 1 | Fetch | the message table and drawer read records, then `decode`; the topic scan of SCH-25 | F | F | The scan is new console work, bounded like Trace's. |
| 0 | Produce | the composer writes `0x00 + id + payload` built by `Codec.Encode` | F | F | Owned by [topics](topics.md). |
| 3 | Metadata | topic list for "topics using it" and `topicSchemas` | F | F | |
| 2 | ListOffsets | scan bounds (earliest, latest) | F | F | |
| 17, 36, 18 | SaslHandshake, SaslAuthenticate, ApiVersions | the console's own connection as `__console` | F | F | Same user is used for the registry (`cmd/doze-kafka/main.go:485`). |

### Registry REST endpoints

Source: `schemaregistry/api.go` routes. "Recorded" is the number of steps for that route in `schemaregistry/testdata/conformance/*.json` (236 steps, 15 files; `protodesc.json` is a separate 5-case file). A recorded route is `recorded`; an unrecorded one is `read`.

| Method and path | Recorded steps | Now | Target | Note |
|---|---|---|---|---|
| `GET /` | 1 | F | F | |
| `GET /subjects` (`subjectPrefix`, `deleted`, `deletedOnly`) | 7 | F | F | `deletedOnly` and `limit`/`offset` are not in the recording. SCH-8. |
| `GET /subjects/{s}/versions` | 13 | F | F | |
| `POST /subjects/{s}/versions` (`normalize`) | 74 | F | F | No `Content-Type` check (SCH-5). Empty `schema` returns the latest, as recorded. |
| `POST /subjects/{s}` (lookup; `deleted`, `normalize`) | 8 | F | F | `deleted=true` on the POST is not recorded. |
| `DELETE /subjects/{s}` (`permanent`) | 8 | F | F | `40405` (not soft deleted first) is not recorded. |
| `GET /subjects/{s}/versions/{v}` (`latest`, `-1`, number) | 40 | F | F | `format=` is ignored; not recorded. |
| `GET .../{v}/schema`, `GET .../{v}/referencedby` | 4 | F | F | |
| `DELETE /subjects/{s}/versions/{v}` (`permanent`) | 10 | F | F | Version number reuse after a permanent delete of the latest is not recorded (section 8). |
| `GET /schemas` (`latestOnly`, `deleted`, `subjectPrefix`) | 3 | F | F | Unbounded; the console calls it on every page (SCH-8). |
| `GET /schemas/types` | 1 | F | F | |
| `GET /schemas/ids/{id}` and `/schema` | 7 | F | F | `fetchMaxId`, `subject`, `format` are ignored (run). Unknown id is 404 / 40403 where Confluent answers 500 (documented difference). |
| `GET /schemas/ids/{id}/subjects`, `/versions` | 6 | F | F | |
| `POST /compatibility/subjects/{s}/versions[/{v}]` (`verbose`, `normalize`) | 11 | F | F | Reasons shape differs for Avro (SCH-6). |
| `GET/PUT/DELETE /config`, `/config/{s}` (`defaultToGlobal`) | 14 | F | F | `PUT` is allowed in READONLY mode (run). |
| `GET/PUT /mode`, `GET/PUT/DELETE /mode/{s}` | 9 | F | F | `READWRITE`, `READONLY` only; `IMPORT`, `READONLY_OVERRIDE` are S (answered `42204`). `force` ignored. |
| `GET /contexts`, `/v1/metadata/id`, `/v1/metadata/version`, `/schemas/guids/{guid}` | 0 | absent (404 JSON) | S | Return the honest 404 today; decide per SCH-35 whether `/v1/metadata/id` is worth a stub. |
| `/_doze/...` (extension: overview, version metadata, request log, resolved view, check at level) | 0 | absent | F (extension) | New backend, SCH-20 to SCH-23. Not Confluent paths; documented as extensions. |

Error codes the registry can return, and whether the recording has them (statuses and `error_code` counted over all 236 steps): `400`, `404`, `405`, `409`, `40401`, `40402`, `40403`, `40404`, `40406`, `40407`, `40408`, `40409`, `42201`, `42202`, `42203`, `42204`, `42205`, `42206` are recorded. **Defined and not recorded:** `40405` (subject not soft deleted; `errors.go:21`), `42207` (invalid request; the three `PUT` handlers use it for a bad body while `readInput` uses `400` for the same mistake, so the two disagree with each other), `50001` (store error; also what an over-long subject produces, SCH-18), `415` (constant `codeUnsupportedMediaType` is declared and never returned), `401` (Confluent's behaviour with basic auth is not recorded), `413`/oversize (nothing returns it). **Not defined at all:** `50002`, `50003` (Confluent's timeout codes; irrelevant here, no leader election).

### Console routes

From `console/console.go:250-258` plus the pages that embed registry data.

| Route | Method | Handler | Mutates | Now | Target | Note |
|---|---|---|---|---|---|---|
| `/schemas` | GET | `schemas` | no | F | F | Calls `GET /schemas?latestOnly=true`: every schema text, to show names. SCH-8. |
| `/new/schema` | GET, POST | `schemaCreatePage`, `schemaCreate` | POST yes | F | F | No references field in the form; `normalize` not offered. |
| `/schemas/{subject}` (`?version=`) | GET | `schemaDetail` | no | F | F | 6 to 8 REST calls per render; `?version=abc` silently shows latest. |
| `/schemas/{subject}/check` | POST | `schemaCheck` | no | F | F | Ignores mode (SCH-14). |
| `/schemas/{subject}/register` | POST | `schemaRegister` | yes | F | F | |
| `/schemas/{subject}/config` | POST | `schemaConfig` | yes | F | F | Subject level only; no global level, no mode. |
| `/schemas/{subject}/delete` | POST | `schemaDelete` | yes | F | F | Soft then permanent in one request (SCH-16, SCH-17). |
| `/topics/{t}` schema chips; message table, drawer, composer menu | GET, POST | `topicSchemas`, `msgView`, `readThroughRegistry`, `composerSchemas` | composer yes | F | F | TopicNameStrategy only (SCH-26). |
| `/schemas/{subject}/mode`, `/schemas/config` (global), `/schemas/{subject}/usage`, `/schemas/{subject}/compare`, `/schemas/ids/{id}` | GET, POST | new | mode, global level yes | absent | F | Boards Sc-09, Sc-11, Sc-15, Sc-17. |

Declined, to be written in `docs/limitations.md` (some already are): `IMPORT` and `READONLY_OVERRIDE` modes, contexts (`:.name:` subjects are stored as literal names, SCH-37), data contracts, schema linking, exporters, `_schemas` topic.

## 3. Where it stands

| Layer | What exists |
|---|---|
| Package tests | `schemaregistry/`: 18 `Test*`/`Fuzz*` functions plus table-driven corpora. Restart (`TestEverythingSurvivesARestart`), failed write (`TestAFailedWriteChangesNothing`), concurrency (`TestConcurrentRegistration`), references (`TestReferencesInJSONAndProtobuf`), unusual names (`TestSubjectsWithUnusualNames`), basic auth (`TestBasicAuthIsRequiredWhenConfigured`), levels (`TestCompatibilityLevelsChooseTheVersionsCompared`), codec round trips (5), descriptors (2). |
| Recorded oracle | `TestConformance`: 236 steps in 15 scripts (register-and-read, not-found-and-bad-input, compatibility-*, transitive, modes, other-schema-types, references, deleting, reregistering-after-delete, listing, normalize, canonical-forms x2, register-checks-the-right-versions). `TestCorpus`: 392 Avro, 467 JSON Schema, 600 Protobuf pairs at BACKWARD, FORWARD, FULL (a third random from a fixed seed). `TestADescriptorIsStoredAsConfluentStoresIt`: 5 descriptors. |
| What the oracle does not compare | The 409 `details:` text and the verbose `messages` except their last three lines (`conformance_test.go:257-286` strips them). Verdicts are compared; reasons are not. |
| Fuzz | `FuzzRegisterAndCompare`, `FuzzDecode`, both in `task fuzz`. `FuzzRegisterAndCompare` skips inputs over 16 KiB (`fuzz_test.go:23`), so the depth and time problems of SCH-2 are outside it. Nothing fuzzes the HTTP layer, references, descriptors or `WireID`/`Decode` against a parsed schema. |
| Console tests | `console/schemas_test.go`: 8 tests (section only with a registry, subject lifecycle, check says why not, names round trip through links, registry-encoded records decoded, composer encodes, palette lists subjects, `diffLines`). |
| Browser | `e2e/tests/schemas.spec.ts`: 8 tests. No test for a refused register, READONLY, a soft-deleted subject, references, Protobuf evolution, or the registry being off. |
| Real clients | `compat/java` `SerdeCheck` (Avro and Protobuf serializer/deserializer round trips, one bad-schema case; **default TopicNameStrategy only**), `compat/python` `serdes.py` (Avro, JSON Schema, Protobuf from a descriptor, `test_compatibility`, refused registration), both plain and SCRAM over TLS with `basic.auth.user.info`. `compat/confluent-sr` and `compat/apicurio` run *other* registries against this broker; they do not exercise `schemaregistry/`. |
| Mutation | none (B3). |

Coverage today (full suite, 2026-10-11):

| Package | Statement coverage | Functions under 80% in the owned files |
|---|---|---|
| `schemaregistry` | 88.4% | `API.Registry` 0%, `Error.Error` 0%, `Codec.Type` 0%, `protoprint extend` 0%, `refsString` 33%, `equalStrings` 33%, `primitiveToAvro` 52%, `stringToBytes` 62%, `jsoncompat resolve` 63%, `intPtr` 60%, `NewCodec` 71%, `canRead` 71%, `prefixParam` 75% |
| `console` | 87.8% | `schemaCreatePage` 40%, `schemaFromForm` 46%, `schemaCreate` 57%, `schemaDelete` 60%, `registryClient.resolve` 31%, `typeLabel` 50%, `DeleteVersion` and `DeleteSubject` 75%, `msgView` 75%, `failSubject` 77% |
| `server` | 79.0% | `SchemaRegistry` in `server/registry.go` has no test in `server/` (it is reached only through `cmd/` and `compat/`) |

## 4. Findings

Evidence: `run` = reproduced against a binary built from `main` at `970d829` on 2026-10-11 (ports 41xxx, 42xxx; instances stopped afterwards); `read` = from the code. Source: `recorded` = settled by the Confluent recording, `spec` = Confluent's REST reference, `memory` = a recollection that goes on the queue (section 8), `doc` = this repository's own documents.

### Bounds, hangs and exposure

| ID | Sev | What | Where | Ev | Src | Test first |
|---|---|---|---|---|---|---|
| SCH-1 | P1 | Reference resolution walks the graph without memoising: `walk` recurses into a reference's references **before** it checks `seen`, so a schema that references the same subject twice per level costs 2^depth. 25 levels of two JSON Schema references each took 7.0 s for one `POST` (1.8 s at 23, doubling each level); depth is capped at 32, so the ceiling is minutes. It runs with `r.mu` held, so every other request (and every serializer in the app) blocks. | `registry.go:223-257` | run | memory | Unit: build the 26-level diamond in a `Registry`, assert `Register` returns within 200 ms (resolve each `(subject, version)` once; cap total resolved nodes at e.g. 1,000 and answer `42201`). |
| SCH-2 | P1 | No budget on schema work. A 3,000-deep nested Avro array compared with a previous version of the same subject took 15.5 s to answer `409` (19 s at 5,000), quadratic in depth, under the global lock. `FuzzRegisterAndCompare` cannot find it (inputs capped at 16 KiB). The body limit is 8 MiB, so the only cap is memory. | `registry.go:323` (`compatibleWith`), `avro.go`, `fuzz_test.go:23` | run | memory | Unit: nested array depth 3000 old vs new, assert under 1 s or refused with `42201` ("schema nested more than N levels"). Add a depth cap in `prepare` for all three types and a per-request deadline for `compare`. |
| SCH-3 | P1 | `PUT /config`, `PUT /config/{s}`, `PUT /mode`, `PUT /mode/{s}` decode `r.Body` with no limit. A 200 MB body with an extra field was accepted (`200`, 0.4 s) and the broker's resident memory went to 510 MB. `readInput` is limited to 8 MiB; these four are not. | `api.go:341`, `api.go:391` | run | read | HTTP: `PUT /config/x` with a 20 MiB body gets `413` (or `42207`) and the broker's heap does not move; same for `/mode`. Wrap `r.Body` in `http.MaxBytesReader` in `ServeHTTP` for every route (64 KiB for config and mode, 8 MiB for schemas). |
| SCH-4 | P1 | The registry answers any `Host`. The console refuses `Host: evil.example.com` with 403 (`console/hostguard.go`, written to stop DNS rebinding); the registry, bound to the same loopback, returns `200` and the schema. A page on a rebinding name can read every schema and, with `Content-Type` unchecked (SCH-5), write them. No `--console-allow-host` equivalent exists. | `cmd/doze-kafka/registry.go:37`, `api.go:40` | run | doc | HTTP: request with `Host: evil.example.com` gets 403; `127.0.0.1`, `localhost`, `*.doze`, the advertised host and `--console-allow-host` names still work. Move the guard into a shared package used by both. |
| SCH-5 | P1 | Content type is ignored: `POST /subjects/x/versions` with `Content-Type: text/plain`, with none, and with `application/xml` all register (run, three subjects). A cross-origin `<form>`/`fetch` with `text/plain` is a "simple request" and needs no preflight, so any web page the developer visits can register schemas, check compatibility and run lookups against `127.0.0.1:8081` when SASL is off. `codeUnsupportedMediaType` (415) is declared and never used. Whether Confluent answers 415 is `memory`; the CSRF reason to require a JSON-ish type holds either way. | `api.go:108` (`readInput`), `errors.go:40` | run | memory | HTTP: body with `text/plain`, no type, `application/xml` each answer 415 `{error_code:415}`; `application/json`, `application/vnd.schemaregistry.v1+json` and `application/vnd.schemaregistry+json` still work; the rest of the accepted list is whatever R4 records. Record first (section 8, R4). |
| SCH-6 | P1 | The 409 body and the verbose compatibility `messages` for Avro are not in Confluent's shape. Recorded: `{errorType:'READER_FIELD_MISSING_DEFAULT_VALUE', description:'The field 'channel' at path '/fields/5' in the new schema has no default value and is missing in the old schema', additionalInfo:'channel'}`. Here: `reader field y is missing in writer schema and has no default` (run). JSON and Protobuf reasons use `{errorType:"X", description:"..."}` with **double** quotes, Confluent's use single. `docs/schema-registry.md` says "the same shape; the three trailing lines are identical, the reasons are worded by this implementation": false for Avro. `TestConformance` strips `details:` so nothing noticed. The board Sc-04 table (errorType, path, fix) cannot be built from strings. | `registry.go:333-345`, `avro.go` (`avroCanRead`), `jsoncompat.go:527`, `protocompat.go:73`, `docs/schema-registry.md` "Where it differs" | run | recorded | Differential: record the `messages` of every corpus pair that is incompatible (R9) and assert the multiset of `errorType` values equals Confluent's; unit: `CompatFinding` carries `{Type, Path, Description, Info}` and `Messages()` renders Confluent's exact format. |
| SCH-7 | P1 | A Protobuf schema sent as a base64 `FileDescriptorProto` is built with `protodesc.NewFile(&fd, protoregistry.GlobalFiles)`: only the well-known types resolve. confluent-kafka for Python registers a file's dependencies first and sends the main descriptor with `references`; the descriptor then names `dep.proto`, which is not in `GlobalFiles`, so `NewFile` fails and the schema is refused `42201`. The recording (`protodesc.json`, 5 cases) and `compat/python` have no dependency. `Register` resolves `in.References` before parsing (`prepare`) but `parseDescriptor` does not use them. | `protofromdesc.go:41-55`, `registry.go:274-286` | read | memory | Unit: build two descriptors (`dep.proto`, `main.proto` importing it) in Go, register `dep` then `main` with a reference, expect `200`; then add the same to `compat/python/serdes.py` using `ProtobufSerializer` on a message that imports another. Confirm with the Docker recording R11 first. |

### Server and durability

| ID | Sev | What | Where | Ev | Src | Test first |
|---|---|---|---|---|---|---|
| SCH-8 | P2 | List endpoints are unbounded and ignore `limit` and `offset` (`/subjects?limit=1&offset=1` returned all three subjects; `/subjects/{s}/versions?limit=1` too). The console calls `GET /schemas?latestOnly=true` on every page render and poll to fill the left list, which sends every latest schema text; with 5,000 subjects that is megabytes per click. Confluent documents `limit` and `offset` on these (`doc`, queue R6). | `api.go:136,152,220`, `console/registry.go:170` | run | doc | HTTP: with 5 subjects, `GET /subjects?limit=2&offset=1` returns 2 names; `limit=0`, `-1`, `abc` as recorded. Console: with 600 subjects the list renders 500 and says "500 of 600"; the sidebar fetch does not carry schema text (use the overview of SCH-23). |
| SCH-9 | P2 | The registry logs nothing. `Options.Logger` is stored and never read; no request line, no auth failure, no 409, no 500. A failing `Apply` returns `50001` with the bolt error and leaves no trace in the broker log. | `api.go:19`, `registry.go:249` | read | doc | HTTP: a 401, a 409 and a 500 each produce one structured log record (method, path, status, user, took); a successful `GET` logs at debug. |
| SCH-10 | P2 | `http.Server` has `ReadHeaderTimeout` only: no `ReadTimeout`, `IdleTimeout` or `MaxHeaderBytes`. A client that sends headers and then trickles a body holds a goroutine and a descriptor indefinitely; combined with SCH-3 the memory is also unbounded. | `cmd/doze-kafka/registry.go:37` | read | doc | `cmd` test: a connection that sends headers and no body for 30 s is closed (set `ReadTimeout` 30 s, `IdleTimeout` 120 s, `MaxHeaderBytes` 64 KiB; shorten under test). |
| SCH-11 | P2 | The registry is plain HTTP even when the broker has `--tls-cert`; with SASL on, clients and the console send basic-auth passwords in clear. `docs/auth-and-tls.md` does not mention the registry at all (`grep` finds none), and `docs/schema-registry.md` says only "requires the same user names and passwords as HTTP basic authentication". | `cmd/doze-kafka/registry.go:33-37` | read | doc | `cmd` test: with TLS configured the registry either serves HTTPS with the broker's certificate (and the console's client trusts it) or the startup summary says "registry: plaintext". Decide in section 11; the document line is owed either way. |
| SCH-12 | P2 | `Server.SchemaRegistry()` builds a new `Registry` over the same `meta` store on every call. Each keeps its own `nextID`, `schemas` and `subj` maps. An embedder who calls it twice (one per listener) gets two registries that disagree, and two different schemas can be given the same id; the second `put(schemaKey(id))` overwrites the first in bolt. | `server/registry.go:25-28`, `registry.go:85-123` | read | doc | `server` test: call `SchemaRegistry()` twice, register different schemas through each, assert distinct ids (return the same handler, cached under a mutex), and that the second call's handler sees the first's writes. |
| SCH-13 | P2 | One unreadable record stops the registry from opening, and `startRegistry` returning the error stops the broker (`main.go:282-285`). A record written by a newer version, or truncated, takes Kafka down for a problem in a side feature. | `registry.go:97-112`, `cmd/doze-kafka/main.go:282` | read | doc | Unit: `MemoryStorage` seeded with `schema\x00 0000000001` = `not json`; `Open` returns an error naming the key; `cmd` test: the broker starts with the registry disabled and the console says why (decision in section 5). |
| SCH-18 | P2 | A subject longer than bbolt's 32,768-byte key limit is parsed and compared, then fails at commit: `500 {"error_code":50001,"message":"Error while committing to the schema store: key too large"}`, and the 40 KB subject is echoed into the error. No subject-length limit, no control-character rule, and `" "` (a space) is a valid subject (SCH-37). | `registry.go:249`, `registry.go:107` (key layout) | run | memory | HTTP: subject of 40,000 characters answers 422 `42207` (or the recorded code) and the message does not echo it; a 255-character subject works. Limit per R7. |
| SCH-29 | P2 | Twelve error paths and nine query parameters have no recording (section 2). The two that matter most: the status/body of `40405`, and what Confluent does to the config and mode of a subject on permanent delete (the code deletes both, `registry.go:834-836`). The recording queue (section 8) lists the scenarios; until then they are `memory`. | `registry.go:781-850` | read | memory | Differential: scripts R1, R2, R3, R6, R8, R10 recorded and replayed with zero "answered differently". |

### Behaviour that differs between paths

| ID | Sev | What | Where | Ev | Src | Test first |
|---|---|---|---|---|---|---|
| SCH-14 | P2 | The console's check form says "Compatible. This schema may be registered under the FULL rule" for a subject in READONLY mode; Register then fails with `42205`. `CheckCompatible` never reads the mode, and `Register` reads it before anything else. (Run: `csrf` was READONLY, check answered Compatible, register answered "Subject csrf is in read-only mode".) The two also differ on an identical schema that is already registered: Register returns the existing id without checking, Check compares and may say no. | `registry.go:850-905` vs `registry.go:373-391`, `console/templates/schemas.html` `schema_check` | run | recorded | Console: READONLY subject, valid schema, check shows Refused `42205` before any verdict. Registry property test: for random pairs and each level, `CheckCompatible` says compatible iff `Register` succeeds, except the documented already-registered case. |
| SCH-34 | P3 | `compatibleWith` (used by Register) and `CheckCompatible` each carry their own copy of "which versions, which level, resolve references, call `compare`". They agree today on the recording; nothing keeps them agreeing. The answer to "is the console's check the same engine as register" is: same `compare`/`canRead` functions, **two copies of the selection loop**. | `registry.go:323-358`, `registry.go:850-905` | read | doc | The property test of SCH-14, run under `-race` with the fuzz corpus as seeds. Then collapse to one function returning `[]CompatFinding`. |
| SCH-15 | P2 | The console turns every registry 409/422/400 into its `errBadInput` and then into HTTP 400 with the bare message: READONLY shows "Subject csrf is in read-only mode [400]" with no `42205`, no "Refused"; a 409 shows the whole `details:` blob. The boards (Sc-04, Sc-15) show the status, the `error_code` and one rule. `regError` carries `Status` and `Code`; the page drops both. | `console/registry.go:34-45`, `console/errors.go:45`, `handlers_schemas.go:72` | run | doc | Console: register an incompatible schema, assert the response carries `data-refusal-name="409"` (A2's `Refusal{Name, Message, Rule}`), the registry's wording, and the rule for the level from the settings table. |
| SCH-16 | P2 | Console delete does more than it says. `DeleteVersion` and `DeleteSubject` issue a soft delete and then `?permanent=true` back to back (`registry.go:248-262`). The confirm says "Delete subject X with all N of its versions? ... nothing can decode them by id afterwards": N counts live versions only (a client's soft-deleted versions go too), and "nothing can decode them" is false when the id is shared with another subject (`idt2` and `idt3` kept id 31, run). If the first call succeeds and the second fails the subject is left soft-deleted with no way to see it (SCH-17). The version delete confirm says only "Delete version 3 of X?". The board's two dialogs (soft delete, then type-the-name permanent) are the right shape and match the API. | `console/registry.go:248-262`, `templates/schemas.html:60-61,78-79` | run | doc | Console: delete v2 of a subject that shares its id with another subject, assert the confirm names the shared id and the test shows the other subject still decodes; for a subject with a soft-deleted v1, the confirm count equals the versions the request removes (A6 test). |
| SCH-17 | P2 | A subject a client soft-deleted cannot be found or removed from the console: the list (`latestOnly`, no `deleted=true`) omits it, `/schemas/{subject}` is a 404 page, and `POST .../delete` answers `404 Subject 'idt' was soft deleted.Set permanent=true to delete permanently` (run) with no way to follow that instruction. | `console/registry.go:170-180`, `handlers_schemas.go:247` | run | recorded | Console: soft-delete a subject over REST; the list shows it under "Deleted" with a "Delete for good" action; the action sends only the `permanent` call. |
| SCH-28 | P2 | The fuzz targets and the HTTP layer: `FuzzRegisterAndCompare` caps inputs at 16 KiB (`fuzz_test.go:23`); nothing fuzzes `readInput`, references, `parseDescriptor` (base64 to `proto.Unmarshal` to `protodesc.NewFile`), `WireID`+`Decode` against a parsed schema of each type, or `schemaFromForm` (references JSON). SCH-1 and SCH-2 are exactly what a time-bounded fuzz would find. | `fuzz_test.go`, `codec_test.go:153` | read | doc | Fuzz: `FuzzRegisterReferences` (graph shapes), `FuzzDescriptor`, `FuzzAPIBody` (every route, random body and `Content-Type`), each with a per-iteration deadline of 2 s; add all three to `task fuzz`. |
| SCH-27 | P2 | No test reaches `server/registry.go`. The SASL wiring (`creds[user]` compared in constant time to the **cleartext** map) is exercised only by `compat/` cells. Nothing asserts that `__console` is accepted, that a SCRAM-only broker accepts the same password over Basic, that every route (including `/` and a 404) answers 401 without credentials, or that a rejected attempt is logged. The registry also has no authorisation: any authenticated user can delete any subject, whatever `--acl-default deny` says for topics. | `server/registry.go:29-41` | read | doc | `server` test: SASL on, `GET /subjects` with no header, wrong password, right password, and `__console`; `OPTIONS` and unknown path both 401. Doc: "the registry authenticates, it does not authorise". |

### Console and decode path

| ID | Sev | What | Where | Ev | Src | Test first |
|---|---|---|---|---|---|---|
| SCH-24 | P2 | `decode` returns `(decoded, bool)`: an unknown id (`40403`), a registry that is off, a 401, a payload that does not match its schema and an unreachable registry all produce the same raw display, and the user is not told why. Boards Sc-14 ("shown raw, with the reason 'registry is off'") and Sc-17 ("40403, schema id 9") need the reason. The negative cache is 5 s for every error, so a table of unknown ids re-asks the registry for each id every 5 s, and the positive cache never refreshes the `subject, version` tag chosen when the id was first seen. The cache is cleared wholesale at 512 entries. | `console/registry.go:276-373` | read | doc | Console: decode a value with id 9 against a registry that has 1 to 8, assert `Reason{Kind:"not-found", Code:40403, ID:9}` in the drawer and the row; a value with a good id and a payload cut short gives `Reason{Kind:"mismatch"}`; registry down gives `Kind:"unreachable"` and no 5-second retry storm (one probe, then back off). |
| SCH-26 | P2 | Naming strategies: the console only links `<topic>-key` and `<topic>-value` (`topicSchemas`). `RecordNameStrategy` and `TopicRecordNameStrategy` subjects register fine (the registry accepts any name) but the topic page shows no schema chip and the neighbours sentence of A5 has no subject for them; the tag in the drawer does find the subject, through `/schemas/ids/{id}/versions`, only when the message has been decoded. Board Sc-01 says "With RecordNameStrategy the subject is the record's full name ... Both work here." No `compat/` cell uses a non-default strategy (`SerdeCheck` sets none). | `handlers_schemas.go:284-300`, `compat/java/.../SerdeCheck.java` | read | doc | Compat cell: Java Avro serializer with `value.subject.name.strategy=RecordNameStrategy` and `TopicRecordNameStrategy`, round trip, then assert the console topic page lists the subject (find it through the ids seen in the topic, SCH-25). |
| SCH-25 | P2 | The scan behind "Topics using it", "messages carry id N", "720 still on v1", the Sc-12 message counts and the Sc-17 undecoded list does not exist. It must read records, so it must be bounded (records, bytes, time) and say what it left unread, as Trace does (`console/trace_scan.go`). | new; model `console/trace_scan.go` | read | doc | Console: seeded topic with ids 7, 4, 1 and one 9; scan returns counts by id, the first and last offsets per id, and `Unread` when the budget stops it. Never scans a topic whose values lack the `0x00` prefix beyond the first 20 records (cheap reject). |
| SCH-19 | P2 | Board Sc-13 ("Subjects not loaded: the registry answered 401") cannot happen with the built-in registry: the console signs in as `__console`, whose password is generated at start and is a registry user (`main.go:419-448`); `console.Options.RegistryURL` is set from the built-in registry only (`main.go:485`) and no flag points the console elsewhere. The board's "5 subjects exist" is unknowable from a 401. "The password is checked against SCRAM" is false: `server/registry.go:35` compares the cleartext map entry. A sign-in form needs a console session store, which the console does not have. | `cmd/doze-kafka/main.go:485`, `server/registry.go:33-37`, board Sc-13 | read | doc | Decision, not code: cut the sign-in form and the count; keep a "Refused, 401" page for an embedder who gives the console a bad registry credential, and write the test that the console shows it (httptest registry that always 401s). |
| SCH-20 | P2 | Needs backend: per-version metadata. `versionRec` is `{subject, version, id, deleted}` (`registry.go:62`): no `createdAt`, no registering principal (basic-auth user), no remote address or `User-Agent`, and nothing about what it was checked against. Boards Sc-05 ("Registered", "Checked against"), Sc-06 (times in the headers), Sc-15 ("mode set at 14:21"), Sc-18 ("registered 14:36:15 by checkout-api") all show these. "By checkout-api" is a **guess**: HTTP carries no Kafka client id. Only the basic-auth user and the `User-Agent` are facts; correlating to a Kafka client by time and address is a traffic-stage inference and the board must say "probably". Mode and level also need `setAt`. | `registry.go:62-67`, `registry.go:437-459` | read | doc | Store: register, restart, read `createdAt`, `by`, `from`, `agent`; old records without the fields decode to zero values and render "unknown". Extension endpoint `GET /_doze/subjects/{s}/versions/{v}` returns them. |
| SCH-21 | P2 | Needs backend: the "wire, underneath" strip on every board lists requests to the registry. The Kafka traffic recorder only sees Kafka requests, and the registry keeps no log (SCH-9). Needs a bounded ring of registry requests (method, path, subject, status, took, user, remote), filterable by subject and by id, newest first with a cursor, feeding the foundation's dock (A4). | new in `api.go` | read | doc | Unit: 2,500 requests keep the newest 2,000; filter by subject returns only that subject's calls (including `/schemas/ids/7` calls that resolve to it, through the id to subject map); the ring never stores request bodies. |
| SCH-22 | P2 | Needs backend: structured findings and check-at-level. Board Sc-04 needs `{type, path, description, info}` per reason (SCH-6); Sc-06 ("Check this change at another level") and Sc-09 ("Test v3 against this level") need a level that is not the one in force; Confluent's endpoint has no such parameter, so it is an extension (`?level=FULL` on the check route, or `POST /_doze/check`). Sc-10 "Resolved view" needs the referenced types inlined, which exists nowhere (the console only lists references). | `registry.go:850`, new | read | doc | Unit: `CheckCompatible` with an explicit level returns the findings for that level without reading the stored one; `Inline(Avro)` of the Sc-10 example equals the board's resolved text. |
| SCH-23 | P2 | Needs backend: an overview for the home board. Per subject it needs type, version count, latest id, level and whether it is the subject's own, mode, last registered; for the registry: next free id, the set of ids in use (to say "id 9 is not in use here"), global level and mode, storage (`meta.db`). Today that is `GET /schemas`, then `GET /config/{s}` and `GET /mode/{s}` per subject (N+1 over HTTP), and no endpoint exposes `nextID`. The "next free id (11)" on Sc-02, Sc-04, Sc-10, Sc-12 and Sc-15 is `r.nextID`. | new in `api.go`; `registry.go:75` | read | doc | HTTP: `GET /_doze/overview` returns the five-subject fixture of B13 in one call, with `nextId` equal to what the next register returns, and the answer is the same after a restart and after a permanent delete of the highest id. |

### Documents and board text

| ID | Sev | What | Where | Ev | Src | Test first |
|---|---|---|---|---|---|---|
| SCH-30 | P3 | Board Sc-15 shows `{"error_code":42205,"message":"Subject 'harbour.depot.dispatch-value' in read-only mode"}`. Recorded text: `Subject md is in read-only mode` (`modes.json`; the code matches the recording). Fix the board, not the code. | board Sc-15, `registry.go:476` | read | recorded | Doc test (B14 style): every error string quoted in a board fixture or template equals a recorded or coded string. |
| SCH-31 | P3 | Board Sc-15 says "a subject in READONLY mode takes no new versions, no deletes and no level changes". The code blocks register and both deletes but **not** `PUT /config` (run: set `FULL` on a READONLY subject, `200`). Whether Confluent blocks it is `memory` (R3). Until recorded the board must not claim it. | board Sc-15, `registry.go:928` | run | memory | Differential R3; then either block `SetCompat` in READONLY or drop the words from the board. |
| SCH-32 | P3 | Board Sc-14: the command `doze-kafka --data ./data --schema-registry=false` uses `--data`; the flag is `--data-dir` (`unknown flag --data - did you mean --data-dir?`, run). The rail hides the Schemas item when the registry is off (`layout.html:69`), so the page the board draws is reachable only by typing `/schemas`. `schemas_off` says "give the console one", for which there is no flag. The board's "snippets leave the registry line out while it is off" is the Connect stage's. | board Sc-14, `templates/layout.html:69`, `templates/schemas.html` `schemas_off` | run | doc | Console: with the registry off the rail shows Schemas with an "off" dot, the page offers the exact command with `--data-dir`, and no sentence offers a way to "give the console one". |
| SCH-33 | P3 | The console says "Schema registered" for every successful register, including a no-op: a schema the subject already holds returns its id and creates nothing (`registry.go:429-437`). Boards Sc-02, Sc-03, Sc-10 promise "Register will create v1 ... the first stored schema gets the next free id (11)": true only for a schema the registry has never seen; otherwise the id is the existing one and the version may be new or not. | `handlers_schemas.go:70-77,213-221` | read | recorded | Console: register the same text twice, second says "Already registered as v1, id N"; register an existing schema under a second subject says "Reused id N". The register response needs `version` (a lookup after register). |
| SCH-35 | P3 | `GET /contexts`, `/v1/metadata/id`, `/v1/metadata/version`, `/schemas/guids/{guid}` answer a JSON 404 `{"error_code":404,"message":"HTTP 404 Not Found"}`. Whether any client or UI calls them is `memory`; AKHQ, Kafka UI and Control Center are the candidates. Honest today; Not implemented (A2) once the console names it. | `api.go` routes | run | memory | Record R12; for each that a compat cell's tool calls, add a cell assertion. |
| SCH-36 | P3 | `refsString` prints `...SchemaReference@0{name=..., subject=..., version=...}` with the hash hard-coded to `0` (33% covered); `codeUnsupportedMediaType` and `codeMethodNotAllowed`-style constants are declared and unused (`errors.go:36-40`). | `registry.go:289-298`, `errors.go` | read | doc | Unit: the 42201 message for a bad reference equals the recorded prefix; delete dead constants or use 415 (SCH-5). |
| SCH-37 | P3 | Subject names are not validated: `" "` registered (`200`, run); a name like `:.ctx:orders` is stored as a literal subject though contexts are declared unsupported, so a Confluent client using contexts gets a registry that quietly behaves as if there were none. | `registry.go:402` | run | memory | Differential R7; then either reject the context syntax with `42207` and a message, or document it. |
| SCH-38 | P3 | Failed authentication is neither logged nor delayed (SCH-9); a loopback attacker can try passwords at HTTP speed. The Kafka listener's SASL failure path is the model. | `api.go:40-47` | read | doc | `schemaregistry` HTTP test: five wrong passwords in a row cause a log record each and a 100 ms delay on the sixth (or document the absence). |

Count: 38 findings. P0: 0, P1: 7, P2: 22, P3: 9.

Looked for and not found: lost writes on a failed commit (`TestAFailedWriteChangesNothing` holds, and `commit` precedes every in-memory change in `registerLocked`, `DeleteVersion`, `DeleteSubject`); id reuse (a permanent delete of the highest id, a SIGKILL and a restart gave the next id from the persisted `nextid` record: 30 deleted, re-register gave 31, after kill -9 the next gave 32, run); a race (`TestConcurrentRegistration` and one mutex around every read and write); injection through names in the console (`schemaURL` uses `groupSegment`, `hx-confirm` and `data-copy` go through `html/template` attribute escaping, and `TestSubjectsWithUnusualNames` covers `/`, spaces, unicode); a panic from hostile payload bytes (`Decode` has `recover` and Avro limits `MaxByteSliceSize 8 MiB`, `MaxSliceAllocSize 1 MiB`, `codec.go:78`).

## 5. What to check

Stage-specific, on top of the common criteria.

- **Id stability.** The same schema text is one id across subjects (run: `"string"` got id 2 under `a/b`, `✓x`, `lc`). Ids survive a restart and a SIGKILL (run). After a permanent delete the id is not reused (run, single session and after kill). Check also: after the hard delete of the highest id **and** a restart **and** a register of different text; after a failed `Apply` (the id must not advance); two concurrent registrations of the same new schema give one id.
- **Hard vs soft delete.** Soft delete keeps the id resolvable at `/schemas/ids/{id}` (recorded). Permanent delete removes the schema only when no other subject holds it (`orphans`). Re-register of a soft-deleted schema replaces the old version number with a new one (recorded). Version numbers after a permanent delete of the latest: here `next = last remaining + 1`, so a number can come back (read; R1).
- **READONLY.** `42205` with the recorded text on register and on both deletes (recorded). `PUT /config` is allowed (SCH-31). The console must show `42205` and the rule (SCH-15) and the check must not say "compatible" (SCH-14). `IMPORT` is **not built**: `PUT /mode {"mode":"IMPORT"}` answers `42204` with a message that names only two modes (recorded text lists three; documented). A client that needs `IMPORT` (schema migration tools) gets a clear refusal; confirm none of the compat tools sends it.
- **Auth.** Basic auth with the SASL users, on every route including `/` and 404s (run). `__console` works for the console (run) and is also accepted by anyone who knows its password (random hex, held in memory). No authorisation, no rate limit, no log (SCH-27, SCH-38). Board 401 (Sc-13) per SCH-19.
- **References.** Resolution depth 32 in the registry, 16 in the console (`console/registry.go:329`) and no cap on fan-out (SCH-1). Nested JSON Schema references with the same `name` at two levels failed `422` (run: `a.json`/`b.json` reused at levels 1 and 2); whether Confluent resolves names per schema or globally is `memory` (R10). A referenced version cannot be deleted soft or permanent (recorded `42206`); a soft-deleted referrer no longer blocks (recorded).
- **Unknown id decode (Sc-17).** `GET /schemas/ids/9` is `404 40403 "Schema 9 not found"` (documented difference: Confluent 500). The console shows raw and no reason (SCH-24). The wire format check is only `len >= 5 && value[0] == 0` (`WireID`), so any binary value beginning with a zero byte triggers a registry call per distinct id.
- **`--schema-registry=false`.** `console.reg == nil`; the section is hidden from the rail (SCH-32) and every route answers a page (GET) or 404 (POST). The registry bucket in `meta.db` is untouched when the flag is off, so "Nothing was lost: schemas stay in meta.db" is true (read; test it).
- **Naming strategies.** Registry: any subject string. Console: only `-key`/`-value` (SCH-26). Subject length: none (SCH-18).
- **Size limits.** Schema body 8 MiB, silently truncated at the limit so an oversize body reports "unexpected end of JSON input" with `400` (run, 9 MiB). Config and mode bodies unlimited (SCH-3). Subject over 32 KiB is a 500 (SCH-18). Avro decoder allocation limits exist; JSON Schema decode does not validate the document; Protobuf decode has no size cap beyond the Fetch size.
- **Is the console's check the same engine as register?** Same `compare`/`canRead`; different selection loop, different mode handling, different already-registered handling (SCH-14, SCH-34). The console goes through HTTP (`POST /compatibility/subjects/{s}/versions?verbose=true`), so it exercises the same handler a client does, but it omits `/latest`, which the board shows.
- **Restart with the registry in use.** `registry.go:Open` rebuilds `byPrint` from every schema record: cost is linear in total schema size, one pass; test with 10,000 subjects and a 1 MB total for start time (budget 1 s).
- **Windows and paths.** The registry has no file paths of its own; the data directory with a space is covered by `meta.db`. The console reaches the registry at `http://localhost:<port>` for a wildcard bind (`httpURL`), which is right on Windows.

## 6. Console

Board status: `real` = the screen exists today; `buildable` = the data exists, the screen does not; `needs backend` = data or behaviour does not exist. The inspector, dock and neighbours pieces come from the foundation (A1, A4, A5); this section names what feeds them. Teaching lines (level descriptions, mode descriptions, normalize) come from the settings tables of B7.

Template for all boards: `console/templates/schemas.html` (to be split into `schemas_home`, `schema_detail`, `schema_register`, `schema_usage`, `schema_compare` partials); handlers in `console/handlers_schemas.go` and a new `handlers_schemas_usage.go`.

| Board | Template and handler | Status | Built from |
|---|---|---|---|
| Sc-Home | `schemas_home` / `schemas` | needs backend | Subjects, type, versions: `GET /schemas?latestOnly`. Latest id, level and own/global, mode, "last registered", `ids 1 to 10, id 9 not in use`, next free id: overview endpoint (SCH-23, SCH-20). "Topics using it": topic scan (SCH-25), approximated by TopicNameStrategy until then. "Point a client here" needs the registry's **public** URL (`ready.registryURL`), which the console is not given today (it has the dial URL); an `Options.PublicRegistryURL` is an ask of [connect](connect.md). Wire dock: SCH-21. |
| Sc-01 | `schemas_home` empty / `schemas` | buildable | `schemas.none.*` copy exists. The three-step serializer explanation is static text from the settings table (`auto.register.schemas` default). |
| Sc-02 | `schema_register` / `schemaCreatePage` | needs backend | Subject, type, schema, references editor (form has no references field today), `normalize` (not passed by `schemaFromForm`), "N lines, parses" (a check against a non-existent subject parses and answers), next free id (SCH-23). Topic exists / does not: Metadata. |
| Sc-03 | `schema_register` / `schemaCheck` | buildable | `POST /compatibility/subjects/{s}/versions/latest?verbose=true` (real). Needs the version compared in the answer ("compatible with v2"), which `is_compatible` does not carry: the console knows it (it asked for `latest`). |
| Sc-04 | `schema_register` refusal / `schemaRegister` | needs backend | The 409 is real; the table of errorType, path and fix needs structured findings (SCH-6, SCH-22). "Add default" fix text is a template keyed by `errorType`. |
| Sc-05 | `schema_detail` / `schemaDetail` | needs backend | Versions, ids, schema text, referencedby, ids/{id}/subjects (real). "What changed" per version: a summary over `diffLines` (buildable for added/removed fields via the engine). "Registered", "Checked against": SCH-20. "Messages on this id": SCH-25. Neighbours: A5. |
| Sc-06 | `schema_compare` / new | needs backend | Two `.../versions/{n}/schema` reads and `diffLines` (real, side by side and unified are view work). "Allowed at BACKWARD" is a check of v3 against v2 (real). "Check at another level": SCH-22. Header times: SCH-20. |
| Sc-07 | `schema_detail` Protobuf / `schemaDetail` | buildable | Canonical printing is the stored text. "1 message type" parsed from it. Encode with a Protobuf schema: `registryClient.encode` (real). Group lag: groups stage. |
| Sc-08 | `schema_detail` JSON Schema / `schemaDetail` | buildable | Properties table from the schema text (client-side). `draft-07` from `$schema`. "Validate a JSON document" is `Codec.Encode` for JSON (validates, `codec_test.go:124`). |
| Sc-09 | `schema_compat` / `schemaConfig` | needs backend | Level per subject (real). Global scope (`PUT /config`), mode toggle (`PUT /mode/{s}`) are real API, absent from the console client. "Test v3 against this level": SCH-22. Level descriptions: B7 table. |
| Sc-10 | `schema_register` references / `schemaRegister` | needs backend | Reference editor (form field), `referencedby` (real). "Resolved view": inlining (SCH-22). "Next free id": SCH-23. |
| Sc-11 | `schema_usage` / new | needs backend | Topic scan (SCH-25); ids to versions: `GET /schemas/ids/{id}/versions` (real). Retention and compaction text: topic config. |
| Sc-12 | confirms / `schemaDelete` | needs backend | Two dialogs match the API (soft, then permanent after soft). Needs the soft-delete-only action (SCH-16), the type-the-name field, "ids stay reserved" (true, run), "N messages carry id" (SCH-25). |
| Sc-13 | refusal page / `failSubject` | needs backend | Per SCH-19 cut the sign-in and the subject count; keep the Refused 401 page and the by-hand `curl -u` snippet. |
| Sc-14 | `schemas_off` / `registryOff` | buildable | Page exists. Needs the rail item kept (SCH-32) and a decode reason "registry is off" (SCH-24). |
| Sc-15 | `schema_detail` READONLY / `schemaRegister` | buildable | Mode real (`GET/PUT /mode/{s}`), `42205` real; console mode client (SCH-15) and the text fixes (SCH-30, SCH-31). "Set at 14:21" is dropped unless SCH-20 is built. |
| Sc-16 | `failSubject` / `failSubject` | real | `classify` + `closeNames` already give "Subject not found", suggestions, a back link (`handlers_schemas.go:162`). Missing: the "was it deleted" hint with `GET /subjects?deleted=true`, and "Register <name>" prefilled. |
| Sc-17 | decode failure, drawer and subject strip / `readThroughRegistry` | needs backend | Reason from `decode` (SCH-24); list of undecoded messages from the scan (SCH-25); "ids 1 to 8 and 10" from the overview (SCH-23). |
| Sc-18 | `schema_detail` "registered by a client" | needs backend | Registering principal, `User-Agent` (SCH-20); "from checkout-api" is a correlation with the traffic recorder by time and peer address, labelled as such (traffic stage). Request strip with Produce and Metadata: SCH-21 plus the Kafka recorder. |

Count: 19 boards (Home plus 18). `real` 1, `buildable` 6, `needs backend` 12.

`new` items and their data: "Topics using it" (scan), "Which schema id the messages carry" (scan), "Try it: encode / check JSON" (codec, real), "Check at another level" (extension), "Resolved view" (inliner), "Registered by" (principal and agent; the Kafka client name is an inference), "Wire underneath" (registry ring), "Sign in" (cut). Keys on every board: `n` register, `d` diff, `[` `]` pair, `u` unified, `⌘↵` check, `⇧⌘↵` register; the key map is declared once (A10) and a test fails for a key bound and not listed.

## 7. Tests to add

Unit / store (`schemaregistry`):

- `TestReferenceGraphIsLinear` (SCH-1), `TestNestingIsBounded` and `TestCompareHasADeadline` (SCH-2), `TestSubjectLengthAndCharacters` (SCH-18, SCH-37), `TestOpenWithABadRecord` (SCH-13), `TestStructuredFindingsRenderConfluentFormat` (SCH-6), `TestCheckEqualsRegister` property test over the corpora (SCH-14, SCH-34), `TestVersionMetadataSurvivesRestart` and `TestOldRecordsWithoutMetadata` (SCH-20), `TestOverviewNextIDMatchesRegister` including after a permanent delete of the highest id and after restart (SCH-23), `TestInlineAvro` (SCH-22), `TestCheckAtLevel` (SCH-22), `TestIDNotReusedAfterHardDeleteAndRestart` (extends `TestEverythingSurvivesARestart` with a register of different text after restart), `TestVersionNumberAfterPermanentDeleteOfLatest` (skipped pending R1), `TestRequestRingBounds` (SCH-21).
- Coverage functions to reach: `API.Registry`, `Error.Error`, `Codec.Type`, `refsString`, `protoprint extend`, `equalStrings`, `primitiveToAvro`, `stringToBytes`, `jsoncompat resolve`, `intPtr`, `NewCodec` error branches, `canRead` type-mismatch branch, `prefixParam`.

HTTP layer (`schemaregistry/api_test.go`, new):

- `TestBodiesAreBounded` (SCH-3), `TestContentTypeIsChecked` (SCH-5; skipped pending R4), `TestHostGuard` (SCH-4), `TestPaginationParameters` (SCH-8), `TestErrorsAreLogged` (SCH-9), `TestAuthFailuresAreLoggedAndDelayed` (SCH-38), `TestEveryRouteRequiresAuth` (SCH-27), `TestUnsupportedRoutesAnswerJSON404` (SCH-35), `TestExtensionRoutesAreAuthenticated` (SCH-20 to SCH-23), `TestReadonlyAndConfig` (SCH-31; skipped pending R3).

`server` and `cmd`:

- `TestSchemaRegistryIsOneInstance` (SCH-12), `TestSchemaRegistryAuthWiring` (SCH-27), `TestRegistryTimeouts` (SCH-10), `TestRegistryTLS` (SCH-11), `TestBrokerStartsWithAnUnreadableRegistry` (SCH-13).

Raw `kmsg`: none; this stage adds no Kafka API behaviour. The decode path is tested by producing a wire-format value with franz-go and reading it back through the console.

Client library:

- franz-go: produce `0x00 + id + Avro` for an unknown id, assert the console drawer shows `40403` (SCH-24); produce a value that is a prefix of a valid one, assert `mismatch`.
- Differential property: for each corpus pair, `Register` through the REST API and `Codec.Decode` of a value written under the old schema read with the new, for the pairs the recording says are BACKWARD compatible (an Avro-only check that the verdict means what it says; hamba does the resolution).

Compat cells (`compat/`):

- `java` + `python`: `RecordNameStrategy` and `TopicRecordNameStrategy` round trips (SCH-26); Python `ProtobufSerializer` with an imported message (SCH-7); wrong basic-auth password gives the client's 401 exception text; `auto.register.schemas=false` against a READONLY subject fails with `42205`; a Java consumer given an unknown id fails with `40403` text; `use.latest.version=true`; `normalize.schemas=true`.
- `kcat -s avro -r <registry>`: decode a record produced by the Java cell (kcat is one of the clients the matrix already has).
- Kafbat UI and Redpanda Console cells: assert they list the subjects and render a decoded value (they already start; add the schema assertions).

Fuzz:

- `FuzzRegisterReferences`, `FuzzDescriptor`, `FuzzAPIBody`, and an uncapped-size variant of `FuzzRegisterAndCompare` with a 2 s deadline (SCH-28). Add `FuzzSchemaFromForm` in `console`.

Console (`console/`):

- `TestRefusalsKeepTheRegistryCode` (SCH-15), `TestCheckShowsReadonly` (SCH-14), `TestDeleteConfirmsMatchTheRequests` (SCH-16, A6), `TestSoftDeletedSubjectIsListedAndRemovable` (SCH-17), `TestSubjectListIsBounded` (SCH-8), `TestDecodeReasons` and `TestUnknownIDBackoff` (SCH-24), `TestTopicScan*` (SCH-25), `TestRegisterSaysAlreadyRegistered` (SCH-33), `TestRailKeepsSchemasWhenOff` (SCH-32), `TestSchemasPageAfterTheRegistryGoesAway` (A7), one test per board that asserts the board's numbers from the B13 fixture.

E2E (`e2e/tests/schemas.spec.ts`, 8 today, 20 target):

- refused register (409) shows name, message, rule; READONLY register shows `42205`; soft delete then permanent delete with the typed name; a client-soft-deleted subject appears under Deleted; references editor adds a reference and the resolved view shows it; Protobuf evolution check; JSON Schema properties table; unknown-id message shows the reason; registry-off page and rail item; global level and mode; keyboard paths `j k Enter / n d [ ]`; 960 and 390 px; broker down mid-poll (A7).

Differential against the real registry (`conformance/recordings/`): scripts R1 to R12 of section 8, replayed without Docker, zero "answered differently". Recorded count at the start of the stage: 236 steps plus 1,459 pairs; count still `memory` is 21 claims (section 8).

Coverage targets (never below baseline): `schemaregistry` 88.4% to **92%**, with the functions listed above covered and every new file at 90% or more; `console` 87.8% to **90%**, with `handlers_schemas.go` and `registry.go` at 92% or more (`schemaCreatePage`, `schemaFromForm`, `schemaCreate`, `schemaDelete`, `registryClient.resolve`, `DeleteVersion`, `DeleteSubject`, `msgView` named); `server` 79.0% to **82%**, with `server/registry.go` at 100%.

## 8. Recording queue

Each is a Docker scenario against `confluentinc/cp-schema-registry:7.7.1` (the method of `TestRecordConformance`, `SR_RECORD_URL=...`); only R-numbers marked "broker" need `apache/kafka:4.0.0`, and none do: this stage makes no claim about Kafka broker behaviour. Everything below is `memory` or `doc` until recorded.

| # | Claim and scenario | Settles |
|---|---|---|
| R1 | After a **permanent** delete of the highest version, does the next register reuse that version number? After a permanent delete of a schema, does re-registering the same text give a new id? Script: register, soft delete, permanent delete, re-register; same at subject level. (`deleting.json` re-registers only after soft deletes.) | SCH-29, section 5 "hard vs soft", the Sc-12 sentence "ids stay reserved" |
| R2 | `DELETE /subjects/{s}?permanent=true` on a live subject (`40405`): status, body, text. Config and mode of the subject after a permanent delete: gone? | `40405`, `registry.go:834` |
| R3 | In READONLY: `PUT /config/{s}`, `DELETE /config/{s}`, `DELETE /subjects/{s}`, `POST /compatibility/...`, `POST /subjects/{s}` (lookup): each status and text. `PUT /mode` to `IMPORT` with subjects present: status and text (`42205`?); `?force=true`. | SCH-14, SCH-31 |
| R4 | `Content-Type` handling: none, `text/plain`, `application/json`, `application/vnd.schemaregistry+json`, `application/octet-stream` on `POST /subjects/{s}/versions`: status and body. | SCH-5 |
| R5 | Basic auth enabled (`authentication.method=BASIC`, a JAAS file): status, body and `WWW-Authenticate` for no credentials, wrong password, unknown user; does `/` need auth; does an unknown path return 401 or 404. | board Sc-13, SCH-27 |
| R6 | `limit` and `offset` on `/subjects`, `/subjects/{s}/versions`, `/schemas`: values 0, 1, negative, past the end, non-numeric; response and headers. | SCH-8 |
| R7 | Subject names: empty, `" "`, 255 and 4,000 characters, control characters, `%2F`, `:.ctx:name`, unicode: status and text. | SCH-18, SCH-37 |
| R8 | Oversize bodies (1 MiB, 10 MiB, 100 MiB) on schema register and on `PUT /config`: status and text. | SCH-3 |
| R9 | Verbose `messages` for every incompatible pair in the corpus (add a `reasons` field to the recorded cases): `errorType` set, `description`, `additionalInfo`, path format, quote style, for Avro, JSON Schema and Protobuf. | SCH-6 |
| R10 | Nested references: the same `name` at two levels (JSON Schema `$ref`, Protobuf import, Avro named type); a diamond; a cycle; depth 40; `referencedby` after a soft-deleted referrer. | section 5 "references", SCH-1 |
| R11 | Protobuf registered as a descriptor with a dependency and a `references` entry (confluent-kafka for Python's own registration order), and one with a dependency and no reference. | SCH-7 |
| R12 | `GET /contexts`, `/v1/metadata/id`, `/v1/metadata/version`, `/schemas/guids/{guid}`, `/schemas/ids/{id}?fetchMaxId=true&subject=x`, `?format=serialized` on a version and on an id, `POST /subjects/{s}?deleted=true`, `GET /subjects?deletedOnly=true`. | SCH-35, section 2 |
| R13 | `POST /compatibility/subjects/{missing}/versions` and `/versions/1` on a missing subject and a missing version (the code returns "compatible" for a missing subject). | `registry.go:859-870` |
| R14 | The wording of `GET /schemas/ids/{unknown}` is 500 in Confluent (documented); record the exact body so the "documented difference" is checked. | docs table |

Claims in this document at `memory`: SCH-1 (Confluent's behaviour on diamonds), SCH-5, SCH-7, SCH-18, SCH-31, SCH-35, SCH-37, plus R1 to R14 above; the count that remains `memory` is written here when each stage closes.

## 9. Owed to integration

- **Topics.** The record table, drawer and composer call into this stage's `decode` and `encode`; the inspector (A1) replaces the drawer, and the drawer's schema tag, "Look up id 9", "raw or decoded" key are this stage's content in topics' container. The composer's "encode with" menu and the value check ("does not fit v3") are `encode`/`Codec.Encode`. The topic scan of SCH-25 reuses the reader built for Trace; agree one scanner.
- **Traffic.** Registry requests are not in the Kafka recorder. Either the recorder takes a second source (SCH-21's ring), or the dock shows a registry strip fed directly. Board Sc-18 also needs "calls from checkout-api", a join of registry requests and Kafka requests by time and peer address; it belongs to the traffic stage's client identity model.
- **Connect.** Needs the registry's public URL and whether SASL applies to it (`basic.auth.*` lines); today `console.Options` has only the dial URL. Board Sc-14 says the snippets drop the registry line when it is off: that is a Connect board item.
- **Groups.** "14 behind for the router" and "reads from an old commit" in neighbours come from group lag and committed offsets.
- **Map.** Subject nodes and `describes` edges come from the same graph as A5; this stage supplies the registry side (subject to topic through id usage, or TopicNameStrategy).
- **Access.** The registry has authentication only; if Access shows "what could User:loyalty do", it must say the registry ignores ACLs.
- **Bad days.** A registry that fails to open (SCH-13) and a registry port taken (`cmd/doze-kafka/registry.go:35` already names `--schema-registry-addr`) are bad-days boards.

### Asks of the foundation

- **A4** takes a non-Kafka event source (a registry request) and filters by subject.
- **A2** needs `Refusal` kinds for registry errors with the registry's numeric `error_code` as the name (`409`, `42205`), not only Kafka error names; the "name comes from the same table the protocol layer uses" rule has no Kafka table for these.
- **B7** gets the registry's levels and modes as settings: name, meaning, `applied` (all seven levels are applied), `IMPORT` marked not implemented.
- **B8** lists the new routes in section 2, with the two mutating GETs (none) and the POSTs.
- **B12** lists registry REST bodies, schema texts, descriptors and forms (already named) with the three new fuzz targets.
- **B13** seeds five subjects with a gap in the ids, a soft-deleted subject, a READONLY subject, a reference, a descriptor-registered Protobuf, and one record carrying an unregistered id.
- **B14** checks that error strings quoted in templates and docs equal coded or recorded strings (SCH-30).
- **New:** a shared Host guard package used by console and registry (SCH-4), and a single bounded-scan helper shared by Trace and this stage.

## 10. Acceptance criteria

On top of the common criteria in [../01-protocol.md](../01-protocol.md).

### Contract and documents

- [ ] `docs/api-support.md` gains a "Schema registry REST" table with one row per route in section 2, tier F, C or S, generated from the route list and checked by a test (`TestEveryRegistryRouteHasALedgerRow`).
- [ ] `docs/schema-registry.md` "Where it differs" no longer says the reasons are "the same shape" (SCH-6); `docs/auth-and-tls.md` says what the registry does with TLS (SCH-11) and that it authenticates without authorising (SCH-27); `docs/limitations.md` lists `IMPORT`, `READONLY_OVERRIDE`, contexts and the missing metadata endpoints.
- [ ] Every extension route under `/_doze/` is in `docs/schema-registry.md` as "not a Confluent path".

### Behaviour

- [ ] `go test ./schemaregistry -run 'TestReferenceGraphIsLinear|TestNestingIsBounded|TestCompareHasADeadline|TestBodiesAreBounded|TestHostGuard|TestPaginationParameters|TestStructuredFindingsRenderConfluentFormat|TestCheckEqualsRegister'` passes and each failed before its fix (mutation by hand noted in the commit).
- [ ] A 26-level diamond registers in under 200 ms; a depth-3,000 nested Avro schema answers in under 1 s; a 20 MiB `PUT /config` answers 413 and the heap does not grow (SCH-1 to SCH-3).
- [ ] `Host: evil.example.com` to the registry answers 403 and the console and registry share the guard (SCH-4).
- [ ] A schema `POST` with `text/plain` is refused as recorded (R4) (SCH-5).
- [ ] `go test ./server -run TestSchemaRegistry` passes: one instance, auth wiring, `__console` accepted (SCH-12, SCH-27).
- [ ] The registry logs 401, 409 and 5xx (`TestErrorsAreLogged`) and the http server has read and idle timeouts (SCH-9, SCH-10).
- [ ] A restart and a SIGKILL variant test keep ids, versions, levels, modes and the new `createdAt`/`by` fields (B5 helper).
- [ ] The registry works with the data directory under a path containing a space, on `127.0.0.1:0`, with the console under a non-default prefix, with SASL off and on, with TLS off and on.
- [ ] `--schema-registry=false`: `go test ./console -run TestRailKeepsSchemasWhenOff` and a `cmd` test that the registry bucket is intact after a start with the flag off.

### Proof

- [ ] Coverage: `schemaregistry` >= 92%, `console` >= 90%, `server` >= 82% (`go tool task coverage`), and `server/registry.go` 100%.
- [ ] Mutation: `go tool task mutate:check -- ./schemaregistry/... ./console/...` has no new survivor; survivors in the engines left with a reason (the corpus kills most compatibility mutants; the HTTP layer and `registry.go` selection loop are where survivors are expected).
- [ ] Fuzz: `go tool task fuzz` runs `FuzzRegisterReferences`, `FuzzDescriptor`, `FuzzAPIBody` and `FuzzSchemaFromForm` clean for 60 s each.
- [ ] Differential: R1 to R14 recorded under `conformance/recordings/` and replayed with zero "answered differently"; the count of Schemas claims still at `memory` after the stage is written in section 8.
- [ ] Compat cells added and passing plain and SCRAM over TLS: Java `RecordNameStrategy`/`TopicRecordNameStrategy`, Python descriptor with an import, Java READONLY refusal, kcat Avro decode; Kafbat UI and Redpanda Console subject listing (`go tool task test:compat -- java python kcat kafbat-ui redpanda-console`).
- [ ] `go tool task lightness` shows no change in idle heap, binary size or module list beyond the request ring's fixed bound; `go tool task api` shows the `server` API unchanged except any cached-handler change documented in `CHANGELOG.md`.

### Console

- [ ] Boards implemented: Sc-Home and Sc-01 to Sc-18 (19), or the document says which were cut; Sc-13's sign-in is cut by decision (SCH-19).
- [ ] Every refusal shows the registry's code and message in the foundation's vocabulary (409, 42205, 42206, 40401, 40403, 401), `go test ./console -run TestRefusalsKeepTheRegistryCode`.
- [ ] Every delete confirm names exactly what the request removes: `TestDeleteConfirmsMatchTheRequests` performs soft version delete, soft subject delete, permanent subject delete against a real registry with a shared id and a client-soft-deleted version.
- [ ] The check form agrees with register: `TestCheckEqualsRegister` and the READONLY console test.
- [ ] `e2e/tests/schemas.spec.ts` has 20 tests, passes with the route gate on, the accessibility sweep is clean on all schema pages, and the pages render at 960 and 390 px.
- [ ] The Schemas pages render with the broker down (A7), with the registry down, with the registry off, and with an empty data directory.

### Close

- [ ] `CHANGELOG.md` names: bounds and timeouts, Host guard, content-type check, structured reasons, pagination, extension endpoints, delete semantics in the console.
- [ ] Findings table has every row closed or moved with a reason; section 8 is complete enough for someone else to record R1 to R14.

## 11. Files and order of work

Half-day units. **Estimate: L, 36 half-days (18 days).** Order puts the defects that can hang or expose the broker first, the recording next (it decides several behaviours), then the backend the boards need, then the console.

| # | Half-days | Work | Findings |
|---|---|---|---|
| 1 | 2 | Red tests for SCH-1, SCH-2, SCH-3, SCH-4, SCH-5, SCH-12, SCH-18 at the lowest layer, each failing. Add the HTTP test file. | |
| 2 | 3 | Record R1 to R14 against Confluent 7.7.1 (extend `TestRecordConformance` scripts; add a `reasons` field to the corpus recorder). Replay green or listed. | SCH-29, SCH-6 evidence |
| 3 | 3 | Fix the bounds: memoised resolution with a node cap, depth cap and a per-request deadline for `compare`, `MaxBytesReader` on every route, `http.Server` timeouts, subject rules from R7, one cached registry instance. Host guard shared package (touches `console/hostguard.go`, moved to `internal/hostguard`). | SCH-1 to SCH-5, 10, 12, 18, 37 |
| 4 | 3 | Structured findings: `CompatFinding` with type, path, description and info for Avro, JSON and Protobuf; Confluent rendering; update docs. Collapse `compatibleWith` and `CheckCompatible` into one function; register-versus-check property test. | SCH-6, 14, 34 |
| 5 | 2 | Pagination, logging, auth failure log, Content-Type per R4, `PUT /config` in READONLY per R3, descriptor dependencies per R11. | SCH-7, 8, 9, 27, 31, 38 |
| 6 | 3 | Backend for the boards: `createdAt`, `by`, `from`, `agent` on versions and `setAt` on level and mode; `GET /_doze/overview`; `GET /_doze/subjects/{s}/versions/{v}`; check at a level; request ring and `GET /_doze/requests`; `Inline` for Avro (JSON Schema and Protobuf list their references). | SCH-20 to SCH-23 |
| 7 | 3 | Console registry client: structured errors with code, mode client, global level, soft-delete-only and permanent-only calls, deleted listing, decode with reasons, bounded negative cache. `Refusal` rendering (A2). | SCH-15, 16, 17, 24 |
| 8 | 3 | Topic scan (bounded, with `Unread`), shared with Trace; usage page and counts. | SCH-25, 26 |
| 9 | 6 | Boards: Home, 01 to 04 (2); 05 to 08 (2); 09 to 12 (1); 13 to 18 (1). Inspector, dock and neighbours wired through A1, A4, A5. | SCH-19, 30 to 33 |
| 10 | 3 | Compat cells (Java strategies, Python imports, READONLY, kcat, UIs), fuzz targets, e2e to 20 tests. | SCH-26, 28 |
| 11 | 2 | Mutation baseline and survivors, coverage floor, documents, `CHANGELOG.md`, status table, actual time. | |
| 12 | 3 | Slack: R-items that disagree with the code (R1, R3, R4, R10 can each change a behaviour the recording had not pinned). | |

Total 36 half-days. Smallest useful cut if time is short: rows 1 to 5 (13 half-days) close all seven P1 findings and the recording; nothing in the console changes until row 7.

### Prior art

- **Confluent Schema Registry** is the specification here, and the oracle: its REST reference (subjects, versions, ids, config, mode, compatibility) and its serializers' wire format (magic byte 0 plus a 4-byte id, Protobuf message indexes). Its `limit`/`offset` list parameters and its content-type handling are the open questions R4 and R6; read the 7.7 REST reference alongside the recording.
- **Redpanda** ships a Confluent-compatible schema registry in the broker and keeps it in a `_schemas` topic; its console shows subjects, versions, a diff and compatibility. The design's subject page is closest to Redpanda Console's. doze-kafka differs by design in storage (`meta.db`), and the Kafka UI and Redpanda Console cells already run against it.
- **Apicurio Registry** exposes a Confluent-compatible endpoint (`/apis/ccompat/...`) beside its own model, with artifacts, versions and rules; its rules (compatibility, validity, integrity) map to levels here. The `compat/apicurio` cell runs it against this broker, not the reverse.
- **Karapace** (Aiven) is an open-source Confluent-compatible registry in Python; its test suite is a useful second source of REST edge cases when a recording is not yet made, as `memory` until recorded.
- **Data contracts and schema linking** (Confluent) are declined here; the design does not draw them.
- **Go's own `net/http` hardening** (`http.MaxBytesReader`, server timeouts) is the whole of the bounds work; no KIP applies, the registry is not part of Apache Kafka.
