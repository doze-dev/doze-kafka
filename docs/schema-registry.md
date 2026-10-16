# Schema Registry

doze-kafka has a schema registry built in. It speaks Confluent's REST API, so the
serializers and tools that expect a registry work with only `schema.registry.url`
changed, and there is no second container to run.

```
    registry  http://schema.myproject.doze   Confluent-compatible schema registry
```

It stores Avro, JSON Schema and Protobuf schemas, hands out ids, versions them per
subject, and enforces compatibility between versions at all seven of Confluent's levels.

## Using it

Point a client at the address the startup summary prints:

```properties
bootstrap.servers=kafka.myproject.doze:9092
schema.registry.url=http://schema.myproject.doze
```

```sh
curl http://schema.myproject.doze/subjects
curl -X POST http://schema.myproject.doze/subjects/orders-value/versions \
  -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
  -d '{"schema": "{\"type\":\"string\"}"}'
```

Where it listens:

| Mode | Address |
|---|---|
| By name (the default) | `http://schema.<name>.doze` through the shared front door, port 8081 on the instance's own address |
| `--listen` | `127.0.0.1:8081`; a free loopback port when `--listen` is on port 0 |
| `--schema-registry-addr host:port` | wherever you say |
| `--schema-registry=false` | not served |
| With `--tls-cert` and `--tls-key` | the same address over `https://`; by name, `https://kafka.<name>.doze:8081` |

A taken port is an error naming `--schema-registry-addr`, not a registry that quietly is
not there. In a container, publish the port and listen on every interface
(`--schema-registry-addr 0.0.0.0:8081`).

With SASL enabled (see [Auth and TLS](auth-and-tls.md)) the registry requires the same
user names and passwords as HTTP basic authentication; a client sets
`basic.auth.credentials.source=USER_INFO` and `basic.auth.user.info=user:password`.

When the broker has a TLS certificate the registry is served over HTTPS with the same
one, so `schema.registry.url` is `https://...` and the client has to trust the
certificate, as it does for the broker (`curl --cacert cert.pem`).
`--schema-registry-tls=false` keeps it plain HTTP.
[What SASL and TLS cover](auth-and-tls.md#what-sasl-and-tls-cover) has the details.

## What it does

- **Subjects and versions**: register, look up, list, soft-delete and permanently delete,
  with Confluent's rules (a permanent delete needs a soft delete first; registering a
  schema a subject had deleted gives it a new, latest version).
- **Ids are global**: the same schema under two subjects is one id, and an id is never
  reused, even after the schema is deleted for good.
- **References**: an Avro named type, a JSON Schema `$ref` and a Protobuf `import` can
  name another registered schema; a referenced version cannot be deleted.
- **Compatibility**: `NONE`, `BACKWARD`, `FORWARD`, `FULL` and the three `_TRANSITIVE`
  levels, globally and per subject, for all three schema types.
- **Modes**: `READWRITE` and `READONLY`, globally and per subject.
- **The endpoints** tools use: `/subjects`, `/subjects/{s}/versions[/{v}[/schema|/referencedby]]`,
  `POST /subjects/{s}` (lookup), `/schemas`, `/schemas/ids/{id}[/schema|/subjects|/versions]`,
  `/schemas/types`, `/config[/{s}]`, `/mode[/{s}]`, and
  `POST /compatibility/subjects/{s}/versions[/{v}]` (`?verbose=true` for the reasons).
- **Persistence**: schemas live in the broker's metadata database (`meta.db`), not in a
  `_schemas` topic. Nothing on the wire can tell, and there is nothing to keep in step
  with a topic. They survive restarts and move with the data directory.

## In the console

The [console](console.md) has a Schemas section: subjects, versions and the changes
between them, the compatibility level, a "check before you register" form, decoded
messages (an Avro, JSON Schema or Protobuf record is shown as JSON, with its schema
named), and a composer that encodes JSON with a subject's schema.

## How faithful it is

The answers are Confluent's because they were checked against Confluent's. The repository
holds recordings of a real Confluent Schema Registry (7.7) answering:

- **A script of requests** per behaviour (`schemaregistry/testdata/conformance`): status
  codes, error codes and messages, response shapes, the stored form of a schema, what
  deleting and re-registering do.
- **About 1,500 schema pairs** (`schemaregistry/testdata/compat`): for every pair, whether
  the second may follow the first at `BACKWARD`, `FORWARD` and `FULL`. A third of them
  are random pairs generated from a fixed seed, so the rules are held to cases nobody
  chose.

- **Protobuf registered as a descriptor** (`schemaregistry/testdata/conformance/protodesc.json`):
  confluent-kafka for Python sends a Protobuf message as the base64 of its
  `FileDescriptorProto`, which Confluent reads when the string is not `.proto` text. The
  same descriptors were registered with the real registry and the text it stored is what
  is stored here (type names keep their leading dot, a map is a nested `*Entry` message,
  an empty message is `{}`).

The tests replay both against this registry and require the same answer. To record
again, run a Confluent registry beside doze-kafka and
`SR_RECORD_URL=http://localhost:8081 go test ./schemaregistry -run 'TestRecord'`.

How each type is compared:

- **Avro** with the resolution rules of the Avro specification (`hamba/avro`), which agree
  with Confluent's on every recorded pair.
- **JSON Schema** by Confluent's own method: not a proof about documents but a keyword by
  keyword comparison that names each change ("maxLength decreased", "property added to an
  open content model") and a fixed list of which names a reader can live with. Some of it
  is surprising (a property added to an open object is a breaking change; `format` is
  ignored), and all of it is Confluent's.
- **Protobuf** by the same method over message and field declarations (the wire-compatible
  type groups, labels, oneofs, message and package names). Enums and services are not
  compared, as in Confluent.

## Where it differs

Deliberately, and each is pinned by a test:

| | Confluent | doze-kafka |
|---|---|---|
| Storage | the `_schemas` topic | the metadata database |
| Mode `IMPORT`, `READONLY_OVERRIDE` | supported | not supported (`READWRITE` and `READONLY` only); the message for an unknown mode names only those |
| Contexts (`:.name:` prefixes), data contracts (rules, metadata, tags), schema linking, exporters | supported | not supported |
| `GET /schemas/ids/{unknown}/schema` | 500 | 404, error code 40403 |
| JSON Schema `exclusiveMaximum` lowered | answers 500 (its checker has no name for the change) | answers the comparison |
| Protobuf a parser would reject | its parser is lenient: an `extend` of a type that was never imported, a method named like a message | rejected as invalid (422) |
| Verbose compatibility reasons | its checker's wording | the same shape; the three trailing lines (`oldSchemaVersion`, `oldSchema`, `validateFields`/`compatibility`) are identical, the reasons are worded by this implementation |
| Avro with `normalize=true` | Avro's normalizer | key order and namespaces as Avro writes them, custom attributes sorted |

## Troubleshooting

- *`Connection refused` on 8081*: the registry is on the instance's own address in name
  mode, not on `localhost`. Use the address the startup summary prints, or
  `doze-kafka env`.
- *`401 Unauthorized`*: SASL is on; send the same credentials as HTTP basic auth.
- *`400 Client sent an HTTP request to an HTTPS server`*: the broker has a TLS
  certificate, so the registry is on `https://`. Use the URL the start-up summary prints.
- *`409` on register*: the schema is incompatible with an earlier version at the level in
  force. `?verbose=true` on the compatibility check says why; `PUT /config/{subject}`
  `{"compatibility":"NONE"}` turns checking off for a subject.
