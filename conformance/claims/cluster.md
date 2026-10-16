# Claims: Cluster stage

Claims about how Apache Kafka answers the requests the Cluster stage owns
(`plan/stages/cluster.md`). `ApiVersions` is the first of them: it is the first request
of every client, and Traffic reads the software name it carries. Every row here is
`recorded`: the scenario was run against `apache/kafka:4.3.1` in Docker, its answers are
the committed file named in Evidence, and the replay (`go test ./conformance`, no Docker)
holds doze-kafka to them step by step with no written-down difference.

In these recordings an `ApiVersions` answer is reduced to its error, the row for
`ApiVersions` itself, and whether there were other rows (`OtherApiKeys`): the list of a
broker's APIs is its whole table, which `docs/api-support.md` is about, and a list of
none or of one is recorded as it is.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| AV1 | From v3 an ApiVersions request names the client's software and its version, and Kafka refuses a name or a version that is empty, or that is not letters and digits with dots and dashes only between them (a space, an underscore, a slash, a plus, a character that is not ASCII, a dash or a dot at either end): INVALID_REQUEST (42) with no APIs listed, at v3 and at v4. Before v3 there is nothing to refuse. doze-kafka matched after a change: it answered 0 and its whole list to all of them, and kept whatever it was sent as the client's software | recorded | conformance/recordings/apiversions-client-software.json | `apiversions-client-software`: five that are taken, v0 to v2, thirteen that are refused. |
| AV2 | The connection a refused ApiVersions went over is still served: a valid ApiVersions and a Metadata request after thirteen refusals are answered 0. doze-kafka matched | recorded | conformance/recordings/apiversions-client-software.json | `apiversions-client-software`: steps `v3-valid-after-the-refusals`, `metadata-after-the-refusals`. |
| AV3 | Kafka refuses no client software name or version for its length: a name of 20,000 characters, and a version of 20,000, of the characters allowed, are answered 0. So a bound on what a broker keeps of it cannot be a refusal: doze-kafka takes them and keeps the first 128 bytes (`protocol.MaxClientSoftware`) | recorded | conformance/recordings/apiversions-client-software.json | `apiversions-client-software`: steps `v3-name-of-200-characters`, `v3-name-of-20000-characters`, `v3-version-of-20000-characters`. |
| AV4 | An ApiVersions request at a version the broker does not serve (5, 99, 32767 against a broker whose highest is 4) is answered UNSUPPORTED_VERSION (35) in the v0 form with exactly one API listed: ApiVersions itself, with the versions served (0 to 4). The request's body is not read: a software name that v3 would refuse changes nothing. The connection is still served afterwards. doze-kafka matched after a change: it listed its whole table (39 APIs), and offered ApiVersions to v3 where Kafka 4 offers v4 | recorded | conformance/recordings/apiversions-unsupported-version.json | `apiversions-unsupported-version`: v4, v5, v99, v32767, v5 with a name v3 refuses, v3. |
