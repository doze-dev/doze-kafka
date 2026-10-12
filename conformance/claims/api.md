# Claims: the API table

What a real Apache Kafka lists in `ApiVersions`, what it does with a request it does
not serve, and what it answers to a default request of every API doze-kafka offers.

`conformance/recordings/api-versions.json` is not a scenario: it is the `ApiVersions`
v0 answer of one real broker of each Kafka major there is a profile for
(`--kafka-version 1` to `4`), recorded by `TestRecordApiVersions`
(`conformance/apiversions_test.go`, which says which image stands for which major and
why). `TestNoProfileServesWhatARealBrokerOfThatMajorDoesNot` (`internal/protocol`)
holds every column of the version table to it.

| ID | Claim | Source | Evidence | Scenario |
|---|---|---|---|---|
| V1 | A broker of each Kafka major lists these APIs and version ranges in ApiVersions: Kafka 4.3.1 lists ListOffsets from v1, OffsetCommit from v2, OffsetFetch from v1, CreateTopics from v2, DeleteTopics from v1, DescribeAcls, CreateAcls and DeleteAcls from v1, DescribeConfigs from v1 and Fetch from v4 (it removed the versions below, KIP-896); Kafka 1.1.1 lists Metadata to v5, FindCoordinator to v1, ListGroups to v1, InitProducerId to v0, SaslAuthenticate to v0 and DeleteGroups to v0. doze-kafka's Kafka 4 profile offered the nine removed versions and its Kafka 1 profile six versions 1.1.1 does not have; no profile now offers an API or a version the real broker of its major does not list, but the two telemetry APIs (traffic R5) | recorded | conformance/recordings/api-versions.json | Not a scenario: `TestRecordApiVersions` (`go tool task conformance:record -- -only=api-versions`), one container per major: `apache/kafka:4.3.1`, `apache/kafka:3.9.1`, `confluentinc/cp-kafka:6.2.15` (Confluent Platform 6.2; that it is Apache Kafka 2.8 is Confluent's documentation) and `confluentinc/cp-kafka:4.1.4` (broker jar `kafka_2.11-1.1.1-cp4.jar`) |
| V2 | Kafka 4.3.1 does not answer a request at a version below the lowest it lists: it closes the connection. Recorded for each of the versions V1 names (ListOffsets v0, OffsetCommit v0 and v1, OffsetFetch v0, CreateTopics v0 and v1, DeleteTopics v0, DescribeAcls v0, CreateAcls v0, DeleteAcls v0, DescribeConfigs v0, Fetch v3). The broker goes on serving: the next connection is answered. doze-kafka answered eleven of the twelve | recorded | conformance/recordings/versions-not-served.json | `versions-not-served`, steps "ListOffsets v0 below or above" to "Fetch v3 below or above", each followed by "… then served" |
| V3 | Kafka 4.3.1 closes the connection on a version above the highest it lists (Metadata v14, FindCoordinator v7, Heartbeat v5, DescribeConfigs v5) and on an API it does not list at all (LeaderAndIsr, ControlledShutdown), with no answer. doze-kafka matched | recorded | conformance/recordings/versions-not-served.json | `versions-not-served`, steps "Metadata v14 below or above", "FindCoordinator v7 below or above", "Heartbeat v5 below or above", "DescribeConfigs v5 below or above", "LeaderAndISR v5 below or above", "ControlledShutdown v3 below or above" |
