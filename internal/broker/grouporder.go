package broker

import "unicode/utf16"

// Kafka answers a DescribeGroups, ConsumerGroupDescribe or DeleteGroups that names
// several groups in an order of its own, the same for every order of the request
// (claim: groups R25a). Its coordinator gathers the groups by the partition of
// __consumer_offsets each belongs to, in a hash map keyed by that partition, and
// answers partition by partition as the map iterates: Java's HashMap, its buckets in
// ascending order, with the partitions that share a bucket in the reverse of the order
// they were first named, and each partition's groups in the request's order. A client
// that pairs answers with its request by position (rather than by the group id each
// carries) sees the same pairing here as on Kafka. Recorded for up to 20 groups over 14
// partitions, which takes the map past one resize: conformance/recordings/
// describe-groups-order.json.

// offsetsPartitions is offsets.topic.num.partitions at Kafka's default, which is what
// the order is a function of.
const offsetsPartitions = 50

// javaStringHash is Java's String.hashCode: over UTF-16 code units.
func javaStringHash(s string) int32 {
	var h int32
	for _, u := range utf16.Encode([]rune(s)) {
		h = 31*h + int32(u)
	}
	return h
}

// offsetsPartitionFor is Kafka's partitionFor(groupId): Utils.abs(hashCode) % 50,
// where Utils.abs of the least int is 0.
func offsetsPartitionFor(group string) int32 {
	h := javaStringHash(group)
	if h == -1<<31 {
		return 0
	}
	if h < 0 {
		h = -h
	}
	return h % offsetsPartitions
}

// topicPartitionHash is Java's TopicPartition.hashCode for __consumer_offsets.
func topicPartitionHash(partition int32) int32 {
	const prime = 31
	result := int32(1)
	result = prime*result + partition
	return prime*result + javaStringHash("__consumer_offsets")
}

// inKafkaGroupOrder returns groups (duplicates kept) in the order Kafka answers them.
// claim: groups R25a
func inKafkaGroupOrder(groups []string) []string {
	var parts []int32 // in the order first named.
	byPart := map[int32][]string{}
	for _, g := range groups {
		p := offsetsPartitionFor(g)
		if _, ok := byPart[p]; !ok {
			parts = append(parts, p)
		}
		byPart[p] = append(byPart[p], g)
	}
	capacity := 16 // a HashMap's default, doubled each time it is more than 3/4 full.
	for len(parts) > capacity*3/4 {
		capacity *= 2
	}
	bucket := func(p int32) int {
		h := uint32(topicPartitionHash(p))
		return int((h ^ h>>16) & uint32(capacity-1))
	}
	out := make([]string, 0, len(groups))
	for b := range capacity {
		for i := len(parts) - 1; i >= 0; i-- { // a bucket's latest partition first.
			if bucket(parts[i]) == b {
				out = append(out, byPart[parts[i]]...)
			}
		}
	}
	return out
}
