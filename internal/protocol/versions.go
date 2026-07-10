package protocol

import "sort"

// GroupProtocol selects which consumer-group protocol(s) a version profile
// advertises. See docs/api-support.md and docs/consumer-groups.md.
type GroupProtocol int

const (
	// GroupClassic advertises only the classic, client-driven protocol
	// (JoinGroup/SyncGroup/Heartbeat). Valid for every supported Kafka major.
	GroupClassic GroupProtocol = iota

	// GroupClassicAndConsumer additionally advertises the KIP-848 broker-driven
	// protocol (ConsumerGroupHeartbeat/Describe). Kafka 4+ only.
	GroupClassicAndConsumer
)

// VersionRange is the inclusive [Min,Max] span of an API supported on the wire.
type VersionRange struct {
	Min int16
	Max int16
}

// AdvertisedAPI is one line of an ApiVersions response: an API key and the
// version range we support for it.
type AdvertisedAPI struct {
	Key APIKey
	Min int16
	Max int16
}

// Profile is the behavioral envelope for a declared Kafka major version: the
// per-API version ranges advertised in ApiVersions, plus the feature toggles
// that change observable behavior (currently the consumer-group protocol).
//
// Selecting a profile is how `version = N` in doze.hcl maps to what clients see.
// Unlike other doze engines, the declared version does NOT fetch an upstream
// binary — our own code is the engine, so the version is a profile we implement.
// See docs/api-support.md.
type Profile struct {
	KafkaMajor    int                     // the Kafka major this profile emulates (1..4).
	GroupProtocol GroupProtocol           // which group protocol(s) to advertise.
	APIRanges     map[APIKey]VersionRange // advertised [min,max] per API; absent key = not advertised.
}

// DefaultKafkaMajor is the version emulated when none is declared.
const DefaultKafkaMajor = 4

// profiles holds one Profile per supported Kafka major. An API is advertised only
// once a handler actually serves it, so clients never negotiate a version the
// broker can't answer. Ranges mirror what a real broker of that major advertises
// (franz-go's kversion is the authoritative reference); the hard floor is record
// batch format v2 (Kafka 0.11), so major 1 is the minimum.
//
// Phase 1 implements ApiVersions + Metadata; phase 2 adds CreateTopics; phase 3
// adds Produce, Fetch, ListOffsets, InitProducerId; phase 4 adds the classic
// consumer-group set (FindCoordinator, Join/Sync/Heartbeat/Leave,
// OffsetCommit/OffsetFetch). Group-API maxes are capped just below where the wire
// form shifts to topic-IDs (OffsetCommit/OffsetFetch v10) — the handlers key on
// topic names.
var profiles = map[int]Profile{
	1: {KafkaMajor: 1, GroupProtocol: GroupClassic, APIRanges: map[APIKey]VersionRange{
		APIApiVersions:     {Min: 0, Max: 1},
		APIMetadata:        {Min: 0, Max: 6},
		APICreateTopics:    {Min: 0, Max: 2},
		APIProduce:         {Min: 0, Max: 5},
		APIFetch:           {Min: 0, Max: 6},
		APIListOffsets:     {Min: 0, Max: 2},
		APIInitProducerID:  {Min: 0, Max: 1},
		APIFindCoordinator: {Min: 0, Max: 2},
		APIJoinGroup:       {Min: 0, Max: 2},
		APISyncGroup:       {Min: 0, Max: 1},
		APIHeartbeat:       {Min: 0, Max: 1},
		APILeaveGroup:      {Min: 0, Max: 1},
		APIOffsetCommit:    {Min: 0, Max: 3},
		APIOffsetFetch:     {Min: 0, Max: 3},
		APIDeleteTopics:    {Min: 0, Max: 1},
		APIListGroups:      {Min: 0, Max: 2},
		APIDescribeGroups:  {Min: 0, Max: 1},
		APIDescribeConfigs: {Min: 0, Max: 1},
	}},
	2: {KafkaMajor: 2, GroupProtocol: GroupClassic, APIRanges: map[APIKey]VersionRange{
		APIApiVersions:     {Min: 0, Max: 2},
		APIMetadata:        {Min: 0, Max: 8},
		APICreateTopics:    {Min: 0, Max: 3},
		APIProduce:         {Min: 0, Max: 7},
		APIFetch:           {Min: 0, Max: 10},
		APIListOffsets:     {Min: 0, Max: 5},
		APIInitProducerID:  {Min: 0, Max: 1},
		APIFindCoordinator: {Min: 0, Max: 2},
		APIJoinGroup:       {Min: 0, Max: 4},
		APISyncGroup:       {Min: 0, Max: 3},
		APIHeartbeat:       {Min: 0, Max: 2},
		APILeaveGroup:      {Min: 0, Max: 2},
		APIOffsetCommit:    {Min: 0, Max: 6},
		APIOffsetFetch:     {Min: 0, Max: 5},
		APIDeleteTopics:    {Min: 0, Max: 3},
		APIListGroups:      {Min: 0, Max: 2},
		APIDescribeGroups:  {Min: 0, Max: 3},
		APIDescribeConfigs: {Min: 0, Max: 2},
	}},
	3: {KafkaMajor: 3, GroupProtocol: GroupClassic, APIRanges: map[APIKey]VersionRange{
		APIApiVersions:     {Min: 0, Max: 3},
		APIMetadata:        {Min: 0, Max: 12},
		APICreateTopics:    {Min: 0, Max: 7},
		APIProduce:         {Min: 0, Max: 9},
		APIFetch:           {Min: 0, Max: 13},
		APIListOffsets:     {Min: 0, Max: 7},
		APIInitProducerID:  {Min: 0, Max: 4},
		APIFindCoordinator: {Min: 0, Max: 4},
		APIJoinGroup:       {Min: 0, Max: 7},
		APISyncGroup:       {Min: 0, Max: 4},
		APIHeartbeat:       {Min: 0, Max: 4},
		APILeaveGroup:      {Min: 0, Max: 4},
		APIOffsetCommit:    {Min: 0, Max: 8},
		APIOffsetFetch:     {Min: 0, Max: 8},
		APIDeleteTopics:    {Min: 0, Max: 5},
		APIListGroups:      {Min: 0, Max: 4},
		APIDescribeGroups:  {Min: 0, Max: 5},
		APIDescribeConfigs: {Min: 0, Max: 4},
		APIDescribeCluster: {Min: 0, Max: 1},
	}},
	4: {KafkaMajor: 4, GroupProtocol: GroupClassicAndConsumer, APIRanges: map[APIKey]VersionRange{
		APIApiVersions:     {Min: 0, Max: 3},
		APIMetadata:        {Min: 0, Max: 12},
		APICreateTopics:    {Min: 0, Max: 7},
		APIProduce:         {Min: 0, Max: 9},
		APIFetch:           {Min: 0, Max: 13},
		APIListOffsets:     {Min: 0, Max: 7},
		APIInitProducerID:  {Min: 0, Max: 4},
		APIFindCoordinator: {Min: 0, Max: 4},
		APIJoinGroup:       {Min: 0, Max: 9},
		APISyncGroup:       {Min: 0, Max: 5},
		APIHeartbeat:       {Min: 0, Max: 4},
		APILeaveGroup:      {Min: 0, Max: 5},
		APIOffsetCommit:    {Min: 0, Max: 9},
		APIOffsetFetch:     {Min: 0, Max: 9},
		APIDeleteTopics:    {Min: 0, Max: 6},
		APIListGroups:      {Min: 0, Max: 5},
		APIDescribeGroups:  {Min: 0, Max: 5},
		APIDescribeConfigs: {Min: 0, Max: 4},
		APIDescribeCluster: {Min: 0, Max: 2},
		// KIP-848 broker-driven protocol — advertised only on the Kafka-4 profile,
		// so a client at default group.protocol=consumer negotiates the new path.
		APIConsumerGroupHeartbeat: {Min: 0, Max: 1},
		APIConsumerGroupDescribe:  {Min: 0, Max: 1},
	}},
}

// ProfileFor returns the profile for a declared Kafka major, or ok=false when the
// major is outside the supported range.
func ProfileFor(kafkaMajor int) (Profile, bool) {
	p, ok := profiles[kafkaMajor]
	return p, ok
}

// RangeFor returns the advertised version range for an API in this profile, or
// ok=false if the profile does not advertise it.
func (p Profile) RangeFor(key APIKey) (VersionRange, bool) {
	r, ok := p.APIRanges[key]
	return r, ok
}

// SupportedKafkaMajors returns the accepted declared versions in ascending order.
func SupportedKafkaMajors() []int {
	majors := make([]int, 0, len(profiles))
	for m := range profiles {
		majors = append(majors, m)
	}
	sort.Ints(majors)
	return majors
}

// Advertised returns the profile's API ranges as a slice sorted by API key — the
// deterministic set to encode into an ApiVersions response.
func (p Profile) Advertised() []AdvertisedAPI {
	out := make([]AdvertisedAPI, 0, len(p.APIRanges))
	for k, r := range p.APIRanges {
		out = append(out, AdvertisedAPI{Key: k, Min: r.Min, Max: r.Max})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
