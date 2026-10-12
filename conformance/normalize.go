package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

// normalizer turns a request or response into plain JSON with everything that
// differs from run to run replaced by a stable placeholder: broker ids, hosts
// and ports, the cluster id, and ids Kafka makes up (topic ids, member ids,
// producer ids), numbered in the order they first appear so two runs that
// behave the same come out the same. One normalizer lives for a whole
// scenario, so an id seen in a response is the same placeholder in the next
// request.
type normalizer struct {
	topicIDs map[string]string
	members  map[string]string
	producer map[int64]string
	// instances and subscriptions are the ids KIP-714 has the broker make up.
	instances     map[string]string
	subscriptions map[string]string
	// What a SASL token holds that differs from run to run (Env.Scrub and
	// Env.ClientNonce; saslToken below).
	scrub        [][2]string
	clientNonces []string
	serverNonces int
}

func newNormalizer() *normalizer {
	return &normalizer{topicIDs: map[string]string{}, members: map[string]string{}, producer: map[int64]string{}, instances: map[string]string{}, subscriptions: map[string]string{}}
}

// nodeKeys hold a broker id; nodeListKeys hold a list of them.
var (
	nodeKeys     = map[string]bool{"NodeID": true, "Leader": true, "ControllerID": true, "BrokerID": true, "PreferredReadReplica": true, "CurrentLeader": false}
	nodeListKeys = map[string]bool{"Replicas": true, "ISR": true, "OfflineReplicas": true, "ReplicaNodes": true}
	memberKeys   = map[string]bool{"MemberID": true, "LeaderID": true, "MemberId": true, "ConsumerID": true}
	dropKeys     = map[string]bool{"Version": true, "UnknownTags": true}
)

// normalize takes a kmsg request or response (any JSON-marshalable value).
func (n *normalizer) normalize(v any) (any, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var tree any
	if err := dec.Decode(&tree); err != nil {
		return nil, err
	}
	return n.walk(tree, ""), nil
}

func (n *normalizer) walk(v any, key string) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, e := range x {
			if dropKeys[k] {
				continue
			}
			out[k] = n.walk(e, k)
		}
		return out
	case []any:
		if key == "ClientInstanceID" && len(x) == 16 {
			if id, ok := uuidOf(x); ok {
				if id == "00000000000000000000000000000000" {
					return "<zero-id>"
				}
				if p, ok := n.instances[id]; ok {
					return p
				}
				p := fmt.Sprintf("<instance:%d>", len(n.instances)+1)
				n.instances[id] = p
				return p
			}
		}
		if key == "TopicID" || key == "ID" && len(x) == 16 {
			if id, ok := uuidOf(x); ok {
				return n.topicID(id)
			}
		}
		x = sortMembersByContent(x)
		out := make([]any, len(x))
		for i, e := range x {
			if nodeListKeys[key] {
				out[i] = "<node>"
				continue
			}
			out[i] = n.walk(e, key)
		}
		if key == "Members" {
			sortMembers(out)
		}
		return sortListedGroups(dropInternalAndSort(out))
	case json.Number:
		switch {
		case nodeKeys[key] && x.String() != "-1":
			return "<node>"
		case key == "Port":
			return "<port>"
		case key == "SubscriptionID":
			// Kafka makes it up from the subscription's content, a client of this
			// repository from a random number: only whether two are the same matters.
			if p, ok := n.subscriptions[x.String()]; ok {
				return p
			}
			p := fmt.Sprintf("<subscription:%d>", len(n.subscriptions)+1)
			n.subscriptions[x.String()] = p
			return p
		case key == "ProducerID" && x.String() != "-1":
			i, _ := x.Int64()
			if p, ok := n.producer[i]; ok {
				return p
			}
			p := fmt.Sprintf("<producer:%d>", len(n.producer)+1)
			n.producer[i] = p
			return p
		}
		return x
	case string:
		switch {
		case key == "Host" || key == "ClientHost":
			return "<host>"
		case key == "Metrics" && len(x) > 512:
			// A pushed payload that is a megabyte of something: kept as its size and
			// a digest, so a recording stays a file one can read.
			raw, err := base64.StdEncoding.DecodeString(x)
			if err != nil {
				return x
			}
			sum := sha256.Sum256(raw)
			return fmt.Sprintf("<%d bytes, sha256 %x>", len(raw), sum[:6])
		case key == "ClusterID":
			return "<cluster>"
		case key == "SASLAuthBytes":
			return n.saslToken(x)
		case memberKeys[key] && x != "":
			if m, ok := n.members[x]; ok {
				return m
			}
			m := fmt.Sprintf("<member:%d>", len(n.members)+1)
			n.members[x] = m
			return m
		}
		// A member id inside a message ("...member consumer-1-5a2c...").
		for id, ph := range n.members {
			if strings.Contains(x, id) {
				x = strings.ReplaceAll(x, id, ph)
			}
		}
		return x
	}
	return v
}

func (n *normalizer) topicID(id string) string {
	if id == "00000000000000000000000000000000" {
		return "<zero-id>"
	}
	if p, ok := n.topicIDs[id]; ok {
		return p
	}
	p := fmt.Sprintf("<topic-id:%d>", len(n.topicIDs)+1)
	n.topicIDs[id] = p
	return p
}

// uuidOf reads the 16 numbers kmsg marshals a [16]byte as.
func uuidOf(x []any) (string, bool) {
	if len(x) != 16 {
		return "", false
	}
	var sb strings.Builder
	for _, e := range x {
		num, ok := e.(json.Number)
		if !ok {
			return "", false
		}
		i, err := num.Int64()
		if err != nil || i < 0 || i > 255 {
			return "", false
		}
		fmt.Fprintf(&sb, "%02x", i)
	}
	return sb.String(), true
}

// dropInternalAndSort makes a list of topics comparable: a broker lists its own
// internal topics (__consumer_offsets exists once any group has committed) and
// in no particular order; neither is something a script can control.
func dropInternalAndSort(list []any) []any {
	topics := true
	for _, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			return list
		}
		if _, has := m["IsInternal"]; !has {
			topics = false
		}
	}
	if !topics || len(list) == 0 {
		return list
	}
	out := list[:0:0]
	for _, e := range list {
		if b, _ := e.(map[string]any)["IsInternal"].(bool); !b {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i].(map[string]any)["Topic"].(string)
		b, _ := out[j].(map[string]any)["Topic"].(string)
		return a < b
	})
	return out
}

// sortMembersByContent orders the members of a ConsumerGroupDescribe answer by what
// they hold, before their ids are numbered: a broker lists them in the order of its own
// hash table and the ids are random, so the order carries nothing and numbering them
// by position would make every run a different recording.
func sortMembersByContent(list []any) []any {
	if len(list) < 2 {
		return list
	}
	keys := make([]string, len(list))
	for i, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			return list
		}
		if _, has := m["MemberID"]; !has {
			return list
		}
		if _, has := m["MemberEpoch"]; !has {
			return list
		}
		a, _ := json.Marshal([]any{m["MemberEpoch"], m["Assignment"], m["TargetAssignment"], m["SubscribedTopics"]})
		keys[i] = string(a)
	}
	idx := make([]int, len(list))
	for i := range idx {
		idx[i] = i
	}
	sort.SliceStable(idx, func(a, b int) bool { return keys[idx[a]] < keys[idx[b]] })
	out := make([]any, len(list))
	for i, j := range idx {
		out[i] = list[j]
	}
	return out
}

// sortListedGroups orders a ListGroups answer by group id, and the topics of an
// OffsetFetch answer by name: a broker answers in the order of its own hash table,
// which says nothing about how it behaves.
func sortListedGroups(list []any) []any {
	key := ""
	for _, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			return list
		}
		switch {
		case m["GroupState"] != nil:
			key = "Group"
		case m["Topic"] != nil && offsetFetchPartitions(m["Partitions"]):
			key = "Topic"
		default:
			return list
		}
	}
	if key == "" {
		return list
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, _ := list[i].(map[string]any)[key].(string)
		b, _ := list[j].(map[string]any)[key].(string)
		return a < b
	})
	return list
}

// offsetFetchPartitions reports whether v is the partitions of an OffsetFetch topic
// (each has an offset, metadata and a leader epoch).
func offsetFetchPartitions(v any) bool {
	ps, ok := v.([]any)
	if !ok || len(ps) == 0 {
		return false
	}
	for _, p := range ps {
		m, ok := p.(map[string]any)
		if !ok || m["Offset"] == nil || m["Metadata"] == nil || m["LeaderEpoch"] == nil {
			return false
		}
	}
	return true
}

// diff lists where got differs from want, as dotted paths.
func diff(path string, want, got any, out *[]Diff) {
	switch w := want.(type) {
	case map[string]any:
		g, ok := got.(map[string]any)
		if !ok {
			*out = append(*out, Diff{path, want, got})
			return
		}
		keys := map[string]bool{}
		for k := range w {
			keys[k] = true
		}
		for k := range g {
			keys[k] = true
		}
		sorted := make([]string, 0, len(keys))
		for k := range keys {
			sorted = append(sorted, k)
		}
		sort.Strings(sorted)
		for _, k := range sorted {
			p := k
			if path != "" {
				p = path + "." + k
			}
			wv, wok := w[k]
			gv, gok := g[k]
			switch {
			case !wok:
				*out = append(*out, Diff{p, "<absent>", gv})
			case !gok:
				*out = append(*out, Diff{p, wv, "<absent>"})
			default:
				diff(p, wv, gv, out)
			}
		}
	case []any:
		g, ok := got.([]any)
		if !ok || len(g) != len(w) {
			// A list of a different length is one difference, at the list.
			*out = append(*out, Diff{path, want, got})
			return
		}
		for i := range w {
			diff(fmt.Sprintf("%s[%d]", path, i), w[i], g[i], out)
		}
	default:
		if fmt.Sprint(want) != fmt.Sprint(got) {
			*out = append(*out, Diff{path, want, got})
		}
	}
}

// Diff is one place a replay answered differently from the recording.
type Diff struct {
	Path string
	Want any // what Apache Kafka answered.
	Got  any // what doze-kafka answered.
}

func (d Diff) String() string {
	w, _ := json.Marshal(d.Want)
	g, _ := json.Marshal(d.Got)
	return fmt.Sprintf("%s: kafka %s, doze %s", d.Path, w, g)
}

// sortMembers puts a group's members in the order of their placeholders: a broker
// lists them in the order of a hash map, which is not something a client can use.
func sortMembers(list []any) {
	for _, e := range list {
		m, ok := e.(map[string]any)
		if !ok {
			return
		}
		if _, ok := m["MemberID"].(string); !ok {
			return
		}
	}
	sort.SliceStable(list, func(i, j int) bool {
		a, _ := list[i].(map[string]any)["MemberID"].(string)
		b, _ := list[j].(map[string]any)["MemberID"].(string)
		return a < b
	})
}

// saslToken makes a SASL token readable and stable. kmsg marshals the bytes as
// base64; a token that is text (every PLAIN and SCRAM message a script sends, and
// every SCRAM message a broker answers) is recorded as that text, anything else as
// "base64:" and the bytes. Of a SCRAM exchange three things differ on every run and
// are replaced, in a broker's server-first and server-final messages only:
//
//   - the server's half of the nonce (what follows a client nonce the script
//     registered with Env.ClientNonce) is <server-nonce:N>, and the same
//     placeholder wherever it appears afterwards, so a client-final message that
//     returns the right nonce reads as such and one that does not keeps what the
//     script wrote;
//   - the salt (doze-kafka draws one per start) is <salt>;
//   - the server signature is <server-signature>: the script checks it against the
//     password itself and fails the run if it is wrong (scenarios_sasl.go).
//
// What a script sends that depends on those (a proof) it names itself, with
// Env.Scrub, so the recording says which proof it was.
func (n *normalizer) saslToken(b64 string) string {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil {
		return b64
	}
	if !utf8.Valid(raw) {
		return "base64:" + b64
	}
	text := string(raw)
	attrs := strings.Split(text, ",")
	serverFirst := len(attrs) >= 3 && strings.HasPrefix(attrs[0], "r=") && strings.HasPrefix(attrs[1], "s=") && strings.HasPrefix(attrs[2], "i=")
	if serverFirst {
		nonce := strings.TrimPrefix(attrs[0], "r=")
		for _, c := range n.clientNonces {
			if rest, ok := strings.CutPrefix(nonce, c); ok && rest != "" {
				n.serverNonces++
				n.scrub = append(n.scrub, [2]string{rest, fmt.Sprintf("<server-nonce:%d>", n.serverNonces)})
				break
			}
		}
		attrs[1] = "s=<salt>"
		text = strings.Join(attrs, ",")
	}
	if len(attrs) == 1 && strings.HasPrefix(text, "v=") {
		text = "v=<server-signature>"
	}
	for _, r := range n.scrub {
		text = strings.ReplaceAll(text, r[0], r[1])
	}
	return text
}
