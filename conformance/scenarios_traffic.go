package conformance

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"math"
	"slices"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Scenarios of the "Traffic" stage (plan/stages/traffic.md, section 8): what a
// real Apache Kafka answers to client telemetry (KIP-714) and to the classic
// group protocol's ordinary refusals, which the Traffic page classes as normal.

func init() {
	Scenarios = append(Scenarios,
		Scenario{Name: "telemetry-empty-subscription", Run: telemetryEmptySubscription, Known: knownTelemetryEmpty},
		Scenario{Name: "telemetry-throttle", Run: telemetryThrottle, Known: knownTelemetryThrottle},
		Scenario{Name: "telemetry-interval", Run: telemetryInterval, Known: knownTelemetryInterval},
		Scenario{Name: "telemetry-subscription-change", Run: telemetrySubscriptionChange, NoReplay: noClientMetrics},
		Scenario{Name: "telemetry-instance-binding-spaced", Run: telemetryInstanceBindingSpaced, Known: knownTelemetrySpaced, Users: telemetryUsers, Admin: "admin"},
		Scenario{Name: "telemetry-terminating", Run: telemetryTerminating, Known: knownTelemetryTerminating},
		Scenario{Name: "telemetry-ask-again", Run: telemetryAskAgain, Known: knownTelemetryAskAgain},
		Scenario{Name: "telemetry-no-console", Run: telemetryNoConsole, Known: knownTelemetryNoConsole,
			Doze: func(c *config.Config) { c.Console = false }},
		Scenario{Name: "telemetry-errors", Run: telemetryErrors, Known: knownTelemetryErrors},
		Scenario{Name: "telemetry-broker-restart", Run: telemetryBrokerRestart, Known: knownTelemetryRestart},
		Scenario{Name: "telemetry-instance-binding", Run: telemetryInstanceBinding, Known: knownTelemetryBinding, Users: telemetryUsers, Admin: "admin"},
		Scenario{Name: "telemetry-acl", Run: telemetryACL, Known: knownTelemetryACL, Users: telemetryUsers, Admin: "admin",
			KafkaEnv: []string{
				"KAFKA_AUTHORIZER_CLASS_NAME=org.apache.kafka.metadata.authorizer.StandardAuthorizer",
				"KAFKA_SUPER_USERS=User:admin;User:ANONYMOUS",
			},
			Doze: func(c *config.Config) { c.ACLDefault, c.ACLSuperUsers = "deny", []string{"User:admin"} }},
		Scenario{Name: "classic-join-flow", Run: classicJoinFlow, Known: knownClassicJoin},
		Scenario{Name: "classic-rebalance-generations", Run: classicRebalanceGenerations, Known: knownClassicGenerations},
		Scenario{Name: "produce-mixed-partition-errors", Run: produceMixedPartitionErrors, Known: knownProduceMixed},
	)
}

// noClientMetrics is why the one scenario that changes a CLIENT_METRICS resource
// under a client is recorded and not replayed: doze-kafka has no such resource and a
// fixed subscription (docs/limitations.md), so a subscription cannot change here.
//
// The scenarios that only need Kafka to use an interval are replayed: they define a
// CLIENT_METRICS resource with the five seconds doze-kafka always uses, as their
// first step (which doze-kafka refuses, a written-down difference), and from then on
// both brokers apply the same interval to the same requests in real time.
const noClientMetrics = "doze-kafka has no CLIENT_METRICS resource, so a subscription cannot change"

// dozeInterval is doze-kafka's push interval, which the replayed scenarios give Kafka.
const dozeInterval = 5 * time.Second

// past is a wait of a whole interval and a little.
const past = dozeInterval + 300*time.Millisecond

var telemetryUsers = map[string]string{"admin": "admin-secret", "alice": "alice-secret", "bob": "bob-secret"}

// What differs from Apache Kafka, in the telemetry scenarios, is what a client is
// subscribed to (docs/limitations.md): the same standard set, cumulative, every five
// seconds, where Kafka with nothing configured subscribes it to nothing every five
// minutes with deltas.
const (
	limSubscription    = "A subscription is the same for every client: the standard metric set, every five seconds, cumulative."
	limNoClientMetrics = "Setting one with `IncrementalAlterConfigs` is refused `INVALID_REQUEST`"
	limNoConsole       = "a broker without a console closes the connection of a client that sends them"
)

// knownDefined is the difference of a scenario that defines the five-second
// CLIENT_METRICS resource first: doze-kafka refuses the definition, and what it
// subscribes a client to differs only in the temporality (the metrics and the
// interval are then the same as Kafka's).
func knownDefined(subscribeSteps ...string) []Known {
	out := []Known{{Step: "define-interval-5s", Path: "Resources[0]", Limitation: limNoClientMetrics}}
	for _, s := range subscribeSteps {
		out = append(out, Known{Step: s, Path: "DeltaTemporality", Limitation: limSubscription})
	}
	return out
}

func knownSubscription(stepPrefix string) []Known {
	return []Known{
		{Step: stepPrefix + "*", Path: "PushIntervalMillis", Limitation: limSubscription},
		{Step: stepPrefix + "*", Path: "RequestedMetrics", Limitation: limSubscription},
		{Step: stepPrefix + "*", Path: "DeltaTemporality", Limitation: limSubscription},
	}
}

var (
	knownTelemetryEmpty    = knownSubscription("subscribe-new")
	knownTelemetryThrottle = knownSubscription("subscribe")
	knownTelemetryErrors   = knownSubscription("subscribe")
	knownTelemetryRestart  = append(knownSubscription("subscribe"), knownSubscription("second-instance-asks-first")...)
	knownTelemetryBinding  = knownSubscription("alice-1-subscribes")
	// With the five-second resource defined, Kafka's subscription is doze-kafka's but
	// for the temporality.
	knownTelemetryInterval    = knownDefined("subscribe*")
	knownTelemetrySpaced      = knownDefined("*")
	knownTelemetryTerminating = knownDefined("subscribe-*")
	// Every subscription request that is answered differs in what it subscribes to.
	knownTelemetryAskAgain = slices.Concat(knownSubscription("subscribe"),
		knownSubscription("wrong-subscription-subscribe-at-once"), knownSubscription("unsupported-compression-subscribe-at-once"),
		knownSubscription("terminating-subscribe-at-once"), knownSubscription("twice-subscribe-at-once"))
	// A broker without a console does not serve the two calls at all.
	knownTelemetryNoConsole = []Known{{Step: "*", Path: "", Limitation: limNoConsole}}
	knownTelemetryACL       = knownSubscription("subscribe")
	knownClassicJoin        []Known
	knownClassicGenerations []Known
	knownProduceMixed       []Known
)

// --- KIP-714 helpers ---

func getSubscription(id [16]byte) *kmsg.GetTelemetrySubscriptionsRequest {
	r := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
	r.ClientInstanceID = id
	return r
}

func pushRequest(id [16]byte, sub int32, compression int8, body []byte, terminating bool) *kmsg.PushTelemetryRequest {
	r := kmsg.NewPtrPushTelemetryRequest()
	r.ClientInstanceID, r.SubscriptionID = id, sub
	r.CompressionType, r.Metrics, r.Terminating = compression, body, terminating
	return r
}

// subscribe asks for a subscription under a new instance id (zero) or an old
// one, and returns what the broker answered.
func (e *Env) subscribe(label string, id [16]byte) *kmsg.GetTelemetrySubscriptionsResponse {
	return e.Do(label, getSubscription(id), 0).(*kmsg.GetTelemetrySubscriptionsResponse)
}

func (e *Env) push(label string, id [16]byte, sub int32, compression int8, body []byte, terminating bool) int16 {
	return e.Do(label, pushRequest(id, sub, compression, body, terminating), 0).(*kmsg.PushTelemetryResponse).ErrorCode
}

// otlpGauge is a minimal OTLP ExportMetricsServiceRequest: one resource with a
// client id attribute, one gauge with one point. It is what a client's push is,
// reduced to what a broker can read.
func otlpGauge(name string, v float64) []byte {
	field := func(num int, b []byte) []byte {
		out := binary.AppendUvarint(nil, uint64(num<<3|2))
		out = binary.AppendUvarint(out, uint64(len(b)))
		return append(out, b...)
	}
	str := func(s string) []byte { return []byte(s) }
	cat := func(parts ...[]byte) []byte { return bytes.Join(parts, nil) }
	point := cat([]byte{0x11}, binary.LittleEndian.AppendUint64(nil, math.Float64bits(v))) // as_double = 4 (fixed64)
	gauge := field(1, point)
	metric := cat(field(1, str(name)), field(5, gauge))
	scope := field(2, metric)
	value := field(1, str("doze-conformance"))
	attr := cat(field(1, str("client_rack")), field(2, value))
	resource := field(1, attr)
	return field(1, cat(field(1, resource), field(2, scope)))
}

func gzipped(b []byte) []byte {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	w.Write(b)
	w.Close()
	return buf.Bytes()
}

// --- R5: what a client is subscribed to with nothing configured ---

func telemetryEmptySubscription(e *Env) {
	first := e.subscribe("subscribe-new-client", [16]byte{})
	e.subscribe("subscribe-again-with-id", first.ClientInstanceID)
	e.push("push-valid", first.ClientInstanceID, first.SubscriptionID, 0, otlpGauge("org.apache.kafka.producer.record-send-rate", 1), false)
}

// --- R1: a push that arrives before its interval has passed ---

func telemetryThrottle(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	sub := e.subscribe("subscribe", [16]byte{})
	e.push("push-first", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)
	e.push("push-at-once", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)
	e.push("push-at-once-again", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)
	// Which refusal comes first when a push is early and also wrong.
	e.push("early-and-unknown-subscription", sub.ClientInstanceID, sub.SubscriptionID+1, 0, body, false)
	e.push("early-and-unsupported-compression", sub.ClientInstanceID, sub.SubscriptionID, 9, body, false)
	e.push("early-and-too-large", sub.ClientInstanceID, sub.SubscriptionID, 0, make([]byte, int(sub.TelemetryMaxBytes)+1), false)
	e.push("early-and-corrupt", sub.ClientInstanceID, sub.SubscriptionID, 1, []byte("not gzip"), false)
	e.push("early-terminating", sub.ClientInstanceID, sub.SubscriptionID, 0, body, true)
	e.push("push-after-terminating", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)
	e.push("push-after-terminating-wrong-subscription", sub.ClientInstanceID, sub.SubscriptionID+1, 0, body, false)
	e.push("push-after-terminating-once-more", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)
	e.subscribe("subscribe-after-terminating", sub.ClientInstanceID)
	e.push("push-after-terminating-and-subscribe", sub.ClientInstanceID, sub.SubscriptionID, 0, body, false)

	// A subscription asked for twice in a row, and a push that follows it.
	again := e.subscribe("subscribe-second-client", [16]byte{})
	e.subscribe("subscribe-second-client-again", again.ClientInstanceID)
	e.push("push-after-throttled-subscribe", again.ClientInstanceID, again.SubscriptionID, 0, body, false)

	// A push for an instance the broker never saw, whose subscription id is not
	// the one it would have given.
	var stranger [16]byte
	stranger[3] = 9
	e.push("push-unknown-instance", stranger, sub.SubscriptionID, 0, body, false)
	e.push("push-unknown-instance-zero-subscription", stranger, 0, 0, body, false)
}

// telemetryInterval is the timing of the throttle, in real time against the five
// seconds doze-kafka uses (Kafka is given the same with a CLIENT_METRICS resource),
// so it is replayed. Each question is asked of an instance of its own where that
// saves a wait.
func telemetryInterval(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	setClientMetrics(e, "define-interval-5s", "5000")

	// Pushes: half an interval and nine tenths of one are early, a whole one is not,
	// and the early ones do not postpone it.
	sub := e.subscribe("subscribe", [16]byte{})
	id, sid := sub.ClientInstanceID, sub.SubscriptionID
	began := time.Now()
	since := func(d time.Duration) { time.Sleep(time.Until(began.Add(d))) }
	e.push("push-first", id, sid, 0, body, false)
	// Meanwhile, a second instance asks the same of subscription requests.
	sub2 := e.subscribe("subscribe-second-instance", [16]byte{})
	id2 := sub2.ClientInstanceID
	since(dozeInterval / 2)
	e.push("push-at-half-interval", id, sid, 0, body, false)
	e.subscribe("subscribe-again-at-half-interval", id2)
	since(dozeInterval * 9 / 10)
	e.push("push-at-nine-tenths", id, sid, 0, body, false)
	e.subscribe("subscribe-again-at-nine-tenths", id2)
	since(past)
	e.push("push-after-interval-early-ones-between", id, sid, 0, body, false)
	// Do a push and a subscription request share the clock?
	e.subscribe("subscribe-right-after-push", id)
	e.subscribe("subscribe-after-interval-early-ones-between", id2)
	e.push("push-right-after-subscribe", id2, sub2.SubscriptionID, 0, body, false)
	e.push("push-again-right-after", id2, sub2.SubscriptionID, 0, body, false)

	// A push some time after a subscription, and inside the interval of the push
	// before the subscription.
	began = time.Now()
	since(past)
	e.subscribe("subscribe-after-an-interval", id)
	since(past + 1500*time.Millisecond)
	e.push("push-1-5s-after-subscribe", id, sid, 0, body, false)
	since(past + 3*time.Second)
	e.subscribe("subscribe-1-5s-after-that-push", id)
	e.push("push-after-refused-subscribe", id, sid, 0, body, false)
}

// telemetrySubscriptionChange is a subscription changing under a client that is
// pushing: a CLIENT_METRICS resource is defined, a client subscribes and pushes, the
// resource is changed. Recorded and not replayed (noClientMetrics).
func telemetrySubscriptionChange(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	setClientMetrics(e, "define-interval-1s", "1000")
	sub := e.subscribe("subscribe", [16]byte{})
	id := sub.ClientInstanceID
	e.push("push-first", id, sub.SubscriptionID, 0, body, false)
	time.Sleep(1100 * time.Millisecond)
	setClientMetrics(e, "change-interval-2s", "2000")
	e.push("push-old-subscription-after-change", id, sub.SubscriptionID, 0, body, false)
	// Straight after the refused push: is the request throttled as after a push taken?
	s3 := e.subscribe("subscribe-right-after-refused-push", id)
	e.push("push-new-subscription-at-once", id, s3.SubscriptionID, 0, body, false)
	time.Sleep(2100 * time.Millisecond)
	e.push("push-new-subscription-after-interval", id, s3.SubscriptionID, 0, body, false)
	// The same with a wait, then a push with the old id again, then a push at once:
	// did the refused one set the clock?
	time.Sleep(2100 * time.Millisecond)
	e.push("push-old-subscription-again", id, sub.SubscriptionID, 0, body, false)
	e.push("push-new-subscription-right-after-refused", id, s3.SubscriptionID, 0, body, false)
}

// --- a terminating push, and what follows it ---

// telemetryTerminating asks what a broker remembers of a client that said it was
// closing: each question of a fresh instance, so that only one of them needs a wait.
func telemetryTerminating(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	setClientMetrics(e, "define-interval-5s", "5000")
	fresh := func(label string) (id [16]byte, sid int32) {
		s := e.subscribe("subscribe-"+label, [16]byte{})
		return s.ClientInstanceID, s.SubscriptionID
	}

	// An instance for the timed question, started first: a push, a terminating push
	// inside its interval, the refused push, and a push an interval after the first
	// (and so inside the interval of the terminating one).
	tid, tsid := fresh("timed")
	began := time.Now()
	e.push("timed-push", tid, tsid, 0, body, false)

	// Terminating pushes in a row.
	id, sid := fresh("in-a-row")
	e.push("in-a-row-terminating-1", id, sid, 0, body, true)
	e.push("in-a-row-terminating-2", id, sid, 0, body, true)
	e.push("in-a-row-terminating-3", id, sid, 0, body, true)
	e.push("in-a-row-push-after", id, sid, 0, body, false)
	e.push("in-a-row-push-after-again", id, sid, 0, body, false)

	// A terminating push that was itself refused, then another, then a push.
	id, sid = fresh("refused-117")
	e.push("refused-117-terminating-wrong-subscription", id, sid+1, 0, body, true)
	e.push("refused-117-terminating-again", id, sid, 0, body, true)
	e.push("refused-117-push-after", id, sid, 0, body, false)
	id, sid = fresh("refused-76")
	e.push("refused-76-terminating-unsupported-compression", id, sid, 9, body, true)
	e.push("refused-76-terminating-again", id, sid, 0, body, true)
	id, sid = fresh("refused-then-push")
	e.push("refused-then-push-terminating-wrong-subscription", id, sid+1, 0, body, true)
	e.push("refused-then-push-push", id, sid, 0, body, false)
	e.push("refused-then-push-push-again", id, sid, 0, body, false)

	// An instance the broker has never seen.
	var stranger [16]byte
	stranger[0], stranger[15] = 0x7e, 0x11
	e.push("never-seen-terminating", stranger, 1, 0, body, true)
	e.push("never-seen-push-after", stranger, 1, 0, body, false)

	// A terminating push after an ordinary one, at once; the refused push; then a
	// terminating one again (the refused push was not terminating), and another.
	id, sid = fresh("after-push")
	e.push("after-push-push", id, sid, 0, body, false)
	e.push("after-push-terminating-at-once", id, sid, 0, body, true)
	e.push("after-push-push-refused", id, sid, 0, body, false)
	e.push("after-push-terminating-after-refused-push", id, sid, 0, body, true)
	e.push("after-push-terminating-once-more", id, sid, 0, body, true)

	// A subscription request does not clear it.
	id, sid = fresh("subscribe-between")
	e.push("subscribe-between-terminating", id, sid, 0, body, true)
	e.subscribe("subscribe-between-subscribe", id)
	e.push("subscribe-between-push-after", id, sid, 0, body, false)

	// The timed instance: 3 s after its push a terminating one, the refused push, and
	// at 5.5 s (2.5 s after the terminating one) a push.
	time.Sleep(time.Until(began.Add(3 * time.Second)))
	e.push("timed-terminating-at-3s", tid, tsid, 0, body, true)
	e.push("timed-push-refused", tid, tsid, 0, body, false)
	// Terminating pushes in a row, spaced by an interval.
	sid2, ssid2 := fresh("spaced")
	e.push("spaced-terminating-1", sid2, ssid2, 0, body, true)
	e.push("spaced-terminating-2", sid2, ssid2, 0, body, true)
	time.Sleep(time.Until(began.Add(dozeInterval + 500*time.Millisecond)))
	e.push("timed-push-at-5-5s", tid, tsid, 0, body, false)
	time.Sleep(time.Until(began.Add(3*time.Second + past)))
	e.push("spaced-terminating-3-after-an-interval", sid2, ssid2, 0, body, true)
	e.push("spaced-terminating-4", sid2, ssid2, 0, body, true)
}

// telemetryAskAgain is the subscription request a client sends after a push was
// refused for its subscription id or its compression type: the refusal tells it to
// ask again, so is the request throttled as one straight after a push is?
func telemetryAskAgain(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	for _, c := range []struct {
		label       string
		sub         int32
		compression int8
	}{{"wrong-subscription", 1, 0}, {"unsupported-compression", 0, 9}} {
		s := e.subscribe("subscribe-"+c.label, [16]byte{})
		id := s.ClientInstanceID
		e.push(c.label+"-push", id, s.SubscriptionID+c.sub, c.compression, body, false)
		e.subscribe(c.label+"-subscribe-at-once", id)
		e.push(c.label+"-push-at-once", id, s.SubscriptionID, 0, body, false)
		e.subscribe(c.label+"-subscribe-after-early-push", id)
		e.subscribe(c.label+"-subscribe-again", id)
	}
	// The same after a push that was taken, for comparison, and after a refused
	// terminating one.
	s := e.subscribe("subscribe-taken", [16]byte{})
	e.push("taken-push", s.ClientInstanceID, s.SubscriptionID, 0, body, false)
	e.subscribe("taken-subscribe-at-once", s.ClientInstanceID)
	s = e.subscribe("subscribe-terminating", [16]byte{})
	e.push("terminating-wrong-subscription-push", s.ClientInstanceID, s.SubscriptionID+1, 0, body, true)
	e.subscribe("terminating-subscribe-at-once", s.ClientInstanceID)
	e.push("terminating-push-refused", s.ClientInstanceID, s.SubscriptionID, 0, body, false)
	e.subscribe("terminating-subscribe-after-refused-push", s.ClientInstanceID)
	// Is it once? Two subscription requests at once after the refused push.
	s = e.subscribe("subscribe-twice", [16]byte{})
	e.push("twice-wrong-subscription-push", s.ClientInstanceID, s.SubscriptionID+1, 0, body, false)
	e.subscribe("twice-subscribe-at-once", s.ClientInstanceID)
	e.subscribe("twice-subscribe-at-once-again", s.ClientInstanceID)
}

// telemetryNoConsole sends the two calls to a broker that does not advertise them:
// Apache Kafka with nothing configured, and doze-kafka without its console.
func telemetryNoConsole(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	// Nothing here depends on an answer: doze-kafka gives none.
	e.subscribe("subscribe-not-advertised", [16]byte{})
	var stranger [16]byte
	stranger[0], stranger[15] = 0x3c, 0xc3
	e.push("push-not-advertised", stranger, 0, 0, body, false)
	e.push("push-not-advertised-no-instance", [16]byte{}, 0, 0, body, false)
}

// setClientMetrics defines (or changes) the CLIENT_METRICS resource "conformance"
// for every client: the standard prefix, and the interval given.
func setClientMetrics(e *Env, label, intervalMs string) {
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType, res.ResourceName = kmsg.ConfigResourceTypeClientMetrics, "conformance"
	for _, kv := range [][2]string{{"metrics", "org.apache.kafka."}, {"interval.ms", intervalMs}} {
		c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		c.Name, c.Value, c.Op = kv[0], str(kv[1]), kmsg.IncrementalAlterConfigOpSet
		res.Configs = append(res.Configs, c)
	}
	req.Resources = append(req.Resources, res)
	e.Do(label, req, 1)
}

// --- R3: the refusals ---

func telemetryErrors(e *Env) {
	fresh := func(label string) *kmsg.GetTelemetrySubscriptionsResponse {
		return e.subscribe(label, [16]byte{})
	}
	good := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	s := fresh("subscribe")
	e.push("zero-instance-id", [16]byte{}, s.SubscriptionID, 0, good, false)
	var stranger [16]byte
	stranger[0], stranger[15] = 0x5a, 0xa5
	e.push("unknown-instance-id", stranger, s.SubscriptionID, 0, good, false)
	e.push("unknown-subscription-id", s.ClientInstanceID, s.SubscriptionID+1, 0, good, false)

	s = fresh("subscribe-compression-9")
	e.push("compression-unsupported-9", s.ClientInstanceID, s.SubscriptionID, 9, good, false)
	s = fresh("subscribe-compression-negative")
	e.push("compression-unsupported-negative", s.ClientInstanceID, s.SubscriptionID, -1, good, false)

	s = fresh("subscribe-too-large")
	e.push("payload-over-telemetry-max-bytes", s.ClientInstanceID, s.SubscriptionID, 0, make([]byte, int(s.TelemetryMaxBytes)+1), false)
	s = fresh("subscribe-at-limit")
	e.push("payload-at-telemetry-max-bytes", s.ClientInstanceID, s.SubscriptionID, 0, make([]byte, int(s.TelemetryMaxBytes)), false)
	s = fresh("subscribe-inflated")
	e.push("gzip-inflates-past-telemetry-max-bytes", s.ClientInstanceID, s.SubscriptionID, 1, gzipped(make([]byte, 4<<20)), false)

	for _, c := range []struct {
		label string
		code  int8
	}{{"corrupt-gzip", 1}, {"corrupt-snappy", 2}, {"corrupt-lz4", 3}, {"corrupt-zstd", 4}} {
		s = fresh("subscribe-" + c.label)
		e.push(c.label, s.ClientInstanceID, s.SubscriptionID, c.code, []byte("this is not a compressed body"), false)
	}
	s = fresh("subscribe-not-otlp")
	e.push("not-otlp-uncompressed", s.ClientInstanceID, s.SubscriptionID, 0, []byte("this is not a metrics payload"), false)
	s = fresh("subscribe-gzip-valid")
	e.push("gzip-valid-otlp", s.ClientInstanceID, s.SubscriptionID, 1, gzipped(good), false)
	s = fresh("subscribe-gzip-text")
	e.push("gzip-of-text", s.ClientInstanceID, s.SubscriptionID, 1, gzipped([]byte("this is not a metrics payload")), false)
	s = fresh("subscribe-empty")
	e.push("empty-body", s.ClientInstanceID, s.SubscriptionID, 0, nil, false)
}

// --- C2: a broker that restarted ---

func telemetryBrokerRestart(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	s := e.subscribe("subscribe", [16]byte{})
	e.push("push-before-restart", s.ClientInstanceID, s.SubscriptionID, 0, body, false)
	// A second client, which after the restart asks before it pushes.
	other := e.subscribe("subscribe-second-instance", [16]byte{})
	e.Restart("restart")
	e.subscribe("second-instance-asks-first-after-restart", other.ClientInstanceID)
	e.push("push-old-subscription-after-restart", s.ClientInstanceID, s.SubscriptionID, 0, body, false)
	again := e.subscribe("subscribe-with-old-instance-id-after-restart", s.ClientInstanceID)
	e.push("push-new-subscription", s.ClientInstanceID, again.SubscriptionID, 0, body, false)
}

// --- R4: whose instance id it is ---

func telemetryInstanceBinding(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	e.Open("alice-1", "alice", telemetryUsers["alice"])
	e.Open("alice-2", "alice", telemetryUsers["alice"])
	e.Open("bob", "bob", telemetryUsers["bob"])

	// One principal, two connections.
	e.Use("alice-1")
	a := e.subscribe("alice-1-subscribes", [16]byte{})
	e.Use("alice-2")
	e.push("alice-2-pushes-alice-1s-instance", a.ClientInstanceID, a.SubscriptionID, 0, body, false)

	// Another principal pushes an instance nobody has pushed yet.
	e.Use("alice-1")
	b := e.subscribe("alice-1-subscribes-second", [16]byte{})
	e.Use("bob")
	e.push("bob-pushes-alices-unpushed-instance", b.ClientInstanceID, b.SubscriptionID, 0, body, false)
	e.Use("alice-1")
	e.push("alice-1-pushes-after-bob", b.ClientInstanceID, b.SubscriptionID, 0, body, false)

	// Another principal asks for a subscription under alice's instance id.
	c := e.subscribe("alice-1-subscribes-third", [16]byte{})
	e.Use("bob")
	d := e.subscribe("bob-subscribes-with-alices-instance-id", c.ClientInstanceID)
	e.push("bob-pushes-the-id-he-was-given", d.ClientInstanceID, d.SubscriptionID, 0, body, false)
	e.Use("alice-1")
	e.push("alice-1-pushes-her-own", c.ClientInstanceID, c.SubscriptionID, 0, body, false)
}

// telemetryInstanceBindingSpaced is the same with a wait of an interval wherever
// the throttle would otherwise be the answer: what is left is whose instance it is.
// The interval is doze-kafka's five seconds on both brokers, so it is replayed; a
// second instance (k) is worked on while the first (a) waits.
func telemetryInstanceBindingSpaced(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	e.Open("admin", "admin", telemetryUsers["admin"])
	e.Open("alice-1", "alice", telemetryUsers["alice"])
	e.Open("alice-2", "alice", telemetryUsers["alice"])
	e.Open("bob", "bob", telemetryUsers["bob"])
	e.Use("admin")
	setClientMetrics(e, "define-interval-5s", "5000")
	pause := func() { time.Sleep(past) }

	e.Use("alice-1")
	a := e.subscribe("alice-1-subscribes", [16]byte{})
	e.Use("alice-2")
	e.push("alice-2-pushes-alice-1s-instance", a.ClientInstanceID, a.SubscriptionID, 0, body, false)
	// A subscription request between a terminating push and the next push (k).
	k := e.subscribe("subscribe-before-terminating", [16]byte{})
	e.push("terminating-for-subscribe-test", k.ClientInstanceID, k.SubscriptionID, 0, body, true)
	pause()
	e.Use("bob")
	e.push("bob-pushes-alices-instance", a.ClientInstanceID, a.SubscriptionID, 0, body, false)
	e.subscribe("subscribe-an-interval-after-terminating", k.ClientInstanceID)
	e.push("push-after-that-subscribe", k.ClientInstanceID, k.SubscriptionID, 0, body, false)
	e.push("push-once-more", k.ClientInstanceID, k.SubscriptionID, 0, body, false)
	pause()
	d := e.subscribe("bob-subscribes-with-alices-instance-id", a.ClientInstanceID)
	e.push("bob-pushes-the-id-he-was-given", d.ClientInstanceID, d.SubscriptionID, 0, body, false)
	pause()
	e.Use("alice-1")
	e.push("alice-1-pushes-her-own-after-bob", a.ClientInstanceID, a.SubscriptionID, 0, body, false)
	pause()
	e.Use("alice-2")
	f := e.subscribe("alice-2-subscribes-with-alice-1s-instance-id", a.ClientInstanceID)
	e.push("alice-2-pushes-what-she-was-given", a.ClientInstanceID, f.SubscriptionID, 0, body, false)
	e.Use("alice-1")
	e.push("alice-1-pushes-terminating", a.ClientInstanceID, f.SubscriptionID, 0, body, true)
	e.Use("alice-2")
	e.push("alice-2-pushes-after-terminating", a.ClientInstanceID, f.SubscriptionID, 0, body, false)
	pause()
	e.push("pushes-after-terminating-again", a.ClientInstanceID, f.SubscriptionID, 0, body, false)
	pause()
	e.push("pushes-after-terminating-wrong-subscription", a.ClientInstanceID, f.SubscriptionID+1, 0, body, false)
	pause()
	g := e.subscribe("subscribes-after-terminating", a.ClientInstanceID)
	e.push("pushes-after-subscribing-again", a.ClientInstanceID, g.SubscriptionID, 0, body, false)
	e.push("pushes-terminating-again", a.ClientInstanceID, g.SubscriptionID, 0, body, true)
	e.push("pushes-terminating-first", a.ClientInstanceID, g.SubscriptionID, 0, body, true)

	// The refusals in order, each from a fresh instance (whose first push is never
	// early): which one a push that has several of them is answered.
	fresh := func(label string) *kmsg.GetTelemetrySubscriptionsResponse {
		return e.subscribe(label, [16]byte{})
	}
	big := make([]byte, 1<<20+1)
	h := fresh("subscribe-order-1")
	e.push("wrong-subscription-and-unsupported-compression", h.ClientInstanceID, h.SubscriptionID+1, 9, body, false)
	h = fresh("subscribe-order-2")
	e.push("wrong-subscription-and-too-large", h.ClientInstanceID, h.SubscriptionID+1, 0, big, false)
	h = fresh("subscribe-order-3")
	e.push("unsupported-compression-and-too-large", h.ClientInstanceID, h.SubscriptionID, 9, big, false)
	h = fresh("subscribe-order-4")
	e.push("terminating-and-wrong-subscription", h.ClientInstanceID, h.SubscriptionID+1, 0, body, true)
	h = fresh("subscribe-order-5")
	e.push("terminating-and-unsupported-compression", h.ClientInstanceID, h.SubscriptionID, 9, body, true)
	h = fresh("subscribe-order-6")
	e.push("terminating-and-corrupt", h.ClientInstanceID, h.SubscriptionID, 1, []byte("not gzip"), true)
	h = fresh("subscribe-order-7")
	e.push("terminating-and-too-large", h.ClientInstanceID, h.SubscriptionID, 0, big, true)
}

// --- R2: no ACL is needed ---

func telemetryACL(e *Env) {
	body := otlpGauge("org.apache.kafka.consumer.fetch-rate", 1)
	e.Open("alice", "alice", telemetryUsers["alice"])
	e.Use("alice")
	// Control: the authorizer is on, and a user with no ACLs is refused a topic.
	e.Do("metadata-refused", metadata(false, "acl-topic"), 9)
	s := e.subscribe("subscribe-without-acls", [16]byte{})
	e.push("push-without-acls", s.ClientInstanceID, s.SubscriptionID, 0, body, false)
}

// --- R6, R8: the classic group protocol ---

type classicMember struct {
	conn   string
	id     string
	gen    int32
	leader bool
}

func classicGroupSetup(e *Env, group string) {
	e.Do("create-topic", createTopics(false, newTopic("cg", 2, 1)), 7)
	fc := kmsg.NewPtrFindCoordinatorRequest()
	fc.CoordinatorKeys = []string{group}
	e.Do("find-coordinator", fc, 4)
}

func joinRequest(group, member string, session int32) *kmsg.JoinGroupRequest {
	r := kmsg.NewPtrJoinGroupRequest()
	r.Group, r.MemberID = group, member
	r.SessionTimeoutMillis, r.RebalanceTimeoutMillis = session, 20000
	r.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name, p.Metadata = "range", consumerSubscription("cg")
	r.Protocols = append(r.Protocols, p)
	return r
}

func heartbeatRequest(group, member string, gen int32) *kmsg.HeartbeatRequest {
	r := kmsg.NewPtrHeartbeatRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	return r
}

// assignment is one member's share, in the order a script lists them (never sorted
// by member id, which differs from run to run).
type assignment struct {
	member string
	data   []byte
}

func syncRequest(group, member string, gen int32, assign []assignment) *kmsg.SyncGroupRequest {
	r := kmsg.NewPtrSyncGroupRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	r.ProtocolType, r.Protocol = str("consumer"), str("range")
	for _, as := range assign {
		a := kmsg.NewSyncGroupRequestGroupAssignment()
		a.MemberID, a.MemberAssignment = as.member, as.data
		r.GroupAssignment = append(r.GroupAssignment, a)
	}
	return r
}

func commitRequest(group, member string, gen int32) *kmsg.OffsetCommitRequest {
	r := kmsg.NewPtrOffsetCommitRequest()
	r.Group, r.MemberID, r.Generation = group, member, gen
	t := kmsg.NewOffsetCommitRequestTopic()
	t.Topic = "cg"
	p := kmsg.NewOffsetCommitRequestTopicPartition()
	p.Partition, p.Offset = 0, 1
	t.Partitions = append(t.Partitions, p)
	r.Topics = append(r.Topics, t)
	return r
}

func leaveRequest(group, member string) *kmsg.LeaveGroupRequest {
	r := kmsg.NewPtrLeaveGroupRequest()
	r.Group = group
	m := kmsg.NewLeaveGroupRequestMember()
	m.MemberID = member
	r.Members = append(r.Members, m)
	return r
}

const (
	joinV      = 7
	syncV      = 5
	heartbeatV = 4
	commitV    = 8
	leaveV     = 5
)

// enter makes a member join an empty or stable group the way a client does: a
// first JoinGroup with no id (answered MEMBER_ID_REQUIRED, with the id to use),
// then the second with it.
func (e *Env) enterFirst(group, conn string) *classicMember {
	e.Open(conn, "", "")
	e.Use(conn)
	a := e.Do(conn+"-join-no-id", joinRequest(group, "", 10000), joinV).(*kmsg.JoinGroupResponse)
	return &classicMember{conn: conn, id: a.MemberID}
}

func classicJoinFlow(e *Env) {
	const g = "flow"
	classicGroupSetup(e, g)

	// Consumer one: alone in the group.
	c1 := e.enterFirst(g, "c1")
	r := e.Do("c1-join-with-id", joinRequest(g, c1.id, 10000), joinV).(*kmsg.JoinGroupResponse)
	c1.gen = r.Generation
	e.Do("c1-sync", syncRequest(g, c1.id, c1.gen, []assignment{{c1.id, consumerAssignment("cg", 0, 1)}}), syncV)
	e.Do("c1-heartbeat", heartbeatRequest(g, c1.id, c1.gen), heartbeatV)
	e.Do("c1-commit", commitRequest(g, c1.id, c1.gen), commitV)

	// Consumer two joins a stable group: c1 has to rejoin.
	c2 := e.enterFirst(g, "c2")
	p2 := (func() *Pending { return e.Start("c2-join-with-id", joinRequest(g, c2.id, 10000), joinV) })()
	time.Sleep(500 * time.Millisecond) // let the broker see c2's join before c1's heartbeat.
	e.Use("c1")
	e.Do("c1-heartbeat-during-rebalance", heartbeatRequest(g, c1.id, c1.gen), heartbeatV)
	p1 := e.Start("c1-rejoin", joinRequest(g, c1.id, 10000), joinV)
	r1 := e.Finish(p1).(*kmsg.JoinGroupResponse)
	r2 := e.Finish(p2).(*kmsg.JoinGroupResponse)
	c1.gen, c2.gen = r1.Generation, r2.Generation
	leader, follower := c1, c2
	if r2.LeaderID == c2.id {
		leader, follower = c2, c1
	}
	assign := []assignment{{c1.id, consumerAssignment("cg", 0)}, {c2.id, consumerAssignment("cg", 1)}}
	e.Use(follower.conn)
	pf := e.Start(follower.conn+"-sync-follower", syncRequest(g, follower.id, follower.gen, nil), syncV)
	e.Use(leader.conn)
	e.Do(leader.conn+"-sync-leader", syncRequest(g, leader.id, leader.gen, assign), syncV)
	e.Finish(pf)
	e.Do("leader-heartbeat", heartbeatRequest(g, leader.id, leader.gen), heartbeatV)
	e.Do("leader-commit", commitRequest(g, leader.id, leader.gen), commitV)
	e.Use(follower.conn)
	e.Do("follower-commit", commitRequest(g, follower.id, follower.gen), commitV)

	// Consumer three, the same.
	c3 := e.enterFirst(g, "c3")
	p3 := e.Start("c3-join-with-id", joinRequest(g, c3.id, 10000), joinV)
	time.Sleep(500 * time.Millisecond)
	e.Use("c1")
	e.Do("c1-heartbeat-during-rebalance-3", heartbeatRequest(g, c1.id, c1.gen), heartbeatV)
	e.Use("c2")
	e.Do("c2-heartbeat-during-rebalance-3", heartbeatRequest(g, c2.id, c2.gen), heartbeatV)
	e.Use("c1")
	q1 := e.Start("c1-rejoin-3", joinRequest(g, c1.id, 10000), joinV)
	e.Use("c2")
	q2 := e.Start("c2-rejoin-3", joinRequest(g, c2.id, 10000), joinV)
	a1 := e.Finish(q1).(*kmsg.JoinGroupResponse)
	a2 := e.Finish(q2).(*kmsg.JoinGroupResponse)
	a3 := e.Finish(p3).(*kmsg.JoinGroupResponse)
	_ = a1
	_ = a2
	_ = a3
}

func classicRebalanceGenerations(e *Env) {
	const g = "gens"
	classicGroupSetup(e, g)
	members := map[string]*classicMember{}
	var order []string
	join := func(conn string) {
		m := e.enterFirst(g, conn)
		members[conn] = m
		order = append(order, conn)
	}
	// rejoinAll has every listed member (already known to the group) send a JoinGroup
	// and returns the answers in the order given.
	rejoinAll := func(label string, conns ...string) []*kmsg.JoinGroupResponse {
		var ps []*Pending
		for _, c := range conns {
			e.Use(c)
			ps = append(ps, e.Start(c+"-"+label, joinRequest(g, members[c].id, 6000), joinV))
		}
		var out []*kmsg.JoinGroupResponse
		for i, p := range ps {
			r := e.Finish(p).(*kmsg.JoinGroupResponse)
			members[conns[i]].gen = r.Generation
			out = append(out, r)
		}
		return out
	}
	syncAll := func(label string, conns ...string) {
		var assign []assignment
		for _, c := range conns {
			assign = append(assign, assignment{members[c].id, consumerAssignment("cg", 0)})
		}
		var leader string
		for _, c := range conns {
			if members[c].leader {
				leader = c
			}
		}
		var ps []*Pending
		for _, c := range conns {
			if c == leader {
				continue
			}
			e.Use(c)
			ps = append(ps, e.Start(c+"-sync-"+label, syncRequest(g, members[c].id, members[c].gen, nil), syncV))
		}
		e.Use(leader)
		e.Do(leader+"-sync-"+label, syncRequest(g, members[leader].id, members[leader].gen, assign), syncV)
		for _, p := range ps {
			e.Finish(p)
		}
	}
	setLeaders := func(rs []*kmsg.JoinGroupResponse, conns ...string) {
		for i, c := range conns {
			members[c].leader = rs[i].LeaderID == members[c].id
		}
	}
	hb := func(label, conn string) {
		e.Use(conn)
		e.Do(conn+"-heartbeat-"+label, heartbeatRequest(g, members[conn].id, members[conn].gen), heartbeatV)
	}

	// Round 1: one member.
	join("c1")
	rs := rejoinAll("join-1", "c1")
	setLeaders(rs, "c1")
	syncAll("1", "c1")

	// Round 2: a second member joins.
	join("c2")
	p := e.Start("c2-join-2", joinRequest(g, members["c2"].id, 6000), joinV) // c2 is on its own connection.
	time.Sleep(500 * time.Millisecond)
	hb("during-join-2", "c1")
	e.Use("c1")
	q := e.Start("c1-rejoin-2", joinRequest(g, members["c1"].id, 6000), joinV)
	r1 := e.Finish(q).(*kmsg.JoinGroupResponse)
	r2 := e.Finish(p).(*kmsg.JoinGroupResponse)
	members["c1"].gen, members["c2"].gen = r1.Generation, r2.Generation
	setLeaders([]*kmsg.JoinGroupResponse{r1, r2}, "c1", "c2")
	syncAll("2", "c1", "c2")
	hb("stable-2", "c1")
	hb("stable-2", "c2")

	// Round 3: c2 leaves, and c1 finds out by a heartbeat.
	e.Use("c2")
	e.Do("c2-leave", leaveRequest(g, members["c2"].id), leaveV)
	hb("after-leave", "c1")
	rs = rejoinAll("rejoin-3", "c1")
	setLeaders(rs, "c1")
	syncAll("3", "c1")

	// Round 4: c2 comes back, then goes silent; its session expires.
	e.Use("c2")
	a := e.Do("c2-join-again-no-id", joinRequest(g, "", 6000), joinV).(*kmsg.JoinGroupResponse)
	members["c2"].id = a.MemberID
	p = e.Start("c2-join-4", joinRequest(g, members["c2"].id, 6000), joinV)
	time.Sleep(500 * time.Millisecond)
	hb("during-join-4", "c1")
	e.Use("c1")
	q = e.Start("c1-rejoin-4", joinRequest(g, members["c1"].id, 6000), joinV)
	r1 = e.Finish(q).(*kmsg.JoinGroupResponse)
	r2 = e.Finish(p).(*kmsg.JoinGroupResponse)
	members["c1"].gen, members["c2"].gen = r1.Generation, r2.Generation
	setLeaders([]*kmsg.JoinGroupResponse{r1, r2}, "c1", "c2")
	syncAll("4", "c1", "c2")
	// c2 sends nothing more. c1 keeps its session alive until c2's has expired.
	for i := 0; i < 5; i++ {
		time.Sleep(2 * time.Second)
		e.Use("c1")
		if i < 4 {
			w := e.w
			if _, err := w.roundTrip(heartbeatRequest(g, members["c1"].id, members["c1"].gen), heartbeatV); err != nil {
				e.fail("keepalive: %v", err)
			}
		}
	}
	hb("after-expiry", "c1")
	rs = rejoinAll("rejoin-5", "c1")
	setLeaders(rs, "c1")
	syncAll("5", "c1")
	e.Use("c2")
	e.Do("c2-heartbeat-after-expiry", heartbeatRequest(g, members["c2"].id, members["c2"].gen), heartbeatV)
}

// --- R7: a Produce response with several errors ---

func produceMixedPartitionErrors(e *Env) {
	e.Do("create", createTopics(false, newTopic("pm-a", 2, 1), newTopic("pm-b", 1, 1, "max.message.bytes", "2000")), 7)
	ok := encodeBatch([]ProduceRecord{{TimestampMs: 1000, Key: "k", Value: "v"}})
	big := encodeBatch([]ProduceRecord{{TimestampMs: 1000, Key: "k", Value: string(make([]byte, 5000))}})
	corrupt := append([]byte(nil), ok...)
	corrupt[len(corrupt)-1] ^= 0xff

	type part struct {
		topic string
		p     int32
		data  []byte
	}
	produce := func(label string, parts ...part) {
		r := kmsg.NewPtrProduceRequest()
		r.Acks, r.TimeoutMillis = -1, 10000
		idx := map[string]int{}
		for _, pt := range parts {
			i, seen := idx[pt.topic]
			if !seen {
				t := kmsg.NewProduceRequestTopic()
				t.Topic = pt.topic
				r.Topics = append(r.Topics, t)
				i = len(r.Topics) - 1
				idx[pt.topic] = i
			}
			p := kmsg.NewProduceRequestTopicPartition()
			p.Partition, p.Records = pt.p, pt.data
			r.Topics[i].Partitions = append(r.Topics[i].Partitions, p)
		}
		// A partition that has just been created is not led for a moment (6).
		e.DoUntil(label, r, 9, func(resp kmsg.Response) bool {
			for _, t := range resp.(*kmsg.ProduceResponse).Topics {
				for _, p := range t.Partitions {
					if p.ErrorCode == 6 {
						return false
					}
				}
			}
			return true
		})
	}
	produce("all-ok", part{"pm-a", 0, ok}, part{"pm-a", 1, ok})
	produce("unknown-partition-then-too-large", part{"pm-a", 0, ok}, part{"pm-a", 7, ok}, part{"pm-b", 0, big})
	produce("too-large-then-unknown-partition", part{"pm-b", 0, big}, part{"pm-a", 7, ok}, part{"pm-a", 0, ok})
	produce("unknown-topic-first", part{"pm-nosuch", 0, ok}, part{"pm-b", 0, big}, part{"pm-a", 0, corrupt})
	produce("corrupt-first", part{"pm-a", 0, corrupt}, part{"pm-nosuch", 0, ok}, part{"pm-b", 0, big})
}
