package conformance

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/klauspost/compress/zstd"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KafkaImage is the broker the recordings were made against: the same image the
// compat matrix uses (compat/lib.sh), so one version is "Kafka" everywhere.
const KafkaImage = "apache/kafka:4.3.1"

// Target is a broker a scenario runs against.
type Target interface {
	Addr() string
	// Restart stops the broker and starts it again over the same data.
	Restart() error
	Close()
}

// Scenario is a script of requests and the differences it is known to find.
type Scenario struct {
	Name string
	Run  func(e *Env)
	// DiskMiB, when above zero, runs the real broker on a volume of that size
	// (a tmpfs) so a script can fill it. Such a scenario is recorded and not
	// replayed in-process: doze-kafka's side is the size-limited volume test.
	DiskMiB int
	// KafkaEnv are extra environment settings for the real broker, as KAFKA_NAME=value
	// (offsets.retention.minutes is KAFKA_OFFSETS_RETENTION_MINUTES).
	KafkaEnv []string
	// Replay, when set, configures the doze-kafka the scenario is replayed against, and
	// Speed (1 when 0) is how many times faster than the recording its waits run: a
	// script that waits a minute for Kafka's smallest retention waits Wait(minute) and
	// is replayed against a retention of a minute over Speed. Only the waits scale.
	Replay func(cfg *config.Config)
	Speed  float64
	// Known are the differences doze-kafka has from Apache Kafka that are
	// written down (docs/limitations.md) and not fixed. A replay fails on a
	// difference that is not here, and fails on an entry here that no longer
	// differs, so the list cannot go stale.
	Known []Known
	// Users, when set, make both brokers serve SASL/PLAIN with these users
	// (name to password); Admin names the one that is a super user. The raw
	// connection then logs in with Env.Open.
	Users map[string]string
	Admin string
	// Mechanisms are the SASL mechanisms both brokers offer when Users is set, in
	// the order Apache Kafka lists them in a SaslHandshake answer; nil is PLAIN
	// alone. PLAIN must be among them (the recorder and the readiness probe log in
	// with it), and with a SCRAM mechanism the recorder gives every user a SCRAM
	// credential of the same password before the script starts (targets.go).
	Mechanisms []string
	// NoReplay, when it says why, makes the scenario evidence only: it is
	// recorded against Apache Kafka and not replayed against doze-kafka.
	NoReplay string
	// Doze changes the replay broker's configuration for the scenario, to do
	// what KafkaEnv did to the real one.
	Doze func(*config.Config)
}

// Known is one written-down difference.
type Known struct {
	Step string // the step's label; one ending in * is a prefix.
	Path string // the dotted path of the response field; "" is any in the step.
	// Limitation is a sentence of docs/limitations.md that explains it (a
	// substring; a test checks it is there).
	Limitation string
}

func (k Known) matches(step string) bool {
	if p, ok := strings.CutSuffix(k.Step, "*"); ok {
		return strings.HasPrefix(step, p)
	}
	return k.Step == step
}

// Step is one request and its response, normalised.
type Step struct {
	Label    string `json:"label"`
	Conn     string `json:"conn,omitempty"` // the named connection the request went over.
	API      string `json:"api"`
	Version  int16  `json:"version"`
	Request  any    `json:"request,omitempty"`
	Response any    `json:"response,omitempty"`
}

// Recording is what the recorder writes for a scenario.
type Recording struct {
	Scenario string `json:"scenario"`
	Kafka    string `json:"kafka"`
	Steps    []Step `json:"steps"`
}

// Env is what a script sees: a connection, a way to send a request at a
// version, and the steps that are not requests (restart, broker config).
type Env struct {
	target Target
	speed  float64 // 1 against Kafka; Scenario.Speed against doze-kafka.
	w      *wire
	norm   *normalizer
	steps  []Step
	fail   func(format string, args ...any)
	// Named connections (Open, Use): the current one is w, cur is its name.
	cur   string
	conns map[string]*connSlot
}

type connSlot struct {
	w          *wire
	user, pass string
}

// NewEnv connects to the target. fail is how a script reports a broken run
// (t.Fatalf, or a panic in the recorder).
func NewEnv(target Target, fail func(string, ...any)) (*Env, error) {
	w, err := dial(target.Addr())
	if err != nil {
		return nil, err
	}
	e := &Env{target: target, w: w, norm: newNormalizer(), fail: fail, speed: 1, conns: map[string]*connSlot{"": {w: w}}}
	return e, nil
}

func (e *Env) Close() {
	e.Use(e.cur)
	for _, c := range e.conns {
		if c.w != nil {
			c.w.close()
		}
	}
}

// Open makes a connection of its own under name, logged in as user over
// SASL/PLAIN when user is not empty. The script sends over it after Use(name).
func (e *Env) Open(name, user, pass string) {
	if _, ok := e.conns[name]; ok {
		e.fail("connection %q is open already", name)
		return
	}
	e.conns[name] = &connSlot{user: user, pass: pass}
	e.connect(e.conns[name])
}

func (e *Env) connect(c *connSlot) {
	w, err := dial(e.target.Addr())
	if err != nil {
		e.fail("connect: %v", err)
		return
	}
	if c.user != "" {
		if err := w.saslPlain(c.user, c.pass); err != nil {
			e.fail("login as %s: %v", c.user, err)
			return
		}
	}
	c.w = w
}

// Use makes name the connection the next requests go over ("" is the first).
func (e *Env) Use(name string) {
	e.conns[e.cur].w = e.w
	c, ok := e.conns[name]
	if !ok {
		e.fail("no connection %q", name)
		return
	}
	if c.w == nil {
		e.connect(c)
	}
	e.cur, e.w = name, c.w
}

// Reconnect drops the current connection and makes a new one under the same name.
func (e *Env) Reconnect() {
	e.w.close()
	c := e.conns[e.cur]
	c.w = nil
	e.connect(c)
	e.w = c.w
}

// Wait sleeps d, divided by the scenario's speed when it is replayed against
// doze-kafka. It is not a step: the answers that follow it are.
func (e *Env) Wait(d time.Duration) {
	time.Sleep(time.Duration(float64(d) / e.speed))
}

// Quiet sends req and discards the answer, without recording a step: the keep-alive
// traffic of a script (a member that must not be expired while the script waits).
func (e *Env) Quiet(req kmsg.Request, version int16) {
	e.w.roundTrip(req, version)
}

// Scrub makes literal read as placeholder in every SASL token recorded from here
// on: for the part of a token a script computed from something the broker made up
// (a SCRAM proof), named for what it is.
func (e *Env) Scrub(literal, placeholder string) {
	e.norm.scrub = append(e.norm.scrub, [2]string{literal, placeholder})
}

// ClientNonce tells the normaliser a nonce the script will open a SCRAM exchange
// with, so that what a broker appends to it is known to be the server's half.
func (e *Env) ClientNonce(nonce string) {
	e.norm.clientNonces = append(e.norm.clientNonces, nonce)
}

// lastWasClosed reports whether the broker answered the last step by closing the
// connection (Do then returns an empty response, which is not an answer of 0).
func (e *Env) lastWasClosed() bool {
	if len(e.steps) == 0 {
		return false
	}
	m, _ := e.steps[len(e.steps)-1].Response.(map[string]any)
	_, closed := m["(connection)"]
	return closed
}

// Await sends req over the current connection, again and again for up to thirty
// seconds, until done is true of the answer. It is a wait and not a step: nothing
// is recorded, so nothing it sees can be evidence, and what is the same on every
// run is the state the next step finds. It is how a script waits for something a
// broker finishes after it has answered (an ACL applied, a leader elected) before
// the step that depends on it, instead of letting that step race it.
func (e *Env) Await(what string, req kmsg.Request, version int16, done func(kmsg.Response) bool) {
	deadline := time.Now().Add(30 * time.Second)
	for {
		resp, err := e.w.roundTrip(req, version)
		if err != nil {
			e.fail("waiting until %s: %v", what, err)
			return
		}
		if done(resp) {
			return
		}
		if time.Now().After(deadline) {
			e.fail("waited 30s until %s", what)
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Steps are the recorded steps so far.
func (e *Env) Steps() []Step { return e.steps }

// Do sends req at version and records the answer under label.
func (e *Env) Do(label string, req kmsg.Request, version int16) kmsg.Response {
	return e.DoUntil(label, req, version, nil)
}

// DoUntil is Do for something Kafka finishes after it has answered (an
// auto-created topic exists a moment after the Metadata request that asked for
// it): the request is sent again, up to ten seconds, until done is true of the
// response, and only that last answer is recorded.
func (e *Env) DoUntil(label string, req kmsg.Request, version int16, done func(kmsg.Response) bool) kmsg.Response {
	e.fail2(label)
	nreq, err := e.norm.normalize(req)
	if err != nil {
		e.fail("%s: %v", label, err)
	}
	step := Step{Label: label, Conn: e.cur, API: kmsg.NameForKey(req.Key()), Version: version, Request: nreq}
	var resp kmsg.Response
	// A coordinator that is still loading (or an internal topic that is still
	// being created) is not what a script is about: ask again until it settles.
	for attempt := 0; ; attempt++ {
		var err error
		resp, err = e.w.roundTrip(req, version)
		if err != nil {
			// A broker that closes the connection on a request it will not serve
			// is an answer: it is recorded, and the connection is made again.
			if !isClosed(err) {
				e.fail("%s: %v", label, err)
				return nil
			}
			step.Response = map[string]any{"(connection)": "closed by the broker"}
			e.Reconnect()
			e.steps = append(e.steps, step)
			return req.ResponseKind()
		}
		sortProduceResponse(resp)
		nresp, err := e.norm.normalize(resp)
		if err != nil {
			e.fail("%s: %v", label, err)
		}
		if req.Key() == int16(kmsg.CreateTopics) {
			onlyRequestedConfigs(nreq, nresp)
		}
		step.Response = nresp
		if attempt >= 100 || !coordinatorSettling(nresp) && (done == nil || done(resp)) {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	e.steps = append(e.steps, step)
	return resp
}

// sortProduceResponse puts a Produce response's topics and partitions in name
// and number order. Apache Kafka answers them in the order of a hash map, not
// the order of the request (recorded: produce-mixed-partition-errors), which is
// nothing a client can rely on, so the order is not what is compared.
func sortProduceResponse(r kmsg.Response) {
	p, ok := r.(*kmsg.ProduceResponse)
	if !ok {
		return
	}
	sort.SliceStable(p.Topics, func(i, j int) bool { return p.Topics[i].Topic < p.Topics[j].Topic })
	for i := range p.Topics {
		ps := p.Topics[i].Partitions
		sort.SliceStable(ps, func(a, b int) bool { return ps[a].Partition < ps[b].Partition })
	}
}

// onlyRequestedConfigs trims a CreateTopics response's echo of every topic
// config (Kafka returns the whole table, a hundred rows that are the settings
// table's business, not this scenario's) to the configs the request set, so what
// is compared is "the value I set came back as I set it".
func onlyRequestedConfigs(req, resp any) {
	rq, _ := req.(map[string]any)
	rs, _ := resp.(map[string]any)
	asked := map[string]map[string]bool{}
	reqTopics, _ := rq["Topics"].([]any)
	for _, t := range reqTopics {
		tm, _ := t.(map[string]any)
		name, _ := tm["Topic"].(string)
		set := map[string]bool{}
		cfgs, _ := tm["Configs"].([]any)
		for _, c := range cfgs {
			if cm, ok := c.(map[string]any); ok {
				if n, ok := cm["Name"].(string); ok {
					set[n] = true
				}
			}
		}
		asked[name] = set
	}
	respTopics, _ := rs["Topics"].([]any)
	for _, t := range respTopics {
		tm, _ := t.(map[string]any)
		name, _ := tm["Topic"].(string)
		cfgs, _ := tm["Configs"].([]any)
		if cfgs == nil {
			continue
		}
		kept := []any{}
		for _, c := range cfgs {
			if cm, ok := c.(map[string]any); ok {
				if n, _ := cm["Name"].(string); asked[name][n] {
					kept = append(kept, c)
				}
			}
		}
		tm["Configs"] = kept
	}
}

// coordinatorSettling is true for a response carrying COORDINATOR_LOAD_IN_PROGRESS,
// COORDINATOR_NOT_AVAILABLE or NOT_COORDINATOR anywhere in it.
func coordinatorSettling(v any) bool {
	switch x := v.(type) {
	case map[string]any:
		for k, e := range x {
			if k == "ErrorCode" {
				if n, ok := e.(json.Number); ok && (n.String() == "14" || n.String() == "15" || n.String() == "16") {
					return true
				}
			}
			if coordinatorSettling(e) {
				return true
			}
		}
	case []any:
		for _, e := range x {
			if coordinatorSettling(e) {
				return true
			}
		}
	}
	return false
}

func isClosed(err error) bool {
	return errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE)
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (e *Env) fail2(label string) {
	for _, s := range e.steps {
		if s.Label == label {
			e.fail("duplicate step label %q", label)
		}
	}
}

// Restart stops the broker and starts it again over the same data.
func (e *Env) Restart(label string) {
	e.fail2(label)
	e.Use(e.cur)
	for _, c := range e.conns {
		if c.w != nil {
			c.w.close()
			c.w = nil
		}
	}
	if err := e.target.Restart(); err != nil {
		e.fail("%s: restart: %v", label, err)
		return
	}
	cur := e.conns[e.cur]
	e.connect(cur)
	e.w = cur.w
	e.steps = append(e.steps, Step{Label: label, API: "(restart)"})
}

// SetBrokerConfig sets one dynamic broker-wide config (IncrementalAlterConfigs
// on the cluster default resource) and records the answer.
func (e *Env) SetBrokerConfig(label, name, value string) *kmsg.IncrementalAlterConfigsResponse {
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	r := kmsg.NewIncrementalAlterConfigsRequestResource()
	r.ResourceType = kmsg.ConfigResourceTypeBroker
	r.ResourceName = ""
	c := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	c.Name, c.Value = name, &value
	c.Op = kmsg.IncrementalAlterConfigOpSet
	r.Configs = append(r.Configs, c)
	req.Resources = append(req.Resources, r)
	return e.Do(label, req, 1).(*kmsg.IncrementalAlterConfigsResponse)
}

// ProduceRecord is one record of Produce, with the timestamp the script wants.
type ProduceRecord struct {
	TimestampMs int64
	Key, Value  string
}

// Produce appends records to one partition, acks=all, in one batch (Produce v9).
func (e *Env) Produce(label, topic string, partition int32, recs ...ProduceRecord) *kmsg.ProduceResponse {
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 10000
	t := kmsg.NewProduceRequestTopic()
	t.Topic = topic
	p := kmsg.NewProduceRequestTopicPartition()
	p.Partition = partition
	p.Records = encodeBatch(recs)
	t.Partitions = append(t.Partitions, p)
	req.Topics = append(req.Topics, t)
	return e.Do(label, req, 9).(*kmsg.ProduceResponse)
}

// encodeBatch builds a v2 record batch with no producer id.
func encodeBatch(recs []ProduceRecord) []byte { return encodeBatchCodec(recs, 0) }

// encodeBatchCodec is encodeBatch with the records section compressed (1 gzip, 4 zstd).
func encodeBatchCodec(recs []ProduceRecord, codec int) []byte {
	first, max := recs[0].TimestampMs, recs[0].TimestampMs
	var body []byte
	for i, r := range recs {
		if r.TimestampMs > max {
			max = r.TimestampMs
		}
		var rec []byte
		rec = append(rec, 0) // attributes
		rec = binary.AppendVarint(rec, r.TimestampMs-first)
		rec = binary.AppendVarint(rec, int64(i))
		rec = binary.AppendVarint(rec, int64(len(r.Key)))
		rec = append(rec, r.Key...)
		rec = binary.AppendVarint(rec, int64(len(r.Value)))
		rec = append(rec, r.Value...)
		rec = binary.AppendVarint(rec, 0) // headers
		body = binary.AppendVarint(body, int64(len(rec)))
		body = append(body, rec...)
	}
	var tail []byte // everything the CRC covers: attributes onward.
	tail = binary.BigEndian.AppendUint16(tail, uint16(codec))
	tail = binary.BigEndian.AppendUint32(tail, uint32(len(recs)-1))
	tail = binary.BigEndian.AppendUint64(tail, uint64(first))
	tail = binary.BigEndian.AppendUint64(tail, uint64(max))
	tail = binary.BigEndian.AppendUint64(tail, ^uint64(0)) // producer id -1
	tail = binary.BigEndian.AppendUint16(tail, 0xffff)     // producer epoch -1
	tail = binary.BigEndian.AppendUint32(tail, 0xffffffff) // base sequence -1
	tail = binary.BigEndian.AppendUint32(tail, uint32(len(recs)))
	switch codec {
	case 1:
		var b bytes.Buffer
		w := gzip.NewWriter(&b)
		w.Write(body)
		w.Close()
		body = b.Bytes()
	case 4:
		var b bytes.Buffer
		w, _ := zstd.NewWriter(&b)
		w.Write(body)
		w.Close()
		body = b.Bytes()
	}
	tail = append(tail, body...)

	var out []byte
	out = binary.BigEndian.AppendUint64(out, 0)                       // base offset
	out = binary.BigEndian.AppendUint32(out, uint32(4+1+4+len(tail))) // length after this field
	out = binary.BigEndian.AppendUint32(out, 0xffffffff)              // partition leader epoch -1
	out = append(out, 2)                                              // magic
	out = binary.BigEndian.AppendUint32(out, crc32.Checksum(tail, crc32.MakeTable(crc32.Castagnoli)))
	return append(out, tail...)
}

// Run executes a scenario against a target and returns its steps.
func Run(s Scenario, target Target) (steps []Step, err error) {
	defer func() {
		if r := recover(); r != nil {
			if f, ok := r.(failure); ok {
				err = fmt.Errorf("%s", string(f))
				return
			}
			panic(r)
		}
	}()
	e, err := NewEnv(target, func(format string, args ...any) { panic(failure(fmt.Sprintf(format, args...))) })
	if err != nil {
		return nil, err
	}
	if sp, ok := target.(interface{ Speed() float64 }); ok && sp.Speed() > 0 {
		e.speed = sp.Speed()
	}
	defer e.Close()
	s.Run(e)
	return e.steps, nil
}

type failure string

// Compare returns the differences of a replay from a recording: steps are
// matched in order and by label, so a script that changed shows as such.
func Compare(want Recording, got []Step) []StepDiff {
	var out []StepDiff
	for i, w := range want.Steps {
		if i >= len(got) {
			out = append(out, StepDiff{Step: w.Label, Diffs: []Diff{{Path: "(step)", Want: w.API, Got: "<missing>"}}})
			continue
		}
		g := got[i]
		var ds []Diff
		if g.Label != w.Label || g.API != w.API || g.Version != w.Version {
			ds = append(ds, Diff{Path: "(step)", Want: fmt.Sprintf("%s %s v%d", w.Label, w.API, w.Version), Got: fmt.Sprintf("%s %s v%d", g.Label, g.API, g.Version)})
		}
		// The request is compared too: a script edited after its recording must be
		// recorded again, not quietly replayed as something else.
		diff("request", w.Request, g.Request, &ds)
		diff("", w.Response, g.Response, &ds)
		if len(ds) > 0 {
			out = append(out, StepDiff{Step: w.Label, Diffs: ds})
		}
	}
	if len(got) > len(want.Steps) {
		out = append(out, StepDiff{Step: got[len(want.Steps)].Label, Diffs: []Diff{{Path: "(step)", Want: "<none>", Got: got[len(want.Steps)].Label}}})
	}
	return out
}

// StepDiff is the differences in one step.
type StepDiff struct {
	Step  string
	Diffs []Diff
}

// Unexplained splits a scenario's differences into those it lists as Known and
// those it does not, and returns the Known entries that matched nothing.
func (s Scenario) Unexplained(diffs []StepDiff) (unknown []string, stale []Known) {
	used := make([]bool, len(s.Known))
	for _, sd := range diffs {
		for _, d := range sd.Diffs {
			matched := false
			for i, k := range s.Known {
				if k.matches(sd.Step) && (k.Path == "" || k.Path == d.Path || strings.HasPrefix(d.Path, k.Path+".") || strings.HasPrefix(d.Path, k.Path+"[")) {
					used[i], matched = true, true
				}
			}
			if !matched {
				unknown = append(unknown, fmt.Sprintf("step %q, %s", sd.Step, d))
			}
		}
	}
	for i, k := range s.Known {
		if !used[i] {
			stale = append(stale, k)
		}
	}
	return unknown, stale
}

// RecordingPath is where a scenario's recording lives under dir.
func RecordingPath(dir, name string) string { return filepath.Join(dir, name+".json") }

// Save writes a recording as indented JSON.
func Save(dir string, r Recording) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(RecordingPath(dir, r.Scenario), append(b, '\n'), 0o644)
}

// Load reads a recording.
func Load(dir, name string) (Recording, error) {
	var r Recording
	b, err := os.ReadFile(RecordingPath(dir, name))
	if err != nil {
		return r, err
	}
	// Numbers stay json.Number, as the normalizer makes them, so big ids and
	// timestamps compare as written.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	return r, dec.Decode(&r)
}

// Pending is a request sent whose answer has not been read: a JoinGroup that
// waits for the rest of the group is sent, others are sent over other
// connections, and the answer is taken when it has come (Finish).
type Pending struct {
	label   string
	conn    string
	req     kmsg.Request
	version int16
	nreq    any
	done    chan struct{}
	resp    kmsg.Response
	err     error
}

// Start sends req over the current connection and returns at once.
func (e *Env) Start(label string, req kmsg.Request, version int16) *Pending {
	e.fail2(label)
	nreq, err := e.norm.normalize(req)
	if err != nil {
		e.fail("%s: %v", label, err)
	}
	p := &Pending{label: label, conn: e.cur, req: req, version: version, nreq: nreq, done: make(chan struct{})}
	w := e.w
	go func() {
		p.resp, p.err = w.roundTrip(req, version)
		close(p.done)
	}()
	return p
}

// Finish waits for the answer to Start and records the step.
func (e *Env) Finish(p *Pending) kmsg.Response {
	<-p.done
	if p.err != nil {
		e.fail("%s: %v", p.label, p.err)
		return nil
	}
	nresp, err := e.norm.normalize(p.resp)
	if err != nil {
		e.fail("%s: %v", p.label, err)
	}
	e.steps = append(e.steps, Step{Label: p.label, Conn: p.conn, API: kmsg.NameForKey(p.req.Key()), Version: p.version, Request: p.nreq, Response: nresp})
	return p.resp
}
