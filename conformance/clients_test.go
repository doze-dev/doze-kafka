package conformance

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Real clients against the recorded broker (claims C1 and R9 of
// conformance/claims/traffic.md): which APIs each client sends over which of its
// connections, and whether it pushes its metrics without being asked to.
//
// A script of raw requests cannot show that: it is a client's behaviour, over time, over
// connections of its own. So a small TCP proxy sits between the client and the broker, and
// the broker advertises the proxy as itself, so that every connection the client makes
// (its bootstrap connection and the ones its metadata sends it to) goes through it. The
// proxy reads each request's header and nothing else.

// connLog is one client connection's requests, in order, runs of the same request
// folded: "PushTelemetry v0 x4".
type connLog struct {
	Conn     int      `json:"conn"`
	Requests []string `json:"requests"`
}

type clientRun struct {
	Client  string `json:"client"`
	Version string `json:"version"`
	// Resource is whether a CLIENT_METRICS resource (every metric, every second) was
	// defined before the client started: without one a broker subscribes a client to
	// nothing.
	Resource    bool      `json:"client_metrics_resource"`
	Connections []connLog `json:"connections"`
	// SubscribedOn and PushedOn are the connections that carried a
	// GetTelemetrySubscriptions and a PushTelemetry.
	SubscribedOn []int `json:"subscribed_on"`
	PushedOn     []int `json:"pushed_on"`
}

type clientRecording struct {
	Kafka string `json:"kafka"`
	// Whether a broker lists GetTelemetrySubscriptions and PushTelemetry (keys 71 and 72)
	// in ApiVersions, as started by the recorder and with the plugin that implements
	// ClientTelemetry (conformance/testdata/telemetry-plugin) added to it.
	AdvertisedByDefault  bool        `json:"advertised_by_default"`
	AdvertisedWithPlugin bool        `json:"advertised_with_plugin"`
	Runs                 []clientRun `json:"runs"`
}

const clientRecordingFile = "client-telemetry"

// tapProxy forwards TCP to a broker and keeps each connection's request headers.
type tapProxy struct {
	ln    net.Listener
	to    string
	mu    sync.Mutex
	conns [][]string
}

func startTapProxy(listen, to string) (*tapProxy, error) {
	ln, err := net.Listen("tcp", listen)
	if err != nil {
		return nil, err
	}
	p := &tapProxy{ln: ln, to: to}
	go p.accept()
	return p, nil
}

func (p *tapProxy) accept() {
	for {
		c, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		idx := len(p.conns)
		p.conns = append(p.conns, nil)
		p.mu.Unlock()
		go p.serve(c, idx)
	}
}

func (p *tapProxy) serve(c net.Conn, idx int) {
	defer c.Close()
	b, err := net.Dial("tcp", p.to)
	if err != nil {
		return
	}
	defer b.Close()
	go func() { io.Copy(c, b); c.Close() }()
	r := bufio.NewReader(c)
	for {
		var size [4]byte
		if _, err := io.ReadFull(r, size[:]); err != nil {
			return
		}
		n := binary.BigEndian.Uint32(size[:])
		if n > 64<<20 {
			return
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(r, body); err != nil {
			return
		}
		if len(body) >= 4 {
			key := int16(binary.BigEndian.Uint16(body))
			ver := int16(binary.BigEndian.Uint16(body[2:]))
			p.mu.Lock()
			p.conns[idx] = append(p.conns[idx], fmt.Sprintf("%s v%d", kmsg.NameForKey(key), ver))
			p.mu.Unlock()
		}
		if _, err := b.Write(append(size[:], body...)); err != nil {
			return
		}
	}
}

func (p *tapProxy) close() { p.ln.Close() }

// logs folds each connection's requests into runs and numbers the connections from 1
// in the order they were opened, dropping the ones that sent nothing.
func (p *tapProxy) logs() []connLog {
	p.mu.Lock()
	defer p.mu.Unlock()
	var out []connLog
	for i, reqs := range p.conns {
		if len(reqs) == 0 {
			continue
		}
		var folded []string
		for j := 0; j < len(reqs); {
			k := j
			for k < len(reqs) && reqs[k] == reqs[j] {
				k++
			}
			if k-j > 1 {
				folded = append(folded, fmt.Sprintf("%s x%d", reqs[j], k-j))
			} else {
				folded = append(folded, reqs[j])
			}
			j = k
		}
		out = append(out, connLog{Conn: i + 1, Requests: folded})
	}
	// Renumber so that a connection that sent nothing leaves no gap.
	for i := range out {
		out[i].Conn = i + 1
	}
	return out
}

func carries(conns []connLog, api string) []int {
	var out []int
	for _, c := range conns {
		for _, r := range c.Requests {
			if strings.HasPrefix(r, api+" ") {
				out = append(out, c.Conn)
				break
			}
		}
	}
	return out
}

// clientSpec is a client to run: how it is started, given the address of the proxy and
// what to feed it; it returns when it has finished.
type clientSpec struct {
	name, version string
	// advertise is the host name the broker advertises so that the client reaches the
	// proxy: the host itself, or the host as a container sees it.
	advertise string
	run       func(addr string, stay time.Duration) (output string, err error)
}

func dockerKafkaProducer(image, hostAddr string, stay time.Duration, clientID string) (string, error) {
	cmd := exec.Command("docker", "run", "--rm", "-i", "--add-host", "host.docker.internal:host-gateway",
		"--entrypoint", "/opt/kafka/bin/kafka-console-producer.sh", image,
		"--bootstrap-server", hostAddr, "--topic", "telemetry-client",
		"--producer-property", "client.id="+clientID)
	var out strings.Builder
	cmd.Stdout, cmd.Stderr = &out, &out
	in, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	if err := cmd.Start(); err != nil {
		return "", err
	}
	io.WriteString(in, "hello\n")
	time.Sleep(stay)
	in.Close()
	err = cmd.Wait()
	return out.String(), err
}

func clientSpecs() []clientSpec {
	specs := []clientSpec{
		{name: "java", version: "4.3.1", advertise: "host.docker.internal", run: func(addr string, stay time.Duration) (string, error) {
			return dockerKafkaProducer("apache/kafka:4.3.1", addr, stay, "conformance-java-431")
		}},
		{name: "java", version: "3.7.0", advertise: "host.docker.internal", run: func(addr string, stay time.Duration) (string, error) {
			return dockerKafkaProducer("apache/kafka:3.7.0", addr, stay, "conformance-java-370")
		}},
		{name: "franz-go", version: "1.22.1", advertise: "127.0.0.1", run: func(addr string, stay time.Duration) (string, error) {
			cl, err := kgo.NewClient(kgo.SeedBrokers(addr), kgo.ClientID("conformance-franz"))
			if err != nil {
				return "", err
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "telemetry-client", Value: []byte("hello")}).FirstErr(); err != nil {
				return "", err
			}
			time.Sleep(stay)
			return "", nil
		}},
	}
	if kcat, err := exec.LookPath("kcat"); err == nil {
		out, _ := exec.Command(kcat, "-V").CombinedOutput()
		version := "unknown"
		if i := strings.Index(string(out), "librdkafka "); i >= 0 {
			version = strings.Fields(string(out)[i+len("librdkafka "):])[0]
		}
		specs = append(specs, clientSpec{name: "librdkafka (kcat)", version: version, advertise: "127.0.0.1", run: func(addr string, stay time.Duration) (string, error) {
			cmd := exec.Command(kcat, "-b", addr, "-P", "-t", "telemetry-client", "-X", "client.id=conformance-kcat")
			var out strings.Builder
			cmd.Stdout, cmd.Stderr = &out, &out
			in, err := cmd.StdinPipe()
			if err != nil {
				return "", err
			}
			if err := cmd.Start(); err != nil {
				return "", err
			}
			io.WriteString(in, "hello\n")
			time.Sleep(stay)
			in.Close()
			err = cmd.Wait()
			return out.String(), err
		}})
	}
	return specs
}

// runClient starts a broker that advertises the proxy, optionally defines a
// CLIENT_METRICS resource, runs the client through the proxy and returns what was seen.
func runClient(t *testing.T, spec clientSpec, resource bool, stay time.Duration, kafka func(extra []string) (Target, error)) clientRun {
	t.Helper()
	proxyPort, err := freePort()
	if err != nil {
		t.Fatal(err)
	}
	advertised := fmt.Sprintf("%s:%d", spec.advertise, proxyPort)
	target, err := kafka([]string{"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://" + advertised})
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	px, err := startTapProxy(fmt.Sprintf("0.0.0.0:%d", proxyPort), target.Addr())
	if err != nil {
		t.Fatal(err)
	}
	defer px.close()

	env, err := NewEnv(target, func(f string, a ...any) { t.Fatalf(f, a...) })
	if err != nil {
		t.Fatal(err)
	}
	// The topic exists before the client starts, so that its first produce does not wait
	// for it.
	env.Do("create", createTopics(false, newTopic("telemetry-client", 1, 1)), 7)
	if resource {
		setClientMetrics(env, "define", "1000")
	}
	env.Close()
	bootstrap := advertised
	if spec.advertise == "127.0.0.1" {
		bootstrap = fmt.Sprintf("127.0.0.1:%d", proxyPort)
	}
	output, err := spec.run(bootstrap, stay)
	if err != nil {
		t.Fatalf("%s %s: %v\n%s", spec.name, spec.version, err, output)
	}
	conns := px.logs()
	if len(conns) == 0 {
		t.Logf("%s %s sent nothing through the proxy; it said:\n%s", spec.name, spec.version, output)
	}
	return clientRun{Client: spec.name, Version: spec.version, Resource: resource, Connections: conns,
		SubscribedOn: carries(conns, "GetTelemetrySubscriptions"), PushedOn: carries(conns, "PushTelemetry")}
}

// TestRecordClientTelemetry is the recorder of the client runs: Docker, and for the
// librdkafka run kcat (2.5 or newer) on the host. Nothing is written without -record.
func TestRecordClientTelemetry(t *testing.T) {
	if !*record {
		t.Skip("recorder: run with -record (go tool task conformance:record); needs Docker")
	}
	// -only names one thing to record. It used to be ignored here, so recording one
	// scenario also ran the four clients for two and a half minutes and rewrote
	// client-telemetry.json, which nobody had asked to change.
	if *only != "" && *only != "client-telemetry" {
		t.Skipf("-only=%s: the client runs are recorded with no -only, or -only=client-telemetry", *only)
	}
	if !DockerAvailable() {
		t.Skip("Docker is not available: nothing was recorded")
	}
	rec := clientRecording{Kafka: KafkaImage}
	jar := buildTelemetryPlugin(t)
	plugin := func(extra []string) (Target, error) {
		extra = append(extra, "KAFKA_METRIC_REPORTERS=dozeconformance.ClientTelemetryPlugin")
		return startKafka(KafkaImage, 0, extra, "", "", jar+":/opt/kafka/libs/doze-conformance-telemetry.jar:ro")
	}
	for _, withPlugin := range []bool{false, true} {
		var target Target
		var err error
		if withPlugin {
			target, err = plugin(nil)
		} else {
			target, err = startKafka(KafkaImage, 0, nil, "", "")
		}
		if err != nil {
			t.Fatal(err)
		}
		env, err := NewEnv(target, func(f string, a ...any) { t.Fatalf(f, a...) })
		if err != nil {
			target.Close()
			t.Fatal(err)
		}
		req := kmsg.NewPtrApiVersionsRequest()
		req.ClientSoftwareName, req.ClientSoftwareVersion = "doze-conformance", "1"
		resp := env.Do("api-versions", req, 3).(*kmsg.ApiVersionsResponse)
		has := 0
		for _, k := range resp.ApiKeys {
			if k.ApiKey == int16(kmsg.GetTelemetrySubscriptions) || k.ApiKey == int16(kmsg.PushTelemetry) {
				has++
			}
		}
		if withPlugin {
			rec.AdvertisedWithPlugin = has == 2
		} else {
			rec.AdvertisedByDefault = has == 2
		}
		env.Close()
		target.Close()
	}
	kafka := plugin
	for _, spec := range clientSpecs() {
		for _, resource := range []bool{false, true} {
			stay := 12 * time.Second
			if !resource {
				stay = 4 * time.Second
			}
			run := runClient(t, spec, resource, stay, kafka)
			t.Logf("%s %s resource=%v: subscribed on %v, pushed on %v", spec.name, spec.version, resource, run.SubscribedOn, run.PushedOn)
			rec.Runs = append(rec.Runs, run)
		}
	}
	// The same clients against doze-kafka: not recorded (a recording is what Apache Kafka
	// answered), only held to the same two facts, so a change that stops a client
	// pushing is caught where the clients are.
	dozeBroker := func(extra []string) (Target, error) {
		host, port, _ := strings.Cut(strings.TrimPrefix(extra[0], "KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://"), ":")
		n, _ := strconv.Atoi(port)
		return startDoze(t.TempDir(), func(c *config.Config) { c.AdvertisedHost, c.AdvertisedPort = host, n })
	}
	for _, spec := range clientSpecs() {
		run := runClient(t, spec, false, 12*time.Second, dozeBroker)
		t.Logf("doze-kafka: %s %s: subscribed on %v, pushed on %v", spec.name, spec.version, run.SubscribedOn, run.PushedOn)
		if len(run.SubscribedOn) == 0 || len(run.PushedOn) == 0 {
			t.Errorf("doze-kafka: %s %s subscribed on %v and pushed on %v: want both", spec.name, spec.version, run.SubscribedOn, run.PushedOn)
		}
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(recordingsDir, clientRecordingFile+".json"), append(b, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}

// The client runs are recorded, against the image the matrix uses, and show what the
// claims rest on: a broker started with nothing configured does not advertise the two
// APIs and one with the plugin does; every client pushes without being asked once it is
// subscribed to something, and pushes nothing when it is not; and franz-go subscribes on
// one connection and pushes on another, so an instance cannot be tied to the connection
// that subscribed.
func TestClientTelemetryRecordingShowsTheDefaultPushAndTheTwoConnections(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(recordingsDir, clientRecordingFile+".json"))
	if err != nil {
		t.Fatalf("no recording: %v (go tool task conformance:record)", err)
	}
	var rec clientRecording
	if err := json.Unmarshal(raw, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Kafka != KafkaImage {
		t.Errorf("recorded against %s, the matrix uses %s", rec.Kafka, KafkaImage)
	}
	if rec.AdvertisedByDefault || !rec.AdvertisedWithPlugin {
		t.Errorf("advertised by default %v, with the plugin %v: want false and true", rec.AdvertisedByDefault, rec.AdvertisedWithPlugin)
	}
	pushed := map[string]bool{}
	for _, r := range rec.Runs {
		who := r.Client + " " + r.Version
		if len(r.SubscribedOn) == 0 {
			t.Errorf("%s never asked for its subscription", who)
		}
		if !r.Resource {
			if len(r.PushedOn) != 0 {
				t.Errorf("%s pushed with nothing configured: %v", who, r.PushedOn)
			}
			continue
		}
		pushed[r.Client] = len(r.PushedOn) > 0
		if len(r.PushedOn) == 0 {
			t.Errorf("%s did not push with a CLIENT_METRICS resource defined", who)
		}
		if r.Client == "franz-go" && (len(r.SubscribedOn) == 0 || len(r.PushedOn) == 0 || r.SubscribedOn[0] == r.PushedOn[0]) {
			t.Errorf("franz-go subscribed on %v and pushed on %v: want two different connections", r.SubscribedOn, r.PushedOn)
		}
	}
	for _, want := range []string{"java", "franz-go"} {
		if !pushed[want] {
			t.Errorf("no run of %s that pushed", want)
		}
	}
}

// buildTelemetryPlugin compiles conformance/testdata/telemetry-plugin against the
// kafka-clients jar of the image, in a JDK container, and returns the jar's path.
func buildTelemetryPlugin(t *testing.T) string {
	t.Helper()
	// Under the working directory, not the system's temporary one: that is where Docker
	// Desktop shares files from.
	work, err := os.MkdirTemp(".", "plugin-build-")
	if err != nil {
		t.Fatal(err)
	}
	if work, err = filepath.Abs(work); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(work) })
	run := func(args ...string) {
		t.Helper()
		if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
			t.Fatalf("docker %s: %v\n%s", strings.Join(args, " "), err, out)
		}
	}
	name := fmt.Sprintf("doze-conformance-libs-%d", time.Now().UnixNano())
	run("create", "--name", name, KafkaImage)
	defer exec.Command("docker", "rm", "-f", name).Run()
	libs := filepath.Join(work, "libs")
	if err := os.MkdirAll(libs, 0o755); err != nil {
		t.Fatal(err)
	}
	run("cp", "-L", name+":/opt/kafka/libs/kafka-clients-4.3.1.jar", libs)
	src, err := filepath.Abs(filepath.Join("testdata", "telemetry-plugin"))
	if err != nil {
		t.Fatal(err)
	}
	run("run", "--rm", "-v", work+":/work", "-v", src+":/src:ro", "eclipse-temurin:21-jdk", "bash", "-c",
		"mkdir -p /work/classes && javac --release 17 -cp '/work/libs/*' -d /work/classes /src/ClientTelemetryPlugin.java && jar cf /work/plugin.jar -C /work/classes .")
	return filepath.Join(work, "plugin.jar")
}
