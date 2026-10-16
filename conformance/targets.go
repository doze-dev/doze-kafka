package conformance

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/doze-dev/doze-kafka/internal/config"
	"github.com/doze-dev/doze-kafka/internal/embed"
	"github.com/doze-dev/doze-kafka/internal/serverhook"
	"github.com/doze-dev/doze-kafka/server"
)

// --- Apache Kafka in Docker (the recorder) ---

// DockerAvailable reports whether `docker` can start containers here.
func DockerAvailable() bool {
	return exec.Command("docker", "info").Run() == nil
}

type dockerTarget struct {
	name       string
	port       int
	user, pass string // the login the readiness probe needs, when the listener is SASL.
}

// StartKafka runs a single-node KRaft apache/kafka (the compat matrix's image)
// published on a free loopback port, and waits until it answers. The settings
// are Kafka's own defaults except those a one-node cluster needs.
func StartKafka(image string) (Target, error) { return StartKafkaOn(image, 0) }

// StartKafkaOn is StartKafka with the data directory on a tmpfs of diskMiB
// mebibytes when that is above zero, so the disk can be filled.
func StartKafkaOn(image string, diskMiB int) (Target, error) {
	return startKafka(image, diskMiB, nil, "", "")
}

// StartKafkaFor runs the broker a scenario asks for: its disk, its settings, and
// the SASL users it serves.
func StartKafkaFor(s Scenario) (Target, error) {
	env := append([]string(nil), s.KafkaEnv...)
	probeUser, probePass := "", ""
	if len(s.Users) > 0 {
		jaas := fmt.Sprintf(`org.apache.kafka.common.security.plain.PlainLoginModule required username="%s" password="%s"`, s.Admin, s.Users[s.Admin])
		for _, u := range sortedKeys(s.Users) {
			jaas += fmt.Sprintf(` user_%s="%s"`, u, s.Users[u])
		}
		env = append(env,
			"KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT",
			"KAFKA_SASL_ENABLED_MECHANISMS=PLAIN",
			"KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL=PLAIN",
			"KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
			"KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG="+jaas+";",
		)
		probeUser, probePass = s.Admin, s.Users[s.Admin]
	}
	return startKafka(KafkaImage, s.DiskMiB, env, probeUser, probePass)
}

func startKafka(image string, diskMiB int, extra []string, probeUser, probePass string, mounts ...string) (Target, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("doze-conformance-%d", time.Now().UnixNano())
	args := []string{"run", "-d", "--name", name, "-p", fmt.Sprintf("127.0.0.1:%d:9092", port),
		"-e", "KAFKA_NODE_ID=1",
		"-e", "KAFKA_PROCESS_ROLES=broker,controller",
		"-e", "KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093",
		"-e", fmt.Sprintf("KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://127.0.0.1:%d", port),
		"-e", "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
		"-e", "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT",
		"-e", "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
		"-e", "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR=1",
		"-e", "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR=1",
		"-e", "KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0",
	}
	// A later -e of the same name wins, so a scenario's settings replace the defaults.
	for _, kv := range extra {
		args = append(args, "-e", kv)
	}
	for _, m := range mounts {
		args = append(args, "-v", m)
	}
	args = append(args, image)
	if diskMiB > 0 {
		args = append([]string{"run", "-d", "--tmpfs", fmt.Sprintf("/var/lib/kafka/data:size=%dm,mode=1777", diskMiB),
			"-e", "KAFKA_LOG_DIRS=/var/lib/kafka/data"}, args[2:]...)
	}
	if out, err := exec.Command("docker", args...).CombinedOutput(); err != nil {
		return nil, fmt.Errorf("docker run: %v: %s", err, strings.TrimSpace(string(out)))
	}
	t := &dockerTarget{name: name, port: port, user: probeUser, pass: probePass}
	if err := t.wait(); err != nil {
		logs, _ := exec.Command("docker", "logs", "--tail", "30", name).CombinedOutput()
		t.Close()
		return nil, fmt.Errorf("%v\n%s", err, logs)
	}
	return t, nil
}

func (t *dockerTarget) Addr() string { return fmt.Sprintf("127.0.0.1:%d", t.port) }

// wait polls with a real request until the broker answers it: the port opens
// well before KRaft has elected itself.
func (t *dockerTarget) wait() error {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		if ready(t.Addr(), t.user, t.pass) {
			return nil
		}
		time.Sleep(500 * time.Millisecond)
	}
	return fmt.Errorf("kafka in %s did not become ready", t.name)
}

func (t *dockerTarget) Restart() error {
	if out, err := exec.Command("docker", "restart", "-t", "30", t.name).CombinedOutput(); err != nil {
		return fmt.Errorf("docker restart: %v: %s", err, out)
	}
	return t.wait()
}

func (t *dockerTarget) Close() { exec.Command("docker", "rm", "-f", t.name).Run() }

// ready sends one Metadata request for no topics and wants a broker back.
func ready(addr, user, pass string) bool {
	w, err := dialOnce(addr)
	if err != nil {
		return false
	}
	defer w.close()
	if user != "" && w.saslPlain(user, pass) != nil {
		return false
	}
	return probe(w)
}

// --- doze-kafka in this process (the replay) ---

type dozeTarget struct {
	addr   string
	dir    string
	speed  float64
	cancel context.CancelFunc
	done   chan struct{}
	mod    func(*config.Config)
	srv    *server.Server
}

// StartDoze runs doze-kafka in this process over dir, on a free loopback port,
// with the defaults the binary has.
func StartDoze(dir string) (Target, error) { return startDoze(dir, nil) }

// StartDozeFor is StartDoze with the configuration a scenario asks for: its SASL users,
// what Doze and Replay change, and the speed its waits run at.
func StartDozeFor(dir string, s Scenario) (Target, error) {
	t, err := startDozeWith(dir, func(c *config.Config) {
		// What the recorded broker has (KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 in
		// startKafka); a scenario that sets another sets both.
		c.GroupInitialRebalanceDelay = 0
		if len(s.Users) > 0 {
			c.SASLMechanisms = []string{config.SASLPlain}
			c.Credentials = s.Users
		}
		if s.Doze != nil {
			s.Doze(c)
		}
		if s.Replay != nil {
			s.Replay(c)
		}
	})
	if err != nil {
		return nil, err
	}
	t.speed = s.Speed
	return t, nil
}

func startDoze(dir string, mod func(*config.Config)) (Target, error) {
	t, err := startDozeWith(dir, mod)
	if err != nil {
		return nil, err
	}
	return t, nil
}

func startDozeWith(dir string, mod func(*config.Config)) (*dozeTarget, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	t := &dozeTarget{addr: fmt.Sprintf("127.0.0.1:%d", port), dir: dir, mod: mod}
	return t, t.start()
}

// Speed is how much faster than the recording this target's waits run.
func (t *dozeTarget) Speed() float64 { return t.speed }

func (t *dozeTarget) start() error {
	cfg := config.Default()
	cfg.ListenAddr = t.addr
	cfg.DataDir = filepath.Join(t.dir, "data")
	cfg.MetaDB = filepath.Join(t.dir, "meta.db")
	if t.mod != nil {
		t.mod(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		return err
	}
	srv, err := embed.NewFromConfig(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	if _, err := srv.Bind(ctx); err != nil {
		cancel()
		return err
	}
	t.cancel, t.done = cancel, make(chan struct{})
	t.srv = srv
	go func() { srv.Serve(ctx); close(t.done) }()
	return nil
}

func (t *dozeTarget) Addr() string { return t.addr }

// CheckOffsets applies the offsets retention now (Env.CheckOffsets).
func (t *dozeTarget) CheckOffsets() { serverhook.CheckOffsets(t.srv) }

func (t *dozeTarget) Restart() error {
	t.Close()
	return t.start()
}

func (t *dozeTarget) Close() {
	if t.cancel != nil {
		t.cancel()
		<-t.done
		t.cancel = nil
	}
}

func freePort() (int, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port, nil
}
