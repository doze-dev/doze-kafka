// Package kafka implements the doze engine.Driver for a single-node,
// JVM-free Kafka-protocol broker, backed by the doze-kafka library embedded in
// the plugin binary. Unlike the AWS-builtin modules it is a plain per-instance
// TCP engine (its own domain/endpoint, not the shared :80 ingress), and unlike
// valkey/postgres it downloads no upstream binary — doze-kafka is the engine,
// and the declared `version` selects the advertised Kafka protocol profile
// (1–4), not a fetched artifact.
package kafka

import (
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"

	"github.com/doze-dev/doze-sdk/engine"
)

const socketName = "kafka.sock"

// Driver is the kafka engine driver.
type Driver struct{}

// New returns the driver.
func New() Driver { return Driver{} }

// Type implements engine.Driver.
func (Driver) Type() string { return "kafka" }

// Resolve implements engine.Driver: doze-kafka is embedded, so there is nothing
// to fetch — the version is an advertised protocol profile, returned synthetically.
func (Driver) Resolve(_ context.Context, spec engine.VersionSpec, _ engine.Platform, _ engine.Locker, _ engine.Fetcher) (engine.Toolchain, error) {
	full := spec.String()
	if full == "" {
		full = "4"
	}
	return engine.Toolchain{Engine: "kafka", Full: full}, nil
}

// Provision implements engine.Driver: just the data directory.
func (Driver) Provision(_ context.Context, inst engine.Instance, _ engine.Toolchain) error {
	return os.MkdirAll(inst.DataDir, 0o700)
}

// Provisioned implements engine.Driver.
func (Driver) Provisioned(dataDir string) bool {
	fi, err := os.Stat(dataDir)
	return err == nil && fi.IsDir()
}

// Plan implements engine.Spawner: re-exec this plugin binary as
// `kafka-plugin __serve --socket S --datadir D --version N --advertise H:P`,
// which runs the embedded broker on the unix socket. The advertised address is
// the client-facing endpoint (from Instance.Endpoint.TCPAddr) so Kafka clients,
// which bootstrap then dial the advertised host, reach the doze proxy.
func (Driver) Plan(_ context.Context, inst engine.Instance, tc engine.Toolchain) (engine.SpawnPlan, error) {
	if err := os.MkdirAll(inst.SocketDir, 0o700); err != nil {
		return engine.SpawnPlan{}, fmt.Errorf("creating socket dir: %w", err)
	}
	socket := socketPath(inst.SocketDir)
	_ = os.Remove(socket)
	self, err := os.Executable()
	if err != nil {
		return engine.SpawnPlan{}, fmt.Errorf("locating broker binary: %w", err)
	}
	version := tc.Full
	if version == "" {
		version = "4"
	}
	advertise := inst.Endpoint.TCPAddr // client-facing host:port, populated by core
	if a, ok := inst.Grant.Addr(engine.MainListener); ok {
		advertise = a
	}

	args := []string{"__serve", "--socket", socket, "--datadir", inst.DataDir, "--version", version}
	if c, ok := inst.Grant.Backend("console"); ok {
		// doze holds the console's and the registry's ports and forwards each
		// to a socket here (engine.Networked), so opening either wakes the broker.
		args = append(args, "--advertise", advertise, "--console-socket", c)
		if r, ok := inst.Grant.Backend("registry"); ok {
			args = append(args, "--registry-socket", r)
			if u := inst.Grant.Listeners["registry"].URL; u != "" {
				args = append(args, "--registry-url", u)
			}
		}
	} else if advertise != "" {
		// An older doze gives no grant: the console binds its own port, one up.
		args = append(args, "--advertise", advertise)
		// The web console rides the same per-instance IP one port up (the
		// broker's own port speaks the Kafka wire protocol, which can't share
		// with HTTP) — http://<host>:<port+1> is the convention the dash links.
		if host, port, err := net.SplitHostPort(advertise); err == nil {
			if p, perr := strconv.Atoi(port); perr == nil {
				args = append(args, "--console-addr", net.JoinHostPort(host, strconv.Itoa(p+1)))
			}
		}
	}
	if cfg, ok := inst.Spec.(*Config); ok && cfg != nil {
		if cfg.AutoCreateTopics != nil {
			args = append(args, "--auto-create", strconv.FormatBool(*cfg.AutoCreateTopics))
		}
		if cfg.DefaultPartitions > 0 {
			args = append(args, "--default-partitions", strconv.Itoa(cfg.DefaultPartitions))
		}
		if cfg.RetentionMs > 0 {
			args = append(args, "--retention-ms", strconv.FormatInt(cfg.RetentionMs, 10))
		}
		if cfg.RetentionBytes > 0 {
			args = append(args, "--retention-bytes", strconv.FormatInt(cfg.RetentionBytes, 10))
		}
	}
	return engine.SpawnPlan{Specs: []engine.SpawnSpec{{
		Name:  inst.Name,
		Bin:   self,
		Args:  args,
		Env:   os.Environ(),
		Ready: &engine.Ready{Kind: "socket", Target: socket},
	}}}, nil
}

// BackendSocket implements engine.Driver: the doze proxy splices onto this unix
// socket.
func (Driver) BackendSocket(socketDir string, _ int) string { return socketPath(socketDir) }

func socketPath(socketDir string) string { return filepath.Join(socketDir, socketName) }

// Network implements engine.Networked: the broker on the declared port, the
// web console on the instance's name (http://events.<stack>.doze), and the
// schema registry on its child (http://registry.events.<stack>.doze), Confluent's
// port. Kafka's wire protocol carries no host, so the broker is reached at its
// port; the two HTTP listeners also without one.
func (Driver) Network(engine.Instance) engine.Network {
	return engine.Network{Listeners: []engine.Listener{
		{Name: engine.MainListener, Protocol: engine.ProtocolTCP},
		{Name: "console", Port: consolePort, Protocol: engine.ProtocolHTTP, Routes: []string{engine.RouteSelf}},
		{Name: "registry", Port: registryPort, Protocol: engine.ProtocolHTTP, Routes: []string{"registry"}},
	}}
}

// The console's and the schema registry's ports on the instance's address.
const (
	consolePort  = 9093
	registryPort = 8081 // Confluent's own
)

// ConnString implements engine.Driver: KAFKA_BROKERS is a bare host:port with
// no scheme: the broker's granted address, or the endpoint an older doze gives.
func (Driver) ConnString(inst engine.Instance, ep engine.Endpoint) (string, string) {
	if a, ok := inst.Grant.Addr(engine.MainListener); ok {
		return "KAFKA_BROKERS", a
	}
	host := ep.TCPAddr
	if host == "" {
		host = "localhost:9092"
	}
	return "KAFKA_BROKERS", host
}
