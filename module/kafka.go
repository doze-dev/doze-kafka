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

	args := []string{"__serve", "--socket", socket, "--datadir", inst.DataDir, "--version", version}
	if advertise != "" {
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

// ConnString implements engine.Driver: KAFKA_BROKERS is a bare host:port with
// no scheme (the endpoints layer rewrites it to the instance's domain:port).
func (Driver) ConnString(_ engine.Instance, ep engine.Endpoint) (string, string) {
	host := ep.TCPAddr
	if host == "" {
		host = "localhost:9092"
	}
	return "KAFKA_BROKERS", host
}
