package kafka

import (
	"context"
	"errors"
	"net"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-sdk/engine"
)

// kerrTopicExists is the Kafka error for an already-existing topic.
var kerrTopicExists = kerr.TopicAlreadyExists

// Converge implements engine.Converger: create the declared topics. It speaks
// the Kafka protocol to the broker's own backend socket, so no new admin
// surface is needed on doze-kafka — CreateTopics is already served.
func (Driver) Converge(ctx context.Context, inst engine.Instance, _ engine.Toolchain, ep engine.Endpoint) error {
	cfg, ok := inst.Spec.(*Config)
	if !ok || cfg == nil || len(cfg.Topics) == 0 {
		return nil
	}
	cl, err := unixClient(ep.Backend)
	if err != nil {
		return err
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	for _, t := range cfg.Topics {
		parts := int32(t.Partitions)
		if parts <= 0 {
			parts = 1
		}
		configs := map[string]*string{}
		for k, v := range t.Settings {
			val := v
			configs[k] = &val
		}
		resp, err := adm.CreateTopics(ctx, parts, -1, configs, t.Name)
		if err != nil {
			return err
		}
		// A topic that already exists is fine (idempotent re-converge); any
		// other per-topic error is fatal.
		for _, ct := range resp {
			if ct.Err != nil && !errors.Is(ct.Err, kerrTopicExists) {
				return ct.Err
			}
		}
	}
	return nil
}

// unixClient dials the broker's backend unix socket regardless of the seed
// address (single broker — redirecting every dial to the socket is safe).
func unixClient(socket string, opts ...kgo.Opt) (*kgo.Client, error) {
	return kgo.NewClient(append([]kgo.Opt{
		kgo.SeedBrokers("kafka.local:9092"),
		kgo.Dialer(func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		}),
	}, opts...)...)
}

// CheckHealth implements engine.HealthChecker: an ApiVersions ping over the
// backend socket.
func (Driver) CheckHealth(ctx context.Context, inst engine.Instance) error {
	socket := socketPath(inst.SocketDir)
	cl, err := unixClient(socket)
	if err != nil {
		return err
	}
	defer cl.Close()
	return cl.Ping(ctx)
}
