package broker

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"

	"github.com/doze-dev/doze-kafka/internal/kmsg"

	"github.com/doze-dev/doze-kafka/internal/protocol"
	"github.com/doze-dev/doze-kafka/internal/storage/meta"
)

// AlterConfigs and IncrementalAlterConfigs change a topic's own configuration
// after it has been created: `kafka-configs --alter`, Terraform's kafka provider,
// Spring's KafkaAdmin. Only topics can be altered. The broker's own settings come
// from its flags and config file and are read-only here, and a request to change
// them is refused with a message saying so rather than accepted and ignored.
//
// A change is validated like a topic config at creation, stored with the topic,
// and put into force at once for the keys the broker acts on (see topiclimits.go).

// errBrokerConfigReadOnly is the answer to an attempt to alter a broker resource.
const errBrokerConfigReadOnly = "broker configuration is read-only here: it is set with doze-kafka's flags or config file"

// listConfigs are the topic configs whose value is a comma-separated list, the
// only ones IncrementalAlterConfigs may APPEND to or SUBTRACT from.
var listConfigs = map[string]bool{
	"cleanup.policy": true,
	"follower.replication.throttled.replicas": true,
	"leader.replication.throttled.replicas":   true,
}

// alterError is a refusal to alter one resource: the code and what to say.
type alterError struct {
	code protocol.ErrorCode
	msg  string
}

func (e *alterError) Error() string { return e.msg }

// alterTopic replaces a topic's own configuration with next (already computed by
// the caller from the request) after validating it, unless validateOnly.
func (b *Broker) alterTopic(name string, next func(current map[string]string) (map[string]string, error), validateOnly bool) *alterError {
	// One alter at a time: two read-modify-write changes to the same topic must
	// not each start from the other's "before".
	b.alterMu.Lock()
	defer b.alterMu.Unlock()

	t, ok, err := b.meta.Topic(name)
	if err != nil {
		return &alterError{protocol.ErrUnknownServer, err.Error()}
	}
	if !ok {
		return &alterError{protocol.ErrUnknownTopicOrPart, fmt.Sprintf("there is no topic %q", name)}
	}
	config, err := next(maps.Clone(t.Config))
	if err != nil {
		var ae *alterError
		if errors.As(err, &ae) {
			return ae
		}
		return &alterError{protocol.ErrInvalidConfig, err.Error()}
	}
	if err := validateTopicConfig(config); err != nil {
		return &alterError{protocol.ErrInvalidConfig, err.Error()}
	}
	if validateOnly {
		return nil
	}
	updated, err := b.meta.SetTopicConfig(name, config)
	if err != nil {
		if errors.Is(err, meta.ErrNoSuchTopic) { // deleted since the lookup above.
			return &alterError{protocol.ErrUnknownTopicOrPart, fmt.Sprintf("there is no topic %q", name)}
		}
		b.log.Warn("alter configs: could not store the topic's configuration", "topic", name, "err", err)
		return &alterError{protocol.ErrUnknownServer, err.Error()}
	}
	b.applyTopicLimits(updated.Name, updated.Config)
	return nil
}

// handleAlterConfigs is the original, all-or-nothing form: the configs named are
// the topic's whole configuration from now on, and anything it had that is not
// named goes back to the broker's default.
func (b *Broker) handleAlterConfigs(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	ar := kreq.(*kmsg.AlterConfigsRequest)
	resp := kmsg.NewPtrAlterConfigsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, res := range ar.Resources {
		rr := kmsg.NewAlterConfigsResponseResource()
		rr.ResourceType, rr.ResourceName = res.ResourceType, res.ResourceName
		var ae *alterError
		switch res.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			ae = b.alterTopic(res.ResourceName, func(map[string]string) (map[string]string, error) {
				next := map[string]string{}
				for _, c := range res.Configs {
					if _, dup := next[c.Name]; dup {
						return nil, &alterError{protocol.ErrInvalidRequest, fmt.Sprintf("config %q is named more than once", c.Name)}
					}
					if c.Value == nil {
						return nil, fmt.Errorf("config %q has no value (leave a config out to return it to the default)", c.Name)
					}
					next[c.Name] = *c.Value
				}
				return next, nil
			}, ar.ValidateOnly)
		case kmsg.ConfigResourceTypeBroker:
			ae = &alterError{protocol.ErrInvalidRequest, errBrokerConfigReadOnly}
		default:
			ae = &alterError{protocol.ErrInvalidRequest, "unsupported config resource type"}
		}
		if ae != nil {
			rr.ErrorCode, rr.ErrorMessage = int16(ae.code), &ae.msg
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// handleIncrementalAlterConfigs changes only the configs it names: SET one,
// DELETE one (back to the broker's default), or APPEND to / SUBTRACT from one
// whose value is a list.
func (b *Broker) handleIncrementalAlterConfigs(ctx context.Context, req *protocol.Request) ([]byte, error) {
	kreq, err := protocol.DecodeRequest(req.Header, req.Body)
	if err != nil {
		return nil, err
	}
	ar := kreq.(*kmsg.IncrementalAlterConfigsRequest)
	resp := kmsg.NewPtrIncrementalAlterConfigsResponse()
	resp.SetVersion(req.Header.APIVersion)

	for _, res := range ar.Resources {
		rr := kmsg.NewIncrementalAlterConfigsResponseResource()
		rr.ResourceType, rr.ResourceName = res.ResourceType, res.ResourceName
		var ae *alterError
		switch res.ResourceType {
		case kmsg.ConfigResourceTypeTopic:
			ae = b.alterTopic(res.ResourceName, func(current map[string]string) (map[string]string, error) {
				if current == nil {
					current = map[string]string{}
				}
				seen := map[string]bool{}
				for _, c := range res.Configs {
					if seen[c.Name] {
						return nil, &alterError{protocol.ErrInvalidRequest, fmt.Sprintf("config %q is named more than once", c.Name)}
					}
					seen[c.Name] = true
					if err := applyIncremental(current, c); err != nil {
						return nil, err
					}
				}
				return current, nil
			}, ar.ValidateOnly)
		case kmsg.ConfigResourceTypeBroker:
			ae = &alterError{protocol.ErrInvalidRequest, errBrokerConfigReadOnly}
		default:
			ae = &alterError{protocol.ErrInvalidRequest, "unsupported config resource type"}
		}
		if ae != nil {
			rr.ErrorCode, rr.ErrorMessage = int16(ae.code), &ae.msg
		}
		resp.Resources = append(resp.Resources, rr)
	}
	return protocol.EncodeResponse(req.Header, resp), nil
}

// applyIncremental applies one incremental operation to a topic's configuration.
func applyIncremental(config map[string]string, c kmsg.IncrementalAlterConfigsRequestResourceConfig) error {
	value := ""
	if c.Value != nil {
		value = *c.Value
	}
	switch c.Op {
	case kmsg.IncrementalAlterConfigOpSet:
		if c.Value == nil {
			return fmt.Errorf("SET %q needs a value", c.Name)
		}
		config[c.Name] = value
	case kmsg.IncrementalAlterConfigOpDelete:
		delete(config, c.Name)
	case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
		if !listConfigs[c.Name] {
			return &alterError{protocol.ErrInvalidRequest, fmt.Sprintf("config %q is not a list, so there is nothing to append to or subtract from", c.Name)}
		}
		// A list not set on the topic starts from its default, as Kafka does:
		// appending "compact" to an unset cleanup.policy gives "delete,compact".
		current, set := config[c.Name]
		if !set && c.Name == "cleanup.policy" {
			current = "delete"
		}
		var items []string
		for _, it := range strings.Split(current, ",") {
			if it = strings.TrimSpace(it); it != "" {
				items = append(items, it)
			}
		}
		for _, v := range strings.Split(value, ",") {
			if v = strings.TrimSpace(v); v == "" {
				continue
			}
			at := -1
			for i, it := range items {
				if it == v {
					at = i
				}
			}
			switch {
			case c.Op == kmsg.IncrementalAlterConfigOpAppend && at < 0:
				items = append(items, v)
			case c.Op == kmsg.IncrementalAlterConfigOpSubtract && at >= 0:
				items = append(items[:at], items[at+1:]...)
			}
		}
		config[c.Name] = strings.Join(items, ",")
	default:
		return &alterError{protocol.ErrInvalidRequest, fmt.Sprintf("unknown operation %d on config %q", c.Op, c.Name)}
	}
	return nil
}
