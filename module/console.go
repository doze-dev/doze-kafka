package kafka

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/doze-dev/doze-sdk/engine"
)

// Describe implements engine.Console: what a person wants from a local
// broker is which topics hold how much and how fast they are growing, whether
// a consumer group is keeping up, and a look at what is arriving.
//
// The dash draws this; nothing here knows how. The rate column's Key names the
// running total (the high-water mark), and the host works out the speed.
func (Driver) DescribeConsole() engine.ConsoleSpec {
	return engine.ConsoleSpec{
		Front: "console",
		Panels: []engine.Panel{
			{ID: "topics", Title: "Topics", Kind: "table", Source: "topic", Columns: []engine.Column{
				{Key: "name", Title: "Topic"},
				{Key: "partitions", Title: "Partitions", Align: "right", Role: "count"},
				{Key: "high", Title: "Messages", Align: "right", Role: "count"},
				{Key: "high", Title: "Rate", Align: "right", Role: "rate"},
			}},
			{ID: "groups", Title: "Consumer groups", Kind: "table", Source: "group", Columns: []engine.Column{
				{Key: "name", Title: "Group"},
				{Key: "state", Title: "State", Role: "state"},
				{Key: "members", Title: "Members", Align: "right", Role: "count"},
				{Key: "lag", Title: "Lag", Align: "right", Role: "lag"},
			}},
			{ID: "tail", Title: "Tail", Kind: "stream", Select: "topics", Columns: []engine.Column{
				{Key: "time", Title: "Time", Role: "time"},
				{Key: "partition", Title: "P", Align: "right"},
				{Key: "offset", Title: "Offset", Align: "right"},
				{Key: "key", Title: "Key"},
				{Key: "value", Title: "Value"},
			}},
		},
	}
}

// tailBack is how many messages of history a tail starts with, so opening it
// on a quiet topic still shows what the topic is for.
const tailBack = 10

// maxValue bounds how much of a message is sent to the console.
const maxValue = 240

// Tail implements engine.Console: the messages of the selected topic, the last
// few and then each new one, until the host hangs up.
func (Driver) Tail(ctx context.Context, _ engine.Instance, ep engine.Endpoint, panel, topic string, out chan<- engine.Row) error {
	if panel != "tail" || topic == "" {
		<-ctx.Done() // nothing selected yet: wait to be restarted with a topic
		return nil
	}
	cl, err := unixClient(ep.Backend,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-tailBack)),
	)
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			select {
			case out <- engine.Row{Cells: map[string]string{"value": fe.Err.Error()}, Tone: "bad"}:
			case <-ctx.Done():
				return nil
			}
		}
		var sent bool
		fetches.EachRecord(func(r *kgo.Record) {
			v := string(r.Value)
			if len(v) > maxValue {
				v = v[:maxValue] + "…"
			}
			row := engine.Row{Cells: map[string]string{
				"time":      r.Timestamp.Format(time.TimeOnly),
				"partition": strconv.Itoa(int(r.Partition)),
				"offset":    strconv.FormatInt(r.Offset, 10),
				"key":       string(r.Key),
				"value":     v,
			}}
			select {
			case out <- row:
				sent = true
			case <-ctx.Done():
			}
		})
		_ = sent
	}
	return nil
}

var _ = fmt.Sprintf
