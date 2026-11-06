package consolefeed

import (
	"github.com/doze-dev/doze-kafka/console"
	"github.com/doze-dev/doze-kafka/internal/broker"
)

type logSource struct{ in broker.LogInspector }

// Logs is the console's view of the log cleaner, or nil when there is no broker to ask.
func Logs(in broker.LogInspector) console.LogSource {
	if in == nil {
		return nil
	}
	return logSource{in}
}

func (l logSource) Cleaner(topic string, partition int32) (console.CleanerState, bool) {
	c, ok := l.in.CleanerInfo(topic, partition)
	return console.CleanerState{Running: c.Running, Ran: c.Ran, Complete: c.Complete, LastAt: c.LastAt, LastRemoved: c.LastRemoved,
		LastMarkers: c.LastMarkers, Passes: c.Passes, TotalRemoved: c.TotalRemoved, CleanedTo: c.CleanedTo}, ok
}
