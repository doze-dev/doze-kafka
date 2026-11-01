package console

import "time"

// LogSource is what the console reads of the broker's logs that no Kafka API reports:
// the log cleaner's state on a partition (Tp-21, TOP-28). The embedded module and any
// console for a broker in another process leave it nil, and the page says so.
type LogSource interface {
	// Cleaner is the cleaner's state on a partition, and false for one that is not there.
	Cleaner(topic string, partition int32) (CleanerState, bool)
}

// CleanerState is one partition's cleaner. Totals are since the partition was opened:
// a restart starts them again, and the page says so.
type CleanerState struct {
	Running      bool
	Ran          bool // a pass has cleaned something since the partition was opened.
	Complete     bool
	LastAt       time.Time
	LastRemoved  int64
	LastMarkers  int64
	Passes       int
	TotalRemoved int64
	CleanedTo    int64
}
