package broker

import "time"

// CleanerInfo is what the log cleaner has done on one partition, for a page that shows
// it. Nothing here is written to disk but where the cleaner has got to: a restart starts
// the totals again (TOP-28).
type CleanerInfo struct {
	Running      bool      // a pass is under way.
	Ran          bool      // a pass has cleaned something since the partition was opened.
	Complete     bool      // the last pass reached the end of the sealed log.
	LastAt       time.Time // when the last pass that cleaned something finished.
	LastRemoved  int64     // records it removed.
	LastMarkers  int64     // transaction markers it removed.
	Passes       int       // passes that cleaned something since the partition was opened.
	TotalRemoved int64     // records removed by every one of them.
	CleanedTo    int64     // the offset below which the log has been cleaned.
}

// LogInspector is what the console reads the log cleaner through.
type LogInspector interface {
	CleanerInfo(topic string, partition int32) (CleanerInfo, bool)
}

// CleanerInfo is the cleaner's state on a partition, and false for one that is not there.
func (b *Broker) CleanerInfo(topic string, partition int32) (CleanerInfo, bool) {
	p, err := b.logs.ExistingPartition(topic, partition)
	if err != nil {
		return CleanerInfo{}, false
	}
	st := p.CleanStats()
	return CleanerInfo{Running: st.Running, Ran: st.Last.Ran, Complete: st.Last.Complete, LastAt: st.Last.At,
		LastRemoved: st.Last.RecordsRemoved, LastMarkers: st.Last.MarkersRemoved, Passes: st.Since.Passes,
		TotalRemoved: st.Since.RecordsRemoved, CleanedTo: st.CleanedTo}, true
}
