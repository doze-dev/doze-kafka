package console

import (
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// What a topic takes on disk. Kafka's way to ask is DescribeLogDirs, which this broker
// does not serve; the console runs in the broker's process and is told its data
// directory (BrokerFacts.DataDir), and a partition's log is the directory
// <data dir>/<topic>-<partition> (internal/storage/log, Manager.dirFor). So the size
// is read the way `du` would read it, from the files, and only when the console was
// told where they are: a console for a broker in another process says nothing.

// topicDisk is one topic's files.
type topicDisk struct {
	// Bytes is every file of the topic's partitions: segments, their indexes and the
	// recorded log start. LogBytes is the segments alone, the record batches.
	Bytes, LogBytes int64
	// Bases are the base offsets of each partition's oldest segment (-1: none), by
	// partition: where the bytes on disk begin.
	Bases []int64
}

// topicDisk reads a topic's partition directories. ok is false when the console was
// not told the data directory or a partition directory cannot be read.
func (c *Console) topicDisk(topic string, partitions int) (topicDisk, bool) {
	dir := c.facts.DataDir
	if dir == "" || partitions <= 0 {
		return topicDisk{}, false
	}
	d := topicDisk{Bases: make([]int64, partitions)}
	for p := 0; p < partitions; p++ {
		base := topic + "-" + strconv.Itoa(p)
		if !filepath.IsLocal(base) || strings.ContainsAny(topic, `/\`) {
			return topicDisk{}, false
		}
		entries, err := os.ReadDir(filepath.Join(dir, base))
		if err != nil {
			return topicDisk{}, false
		}
		d.Bases[p] = -1
		for _, e := range entries {
			info, err := e.Info()
			if err != nil || !info.Mode().IsRegular() {
				continue
			}
			d.Bytes += info.Size()
			if name, ok := strings.CutSuffix(e.Name(), ".log"); ok {
				d.LogBytes += info.Size()
				if off, err := strconv.ParseInt(name, 10, 64); err == nil && (d.Bases[p] < 0 || off < d.Bases[p]) {
					d.Bases[p] = off
				}
			}
		}
	}
	return d, true
}

// diskSize writes a size on disk as the boards do: 9.4 KiB, 41 KiB, 5.6 MiB (one
// decimal below ten, none above; binary units, as du -h and the boards use).
func diskSize(n int64) string {
	units := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	f, i := float64(n), 0
	for f >= 1024 && i < len(units)-1 {
		f /= 1024
		i++
	}
	if i == 0 {
		return strconv.FormatInt(n, 10) + " B"
	}
	if f < 10 {
		// Rounded down to a tenth, so 9.96 KiB is "9.9 KiB", never "10.0".
		return strings.TrimSuffix(strconv.FormatFloat(math.Floor(f*10)/10, 'f', 1, 64), ".0") + " " + units[i]
	}
	return strconv.FormatInt(int64(math.Floor(f)), 10) + " " + units[i]
}
