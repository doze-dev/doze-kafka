//go:build faultinject

package log

import "github.com/doze-dev/doze-kafka/internal/faultinject"

func faultAt(point string) error { return faultinject.Hit(point) }

// faultOp is the fault seam's point for an operation at the file seam: "log.fsync"
// for every file fsync, and "log.dirsync", "log.rename", "log.create",
// "log.remove", "log.truncate", "log.mkdir". A write has two points of its own,
// hit where the write is made ("log.write" for a batch appended to a segment,
// "log.file.write" for every other file), so a write is not one here.
func faultOp(op FSOp) error {
	if op == FSWrite {
		return nil
	}
	return faultinject.Hit("log." + string(op))
}
