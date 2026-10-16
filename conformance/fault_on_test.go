//go:build faultinject

package conformance

import (
	"os"
	"syscall"
	"testing"

	"github.com/doze-dev/doze-kafka/internal/faultinject"
)

// With the fault seam compiled in, a scenario that fills the data volume
// (Scenario.DiskMiB) can be replayed against doze-kafka: every write the broker
// makes from then on fails as a full volume fails it.
func init() {
	armDiskFull = func() {
		enospc := faultinject.Fault{Err: &os.PathError{Op: "write", Path: "/data", Err: syscall.ENOSPC}}
		for _, p := range []string{faultinject.LogWrite, faultinject.LogFileWrite, faultinject.LogCreate, faultinject.LogMkdir, faultinject.MetaWrite} {
			faultinject.Set(p, enospc)
		}
	}
}

// TestReplayFullDisk is TestReplay for the scenarios that fill the volume: go
// test -tags faultinject ./conformance -run TestReplayFullDisk.
func TestReplayFullDisk(t *testing.T) {
	if testing.Short() {
		t.Skip("boots a broker per scenario")
	}
	for _, s := range Scenarios {
		if s.DiskMiB == 0 || s.NoReplay != "" || (*only != "" && s.Name != *only) {
			continue
		}
		t.Run(s.Name, func(t *testing.T) {
			t.Cleanup(faultinject.Reset)
			replayOne(t, s)
		})
	}
}
