package log

import (
	"os"
	"path/filepath"
	"strings"
)

// Two small files in the data directory carry, from one run of the broker to the
// next, what cannot be read off the partitions themselves.
//
// "clean-shutdown" is written as the last act of a stop that flushed and closed
// every partition it had open, and removed (durably) as the first act of the next
// start. Finding it means the previous process flushed what it wrote, so what is
// read back from a partition is on disk and needs no fsync before it is
// acknowledged again. Not finding it means the previous process was killed, or
// lost power, or failed a flush on its way out: every partition is then opened as
// "not vouched for" (durability.go). Apache Kafka keeps the same marker
// (.kafka_cleanshutdown) for the same decision.
//
// A stop can only vouch for what it flushed. Partitions open lazily, so after an
// unclean stop the next run may never touch some of them, and its own clean stop
// says nothing about those: the file lists them, and they stay not vouched for
// until a run that opens them stops cleanly.
//
// "unvalidated" lists the partitions whose sealed segments are not known to be
// whole and on disk. A partition created by this version never is one: a segment
// is flushed before its successor exists. A data directory from before that held
// has no such file; every partition in it is listed on the first start, and each
// leaves the list once it has been opened (which checks every batch of every
// segment) and flushed. It is the price, paid once per partition, of knowing.
const (
	cleanStopFile   = "clean-shutdown"
	unvalidatedFile = "unvalidated"

	cleanStopHeader   = "doze-kafka clean shutdown 1"
	unvalidatedHeader = "doze-kafka unvalidated partitions 1"
)

// readNameList reads a file of a header line and then one name per line. ok is
// false if the file is not there or is not such a file (a name that reached the
// disk before its bytes did reads back empty).
func readNameList(path, header string) (names map[string]struct{}, ok bool) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, false
	}
	text := string(raw)
	if !strings.HasSuffix(text, "\n") { // a torn last line is not a name.
		return nil, false
	}
	lines := strings.Split(strings.TrimSuffix(text, "\n"), "\n")
	if lines[0] != header {
		return nil, false
	}
	names = make(map[string]struct{}, len(lines)-1)
	for _, l := range lines[1:] {
		if l != "" {
			names[l] = struct{}{}
		}
	}
	return names, true
}

func writeNameList(path, header string, names []string) error {
	var b strings.Builder
	b.WriteString(header)
	b.WriteByte('\n')
	for _, n := range names {
		b.WriteString(n)
		b.WriteByte('\n')
	}
	return writeFileDurably(path, []byte(b.String()))
}

// partitionDirs lists the partition directories in the data directory.
func partitionDirs(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if _, _, ok := splitPartitionDir(e.Name()); ok && e.IsDir() {
			out = append(out, e.Name())
		}
	}
	return out, nil
}

// loadStopState reads the two files when the manager opens, and takes the clean
// stop's marker away before anything can be written.
func (m *Manager) loadStopState() error {
	dirs, err := partitionDirs(m.dir)
	if err != nil {
		return err
	}
	onDisk := make(map[string]struct{}, len(dirs))
	for _, d := range dirs {
		onDisk[d] = struct{}{}
	}

	// Unvalidated: the file's list, or, for a data directory that has none yet,
	// every partition there is.
	listed, ok := readNameList(filepath.Join(m.dir, unvalidatedFile), unvalidatedHeader)
	if !ok {
		if err := writeNameList(filepath.Join(m.dir, unvalidatedFile), unvalidatedHeader, dirs); err != nil {
			return err
		}
		listed = onDisk
	}
	m.unvalidated = map[string]struct{}{}
	for name := range listed {
		if _, there := onDisk[name]; there {
			m.unvalidated[name] = struct{}{}
		}
	}

	// The last stop.
	mark := filepath.Join(m.dir, cleanStopFile)
	notCovered, clean := readNameList(mark, cleanStopHeader)
	m.cleanStart = clean
	if clean {
		m.unvouched = map[string]struct{}{}
		for name := range notCovered {
			if _, there := onDisk[name]; there {
				m.unvouched[name] = struct{}{}
			}
		}
	} else if len(dirs) > 0 {
		m.log.Info("the last stop was not clean (or did not finish): partitions are checked and flushed again as they are first used", "partitions", len(dirs))
	}
	// Gone before the first write, and gone for good: a marker that came back
	// after a power cut would vouch for a log this run was in the middle of
	// writing. The same flush makes the data directory's own entries (partition
	// directories a killed process had created) ones this process has flushed.
	if err := fsRemove(mark); err != nil {
		return err
	}
	return fsSyncDir(m.dir)
}

// vouchedLocked reports whether the last stop was clean and covered this
// partition directory. The caller holds mu.
func (m *Manager) vouchedLocked(name string) bool {
	if !m.cleanStart {
		return false
	}
	_, not := m.unvouched[name]
	return !not
}

// saveUnvalidated drops from the unvalidated list every open partition that has
// since been checked and flushed, and rewrites the file if the list changed.
func (m *Manager) saveUnvalidated() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.saveUnvalidatedLocked()
}

func (m *Manager) saveUnvalidatedLocked() error {
	if len(m.unvalidated) == 0 {
		return nil
	}
	changed := false
	for tp, p := range m.partitions {
		name := filepath.Base(p.dir)
		if _, listed := m.unvalidated[name]; listed && p.validated() {
			delete(m.unvalidated, name)
			changed = true
		}
		_ = tp
	}
	if !changed {
		return nil
	}
	names := make([]string, 0, len(m.unvalidated))
	for n := range m.unvalidated {
		names = append(names, n)
	}
	return writeNameList(filepath.Join(m.dir, unvalidatedFile), unvalidatedHeader, names)
}

// writeCleanStop records a stop that flushed and closed every partition in
// closed (directory names), listing the partitions it cannot vouch for: those
// the last stop did not cover and this run never opened. The caller holds mu.
func (m *Manager) writeCleanStop(closed map[string]struct{}) error {
	dirs, err := partitionDirs(m.dir)
	if err != nil {
		return err
	}
	var notCovered []string
	for _, name := range dirs {
		if _, ok := closed[name]; ok {
			continue
		}
		if m.vouchedLocked(name) {
			continue
		}
		// Never opened by this run and not covered by the last stop. A directory
		// with no segment in it holds nothing to vouch for.
		if bases, err := listSegmentBases(filepath.Join(m.dir, name)); err == nil && len(bases) == 0 {
			continue
		}
		notCovered = append(notCovered, name)
	}
	return writeNameList(filepath.Join(m.dir, cleanStopFile), cleanStopHeader, notCovered)
}
