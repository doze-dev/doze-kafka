// Package powercut is a model of what a disk holds after the power goes, for
// tests. SIGKILL cannot stand in for that: it keeps the page cache, so a broker
// that never called fsync passes every kill test there is.
//
// The model watches the operations a storage engine makes (the log's file seam
// reports each one; the metadata store reports its commits) and keeps, beside
// the real files, what a power cut is entitled to leave of them:
//
//   - a file's contents are what they were when its last fsync began. Bytes
//     written since are gone, or zeroed, or partly there, or there in some pages
//     and not in others (the four modes of Cut);
//   - a directory's entries are what they were when the directory was last
//     fsynced. A file created since has no name, however thoroughly the file
//     itself was fsynced; a rename since has not happened; a file removed since
//     is back;
//   - a file whose name is durable and whose contents were never fsynced is
//     there, and empty.
//
// That is deliberately the least a POSIX file system promises. Real file systems
// usually keep more; a broker that is only correct on the ones that do is not
// correct.
//
// Cut writes that state to a new directory, which a test then opens as the
// broker would after the outage. The live directory is not touched, and a test
// that carries on with it is modelling a process that was killed and restarted
// with its page cache intact: the model keeps counting from where it was.
package powercut

import (
	"bytes"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// Mode is what a cut does with bytes that were written and not fsynced.
type Mode string

const (
	Truncate Mode = "truncate" // gone: the file ends where its last fsync left it.
	Zero     Mode = "zero"     // the file has its length, and zeroes where they were.
	Prefix   Mode = "prefix"   // a random part of them, from the start, made it.
	Pages    Mode = "pages"    // each page of them made it or is zeroes, independently.
)

// Modes is every mode, for a test that runs under each.
var Modes = []Mode{Truncate, Zero, Prefix, Pages}

// PageSize is the page of the Pages mode. It is small so that the few hundred
// bytes a test writes span several.
const PageSize = 64

type object struct {
	dir    bool
	synced []byte // a file's contents at its last fsync; nil if it never had one.
	has    bool   // synced is meaningful.
}

type entry struct {
	obj *object
}

// Model is one disk. Its methods are safe for concurrent use.
type Model struct {
	mu   sync.Mutex
	root string

	cur     map[string]*object           // what each live path is now.
	entries map[*object]map[string]entry // a directory's entries at its last fsync.

	pendingFile map[string][]byte           // contents captured as an fsync began.
	pendingDir  map[string]map[string]entry // entries captured as a directory fsync began.

	// Counts of completed flushes, by the file's base name suffix and in total.
	fsyncs    int
	dirsyncs  int
	fsyncBy   map[string]int
	ops       int
	failAt    int // the operation (1-based, counted at its start) to fail; 0: none.
	failErr   error
	failedOp  string
	stopAfter int // the operation after whose completion the state is kept; 0: none.
	snap      *Model

	// A kept copy (CutAfter) is frozen: live holds what each file contained at
	// that instant, since the real files have moved on.
	frozen bool
	live   map[*object][]byte
}

// New returns a model of the directory root, which must exist. root's own
// entries are taken to be on disk as they are now; everything below it is
// subject to the rules above from here on.
func New(root string) *Model {
	m := &Model{
		root:        filepath.Clean(root),
		cur:         map[string]*object{},
		entries:     map[*object]map[string]entry{},
		pendingFile: map[string][]byte{},
		pendingDir:  map[string]map[string]entry{},
		fsyncBy:     map[string]int{},
	}
	m.cur[m.root] = &object{dir: true}
	m.entries[m.cur[m.root]] = m.list(m.root)
	return m
}

// AssumeDurable takes everything under the root, as it is now, to be on disk:
// for a test that starts from a directory some earlier, healthy run left.
func (m *Model) AssumeDurable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = filepath.Walk(m.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		o := m.lookup(path)
		if o == nil {
			return nil
		}
		if info.IsDir() {
			m.entries[o] = m.list(path)
		} else if raw, err := os.ReadFile(path); err == nil {
			o.synced, o.has = raw, true
		}
		return nil
	})
}

// lookup is the object at a live path, made on first sight for a file the model
// did not see created (one made by something that does not report: bbolt).
func (m *Model) lookup(path string) *object {
	if o, ok := m.cur[path]; ok {
		return o
	}
	st, err := os.Stat(path)
	if err != nil {
		return nil
	}
	o := &object{dir: st.IsDir()}
	m.cur[path] = o
	return o
}

func (m *Model) list(dir string) map[string]entry {
	out := map[string]entry{}
	des, err := os.ReadDir(dir)
	if err != nil {
		return out
	}
	for _, de := range des {
		if o := m.lookup(filepath.Join(dir, de.Name())); o != nil {
			out[de.Name()] = entry{obj: o}
		}
	}
	return out
}

func (m *Model) inside(path string) bool {
	return path == m.root || strings.HasPrefix(path, m.root+string(filepath.Separator))
}

// Observe takes one operation from the log's file seam: op is its name ("write",
// "truncate", "create", "rename", "remove", "mkdir", "fsync", "dirsync"), and
// done is false as it starts and true once it has succeeded. The error it
// returns, when not nil, is one the test asked for (FailAt) and fails the
// operation in its place.
func (m *Model) Observe(op, path, to string, done bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	path = filepath.Clean(path)
	if !m.inside(path) {
		return nil
	}
	if !done {
		m.ops++
		if m.failAt != 0 && m.ops == m.failAt {
			m.failedOp = op + " " + m.rel(path)
			return m.failErr
		}
		switch op {
		case "fsync":
			raw, err := os.ReadFile(path)
			if err == nil {
				m.pendingFile[path] = raw
			}
		case "dirsync":
			m.pendingDir[path] = m.list(path)
		}
		return nil
	}
	switch op {
	case "create":
		m.cur[path] = &object{}
	case "mkdir":
		// MkdirAll may have made parents too; they are found on first sight.
		m.cur[path] = &object{dir: true}
	case "rename":
		to = filepath.Clean(to)
		if o := m.lookup(path); o != nil {
			m.cur[to] = o
		}
		delete(m.cur, path)
	case "remove":
		for p := range m.cur {
			if p == path || strings.HasPrefix(p, path+string(filepath.Separator)) {
				delete(m.cur, p)
			}
		}
	case "fsync":
		if raw, ok := m.pendingFile[path]; ok {
			if o := m.lookup(path); o != nil {
				o.synced, o.has = raw, true
			}
			delete(m.pendingFile, path)
		}
		m.fsyncs++
		m.fsyncBy[filepath.Ext(path)]++
	case "dirsync":
		if es, ok := m.pendingDir[path]; ok {
			if o := m.lookup(path); o != nil {
				m.entries[o] = es
			}
			delete(m.pendingDir, path)
		}
		m.dirsyncs++
	}
	if m.stopAfter != 0 && m.ops == m.stopAfter && m.snap == nil {
		m.snap = m.cloneLocked()
	}
	return nil
}

// Synced records that the file at path is on disk exactly as it is now: for a
// store that flushes its own file and does not go through the seam.
func (m *Model) Synced(path string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	path = filepath.Clean(path)
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	if o := m.lookup(path); o != nil {
		o.synced, o.has = raw, true
	}
}

// DirSynced records that the directory's entries are on disk as they are now.
func (m *Model) DirSynced(dir string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir = filepath.Clean(dir)
	if o := m.lookup(dir); o != nil {
		m.entries[o] = m.list(dir)
	}
}

// Fsyncs is how many file fsyncs have completed, and Dirsyncs how many directory
// fsyncs.
func (m *Model) Fsyncs() int   { m.mu.Lock(); defer m.mu.Unlock(); return m.fsyncs }
func (m *Model) Dirsyncs() int { m.mu.Lock(); defer m.mu.Unlock(); return m.dirsyncs }

// FsyncsOf is how many fsyncs of files with this extension (".log", ".index")
// have completed.
func (m *Model) FsyncsOf(ext string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.fsyncBy[ext]
}

// Ops is how many operations have started.
func (m *Model) Ops() int { m.mu.Lock(); defer m.mu.Unlock(); return m.ops }

// FailAt makes the n-th operation from now (1 is the next) fail with err instead
// of running. Zero cancels.
func (m *Model) FailAt(n int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedOp = ""
	if n == 0 {
		m.failAt = 0
		return
	}
	m.failAt, m.failErr = m.ops+n, err
}

// Failed names the operation FailAt failed ("rename t-0/000.log"), or "".
func (m *Model) Failed() string { m.mu.Lock(); defer m.mu.Unlock(); return m.failedOp }

// CutAfter arranges for the state of the disk to be kept as it is right after
// the n-th operation from now completes; Kept returns it (nil if that operation
// has not happened). The power goes at that instant, while the test carries on.
func (m *Model) CutAfter(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.snap = nil
	if n == 0 {
		m.stopAfter = 0
		return
	}
	m.stopAfter = m.ops + n
}

// Kept is the model as it was at the instant CutAfter named.
func (m *Model) Kept() *Model { m.mu.Lock(); defer m.mu.Unlock(); return m.snap }

// cloneLocked copies the model, with the live contents of every file as they are
// now (a later Cut of the copy must not see what is written afterwards).
func (m *Model) cloneLocked() *Model {
	c := &Model{root: m.root, cur: map[string]*object{}, entries: map[*object]map[string]entry{},
		pendingFile: map[string][]byte{}, pendingDir: map[string]map[string]entry{}, fsyncBy: map[string]int{}}
	// Make sure every live path has an object before copying.
	_ = filepath.Walk(m.root, func(path string, _ os.FileInfo, err error) error {
		if err == nil {
			m.lookup(path)
		}
		return nil
	})
	twin := map[*object]*object{}
	copyOf := func(o *object) *object {
		if t, ok := twin[o]; ok {
			return t
		}
		t := &object{dir: o.dir, synced: o.synced, has: o.has}
		twin[o] = t
		return t
	}
	c.live = map[*object][]byte{}
	for p, o := range m.cur {
		t := copyOf(o)
		c.cur[p] = t
		if !o.dir {
			if raw, err := os.ReadFile(p); err == nil {
				c.live[t] = raw
			}
		}
	}
	for o, es := range m.entries {
		t := copyOf(o)
		c.entries[t] = map[string]entry{}
		for name, e := range es {
			c.entries[t][name] = entry{obj: copyOf(e.obj)}
		}
	}
	c.frozen = true
	return c
}

func (m *Model) rel(path string) string {
	if r, err := filepath.Rel(m.root, path); err == nil {
		return r
	}
	return path
}

// Cut writes to dst (which must not exist, or be empty) what the disk holds if
// the power goes now, and returns dst.
func (m *Model) Cut(dst string, mode Mode, rng *rand.Rand) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if rng == nil {
		rng = rand.New(rand.NewSource(1))
	}
	// What each file object holds right now, to derive the unsynced tail from.
	live := m.live
	if !m.frozen {
		live = map[*object][]byte{}
		for p, o := range m.cur {
			if !o.dir {
				if raw, err := os.ReadFile(p); err == nil {
					live[o] = raw
				}
			}
		}
	}
	// Where each directory object lives, to recurse by.
	var walk func(o *object, to string) error
	walk = func(o *object, to string) error {
		if err := os.MkdirAll(to, 0o755); err != nil {
			return err
		}
		es := m.entries[o]
		names := make([]string, 0, len(es))
		for n := range es {
			names = append(names, n)
		}
		sort.Strings(names) // so a seed means the same thing every run.
		for _, name := range names {
			e := es[name]
			if e.obj.dir {
				if err := walk(e.obj, filepath.Join(to, name)); err != nil {
					return err
				}
				continue
			}
			if err := os.WriteFile(filepath.Join(to, name), survive(e.obj, live[e.obj], mode, rng), 0o644); err != nil {
				return err
			}
		}
		return nil
	}
	rootObj := m.cur[m.root]
	if rootObj == nil {
		return "", fmt.Errorf("powercut: no root")
	}
	return dst, walk(rootObj, dst)
}

// survive is what is left of a file: its contents at the last fsync, and of
// whatever was appended since, as much as the mode allows.
func survive(o *object, now []byte, mode Mode, rng *rand.Rand) []byte {
	base := o.synced
	if !o.has {
		base = nil
	}
	// Only bytes appended after the fsynced contents are "an unsynced tail". A
	// file changed in place since its fsync is left as the fsync had it.
	if len(now) <= len(base) || !bytes.Equal(now[:len(base)], base) {
		return bytes.Clone(base)
	}
	tail := now[len(base):]
	out := bytes.Clone(base)
	switch mode {
	case Zero:
		out = append(out, make([]byte, len(tail))...)
	case Prefix:
		out = append(out, tail[:rng.Intn(len(tail)+1)]...)
	case Pages:
		// Pages are aligned to the file, not to the tail.
		kept := make([]byte, len(tail))
		for start := len(base) - len(base)%PageSize; start < len(now); start += PageSize {
			if rng.Intn(2) == 0 {
				continue
			}
			for i := max(start, len(base)); i < min(start+PageSize, len(now)); i++ {
				kept[i-len(base)] = now[i]
			}
		}
		out = append(out, kept...)
	}
	return out
}
