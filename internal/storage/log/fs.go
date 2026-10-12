package log

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
)

// The file seam. Every operation of this package that changes what a power cut
// would leave on the disk goes through one of the functions below: a write, a
// truncation, the creation, renaming or removal of a file, a new directory, and
// the two flushes that make any of those durable (fsync of a file, fsync of a
// directory). Nothing else in the package calls os.Rename, f.Sync and friends
// directly, which is what lets a test
//
//   - count the real fsyncs behind an acknowledgement, rather than the calls that
//     were supposed to lead to one;
//   - fail any single operation and see what the log is like afterwards;
//   - rebuild what the disk would hold if the power went at that moment (only
//     what was flushed, with names that were never flushed gone).
//
// A release build pays one atomic load per operation for it.

// FSOp names an operation at the file seam.
type FSOp string

const (
	FSWrite    FSOp = "write"    // bytes written to a file.
	FSTruncate FSOp = "truncate" // a file cut to a length.
	FSCreate   FSOp = "create"   // a file that was not there is created.
	FSRename   FSOp = "rename"   // Path is renamed to To.
	FSRemove   FSOp = "remove"   // a file is removed.
	FSMkdir    FSOp = "mkdir"    // a directory that was not there is created.
	FSFsync    FSOp = "fsync"    // a file's contents are flushed.
	FSDirSync  FSOp = "dirsync"  // a directory's entries are flushed.
)

// FSEvent is one operation at the file seam. The hook sees each operation twice:
// before it runs (Done false; an error returned then fails the operation in its
// place) and after it has succeeded (Done true; the returned error is ignored).
type FSEvent struct {
	Op   FSOp
	Path string
	To   string // the new name, for a rename.
	Done bool
}

var fsHook atomic.Pointer[func(FSEvent) error]

// SetFSHookForTest installs a function that sees every operation at the file seam,
// and returns a func that removes it. It exists for tests, here and in other
// packages (a power-cut model has to see every flush); nothing in the broker
// calls it.
func SetFSHookForTest(fn func(FSEvent) error) (restore func()) {
	old := fsHook.Swap(&fn)
	return func() { fsHook.Store(old) }
}

// fsBefore runs the hook and the fault seam's point for an operation that is
// about to happen.
func fsBefore(op FSOp, path, to string) error {
	if h := fsHook.Load(); h != nil && *h != nil {
		if err := (*h)(FSEvent{Op: op, Path: path, To: to}); err != nil {
			return err
		}
	}
	return faultOp(op)
}

func fsDone(op FSOp, path, to string) {
	if h := fsHook.Load(); h != nil && *h != nil {
		_ = (*h)(FSEvent{Op: op, Path: path, To: to, Done: true})
	}
}

// fsyncFile is the fsync syscall. A variable so a test can make one fail or stall,
// which no real disk will do on request.
var fsyncFile = func(f *os.File) error { return f.Sync() }

// SetFsyncForTest replaces the function every file fsync of this package goes
// through, and returns a func that puts the real one back. Nothing in the broker
// calls it.
func SetFsyncForTest(fn func(f *os.File) error) (restore func()) {
	old := fsyncFile
	fsyncFile = fn
	return func() { fsyncFile = old }
}

// fsSync flushes a file's contents to stable storage.
func fsSync(f *os.File) error {
	if err := fsBefore(FSFsync, f.Name(), ""); err != nil {
		return err
	}
	if err := fsyncFile(f); err != nil {
		return err
	}
	fsDone(FSFsync, f.Name(), "")
	return nil
}

// fsSyncDir flushes a directory's entries to stable storage: until it has, a file
// created, renamed or removed in it is so only in memory, and a power cut can
// take a fully fsynced file away with its name. On a platform that cannot flush
// a directory (see dirsync_windows.go) it does nothing and says so by returning
// nil; everywhere else a failure is a failure of the storage, like any other
// fsync, and is returned.
func fsSyncDir(dir string) error {
	if err := fsBefore(FSDirSync, dir, ""); err != nil {
		return err
	}
	if err := syncDirectory(dir); err != nil {
		return err
	}
	fsDone(FSDirSync, dir, "")
	return nil
}

// fsWriteAt writes b at off.
func fsWriteAt(f *os.File, b []byte, off int64) error {
	if err := fsBefore(FSWrite, f.Name(), ""); err != nil {
		return err
	}
	if _, err := f.WriteAt(b, off); err != nil {
		return err
	}
	fsDone(FSWrite, f.Name(), "")
	return nil
}

// fsWriter is an io.Writer over a file whose writes go through the seam.
type fsWriter struct{ f *os.File }

func (w fsWriter) Write(b []byte) (int, error) {
	if err := fsBefore(FSWrite, w.f.Name(), ""); err != nil {
		return 0, err
	}
	n, err := w.f.Write(b)
	if err == nil {
		fsDone(FSWrite, w.f.Name(), "")
	}
	return n, err
}

func fsTruncate(f *os.File, size int64) error {
	if err := fsBefore(FSTruncate, f.Name(), ""); err != nil {
		return err
	}
	if err := f.Truncate(size); err != nil {
		return err
	}
	fsDone(FSTruncate, f.Name(), "")
	return nil
}

// fsOpenFile is os.OpenFile. created reports that the call made the file: its
// name is then only in memory until its directory is flushed.
func fsOpenFile(path string, flag int, perm os.FileMode) (f *os.File, created bool, err error) {
	if flag&os.O_CREATE != 0 {
		if _, statErr := os.Lstat(path); errors.Is(statErr, fs.ErrNotExist) {
			created = true
			if err := fsBefore(FSCreate, path, ""); err != nil {
				return nil, false, err
			}
		}
	}
	f, err = os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, false, err
	}
	if created {
		fsDone(FSCreate, path, "")
	}
	return f, created, nil
}

func fsRename(from, to string) error {
	if err := fsBefore(FSRename, from, to); err != nil {
		return err
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	fsDone(FSRename, from, to)
	return nil
}

// fsRemove removes a file. One that is not there is not an error.
func fsRemove(path string) error {
	if err := fsBefore(FSRemove, path, ""); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err
	}
	fsDone(FSRemove, path, "")
	return nil
}

// fsMkdir creates one directory (its parent must exist) and reports whether it
// made it.
func fsMkdir(dir string) (created bool, err error) {
	if st, err := os.Stat(dir); err == nil && st.IsDir() {
		return false, nil
	}
	if err := fsBefore(FSMkdir, dir, ""); err != nil {
		return false, err
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		// Made by someone else in the meantime is fine; something that is not a
		// directory in its place is not.
		if st, serr := os.Stat(dir); errors.Is(err, fs.ErrExist) && serr == nil && st.IsDir() {
			return false, nil
		}
		return false, err
	}
	fsDone(FSMkdir, dir, "")
	return true, nil
}

// writeFileDurably replaces path with data so that a crash at any point leaves
// either the old contents or the new, never a torn or empty file: the bytes go to
// a temporary file that is flushed, the temporary file is renamed over path, and
// the directory is flushed so the rename is on disk too.
func writeFileDurably(path string, data []byte) error {
	tmp := path + ".tmp"
	f, _, err := fsOpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	fail := func(err error) error {
		f.Close()
		_ = fsRemove(tmp)
		return err
	}
	if err := fsTruncate(f, 0); err != nil { // O_TRUNC, said at the seam.
		return fail(err)
	}
	if _, err := (fsWriter{f}).Write(data); err != nil {
		return fail(err)
	}
	if err := fsSync(f); err != nil {
		return fail(err)
	}
	if err := f.Close(); err != nil {
		_ = fsRemove(tmp)
		return err
	}
	if err := fsRename(tmp, path); err != nil {
		_ = fsRemove(tmp)
		return err
	}
	return fsSyncDir(filepath.Dir(path))
}
