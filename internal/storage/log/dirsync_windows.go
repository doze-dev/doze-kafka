//go:build windows

package log

// syncDirectory does nothing on Windows, which has no way to flush a directory:
// FlushFileBuffers on a directory handle is refused. NTFS journals its metadata,
// so a name that was created is not lost in the way an unflushed directory entry
// is on a POSIX file system, and there is nothing further this broker can ask
// for. (docs/storage-and-durability.md says so.)
func syncDirectory(string) error { return nil }

// renameOverOpen: Windows refuses to rename a file that is open, or to rename
// another over it, so the cleaner closes a segment before it swaps its files.
const renameOverOpen = false
