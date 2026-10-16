//go:build windows

package meta

// syncDirectory does nothing on Windows, which cannot flush a directory (see the
// log package's dirsync_windows.go).
func syncDirectory(string) error { return nil }
