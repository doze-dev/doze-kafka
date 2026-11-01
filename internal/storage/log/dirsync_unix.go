//go:build !windows

package log

import "os"

// syncDirectory fsyncs a directory.
func syncDirectory(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	if err := d.Sync(); err != nil {
		d.Close()
		return err
	}
	return d.Close()
}

// renameOverOpen: a file that is open can be renamed, and renamed over.
const renameOverOpen = true
