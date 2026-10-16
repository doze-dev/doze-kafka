//go:build !windows

package meta

import "os"

// syncDirectory fsyncs a directory, so that a file created in it has a name
// that survives a power cut.
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
