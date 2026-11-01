//go:build !faultinject

package meta

// faultAt is where the fault seam would be. A normal build has none of it.
func faultAt(string) error { return nil }
