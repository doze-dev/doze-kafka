//go:build !faultinject

package log

// faultAt is where the fault seam would be. A normal build has none of it.
func faultAt(string) error { return nil }

// faultOp is the fault seam's point for an operation at the file seam.
func faultOp(FSOp) error { return nil }
