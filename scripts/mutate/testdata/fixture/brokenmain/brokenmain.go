// Package brokenmain has a test binary that exits before any test starts, as a
// malformed flag makes one do.
package brokenmain

func Two() int { return 1 + 1 }
