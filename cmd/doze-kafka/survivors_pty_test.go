//go:build unix

package main

import (
	"testing"

	"github.com/creack/pty"
)

// A terminal is a terminal: the other half of TestOnlyATerminalIsATerminal.
func TestAPseudoTerminalIsATerminal(t *testing.T) {
	ptmx, tty, err := pty.Open()
	if err != nil {
		t.Skipf("no pseudo-terminal here: %v", err)
	}
	defer ptmx.Close()
	defer tty.Close()
	if !isTerminal(tty) {
		t.Error("the terminal side of a pseudo-terminal was not taken for a terminal")
	}
}
