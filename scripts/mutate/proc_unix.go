//go:build !windows

package main

import (
	"os/exec"
	"syscall"
)

// ownGroup puts the command in a process group of its own, so that a test that
// started a broker (or a mutant that never returns) can be killed with what it
// started.
func ownGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	}
}
