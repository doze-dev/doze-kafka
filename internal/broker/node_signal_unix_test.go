//go:build !windows

package broker_test

import (
	"os"
	"syscall"
)

func signalStop(p *os.Process) { p.Signal(syscall.SIGSTOP) }
func signalCont(p *os.Process) { p.Signal(syscall.SIGCONT) }
