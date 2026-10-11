//go:build windows

package broker_test

import "os"

// Windows has no SIGSTOP; a paused node is not available there.
func signalStop(p *os.Process) { panic("node: Pause is not available on Windows") }
func signalCont(p *os.Process) {}
