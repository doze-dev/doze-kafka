package main

import "os/exec"

// ownGroup on Windows leaves the default (the process alone is killed).
func ownGroup(cmd *exec.Cmd) {}
