//go:build unix

package test

import (
	"os/exec"
	"syscall"
)

// pauseProcess pauses a process using Unix signals
func pauseProcess(cmd *exec.Cmd) error {
	return cmd.Process.Signal(syscall.SIGSTOP)
}

// resumeProcess resumes a process using Unix signals
func resumeProcess(cmd *exec.Cmd) error {
	return cmd.Process.Signal(syscall.SIGCONT)
}
