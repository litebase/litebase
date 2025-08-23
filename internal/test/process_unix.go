//go:build unix

package test

import (
	"os"
	"os/exec"
	"syscall"
)

// setupProcessGroup sets up process group for Unix systems
func setupProcessGroup(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
}

// killProcessGroup kills the entire process group on Unix systems
func killProcessGroup(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}

	// Send SIGINT to the process group (negative PID)
	return syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
}

// killProcess kills a single process
func killProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}

	return cmd.Process.Signal(os.Interrupt)
}
