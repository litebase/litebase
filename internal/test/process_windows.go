//go:build windows

package test

import (
	"os"
	"os/exec"
)

// setupProcessGroup sets up process group for Windows systems
func setupProcessGroup(cmd *exec.Cmd) {
	// On Windows, we don't need to set up process groups the same way
	// The default behavior should work for our use case
}

// killProcessGroup kills the entire process group on Windows systems
func killProcessGroup(cmd *exec.Cmd) error {
	// On Windows, we'll try to kill the process directly first
	// since process group handling is different
	if cmd.Process == nil {
		return nil
	}

	return cmd.Process.Signal(os.Interrupt)
}

// killProcess kills a single process
func killProcess(cmd *exec.Cmd) error {
	if cmd.Process == nil {
		return nil
	}

	return cmd.Process.Signal(os.Interrupt)
}
