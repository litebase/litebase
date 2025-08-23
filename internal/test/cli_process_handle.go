package test

import (
	"context"
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// ProcessHandle represents a running long-lived process
type ProcessHandle struct {
	cancel     context.CancelFunc
	closed     bool
	closeOnce  sync.Once
	cmd        *exec.Cmd
	ctx        context.Context
	doneChan   chan struct{}
	errorChan  chan error
	isRunning  bool
	mutex      sync.RWMutex
	output     strings.Builder
	outputChan chan string
}

// NewProcessHandle creates a new ProcessHandle with a cancellable context and
// initialized channels for managing long-running processes.
func NewProcessHandle(ctx context.Context) *ProcessHandle {
	ctx, cancel := context.WithCancel(ctx)

	return &ProcessHandle{
		cancel:     cancel,
		ctx:        ctx,
		doneChan:   make(chan struct{}),
		errorChan:  make(chan error, 1),
		outputChan: make(chan string, 100),
	}
}

// Cancel stops the running process
func (ph *ProcessHandle) Cancel() error {
	ph.mutex.Lock()

	if !ph.isRunning {
		ph.mutex.Unlock()
		return nil
	}

	ph.cancel()

	if ph.cmd != nil && ph.cmd.Process != nil {
		// Try process group kill first for better signal propagation
		if err := killProcessGroup(ph.cmd); err != nil {
			// If process group kill fails, try direct process signal
			if err := killProcess(ph.cmd); err != nil {
				// If interrupt is not supported (e.g., on Windows), fallback to Kill
				if err := ph.cmd.Process.Kill(); err != nil {
					ph.mutex.Unlock()

					return fmt.Errorf("failed to kill process: %w", err)
				}
			}
		}
	}

	ph.mutex.Unlock()

	// Wait for the process to actually finish by waiting on the done channel
	timeout := time.After(5 * time.Second) // Increased timeout to 5 seconds

	select {
	case <-timeout:
		return fmt.Errorf("process did not stop within timeout")
	case <-ph.doneChan:
		return nil
	}
}

// IsRunning returns true if the process is currently running
func (ph *ProcessHandle) IsRunning() bool {
	ph.mutex.RLock()
	defer ph.mutex.RUnlock()

	return ph.isRunning
}

// GetOutput returns all output captured so far
func (ph *ProcessHandle) GetOutput() string {
	ph.mutex.RLock()
	defer ph.mutex.RUnlock()

	return ph.output.String()
}

// GetOutputChan returns a channel that receives real-time output lines
func (ph *ProcessHandle) GetOutputChan() <-chan string {
	return ph.outputChan
}

// Wait waits for the process to complete or be cancelled
func (ph *ProcessHandle) Wait() error {
	select {
	case <-ph.doneChan:
		return nil
	case err := <-ph.errorChan:
		return err
	case <-ph.ctx.Done():
		return ph.ctx.Err()
	}
}

// WaitForOutput waits for a specific string to appear in the output with a timeout
func (ph *ProcessHandle) WaitForOutput(expectedText string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)

	// First check if the text is already in the output
	if strings.Contains(ph.GetOutput(), expectedText) {
		return nil
	}

	for {
		select {
		case line := <-ph.outputChan:
			if strings.Contains(line, expectedText) {
				return nil
			}
		case <-time.After(time.Until(deadline)):
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for output: %s", expectedText)
			}
		case <-ph.ctx.Done():
			return ph.ctx.Err()
		}
	}
}
