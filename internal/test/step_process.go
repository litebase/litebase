package test

import (
	"fmt"
	"os"
)

type StepProcess struct {
	sp               *StepProcessor
	processName      string
	expectedExitCode int
}

// Indicate that this process should exit with the given code
func (s *StepProcess) ShouldExitWith(code int) {
	s.expectedExitCode = code
}

// Broadcast a step to all processes
func (s *StepProcess) Step(name string) {
	if s.sp != nil {
		// Check if we're in coordinator mode (not running as child process)
		processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")
		isChildProcess := processName != ""

		if !isChildProcess {
			// In coordinator mode, send the step message to other processes
			s.sp.sendStepToProcesses(s.processName, name)
		} else {
			// In child process mode, send sync message via Unix socket
			s.sp.connMutex.RLock()
			conn, exists := s.sp.connections[processName]
			s.sp.connMutex.RUnlock()

			if exists {
				message := fmt.Sprintf("LITEBASE_TEST_SYNC=%s\n", name)

				_, err := conn.Write([]byte(message))

				if err != nil {
					fmt.Printf("[CHILD %s] Error sending sync message: %v\n", processName, err)
				}
			} else {
				fmt.Printf("[CHILD %s] No connection available for sending sync message\n", processName)
			}
		}
	}
}

// Wait a step to be broadcast from another process
func (s *StepProcess) WaitForStep(name string) {
	if s.sp == nil {
		return
	}

	// In both coordinator and child mode, wait for the step message
	s.sp.waitForStep(name)
}
