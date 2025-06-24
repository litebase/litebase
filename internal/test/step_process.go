package test

import (
	"fmt"
	"os"
	"time"
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
	s.sp.WaitForStep(name)
}

// Pause another process from this process
func (s *StepProcess) Pause(targetProcessName string) {
	if s.sp == nil {
		return
	}

	processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")
	isChildProcess := processName != ""

	if !isChildProcess {
		// In coordinator mode, pause directly
		err := s.sp.Pause(targetProcessName)

		if err != nil {
			fmt.Printf("[COORDINATOR] Error pausing process %s: %v\n", targetProcessName, err)
		}
	} else {
		// In child process mode, send pause command via Unix socket
		s.sp.connMutex.RLock()
		conn, exists := s.sp.connections[processName]
		s.sp.connMutex.RUnlock()

		if exists {
			message := fmt.Sprintf("LITEBASE_TEST_PAUSE=%s\n", targetProcessName)

			_, err := conn.Write([]byte(message))

			if err != nil {
				fmt.Printf("[CHILD %s] Error sending pause command: %v\n", processName, err)
			}
		} else {
			fmt.Printf("[CHILD %s] No connection available for sending pause command\n", processName)
		}
	}
}

// Resume another process from this process
func (s *StepProcess) Resume(targetProcessName string) {
	if s.sp == nil {
		return
	}

	processName := os.Getenv("LITEBASE_DISTRIBUTED_TEST_RUN")
	isChildProcess := processName != ""

	if !isChildProcess {
		// In coordinator mode, resume directly
		err := s.sp.Resume(targetProcessName)

		if err != nil {
			fmt.Printf("[COORDINATOR] Error resuming process %s: %v\n", targetProcessName, err)
		}
	} else {
		// In child process mode, send resume command via Unix socket
		s.sp.connMutex.RLock()
		conn, exists := s.sp.connections[processName]
		s.sp.connMutex.RUnlock()

		if exists {
			message := fmt.Sprintf("LITEBASE_TEST_RESUME=%s\n", targetProcessName)

			_, err := conn.Write([]byte(message))

			if err != nil {
				fmt.Printf("[CHILD %s] Error sending resume command: %v\n", processName, err)
			}
		} else {
			fmt.Printf("[CHILD %s] No connection available for sending resume command\n", processName)
		}
	}
}

// PauseAndResume pauses a process, waits for the specified duration, then resumes it
// This is a convenience method for common pause/resume patterns
func (s *StepProcess) PauseAndResume(targetProcessName string, pauseDuration time.Duration) {
	s.Pause(targetProcessName)
	time.Sleep(pauseDuration)
	s.Resume(targetProcessName)
}
