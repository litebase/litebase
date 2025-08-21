package test

import (
	"os"
	"testing"
	"time"
)

func TestStepProcessor(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Just sends a step and exits
		sp.Run("sender", func(dp *StepProcess) {
			time.Sleep(100 * time.Millisecond) // Give other process time to start
			dp.Step("hello")
		})

		// Process B: Waits for the step and exits
		sp.Run("receiver", func(dp *StepProcess) {
			err := dp.WaitForStep("hello")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'hello', got: %v", err)
			}
		})
	})
}

func TestStepProcessorTest_MoreSteps(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Sends a step message
		sp.Run("process_a", func(s *StepProcess) {
			// Do some work
			time.Sleep(100 * time.Millisecond)

			// Signal that we've completed step 1
			s.Step("step_1_completed")

			// Wait for process B to complete its work
			err := s.WaitForStep("step_2_completed")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_2_completed', got: %v", err)
			}

			// Continue with final work
			time.Sleep(50 * time.Millisecond)
		})

		// Process B: Waits for step message and responds
		sp.Run("process_b", func(s *StepProcess) {
			// Wait for process A to complete step 1
			err := s.WaitForStep("step_1_completed")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_1_completed', got: %v", err)
			}

			// Do some work after receiving the signal
			time.Sleep(200 * time.Millisecond)

			// Signal that we've completed step 2
			s.Step("step_2_completed")
		})

		// Process C: Independent process that also waits for step 1
		sp.Run("process_c", func(s *StepProcess) {
			// Wait for process A to complete step 1
			err := s.WaitForStep("step_1_completed")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_1_completed', got: %v", err)
			}

			// Do some parallel work
			time.Sleep(150 * time.Millisecond)
		})
	})
}

func TestStepProcessor_WithExpectedFailingSteps(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Sends a step message
		sp.Run("process_a", func(s *StepProcess) {
			// Signal that we've completed step 1
			s.Step("step_1_completed")

			// Wait for process B to complete its work
			if err := s.WaitForStep("step_2_completed"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_2_completed', got: %v", err)
			}

			os.Exit(1)
		}).ShouldExitWith(1)

		// Process B: Waits for step message and responds
		sp.Run("process_b", func(s *StepProcess) {
			// Wait for process A to complete step 1
			if err := s.WaitForStep("step_1_completed"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_1_completed', got: %v", err)
			}

			// Do some work after receiving the signal
			time.Sleep(200 * time.Millisecond)

			// Signal that we've completed step 2
			s.Step("step_2_completed")

			os.Exit(1)
		}).ShouldExitWith(1)

		// Process C: Independent process that also waits for step 1
		sp.Run("process_c", func(s *StepProcess) {
			// Wait for process A to complete step 1
			if err := s.WaitForStep("step_1_completed"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'step_1_completed', got: %v", err)
			}

			// Do some parallel work
			time.Sleep(150 * time.Millisecond)
		})
	})
}

func TestStepProcessorPauseResume(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Gets paused and then resumed by process B
		sp.Run("process_a", func(dp *StepProcess) {
			dp.Step("start")

			// This should pause until process B resumes us
			time.Sleep(500 * time.Millisecond)

			dp.Step("after_resume")
		})

		// Process B: Controls process A by pausing and resuming it
		sp.Run("process_b", func(dp *StepProcess) {
			// Wait for process A to start
			err := dp.WaitForStep("start")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'start', got: %v", err)
			}

			// Pause process A
			dp.Pause("process_a")

			// Wait a bit while process A is paused
			time.Sleep(200 * time.Millisecond)

			// Resume process A
			dp.Resume("process_a")

			// Wait for process A to complete after resuming
			err = dp.WaitForStep("after_resume")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'after_resume', got: %v", err)
			}
		})
	})
}

func TestStepProcessorCrossProcessPauseResume(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Gets controlled by process B
		sp.Run("controlled_process", func(dp *StepProcess) {
			dp.Step("ready")

			// Do some work that can be interrupted
			for i := 0; i < 10; i++ {
				time.Sleep(50 * time.Millisecond)
			}

			dp.Step("work_completed")
		})

		// Process B: Controls process A by pausing and resuming it
		sp.Run("controller_process", func(dp *StepProcess) {
			// Wait for controlled process to be ready
			err := dp.WaitForStep("ready")

			if err != nil {
				t.Fatalf("Expected no error when waiting for step 'ready', got: %v", err)
			}

			// Let it work for a bit
			time.Sleep(100 * time.Millisecond)

			// Pause the controlled process
			dp.Pause("controlled_process")
			dp.Step("paused_process")

			// Keep it paused for a while
			time.Sleep(200 * time.Millisecond)

			// Resume the controlled process
			dp.Resume("controlled_process")
			dp.Step("resumed_process")

			// Wait for it to complete its work
			if err := dp.WaitForStep("work_completed"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'work_completed', got: %v", err)
			}
		})

		// Process C: Monitors the control actions
		sp.Run("monitor_process", func(dp *StepProcess) {
			if err := dp.WaitForStep("paused_process"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'paused_process', got: %v", err)
			}

			if err := dp.WaitForStep("resumed_process"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'resumed_process', got: %v", err)
			}

			if err := dp.WaitForStep("work_completed"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'work_completed', got: %v", err)
			}

			dp.Step("monitoring_complete")
		})
	})
}

func TestStepProcessorPauseAndResumeConvenience(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Gets paused and resumed using the convenience method
		sp.Run("target_process", func(dp *StepProcess) {
			dp.Step("started")

			// This should be interrupted by a pause/resume cycle
			time.Sleep(400 * time.Millisecond)

			dp.Step("finished")
		})

		// Process B: Uses convenience method to pause and resume
		sp.Run("control_process", func(dp *StepProcess) {
			if err := dp.WaitForStep("started"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'started', got: %v", err)
			}

			// Use the convenience method to pause for 200ms then resume
			dp.PauseAndResume("target_process", 200*time.Millisecond)

			if err := dp.WaitForStep("finished"); err != nil {
				t.Fatalf("Expected no error when waiting for step 'finished', got: %v", err)
			}
		})
	})
}
