package test

import (
	"os"
	"testing"
	"time"
)

func TestStepProcessorTest(t *testing.T) {
	WithSteps(t, func(sp *StepProcessor) {
		// Process A: Just sends a step and exits
		sp.Run("sender", func(dp *StepProcess) {
			time.Sleep(100 * time.Millisecond) // Give other process time to start
			dp.Step("hello")
		})

		// Process B: Waits for the step and exits
		sp.Run("receiver", func(dp *StepProcess) {
			dp.WaitForStep("hello")
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
			s.WaitForStep("step_2_completed")

			// Continue with final work
			time.Sleep(50 * time.Millisecond)
		})

		// Process B: Waits for step message and responds
		sp.Run("process_b", func(s *StepProcess) {
			// Wait for process A to complete step 1
			s.WaitForStep("step_1_completed")

			// Do some work after receiving the signal
			time.Sleep(200 * time.Millisecond)

			// Signal that we've completed step 2
			s.Step("step_2_completed")
		})

		// Process C: Independent process that also waits for step 1
		sp.Run("process_c", func(s *StepProcess) {
			// Wait for process A to complete step 1
			s.WaitForStep("step_1_completed")

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
			s.WaitForStep("step_2_completed")

			t.Fatal("Expected failure")
		}).ShouldExitWith(1)

		// Process B: Waits for step message and responds
		sp.Run("process_b", func(s *StepProcess) {
			// Wait for process A to complete step 1
			s.WaitForStep("step_1_completed")

			// Do some work after receiving the signal
			time.Sleep(200 * time.Millisecond)

			// Signal that we've completed step 2
			s.Step("step_2_completed")

			os.Exit(1)
		}).ShouldExitWith(1)

		// Process C: Independent process that also waits for step 1
		sp.Run("process_c", func(s *StepProcess) {
			// Wait for process A to complete step 1
			s.WaitForStep("step_1_completed")

			// Do some parallel work
			time.Sleep(150 * time.Millisecond)
		})
	})
}
