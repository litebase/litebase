package cmd_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
)

func TestStartCmd(t *testing.T) {
	test.Run(t, func() {
		cli := test.NewTestCLI(nil)

		// Start the server in the background
		err := cli.WithArgs("start", "--port", "8083").
			RunInBackground(func(handle *test.ProcessHandle) {

				if !handle.IsRunning() {
					t.Fatal("expected server to be running")
				}

				// Wait for the server to start (look for server info output)
				err := handle.WaitForOutput("Litebase Server", 3*time.Second)

				if err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for server to start: %v", err)
				}

				// Wait for the server to start (look for server info output)
				err = handle.WaitForOutput("Port", 1*time.Second)

				if err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for port output: %v", err)
				}

				// Wait for the server to start (look for server info output)
				err = handle.WaitForOutput("Cluster ID", 1*time.Second)

				if err != nil {
					t.Fatalf("timeout waiting for cluster ID output: %v", err)
				}

				err = handle.WaitForOutput("Node ID", 1*time.Second)

				if err != nil {
					t.Fatalf("timeout waiting for node ID output: %v", err)
				}

				// Comment out the cancel to see if the test blocks indefinitely
				err = cli.Cancel()

				if err != nil {
					t.Fatalf("failed to cancel server: %v", err)
				}

				t.Log(handle.GetOutput())
			})

		if err != nil {
			t.Fatalf("failed to start command in background: %v", err)
		}
	})
}
