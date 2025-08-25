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
		err := cli.WithArgs("start", "--port", "9876").
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
				var port string

				if port, err = handle.WaitForOutputLine("Port", 1*time.Second); err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for port output: %v", err)
				}

				if port != "9876" {
					t.Fatalf("expected port 9876, got %s", port)
				}

				// Wait for the server to start (look for server info output)
				var clusterID string

				if clusterID, err = handle.WaitForOutputLine("Cluster ID", 1*time.Second); err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for cluster ID output: %v", err)
				}

				if clusterID == "" {
					t.Fatalf("cluster ID should not be empty")
				}

				var nodeID string

				if nodeID, err = handle.WaitForOutputLine("Node ID", 1*time.Second); err != nil {
					t.Fatalf("timeout waiting for node ID output: %v", err)
				}

				if nodeID == "" {
					t.Fatalf("node ID should not be empty")
				}

				// Comment out the cancel to see if the test blocks indefinitely
				err = cli.Cancel()

				if err != nil {
					t.Fatalf("failed to cancel: %v", err)
				}
			})

		if err != nil {
			t.Fatalf("failed to start command in background: %v", err)
		}
	})
}
