package cmd_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
)

func TestStartCmdWithTLS(t *testing.T) {
	test.RunWithTearDown(t, func() {
		t.Run("Start with --tls-cert", func(t *testing.T) {
			tmpDir := t.TempDir()

			// Generate TLS certificate and key
			tlsCert, err := test.GenerateTLSCertificate(tmpDir)
			if err != nil {
				t.Fatalf("failed to generate TLS certificate: %v", err)
			}

			cli := test.NewTestCLI(t, nil)

			// Start the server in the background with TLS
			err = cli.WithArgs(
				"start",
				"--port", "9877",
				"--tls-cert-path", tlsCert.CertPath,
				"--tls-key-path", tlsCert.KeyPath,
			).
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

					if port != "9877" {
						t.Log(handle.GetOutput())
						t.Fatalf("expected port 9877, got %s", port)
					}

					// Check that TLS is enabled
					var tlsStatus string

					if tlsStatus, err = handle.WaitForOutputLine("TLS", 1*time.Second); err != nil {
						t.Log(handle.GetOutput())
						t.Fatalf("timeout waiting for TLS status output: %v", err)
					}

					if tlsStatus != "Enabled" {
						t.Log(handle.GetOutput())
						t.Fatalf("expected TLS to be enabled, got %s", tlsStatus)
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

					err = cli.Cancel()

					if err != nil {
						t.Fatalf("failed to cancel: %v", err)
					}
				})

			if err != nil {
				t.Fatalf("failed to start command in background: %v", err)
			}
		})
	}, test.CleanupPort("9877"))
}
