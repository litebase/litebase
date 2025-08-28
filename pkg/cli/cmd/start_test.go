package cmd_test

import (
	"os"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cli/cmd"
	"go.yaml.in/yaml/v4"
)

func TestStartCmd(t *testing.T) {
	test.RunWithTearDown(t, func() {
		t.Run("Start", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

			// Start the server in the background
			err := cli.WithArgs("start").
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

					if port != "8080" {
						t.Log(handle.GetOutput())
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

					err = cli.Cancel()

					if err != nil {
						t.Fatalf("failed to cancel: %v", err)
					}
				})

			if err != nil {
				t.Fatalf("failed to start command in background: %v", err)
			}
		})

		t.Run("Start with port", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

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
						t.Log(handle.GetOutput())
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

					err = cli.Cancel()

					if err != nil {
						t.Fatalf("failed to cancel: %v", err)
					}
				})

			if err != nil {
				t.Fatalf("failed to start command in background: %v", err)
			}
		})

		t.Run("Start with --debug", func(t *testing.T) {
			cli := test.NewTestCLI(t, nil)

			// Start the server in the background
			err := cli.WithArgs("start", "--port", "9876", "--debug").
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
					var debug string

					if debug, err = handle.WaitForOutputLine("Debug Mode", 1*time.Second); err != nil {
						t.Log(handle.GetOutput())
						t.Fatalf("timeout waiting for debug mode output: %v", err)
					}

					if debug != "Enabled" {
						t.Fatalf("expected debug mode to be enabled, got %s", debug)
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
	}, test.CleanupPort("9876"), test.CleanupPort("9877"), test.CleanupPort("8888"), test.CleanupPort("8080"))
}

func TestStartCmdWithLocalConfigurationFile(t *testing.T) {
	test.RunWithTearDown(t, func() {
		cli := test.NewTestCLI(t, nil)

		tmpDirectory := t.TempDir()

		// Create .litebase directory in the temp directory
		litebaseDir := tmpDirectory + "/.litebase"
		if err := os.MkdirAll(litebaseDir, 0755); err != nil {
			t.Fatalf("failed to create .litebase directory: %v", err)
		}

		file, err := os.Create(litebaseDir + "/config.yml")

		if err != nil {
			t.Fatalf("failed to create config file: %v", err)
		}

		data, err := yaml.Marshal(cmd.StartConfig{
			Debug: true,
			Port:  "8888",
		})

		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}

		if _, err := file.Write(data); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close config file: %v", err)
		}

		err = cli.WithArgs("start", "--config", litebaseDir+"/config.yml").
			RunInBackground(func(handle *test.ProcessHandle) {
				if !handle.IsRunning() {
					t.Fatal("expected server to be running")
				}

				// Wait for the server to start (look for server info output)
				if err := handle.WaitForOutput("Litebase Server", 3*time.Second); err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for server to start: %v", err)
				}

				if debug, err := handle.WaitForOutputLine("Debug Mode", 3*time.Second); debug != "Enabled" || err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("debug mode not enabled or timeout waiting for debug mode output: %v", err)
				}

				if port, err := handle.WaitForOutputLine("Port", 3*time.Second); port != "8888" || err != nil {
					t.Log(handle.GetOutput())

					if err != nil {
						t.Fatalf("timeout waiting for port output: %v", err)
					} else {
						t.Fatalf("unexpected port output: got %s, want 8888", port)
					}
				}

				err = cli.Cancel()

				if err != nil {
					t.Fatalf("failed to cancel: %v", err)
				}
			})

		if err != nil {
			t.Fatalf("failed to start command in background: %v", err)
		}
	}, test.CleanupPort("8888"))
}

func TestStartCmdWithLocalConfigurationFileAndFlags(t *testing.T) {
	test.RunWithTearDown(t, func() {
		cli := test.NewTestCLI(t, nil)

		tmpDirectory := t.TempDir()

		// Create .litebase directory in the temp directory
		litebaseDir := tmpDirectory + "/.litebase"
		if err := os.MkdirAll(litebaseDir, 0755); err != nil {
			t.Fatalf("failed to create .litebase directory: %v", err)
		}

		file, err := os.Create(litebaseDir + "/config.yml")

		if err != nil {
			t.Fatalf("failed to create config file: %v", err)
		}

		data, err := yaml.Marshal(cmd.StartConfig{
			Port: "8888",
		})

		if err != nil {
			t.Fatalf("failed to marshal config: %v", err)
		}

		if _, err := file.Write(data); err != nil {
			t.Fatalf("failed to write config file: %v", err)
		}

		if err := file.Close(); err != nil {
			t.Fatalf("failed to close config file: %v", err)
		}

		err = cli.WithArgs("start", "--config", litebaseDir+"/config.yml", "--debug").
			RunInBackground(func(handle *test.ProcessHandle) {
				if !handle.IsRunning() {
					t.Fatal("expected server to be running")
				}

				// Wait for the server to start (look for server info output)
				if err := handle.WaitForOutput("Litebase Server", 3*time.Second); err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("timeout waiting for server to start: %v", err)
				}

				if debug, err := handle.WaitForOutputLine("Debug Mode", 3*time.Second); debug != "Enabled" || err != nil {
					t.Log(handle.GetOutput())
					t.Fatalf("debug mode not enabled or timeout waiting for debug mode output: %v", err)
				}

				if port, err := handle.WaitForOutputLine("Port", 3*time.Second); port != "8888" || err != nil {
					t.Log(handle.GetOutput())

					if err != nil {
						t.Fatalf("timeout waiting for port output: %v", err)
					} else {
						t.Fatalf("unexpected port output: got %s, want 8888", port)
					}
				}

				err = cli.Cancel()

				if err != nil {
					t.Fatalf("failed to cancel: %v", err)
				}
			})

		if err != nil {
			t.Fatalf("failed to start command in background: %v", err)
		}
	}, test.CleanupPort("8888"))
}

func TestStartCmdWithStoragePathFlags(t *testing.T) {
	test.RunWithTearDown(t, func() {
		storagePath := t.TempDir()
		networkStoragePath := t.TempDir()
		tmpPath := t.TempDir()

		cli := test.NewTestCLI(t, nil)

		if _, err := os.Stat(storagePath); os.IsNotExist(err) {
			t.Logf("expected storage path %s to exist, but it does not", storagePath)
		}

		// Start the server in the background with a specific storage path
		err := cli.WithArgs(
			"start",
			"--storage-path", storagePath,
			"--storage-network-path", networkStoragePath,
			"--storage-tmp-path", tmpPath,
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

				if _, err := os.Stat(storagePath); os.IsNotExist(err) {
					t.Fatalf("expected storage path %s to exist, but it does not", storagePath)
				} else if err != nil {
					t.Fatalf("error checking storage path %s: %v", storagePath, err)
				}

				if _, err := os.Stat(networkStoragePath); os.IsNotExist(err) {
					t.Fatalf("expected storage network path %s to exist, but it does not", networkStoragePath)
				} else if err != nil {
					t.Fatalf("error checking storage network path %s: %v", networkStoragePath, err)
				}

				if _, err := os.Stat(tmpPath); os.IsNotExist(err) {
					t.Fatalf("expected storage tmp path %s to exist, but it does not", tmpPath)
				} else if err != nil {
					t.Fatalf("error checking storage tmp path %s: %v", tmpPath, err)
				}

				err = cli.Cancel()

				if err != nil {
					t.Fatalf("failed to cancel: %v", err)
				}
			})

		if err != nil {
			t.Fatalf("failed to start command in background: %v", err)
		}
	}, test.CleanupPort("8888"))
}
