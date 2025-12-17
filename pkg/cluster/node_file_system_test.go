package cluster_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/config"
	"github.com/litebase/litebase/pkg/server"
)

func TestNodeFileSystem(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("LocalFS", func(t *testing.T) {
			fs := app.Cluster.LocalFS()

			if fs == nil {
				t.Error("LocalFS() returned nil")
			}
		})

		t.Run("ObjectFS", func(t *testing.T) {
			fs := app.Cluster.ObjectFS()

			if fs == nil {
				t.Error("ObjectFS() returned nil")
			}
		})

		t.Run("TieredFS", func(t *testing.T) {
			fs := app.Cluster.TieredFS()

			if fs == nil {
				t.Error("TieredFS() returned nil")
			}
		})

		t.Run("TmpFS", func(t *testing.T) {
			fs := app.Cluster.TmpFS()

			if fs == nil {
				t.Error("TmpFS() returned nil")
			}
		})

		t.Run("TmpTieredFS", func(t *testing.T) {
			fs := app.Cluster.TmpTieredFS()

			if fs == nil {
				t.Error("TmpTieredFS() returned nil")
			}
		})

		t.Run("NetworkFS", func(t *testing.T) {
			fs := app.Cluster.NetworkFS()

			if fs == nil {
				t.Error("NetworkFS() returned nil")
			}
		})
	})
}

func TestNodeFileSystem_NoNetworkStoragePath(t *testing.T) {
	// Create a temporary directory for this test
	tmpDir := t.TempDir()

	// Create a config with NO network storage path
	cfg := &config.Config{
		ClusterId:          "test-cluster",
		StorageLocalPath:   tmpDir,
		StorageNetworkPath: "", // Empty - this is what we're testing
		StorageTmpPath:     filepath.Join(tmpDir, "tmp"),
		StorageObjectMode:  "local",
	}

	// Create a minimal cluster instance
	testCluster, err := cluster.NewCluster(cfg)

	if err != nil {
		t.Fatalf("Failed to create cluster: %v", err)
	}

	defer testCluster.ShutdownStorage()

	// Verify that config has empty network path
	if cfg.StorageNetworkPath != "" {
		t.Fatalf("Expected empty StorageNetworkPath, got: %s", cfg.StorageNetworkPath)
	}

	// Verify NetworkFS falls back to local storage
	networkFS := testCluster.NetworkFS()

	if networkFS == nil {
		t.Fatal("NetworkFS() returned nil when StorageNetworkPath is empty")
	}

	// Test that we can write and read files
	testFile := "test-network-fallback.txt"
	testContent := []byte("test content for network fallback")

	err = networkFS.WriteFile(testFile, testContent, 0644)

	if err != nil {
		t.Fatalf("Failed to write file to NetworkFS: %v", err)
	}

	readContent, err := networkFS.ReadFile(testFile)

	if err != nil {
		t.Fatalf("Failed to read file from NetworkFS: %v", err)
	}

	if string(readContent) != string(testContent) {
		t.Errorf("Content mismatch: expected %q, got %q", string(testContent), string(readContent))
	}

	// Verify ObjectFS works
	objectFS := testCluster.ObjectFS()

	if objectFS == nil {
		t.Fatal("ObjectFS() returned nil")
	}
}

func TestTieredFS_SyncsDirtyFiles(t *testing.T) {
	// Speed up the lease duration for testing purposes
	defaultLeaseDuration := cluster.LeaseDuration
	defer func() { cluster.LeaseDuration = defaultLeaseDuration }()
	cluster.LeaseDuration = 1 * time.Second

	defaultNodeStoreAddressInterval := cluster.NodeStoreAddressInterval
	defer func() { cluster.NodeStoreAddressInterval = defaultNodeStoreAddressInterval }()
	cluster.NodeStoreAddressInterval = 1 * time.Second

	test.WithSteps(t, func(sp *test.StepProcessor) {
		sp.Run("PRIMARY", func(s *test.StepProcess) {

			// Primary will crash
			test.RunWithoutCleanup(t, func(app *server.App) {
				s.Step("PRIMARY_INIT")

				// Give the node a moment to initialize then signal ready
				time.Sleep(1 * time.Second)
				s.Step("PRIMARY_READY")

				err := s.WaitForStep("REPLICA_READY")

				if err != nil {
					t.Fatal(err)
				}

				// Write a file to the tiered filesystem (this will be dirty)
				file, err := app.Cluster.TieredFS().OpenFile("test", os.O_RDWR|os.O_CREATE, 0600)

				if err != nil {
					t.Fatal(err)
				}

				_, err = file.Write([]byte("helloworld"))

				if err != nil {
					t.Fatal(err)
				}

				// Close the file to ensure data is flushed to disk before crash
				err = file.Close()

				if err != nil {
					t.Fatal(err)
				}

				// Signal that file has been written and crash immediately
				s.Step("FILE_WRITTEN")
				os.Exit(1) // Simulate a crash
			})
		}).ShouldExitWith(1)

		sp.Run("REPLICA", func(s *test.StepProcess) {
			err := s.WaitForStep("PRIMARY_INIT")

			if err != nil {
				t.Fatal(err)
			}

			test.RunWithoutCleanup(t, func(app *server.App) {
				if err := s.WaitForStep("PRIMARY_READY"); err != nil {
					t.Fatal(err)
				}

				// Verify file doesn't exist in object storage yet
				_, err := app.Cluster.ObjectFS().Stat("test")

				if err == nil {
					t.Fatal("File should not exist in object storage yet (should be dirty on primary)")
				}

				s.Step("REPLICA_READY")

				err = s.WaitForStep("FILE_WRITTEN")

				if err != nil {
					t.Fatal(err)
				}

				// Wait for the node to become primary (after the first server crashes)
				timeout := time.After(10 * time.Second)

			waitForPrimary:
				for {
					select {
					case <-timeout:
						t.Fatal("Timed out waiting for node to become primary")
					default:
						if app.Cluster.Node().IsPrimary() {
							break waitForPrimary
						}

						time.Sleep(100 * time.Millisecond)
					}
				}

				// Initialize the tiered file system to trigger dirty file syncing
				app.Cluster.TieredFS()

				if _, err := app.Cluster.ObjectFS().Stat("test"); err != nil {
					t.Fatal("File should exist in object storage after recovery")
				}

				data, err := app.Cluster.ObjectFS().ReadFile("test")

				if err != nil {
					t.Fatal("File should exist in object storage after recovery")
				}

				if string(data) != "helloworld" {
					t.Fatalf("File contents do not match: expected 'helloworld', got '%s'", string(data))
				}
			})
		})
	})
}
