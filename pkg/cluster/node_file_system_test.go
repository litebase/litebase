package cluster_test

import (
	"os"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestLocalFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.LocalFS()

		if fs == nil {
			t.Error("LocalFS() returned nil")
		}
	})
}

func TestObjectFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.ObjectFS()

		if fs == nil {
			t.Error("ObjectFS() returned nil")
		}
	})
}

func TestTieredFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
		}
	})
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
				if !app.Cluster.Node().IsPrimary() {
					t.Fatal("Server is not primary")
				}

				s.Step("PRIMARY_READY")

				s.WaitForStep("REPLICA_READY")

				// Write a file to the tiered filesystem (this will be dirty)
				file, err := app.Cluster.TieredFS().OpenFile("test", os.O_RDWR|os.O_CREATE, 0600)
				if err != nil {
					t.Fatal(err)
				}

				_, err = file.Write([]byte("helloworld"))
				if err != nil {
					t.Fatal(err)
				}

				// Signal that file has been written
				s.Step("FILE_WRITTEN")

				// Crash the primary node to simulate an unclean shutdown
				os.Exit(1)
			})
		}).ShouldExitWith(1)

		sp.Run("REPLICA", func(s *test.StepProcess) {
			s.WaitForStep("PRIMARY_READY")

			test.RunWithoutCleanup(t, func(app *server.App) {
				// Verify file doesn't exist in object storage yet
				_, err := app.Cluster.ObjectFS().Stat("test")

				if err == nil {
					t.Fatal("File should not exist in object storage yet (should be dirty on primary)")
				}

				s.Step("REPLICA_READY")

				s.WaitForStep("FILE_WRITTEN")

				// Wait for the node to become primary (after the first server crashes)
				timeout := time.After(15 * time.Second)

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

func TestTmpFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TmpFS()

		if fs == nil {
			t.Error("TmpFS() returned nil")
		}
	})
}

func TestTmpTieredFS(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TmpTieredFS()

		if fs == nil {
			t.Error("TmpTieredFS() returned nil")
		}
	})
}
