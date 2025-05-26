package cluster_test

import (
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
)

func TestClearFSFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		fs := app.Cluster.TieredFS()

		if fs == nil {
			t.Error("TieredFS() returned nil")
		}

		_, err := fs.Create("test.txt")

		if err != nil {
			t.Error(err)
		}

		_, err = os.Stat(fmt.Sprintf("%s/%s/test.txt", app.Config.DataPath, "tiered"))

		if err != nil {
			t.Error(err)
		}

		app.Cluster.ClearFSFiles()

		if err != nil {
			t.Error(err)
		}

		_, err = os.Stat(fmt.Sprintf("%s/%s/test.txt", app.Config.DataPath, "tiered"))

		if err == nil {
			t.Error("tiered file system files were not cleared")
		}
	})
}

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
	// Create a primary server
	if os.Getenv("LITEBASE_TEST_SERVER") == "1" {
		test.RunWithApp(t, func(app *server.App) {
			// Write a file
			err := app.Cluster.TieredFS().WriteFile("test", []byte("helloworld"), 0644)

			if err != nil {
				t.Fatal(err)
			}

			// Crash the primary server
			os.Exit(1)
		})
	}

	// Create a replica server
	if os.Getenv("LITEBASE_TEST_SERVER") == "2" {
		test.RunWithApp(t, func(app *server.App) {
			// Create the instance of the tiered file systemt to innitialize the
			// dirty file syncing process
			app.Cluster.TieredFS()

			_, err := app.Cluster.ObjectFS().Stat("test")

			if err != nil {
				t.Fatal(err)
			}
		})
	}

	if os.Getenv("LITEBASE_TEST_SERVER") == "" {
		dataPath := fmt.Sprintf("./../../.test/%s", test.CreateHash(32))
		signature := test.CreateHash(64)

		// Start the first server
		cmd := exec.Command("go", "test", "-run", "TestTieredFS_SyncsDirtyFiles")
		cmd.Env = append(os.Environ(), "LITEBASE_TEST_SERVER=1", fmt.Sprintf("LITEBASE_TEST_DATA_PATH=%s", dataPath), fmt.Sprintf("LITEBASE_TEST_SIGNATURE=%s", signature))

		// Start the second server
		cmd2 := exec.Command("go", "test", "-run", "TestTieredFS_SyncsDirtyFiles")
		cmd2.Env = append(os.Environ(), "LITEBASE_TEST_SERVER=2", fmt.Sprintf("LITEBASE_TEST_DATA_PATH=%s", dataPath), fmt.Sprintf("LITEBASE_TEST_SIGNATURE=%s", signature))

		//  Wait for the commands to finish
		wg := &sync.WaitGroup{}
		wg.Add(2)

		go func() {
			defer wg.Done()
			err := cmd.Run()

			if exitErr, ok := err.(*exec.ExitError); ok {
				if exitErr.Success() {
					t.Error("Server 1 should have exited with an error")
				}
			}
		}()

		go func() {
			defer wg.Done()

			time.Sleep(250 * time.Millisecond) // Ensure the first server has time to write the file
			err := cmd2.Run()

			if exitErr, ok := err.(*exec.ExitError); ok {
				if !exitErr.Success() {
					t.Error("Error waiting for second server:", exitErr)
				}
			}
		}()

		wg.Wait()
	}
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
