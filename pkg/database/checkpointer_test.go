package database_test

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewCheckpointer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		cp, err := database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		if cp == nil {
			t.Fatal("CheckPointer is nil")
		}
	})
}

func TestCheckpointer_Begin(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		cp, err := database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin(0)

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint == nil {
			t.Fatal("Checkpoint is nil after Begin")
		}

		err = cp.Begin(0)

		if err != database.ErrorCheckpointAlreadyInProgressError {
			t.Fatal("Expected CheckpointAlreadyInProgressError")
		}
	})
}

func TestCheckpointer_CheckpointBarrier(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		checkpointer, err := app.DatabaseManager.Resources(db.Branch).Checkpointer()

		if err != nil {
			t.Fatalf("Failed to create checkpointer: %v", err)
		}

		if checkpointer == nil {
			t.Fatal("Expected checkpointer to be created, but got nil")
		}

		wg := sync.WaitGroup{}

		wg.Add(1)

		go func() {
			defer wg.Done()

			err := checkpointer.CheckpointBarrier(func() error {
				time.Sleep(10 * time.Millisecond)
				return nil
			})
			
			if err != nil {
				t.Errorf("First checkpoint barrier failed: %v", err)
			}
		}()

		wg.Add(1)

		go func() {
			defer wg.Done()

			time.Sleep(1 * time.Millisecond)

			err := checkpointer.CheckpointBarrier(func() error {
				return nil
			})

			if err == nil {
				t.Error("Expected error due to checkpoint barrier, but got nil")
			}
		}()

		wg.Wait()
	})
}

func TestCheckpointer_CheckpointPage(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		cp, err := database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, []byte("test data"))

		if err != database.ErrorNoCheckpointInProgressError {
			t.Fatal("Expected NoCheckpointInProgressError")
		}

		err = cp.Begin(0)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, make([]byte, 4096))

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint.LargestPageNumber != 1 {
			t.Fatal("Page was not added")
		}
	})
}

func TestCheckpointer_Commit(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		cp, err := database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Commit()

		if err != database.ErrorNoCheckpointInProgressError {
			t.Fatal("Expected NoCheckpointInProgressError")
		}

		err = cp.Begin(0)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, make([]byte, 4096))

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Commit()

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint != nil {
			t.Fatal("Checkpoint should be nil after Commit")
		}

		pageCount = dfs.Metadata().PageCount

		if pageCount != 1 {
			t.Fatal("Expected page count to be 1 after commit")
		}
	})
}

func TestCheckpointer_Rollback(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		dfs := app.DatabaseManager.Resources(mock.Branch).FileSystem()

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		fileSystem := app.DatabaseManager.Resources(mock.Branch).FileSystem()

		cp, err := database.NewCheckpointer(
			mock.Branch,
			fileSystem,
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin(time.Now().UTC().UnixNano())

		if err != nil {
			t.Fatal(err)
		}

		data := make([]byte, 4096)

		err = cp.CheckpointPage(1, data)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Commit()

		if err != nil {
			t.Fatal(err)
		}

		pageCount = dfs.Metadata().PageCount

		if pageCount != 1 {
			t.Fatal("Expected initial page count to be 1")
		}

		err = cp.Rollback()

		if err != database.ErrorNoCheckpointInProgressError {
			t.Fatal("Expected NoCheckpointInProgressError")
		}

		err = cp.Begin(0)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(2, data)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Rollback()

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint != nil {
			t.Fatal("Checkpoint should be nil after Rollback")
		}

		pageCount = dfs.Metadata().PageCount

		if pageCount != 1 {
			t.Fatal("Expected initial page count to be 1")
		}
	})
}

func TestCheckpointer_Rollback_AfterCrash(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		cp, err := database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin(1234567890)

		if err != nil {
			t.Fatal(err)
		}

		_, err = database.NewCheckpointer(
			mock.Branch,
			app.DatabaseManager.Resources(mock.Branch).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.Branch).PageLogger(),
			app.DatabaseManager.Resources(mock.Branch).SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestCheckpointer_NoRollbackLogsWhenIncrementalBackupsDisabled(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Disable incremental backups
		mock.Branch.Settings.IncrementalBackupsEnabled = false

		err := mock.Branch.UpdateBranchSettings(mock.Branch.Settings)

		if err != nil {
			t.Fatal(err)
		}

		// Reload the branch to get updated settings
		branch, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatal(err)
		}

		updatedBranch, err := branch.BranchByID(mock.DatabaseBranchID)

		if err != nil {
			t.Fatal(err)
		}

		resources := app.DatabaseManager.Resources(updatedBranch)

		cp, err := database.NewCheckpointer(
			updatedBranch,
			resources.FileSystem(),
			app.Cluster.NetworkFS(),
			resources.PageLogger(),
			resources.SnapshotLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		// Begin a checkpoint
		timestamp := time.Now().UTC().UnixNano()

		err = cp.Begin(timestamp)

		if err != nil {
			t.Fatal(err)
		}

		// The checkpoint should have been created with zero offset and size
		// since no rollback frame was started
		if cp.Checkpoint.Offset != 0 {
			t.Fatalf("Expected offset to be 0, got %d", cp.Checkpoint.Offset)
		}

		if cp.Checkpoint.Size != 0 {
			t.Fatalf("Expected size to be 0, got %d", cp.Checkpoint.Size)
		}

		// Add a page to the checkpoint
		data := make([]byte, 4096)

		err = cp.CheckpointPage(1, data)

		if err != nil {
			t.Fatal(err)
		}

		// Size should still be 0 since we didn't log to rollback logger
		if cp.Checkpoint.Size != 0 {
			t.Fatalf("Expected size to remain 0 after checkpoint page, got %d", cp.Checkpoint.Size)
		}

		// Commit the checkpoint
		err = cp.Commit()

		if err != nil {
			t.Fatal(err)
		}

		// Verify that no rollback logs were created by checking the rollback log directory
		rollbackLogPath := file.GetDatabaseRollbackDirectory(mock.DatabaseID, mock.DatabaseBranchID)

		entries, err := resources.FileSystem().FileSystem().ReadDir(rollbackLogPath)

		if err != nil && !os.IsNotExist(err) {
			t.Fatal(err)
		}

		// The directory might not exist or be empty - both are valid
		if len(entries) > 0 {
			t.Fatalf("Expected no rollback log entries when incremental backups disabled, found %d", len(entries))
		}
	})
}
