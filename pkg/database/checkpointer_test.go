package database_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/server"
)

func TestNewCheckpointer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		cp, err := database.NewCheckpointer(
			mock.DatabaseId,
			mock.BranchId,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
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
			mock.DatabaseId,
			mock.BranchId,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
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

func TestCheckpointer_CheckpointPage(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		cp, err := database.NewCheckpointer(
			mock.DatabaseId,
			mock.BranchId,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
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
		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		cp, err := database.NewCheckpointer(
			mock.DatabaseId,
			mock.BranchId,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
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
		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		fileSystem := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()

		cp, err := database.NewCheckpointer(
			mock.DatabaseId,
			mock.BranchId,
			fileSystem,
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
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
	databaseId := "database"
	branchId := "branch"

	test.RunWithApp(t, func(app *server.App) {
		cp, err := database.NewCheckpointer(
			databaseId,
			branchId,
			app.DatabaseManager.Resources(databaseId, branchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(databaseId, branchId).PageLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin(1234567890)

		if err != nil {
			t.Fatal(err)
		}

		_, err = database.NewCheckpointer(
			databaseId,
			branchId,
			app.DatabaseManager.Resources(databaseId, branchId).FileSystem(),
			app.Cluster.NetworkFS(),
			app.DatabaseManager.Resources(databaseId, branchId).PageLogger(),
		)

		if err != nil {
			t.Fatal(err)
		}
	})
}

// func TestCheckpointer_SetTimestamp(t *testing.T) {
// 	test.RunWithApp(t, func(app *server.App) {
// 		mock := test.MockDatabase(app)

// 		cp, err := database.NewCheckpointer(
// 			mock.DatabaseId,
// 			mock.BranchId,
// 			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
// 			app.Cluster.NetworkFS(),
// 			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger(),
// 		)

// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		// cp.SetTimestamp(1)

// 		// if cp.Timestamp != 1 {
// 		// 	t.Fatal("Timestamp was not set")
// 		// }
// 	})
// }
