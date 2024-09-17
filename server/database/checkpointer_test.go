package database_test

import (
	"litebase/internal/test"
	"litebase/server/database"
	"testing"
)

func TestNewCheckpointer(t *testing.T) {
	test.Run(t, func() {
		db := test.MockDatabase()

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(db.DatabaseUuid, db.BranchUuid),
			db.DatabaseUuid,
			db.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		if cp == nil {
			t.Fatal("CheckPointer is nil")
		}
	})
}

func TestCheckpointerBegin(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin()

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint == nil {
			t.Fatal("Checkpoint is nil after Begin")
		}

		err = cp.Begin()

		if err != database.ErrorCheckpointAlreadyInProgressError {
			t.Fatal("Expected CheckpointAlreadyInProgressError")
		}
	})
}

func TestCheckpointerCheckpointPage(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, []byte("test data"))

		if err != database.ErrorNoCheckpointInProgressError {
			t.Fatal("Expected NoCheckpointInProgressError")
		}

		err = cp.Begin()

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, []byte("test data"))

		if err != nil {
			t.Fatal(err)
		}

		if cp.Checkpoint.LargestPageNumber != 1 {
			t.Fatal("Page was not added")
		}
	})
}

func TestCheckpointerCommit(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		dfs := database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid)

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Commit()

		if err != database.ErrorNoCheckpointInProgressError {
			t.Fatal("Expected NoCheckpointInProgressError")
		}

		err = cp.Begin()

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, []byte("test data"))

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

func TestCheckpointerRollback(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		dfs := database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid)

		pageCount := dfs.Metadata().PageCount

		if pageCount != 0 {
			t.Fatal("Expected initial page count to be 0")
		}

		cp, err := database.NewCheckpointer(
			database.DatabaseResources().FileSystem(mock.DatabaseUuid, mock.BranchUuid),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if err != nil {
			t.Fatal(err)
		}

		err = cp.Begin()

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(1, []byte("test data"))

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

		err = cp.Begin()

		if err != nil {
			t.Fatal(err)
		}

		err = cp.CheckpointPage(2, []byte("test data"))

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
