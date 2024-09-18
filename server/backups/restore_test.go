package backups_test

import (
	"context"
	"fmt"
	"litebase/internal/test"
	"litebase/server/backups"
	"litebase/server/database"
	"testing"
	"time"
)

func TestRestoreFromTimestamp(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		dfs := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Create a test table and insert some data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), "BEGIN")

		for i := 0; i < 1000; i++ {
			_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (value) VALUES (?)", "value")

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), "COMMIT")

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), "BEGIN")

		for i := 0; i < 1000; i++ {
			_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (value) VALUES (?)", "value")

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), "COMMIT")

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Get the lastest snapshot timestamp
		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, snapshots[len(snapshots)-1].Timestamp)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		restorePoint, err := backups.GetRestorePoint(mock.DatabaseUuid, mock.BranchUuid, snapshot.RestorePoints.Data[0])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		var restored bool

		// Mock the onComplete function
		onComplete := func(restoreFunc func() error) error {
			restored = true
			return nil
		}

		// Call the RestoreFromTimestamp function
		err = backups.RestoreFromTimestamp(mock.DatabaseUuid, mock.BranchUuid, restorePoint.Timestamp, dfs, onComplete)

		// Check for errors
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if !restored {
			t.Error("Expected onComplete to be called")
		}

		database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		db, err = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Verify the data is restored correctly
		result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT * FROM test")

		if err == nil || err.Error() != "SQLite3 Error[1]: no such table: test" {
			t.Errorf("Expected an error indicating the table does not exist, got %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows, got %d", len(result.Rows))
		}

		if dfs.Metadata().PageCount != restorePoint.PageCount {
			t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, dfs.Metadata().PageCount)
		}
	})
}

func TestRestoreFromDuplicateTimestamp(t *testing.T) {
	timeouts := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		1000 * time.Millisecond,
		1000 * time.Millisecond,
	}

	for _, timeout := range timeouts {
		t.Run(fmt.Sprintf("restore with timeout: %s", timeout), func(t *testing.T) {
			test.Run(t, func() {
				mock := test.MockDatabase()

				dfs := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem()

				db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				// Create a test table and insert some data
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)")

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				time.Sleep(timeout) // Ensure a different timestamp

				// Insert some test data
				db.GetConnection().SqliteConnection().Exec(context.Background(), "BEGIN")

				for i := 0; i < 1000; i++ {
					_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (value) VALUES (?)", "value")

					if err != nil {
						t.Errorf("Expected no error, got %v", err)
					}
				}

				db.GetConnection().SqliteConnection().Exec(context.Background(), "COMMIT")
				err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				time.Sleep(timeout) // Ensure a different timestamp

				// Insert some test data
				db.GetConnection().SqliteConnection().Exec(context.Background(), "BEGIN")

				for i := 0; i < 1000; i++ {
					_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (value) VALUES (?)", "value")

					if err != nil {
						t.Errorf("Expected no error, got %v", err)
					}
				}

				db.GetConnection().SqliteConnection().Exec(context.Background(), "COMMIT")

				err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				// Get the lastest snapshot timestamp
				snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, snapshots[len(snapshots)-1].Timestamp)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				restorePoint, err := backups.GetRestorePoint(mock.DatabaseUuid, mock.BranchUuid, snapshot.RestorePoints.Data[0])

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				var restored bool

				// Mock the onComplete function
				onComplete := func(restoreFunc func() error) error {
					restored = true
					return nil
				}

				// Call the RestoreFromTimestamp function
				err = backups.RestoreFromTimestamp(mock.DatabaseUuid, mock.BranchUuid, restorePoint.Timestamp, dfs, onComplete)

				// Check for errors
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				if !restored {
					t.Error("Expected onComplete to be called")
				}

				database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

				db, err = database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				// Verify the data is restored correctly
				result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT * FROM test")

				if err == nil || err.Error() != "SQLite3 Error[1]: no such table: test" {
					t.Errorf("Expected an error indicating the table does not exist, got %v", err)
				}

				if len(result.Rows) != 0 {
					t.Errorf("Expected 0 rows, got %d", len(result.Rows))
				}

				if dfs.Metadata().PageCount != restorePoint.PageCount {
					t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, dfs.Metadata().PageCount)
				}
			})
		})
	}
}
