package backups_test

import (
	"context"
	"fmt"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/backups"
	"litebase/server/file"
	"litebase/server/sqlite3"
	"litebase/server/storage"
	"testing"
	"time"
)

func TestCopySourceDatabaseToTargetDatabase(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		source := test.MockDatabase(app)
		target := test.MockDatabase(app)
		sourceDirectory := file.GetDatabaseFileDir(source.DatabaseId, source.BranchId)
		targetDirectory := file.GetDatabaseFileDir(target.DatabaseId, target.BranchId)

		sourceDfs := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem()
		targetDfs := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem()

		for i := 1; i <= 10; i++ {
			sourceDfs.FileSystem().Create(fmt.Sprintf("%s%010d", sourceDirectory, i))
		}

		err := backups.CopySourceDatabaseToTargetDatabase(
			5*storage.DataRangeMaxPages,
			source.DatabaseId,
			source.BranchId,
			target.DatabaseId,
			target.BranchId,
			sourceDfs,
			targetDfs,
		)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Verify that only the first 5 pages were copied
		entries, err := targetDfs.FileSystem().ReadDir(targetDirectory)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// There should be 5 entries in the target directory, including the metadata file
		if len(entries) != 6 {
			t.Errorf("Expected 5 entries, got %d", len(entries))
		}
	})
}

func TestRestoreFromTimestamp(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		source := test.MockDatabase(app)
		target := test.MockDatabase(app)

		snapshotLogger := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()
		sourceDfs := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem()
		targetDfs := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem()

		db, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, db)

		// Create a test table and insert some data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"))

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte([]byte("BEGIN")))

		for i := 0; i < 1000; i++ {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  "TEXT",
					Value: "value",
				},
			)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("BEGIN"))

		for i := 0; i < 1000; i++ {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  "TEXT",
					Value: "value",
				},
			)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Get the snapshots
		snapshotLogger.GetSnapshots()

		// Get the lastest snapshot timestamp
		snapshotKeys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[0])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		var restored bool

		// source the onComplete function
		onComplete := func(restoreFunc func() error) error {
			restored = true
			return nil
		}

		// Call the RestoreFromTimestamp function
		err = backups.RestoreFromTimestamp(
			app.Config,
			app.Cluster.TieredFS(),
			source.DatabaseId,
			source.BranchId,
			target.DatabaseId,
			target.BranchId,
			restorePoint.Timestamp,
			snapshotLogger,
			sourceDfs,
			targetDfs,
			onComplete,
		)

		// Check for errors
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if !restored {
			t.Error("Expected onComplete to be called")
		}

		db, err = app.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(target.DatabaseId, target.BranchId, db)

		// Verify the data is restored correctly
		result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT * FROM test"))

		if err == nil || err.Error() != "SQLite3 Error[1]: no such table: test" {
			t.Errorf("Expected an error indicating the table does not exist, got %v", err)
		}

		if len(result.Rows) != 0 {
			t.Errorf("Expected 0 rows, got %d", len(result.Rows))
		}

		if targetDfs.Metadata().PageCount != restorePoint.PageCount {
			t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
		}
	})
}

func TestRestoreFromInvalidBackup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		source := test.MockDatabase(app)
		target := test.MockDatabase(app)

		snapshotLogger := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()
		sourceDfs := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem()
		targetDfs := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem()

		db, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, db)

		// Create a test table and insert some data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"))

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("BEGIN"))

		for i := 0; i < 1000; i++ {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  "TEXT",
					Value: "value",
				},
			)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Get the snapshots
		snapshotLogger.GetSnapshots()

		// Get the lastest snapshot timestamp
		snapshotKeys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[0])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Call the RestoreFromTimestamp function
		err = backups.RestoreFromBackup(
			restorePoint.Timestamp,
			"test",
			source.DatabaseId,
			source.BranchId,
			target.DatabaseId,
			target.BranchId,
			sourceDfs,
			targetDfs,
		)

		// Check for errors
		if err == nil {
			t.Error("Expected an error, got nil")
		}

		if err != backups.ErrorRestoreBackupNotFound {
			t.Errorf("Expected error %v, got %v", backups.ErrorRestoreBackupNotFound, err)
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
			test.RunWithApp(t, func(app *server.App) {
				source := test.MockDatabase(app)
				target := test.MockDatabase(app)

				snapshotLogger := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()
				sourceDfs := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem()
				targetDfs := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem()

				db, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, db)

				// Create a test table and insert some data
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)"))

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				time.Sleep(timeout) // Ensure a different timestamp

				// Insert some test data
				db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("BEGIN"))

				for i := 0; i < 1000; i++ {
					_, err = db.GetConnection().SqliteConnection().Exec(
						context.Background(),
						[]byte("INSERT INTO test (value) VALUES (?)"),
						sqlite3.StatementParameter{
							Type:  "TEXT",
							Value: "value",
						},
					)

					if err != nil {
						t.Errorf("Expected no error, got %v", err)
					}
				}

				db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))
				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				time.Sleep(timeout) // Ensure a different timestamp

				// Insert some test data
				db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("BEGIN"))

				for i := 0; i < 1000; i++ {
					_, err = db.GetConnection().SqliteConnection().Exec(
						context.Background(),
						[]byte("INSERT INTO test (value) VALUES (?)"),
						sqlite3.StatementParameter{
							Type:  "TEXT",
							Value: "value",
						},
					)

					if err != nil {
						t.Errorf("Expected no error, got %v", err)
					}
				}

				db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				// Get the snapshots
				snapshotLogger.GetSnapshots()

				// Get the lastest snapshot timestamp
				snapshotKeys := snapshotLogger.Keys()

				snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[0])

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
				err = backups.RestoreFromTimestamp(
					app.Config,
					app.Cluster.TieredFS(),
					source.DatabaseId,
					source.BranchId,
					target.DatabaseId,
					target.BranchId,
					restorePoint.Timestamp,
					snapshotLogger,
					sourceDfs,
					targetDfs,
					onComplete,
				)

				// Check for errors
				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				if !restored {
					t.Error("Expected onComplete to be called")
				}

				db, err = app.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(target.DatabaseId, target.BranchId, db)

				// Verify the data is restored correctly
				result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT * FROM test"))

				if err == nil || err.Error() != "SQLite3 Error[1]: no such table: test" {
					t.Errorf("Expected an error indicating the table does not exist, got %v", err)
				}

				if len(result.Rows) != 0 {
					t.Errorf("Expected 0 rows, got %d", len(result.Rows))
				}

				if targetDfs.Metadata().PageCount != restorePoint.PageCount {
					t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
				}
			})
		})
	}
}
