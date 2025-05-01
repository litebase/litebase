package backups_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/server/backups"

	"github.com/litebase/litebase/server/database"

	"github.com/litebase/litebase/server/sqlite3"

	"github.com/litebase/litebase/server/file"

	"github.com/litebase/litebase/internal/test"

	"github.com/litebase/litebase/server/storage"

	"github.com/litebase/litebase/server"
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
			5*storage.RangeMaxPages,
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

		for range 10 {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte("value"),
				},
			)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
		}

		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("COMMIT"))

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Insert some test data
		db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("BEGIN"))

		for range 10 {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte("value"),
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
			t.Fatalf("Expected an error indicating the table does not exist, got %v", err)
		}

		if result != nil {
			t.Fatalf("Expected result to be nil")
		}

		if targetDfs.Metadata().PageCount != restorePoint.PageCount {
			t.Fatalf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
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

		for range 1000 {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (value) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  sqlite3.ParameterTypeText,
					Value: []byte("value"),
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
	// Snapshots are taken in 1 second increments, so timeouts that are less than
	// 1 second should always revert to the same point.
	timeouts := []time.Duration{
		250 * time.Millisecond,
		500 * time.Millisecond,
		750 * time.Millisecond,
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
					t.Fatalf("Expected no error, got %v", err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, db)

				db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					// Create a test table and insert some data
					_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					return nil
				})

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					// Insert some test data
					for range 1000 {
						result, err := db.Exec(
							"INSERT INTO test (value) VALUES (?)",
							[]sqlite3.StatementParameter{
								{
									Type:  sqlite3.ParameterTypeText,
									Value: []byte("value"),
								},
							},
						)

						if err != nil {
							t.Fatalf("Expected no error, got %v", err)
							break
						}

						if result != nil && db.Changes() != 1 {
							t.Fatalf("Expected there to be 1 change, got %d", db.Changes())
						}
					}

					return nil
				})

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				time.Sleep(timeout) // Ensure a different timestamp

				// Insert some test data
				db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					for range 1000 {
						result, err := db.Exec(
							"INSERT INTO test (value) VALUES (?)",
							[]sqlite3.StatementParameter{
								{
									Type:  sqlite3.ParameterTypeText,
									Value: []byte("value"),
								},
							},
						)

						if err != nil {
							t.Fatalf("Expected no error, got %v", err)
						}

						if result != nil && db.Changes() != 1 {
							t.Fatalf("Expected there to be 1 change, got %d", db.Changes())
						}
					}

					return nil
				})

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				// Get the snapshots
				snapshotLogger.GetSnapshots()

				// Get the lastest snapshot timestamp
				snapshotKeys := snapshotLogger.Keys()

				snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[0])

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
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
					t.Fatalf("Expected no error, got %v", err)
				}

				if !restored {
					t.Fatalf("Expected onComplete to be called")
				}

				db, err = app.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				defer app.DatabaseManager.ConnectionManager().Release(target.DatabaseId, target.BranchId, db)

				// Verify the data is restored correctly
				result, err := db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT * FROM test"))

				if err == nil || err.Error() != "SQLite3 Error[1]: no such table: test" {
					t.Fatalf("Expected an error indicating the table does not exist, got %v", err)
				}

				if result != nil && len(result.Rows) != 0 {
					t.Fatalf("Expected 0 rows, got %d", len(result.Rows))
				}

				if targetDfs.Metadata().PageCount != restorePoint.PageCount {
					t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
				}
			})
		})
	}
}

func TestRestoreFromTimestampRolling(t *testing.T) {
	testCases := []struct {
		sourceCount int64
		targetCount int64
	}{
		{sourceCount: 1000, targetCount: 0},
		{sourceCount: 2000, targetCount: 1000},
		{sourceCount: 3000, targetCount: 2000},
		{sourceCount: 4000, targetCount: 3000},
	}

	test.RunWithApp(t, func(app *server.App) {
		source := test.MockDatabase(app)
		sourceDfs := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem()
		snapshotLogger := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()

		sourceDb, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
			_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			return nil
		})

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, sourceDb)

		for i, testcase := range testCases {
			t.Run(fmt.Sprintf("rolling restore: %d", i), func(t *testing.T) {
				sourceDb, _ := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)
				defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, sourceDb)
				target := test.MockDatabase(app)

				targetDfs := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem()

				sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					// Insert some test data
					for range testcase.sourceCount - testcase.targetCount {
						result, err := db.Exec(
							"INSERT INTO test (value) VALUES (?)",
							[]sqlite3.StatementParameter{
								{
									Type:  sqlite3.ParameterTypeText,
									Value: []byte("value"),
								},
							},
						)

						if err != nil {
							t.Fatalf("Expected no error, got %v", err)
							break
						}

						if result != nil && db.Changes() != 1 {
							t.Fatalf("Expected there to be 1 change, got %d", db.Changes())
						}
					}

					return nil
				})

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				// Get the snapshots
				snapshotLogger.GetSnapshots()

				// Get the lastest snapshot timestamp
				snapshotKeys := snapshotLogger.Keys()

				snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[len(snapshot.RestorePoints.Data)-1])

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
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
					t.Fatalf("Expected no error, got %v", err)
				}

				if !restored {
					t.Fatalf("Expected onComplete to be called")
				}

				if targetDfs.Metadata().PageCount != restorePoint.PageCount {
					t.Errorf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
				}

				// ensure the source and target databases have the right count of records
				sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					if result.Rows[0][0].Int64() != testcase.sourceCount {
						t.Fatalf("Expected %d rows, got %d", testcase.sourceCount, result.Rows[0][0].Int64())
					}

					return nil
				})

				targetDb, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				targetDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
					result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

					if err != nil {
						t.Fatalf("Expected no error, got %v - %s/%s", err, target.DatabaseId, target.BranchId)
					}

					if result != nil && result.Rows[0][0].Int64() != testcase.targetCount {
						t.Fatalf("Expected %d rows, got %d", testcase.targetCount, result.Rows[0][0].Int64())
					}

					return nil
				})

				app.DatabaseManager.ConnectionManager().Release(target.DatabaseId, target.BranchId, targetDb)
			})

		}
	})
}
