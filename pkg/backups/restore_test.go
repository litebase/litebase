package backups_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
	"github.com/litebase/litebase/pkg/storage"
)

func TestRestore(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CopySourceDatabaseToTargetDatabase", func(t *testing.T) {
			source := test.MockDatabase(app)
			target := test.MockDatabase(app)
			checkpointer, _ := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).Checkpointer()
			targetDirectory := file.GetDatabaseFileDir(target.DatabaseID, target.BranchID)

			sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
			targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

			for i := 1; i <= 10; i++ {
				sourceDfs.GetRangeFile(int64(i))
			}

			err := backups.CopySourceDatabaseToTargetDatabase(
				5*storage.RangeMaxPages,
				source.DatabaseID,
				source.BranchID,
				target.DatabaseID,
				target.BranchID,
				sourceDfs,
				targetDfs,
				checkpointer,
			)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Verify that only the first 5 pages were copied
			entries, err := targetDfs.FileSystem().ReadDir(targetDirectory)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// There should be 5 entries in the target directory, including the metadata file, range index
			if len(entries) != 7 {
				t.Errorf("Expected 7 entries, got %d", len(entries))
			}
		})

		t.Run("RestoreFromTimestamp", func(t *testing.T) {
			source := test.MockDatabase(app)
			target := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()
			checkpointer, _ := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).Checkpointer()
			sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
			targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

			sourceDb, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

			// Create an initial checkpoint before creating the table (this will be restore point 0)
			sourceDb.GetConnection().Checkpoint()

			// Create a test table and insert some data
			_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			sourceDb.GetConnection().Checkpoint()

			// Insert some test data in a transaction for consistency
			sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				for range 10 {
					_, err = db.Exec(
						"INSERT INTO test (value) VALUES (?)",
						[]sqlite3.StatementParameter{{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("value"),
						}},
					)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}
				}

				return nil
			})

			sourceDb.GetConnection().Checkpoint()

			// Insert more test data in another transaction
			sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				for range 10 {
					_, err = db.Exec(
						"INSERT INTO test (value) VALUES (?)",
						[]sqlite3.StatementParameter{{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("value"),
						}},
					)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}
				}

				return nil
			})

			sourceDb.GetConnection().Checkpoint()

			// Get the snapshots
			snapshotLogger.GetSnapshots()

			// Get the latest snapshot timestamp
			snapshotKeys := snapshotLogger.Keys()

			snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(snapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
			}

			// Use the second restore point (table created but no data) for deterministic behavior
			// This avoids potential issues with restoring to a completely empty database state
			if len(snapshot.RestorePoints.Data) < 2 {
				t.Fatalf("Expected at least 2 restore points, got %d", len(snapshot.RestorePoints.Data))
			}

			restorePointTimestamp := snapshot.RestorePoints.Data[1] // Table exists but no data

			restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

			if err != nil {
				t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
			}

			var restored bool

			// Mock the onComplete function - must call the restoreFunc to complete the operation
			onComplete := func(restoreFunc func() error) error {
				restored = true
				return restoreFunc() // Actually execute the restore completion function
			}

			// Call the RestoreFromTimestamp function
			err = backups.RestoreFromTimestamp(
				app.Config,
				app.Cluster.TieredFS(),
				source.DatabaseID,
				source.BranchID,
				target.DatabaseID,
				target.BranchID,
				restorePoint.Timestamp,
				snapshotLogger,
				sourceDfs,
				targetDfs,
				checkpointer,
				onComplete,
			)

			// Check for errors
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if !restored {
				t.Fatalf("Expected onComplete to be called")
			}

			targetDb, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(targetDb)

			// Verify the data is restored correctly - should have the table but no data
			// Use a transaction like in the rolling test for consistency
			err = targetDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
				result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

				if err != nil {
					return fmt.Errorf("Expected no error, got %v", err)
				}

				if result == nil || len(result.Rows) == 0 {
					return fmt.Errorf("Expected result to have rows")
				}

				count := result.Rows[0][0].Int64()
				if count != 0 {
					return fmt.Errorf("Expected 0 rows in restored table, got %d", count)
				}

				return nil
			})

			if err != nil {
				t.Fatalf("Transaction failed: %v", err)
			}

			if targetDfs.Metadata().PageCount != restorePoint.PageCount {
				t.Fatalf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
			}
		})

		t.Run("RestoreFromTimestampWithoutCompletedCallback", func(t *testing.T) {
			source := test.MockDatabase(app)
			target := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()
			checkpointer, _ := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).Checkpointer()
			sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
			targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

			sourceDb, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

			// Create an initial checkpoint before creating the table (this will be restore point 0)
			sourceDb.GetConnection().Checkpoint()

			// Create a test table and insert some data
			_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			sourceDb.GetConnection().Checkpoint()

			// Insert some test data in a transaction for consistency
			sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				for range 10 {
					_, err = db.Exec(
						"INSERT INTO test (value) VALUES (?)",
						[]sqlite3.StatementParameter{{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("value"),
						}},
					)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}
				}

				return nil
			})

			sourceDb.GetConnection().Checkpoint()

			// Insert more test data in another transaction
			sourceDb.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				for range 10 {
					_, err = db.Exec(
						"INSERT INTO test (value) VALUES (?)",
						[]sqlite3.StatementParameter{{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("value"),
						}},
					)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}
				}

				return nil
			})

			sourceDb.GetConnection().Checkpoint()

			// Get the snapshots
			snapshotLogger.GetSnapshots()

			// Get the latest snapshot timestamp
			snapshotKeys := snapshotLogger.Keys()

			snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			if len(snapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
			}

			// Use the second restore point (table created but no data) for deterministic behavior
			// This avoids potential issues with restoring to a completely empty database state
			if len(snapshot.RestorePoints.Data) < 2 {
				t.Fatalf("Expected at least 2 restore points, got %d", len(snapshot.RestorePoints.Data))
			}

			restorePointTimestamp := snapshot.RestorePoints.Data[1] // Table exists but no data

			restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

			if err != nil {
				t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
			}

			// Call the RestoreFromTimestamp function
			err = backups.RestoreFromTimestamp(
				app.Config,
				app.Cluster.TieredFS(),
				source.DatabaseID,
				source.BranchID,
				target.DatabaseID,
				target.BranchID,
				restorePoint.Timestamp,
				snapshotLogger,
				sourceDfs,
				targetDfs,
				checkpointer,
				nil,
			)

			// Check for errors
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			targetDb, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(targetDb)

			// Verify the data is restored correctly - should have the table but no data
			// Use a transaction like in the rolling test for consistency
			err = targetDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
				result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

				if err != nil {
					return fmt.Errorf("Expected no error, got %v", err)
				}

				if result == nil || len(result.Rows) == 0 {
					return fmt.Errorf("Expected result to have rows")
				}

				count := result.Rows[0][0].Int64()
				if count != 0 {
					return fmt.Errorf("Expected 0 rows in restored table, got %d", count)
				}

				return nil
			})

			if err != nil {
				t.Fatalf("Transaction failed: %v", err)
			}

			if targetDfs.Metadata().PageCount != restorePoint.PageCount {
				t.Fatalf("Expected PageCount %d, got %d", restorePoint.PageCount, targetDfs.Metadata().PageCount)
			}
		})

		t.Run("RestoreFromInvalidBackup", func(t *testing.T) {
			source := test.MockDatabase(app)
			target := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()
			sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
			targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

			db, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table and insert some data
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Insert some test data
			db.GetConnection().Exec("BEGIN", nil)

			for range 1000 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (value) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: []byte("value"),
						},
					},
				)

				if err != nil {
					t.Errorf("Expected no error, got %v", err)
				}
			}

			db.GetConnection().Exec("COMMIT", nil)

			// Get the snapshots
			snapshotLogger.GetSnapshots()

			// Get the lastest snapshot timestamp
			snapshotKeys := snapshotLogger.Keys()

			snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if len(snapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
			}

			restorePoint, err := snapshot.GetRestorePoint(snapshot.RestorePoints.Data[0])

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Call the RestoreFromTimestamp function
			err = backups.RestoreFromBackup(
				restorePoint.Timestamp,
				source.DatabaseID,
				source.BranchID,
				target.DatabaseID,
				target.BranchID,
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

		t.Run("RestoreFromDuplicateTimestamp", func(t *testing.T) {
			for i := range 3 {
				t.Run(fmt.Sprintf("restore: %d", i), func(t *testing.T) {
					source := test.MockDatabase(app)
					target := test.MockDatabase(app)

					snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()
					checkpointer, _ := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).Checkpointer()
					sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
					targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

					db, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					defer app.DatabaseManager.ConnectionManager().Release(db)

					// Force an initial checkpoint to create a restore point representing empty database state
					err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					db.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
						// Create a test table and insert some data
						_, err = db.Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

						if err != nil {
							t.Fatalf("Expected no error, got %v", err)
						}

						return nil
					})

					err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

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

					err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

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

					err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

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

					if len(snapshot.RestorePoints.Data) == 0 {
						t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
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
						source.DatabaseID,
						source.BranchID,
						target.DatabaseID,
						target.BranchID,
						restorePoint.Timestamp,
						snapshotLogger,
						sourceDfs,
						targetDfs,
						checkpointer,
						onComplete,
					)

					// Check for errors
					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					if !restored {
						t.Fatalf("Expected onComplete to be called")
					}

					db, err = app.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					defer app.DatabaseManager.ConnectionManager().Release(db)

					// Verify the data is restored correctly
					result, err := db.GetConnection().Exec("SELECT * FROM test", nil)

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
			}
		})

		t.Run("Restore_Rolling", func(t *testing.T) {
			testCases := []struct {
				sourceCount int64
				targetCount int64
			}{
				{sourceCount: 1000, targetCount: 0},
				{sourceCount: 2000, targetCount: 1000},
				{sourceCount: 3000, targetCount: 2000},
				{sourceCount: 4000, targetCount: 3000},
			}

			restorePointIndex := -1

			source := test.MockDatabase(app)
			target := test.MockDatabase(app)

			snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).SnapshotLogger()
			checkpointer, _ := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).Checkpointer()
			sourceDfs := app.DatabaseManager.Resources(source.DatabaseID, source.BranchID).FileSystem()
			targetDfs := app.DatabaseManager.Resources(target.DatabaseID, target.BranchID).FileSystem()

			sourceDb, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

			// Create an initial checkpoint before creating the table (this will be restore point 0)
			sourceDb.GetConnection().Checkpoint()

			restorePointIndex++

			// Create a test table and insert some data
			_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			sourceDb.GetConnection().Checkpoint()

			for i, testcase := range testCases {
				t.Run(fmt.Sprintf("rolling restore: %d", i), func(t *testing.T) {
					sourceDb, _ := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.BranchID)
					defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

					// Insert some test data
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

					err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					restorePointIndex++

					// Get the snapshots
					snapshotLogger.GetSnapshots()

					// Get the lastest snapshot timestamp
					snapshotKeys := snapshotLogger.Keys()

					snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

					if err != nil {
						t.Errorf("Expected no error, got %v", err)
					}

					if len(snapshot.RestorePoints.Data) == 0 {
						t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
					}

					if restorePointIndex >= len(snapshot.RestorePoints.Data) {
						t.Fatalf("Not enough restore points for targetCount %d. Expected at least %d unique restore points, got %d",
							testcase.targetCount, restorePointIndex+1, len(snapshot.RestorePoints.Data))
					}

					restorePointTimestamp := snapshot.RestorePoints.Data[restorePointIndex]

					restorePoint, err := snapshot.GetRestorePoint(restorePointTimestamp)

					if err != nil {
						t.Fatalf("Expected no error getting restore point for timestamp %d, got %v", restorePointTimestamp, err)
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
						source.DatabaseID,
						source.BranchID,
						target.DatabaseID,
						target.BranchID,
						restorePoint.Timestamp,
						snapshotLogger,
						sourceDfs,
						targetDfs,
						checkpointer,
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

					targetDb, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.BranchID)

					if err != nil {
						t.Fatalf("Expected no error, got %v", err)
					}

					defer app.DatabaseManager.ConnectionManager().Release(targetDb)

					err = targetDb.GetConnection().Transaction(true, func(db *database.DatabaseConnection) error {
						result, err := db.Exec("SELECT COUNT(*) FROM test", nil)

						if err != nil {
							return fmt.Errorf("Expected no error, got %v - %s/%s", err, target.DatabaseID, target.BranchID)
						}

						if result != nil && result.Rows[0][0].Int64() != testcase.targetCount {
							return fmt.Errorf("Expected %d rows, got %d", testcase.targetCount, result.Rows[0][0].Int64())
						}

						return nil
					})

					if err != nil {
						t.Fatalf("Expected no error, got %v - %s/%s", err, target.DatabaseID, target.BranchID)
					}
				})
			}
		})
	})
}
