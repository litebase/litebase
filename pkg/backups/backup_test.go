package backups_test

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestBackup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("GetBackup", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.GetBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				time.Now().UTC().UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			if backup == nil {
				t.Fatal("expected backup to be non-nil")
			}

			if backup.DatabaseID != mock.DatabaseID {
				t.Errorf("expected %s, got %s", mock.DatabaseID, backup.DatabaseID)
			}

			if backup.DatabaseBranchID != mock.DatabaseBranchID {
				t.Errorf("expected %s, got %s", mock.DatabaseBranchID, backup.DatabaseBranchID)
			}
		})

		t.Run("GetNextBackup", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table and insert data
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Insert some test data to ensure the database has actual content
			for i := range 10 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-record-%d", i),
						},
					},
				)

				if err != nil {
					t.Fatalf("expected no error inserting data, got %v", err)
				}
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Compact the page logger to move data from page logs to range files
			err = app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).PageLogger().Compact(
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Errorf("expected no error compacting page logger, got %v", err)
			}

			// Get the naturally created snapshots from the checkpoint process
			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

			if err != nil {
				t.Fatalf("expected no error getting snapshots, got %v", err)
			}

			if len(snapshots) == 0 {
				t.Fatalf("Expected at least one snapshot, got %d", len(snapshots))
			}

			// Get the latest snapshot
			keys := snapshotLogger.Keys()
			latestSnapshot := snapshots[keys[len(keys)-1]]

			if len(latestSnapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(latestSnapshot.RestorePoints.Data))
			}

			// Create a backup using an actual restore point timestamp
			_, error := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				snapshotLogger,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if error != nil {
				t.Errorf("expected no error, got %v", error)
			}

			nextBackup, err := backups.GetNextBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				time.Now().UTC().Add(-time.Duration(10)*time.Second).UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			if nextBackup == nil {
				t.Fatal("expected nextBackup to be non-nil")
			}

			if nextBackup.DatabaseID != mock.DatabaseID {
				t.Errorf("expected %s, got %s", mock.DatabaseID, nextBackup.DatabaseID)
			}

			if nextBackup.DatabaseBranchID != mock.DatabaseBranchID {
				t.Errorf("expected %s, got %s", mock.DatabaseBranchID, nextBackup.DatabaseBranchID)
			}
		})

		t.Run("Delete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table and insert data
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Insert some test data to ensure the database has actual content
			for i := range 10 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-record-%d", i),
						},
					},
				)
				if err != nil {
					t.Fatalf("expected no error inserting data, got %v", err)
				}
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Compact the page logger to move data from page logs to range files
			err = app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).PageLogger().Compact(
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Errorf("expected no error compacting page logger, got %v", err)
			}

			// Get the naturally created snapshots from the checkpoint process
			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

			if err != nil {
				t.Fatalf("Expected no error getting snapshots, got %v", err)
			}

			if len(snapshots) == 0 {
				t.Fatalf("Expected at least one snapshot, got %d", len(snapshots))
			}

			// Get the latest snapshot
			keys := snapshotLogger.Keys()
			latestSnapshot := snapshots[keys[len(keys)-1]]

			if len(latestSnapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(latestSnapshot.RestorePoints.Data))
			}

			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				snapshotLogger,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if backup == nil {
				t.Fatal("expected backup to be non-nil")
			}

			path := backup.FilePath(1)

			// Check if the backup file exists
			if _, err := app.Cluster.ObjectFS().Stat(path); os.IsNotExist(err) {
				t.Errorf("expected backup file to exist at %s", path)
			}

			err = backup.Delete()

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Check if the backup file was deleted
			if _, err := app.Cluster.ObjectFS().Stat(path); !os.IsNotExist(err) {
				t.Errorf("expected backup file to be deleted at %s", path)
			}
		})

		t.Run("DirectoryPath", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.GetBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID, time.Now().UTC().UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			expectedPath := fmt.Sprintf(
				"%s%d/",
				file.GetDatabaseBackupsDirectory(backup.DatabaseID, backup.DatabaseBranchID),
				backup.RestorePoint.Timestamp,
			)

			if backup.DirectoryPath() != expectedPath {
				t.Errorf("expected %s, got %s", expectedPath, backup.DirectoryPath())
			}
		})

		t.Run("FilePath", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.GetBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				time.Now().UTC().UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			expectedPath := fmt.Sprintf(
				"%s%s",
				backup.DirectoryPath(),
				backup.Key(1),
			)

			if backup.FilePath(1) != expectedPath {
				t.Errorf("expected %s, got %s", expectedPath, backup.FilePath(1))
			}
		})

		t.Run("GetAndSetMaxPartSize", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.GetBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID, time.Now().UTC().UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			if backup.GetMaxPartSize() != backups.BACKUP_MAX_PART_SIZE {
				t.Errorf("expected %d, got %d", backups.BACKUP_MAX_PART_SIZE, backup.GetMaxPartSize())
			}

			backup.SetMaxPartSize(1024)

			if backup.GetMaxPartSize() != 1024 {
				t.Errorf("expected 1024, got %d", backup.GetMaxPartSize())
			}
		})

		t.Run("Key", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.GetBackup(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				time.Now().UTC().UnixNano(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Since only one backup exists per directory, we use simple naming
			expectedKey := "backup-1.tar.gz"

			key := backup.Key(1)

			if key != expectedKey {
				t.Errorf("expected %s, got %s", expectedKey, key)
			}
		})

		t.Run("Run", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table and insert data
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Insert some test data to ensure the database has actual content
			for i := range 10 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-record-%d", i),
						},
					},
				)

				if err != nil {
					t.Fatalf("expected no error inserting data, got %v", err)
				}
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Compact the page logger to move data from page logs to range files
			err = app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).PageLogger().Compact(
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Errorf("expected no error compacting page logger, got %v", err)
			}

			// Get the snapshots and find a restore point
			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()

			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				snapshotLogger,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if backup.DatabaseID != mock.DatabaseID {
				t.Errorf("expected %s, got %s", mock.DatabaseID, backup.DatabaseID)
			}

			if backup.DatabaseBranchID != mock.DatabaseBranchID {
				t.Errorf("expected %s, got %s", mock.DatabaseBranchID, backup.DatabaseBranchID)
			}
		})

		t.Run("RunOnlyOneBackupAtATime", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Insert some data to create actual database content
			for i := range 100 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-data-%d", i),
						},
					},
				)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Compact the page logger to move data from page logs to range files
			err = app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).PageLogger().Compact(
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Errorf("expected no error compacting page logger, got %v", err)
			}

			// Get the naturally created snapshots from the checkpoint process
			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

			if err != nil {
				t.Fatalf("Expected no error getting snapshots, got %v", err)
			}

			if len(snapshots) == 0 {
				t.Fatalf("Expected at least one snapshot, got %d", len(snapshots))
			}

			// Get the latest snapshot
			keys := snapshotLogger.Keys()
			latestSnapshot := snapshots[keys[len(keys)-1]]

			if len(latestSnapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(latestSnapshot.RestorePoints.Data))
			}

			wg := sync.WaitGroup{}

			var errors []error
			var bkps []*backups.Backup

			wg.Add(2)
			go func() {
				defer wg.Done()

				backup, err := backups.Run(
					app.Config,
					app.Cluster.ObjectFS(),
					mock.DatabaseID,
					mock.DatabaseBranchID,
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
				)

				if err != nil {
					errors = append(errors, err)
				} else {
					bkps = append(bkps, backup)
				}
			}()

			go func() {
				defer wg.Done()

				backup, err := backups.Run(
					app.Config,
					app.Cluster.ObjectFS(),
					mock.DatabaseID,
					mock.DatabaseBranchID,
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
				)

				if err != nil {
					errors = append(errors, err)
				} else {
					bkps = append(bkps, backup)
				}
			}()

			wg.Wait()

			if len(errors) != 1 {
				t.Errorf("expected one error, got %d", len(errors))
			}

			if len(bkps) > 1 {
				t.Errorf("expected one backup, got %d", len(bkps))

				for _, backup := range bkps {
					t.Log("Backup created successfully:", backup.RestorePoint.Timestamp)
				}

				if bkps[0].RestorePoint.Timestamp == bkps[1].RestorePoint.Timestamp {
					t.Errorf("expected different restore points, got %d and %d", bkps[0].RestorePoint.Timestamp, bkps[1].RestorePoint.Timestamp)
				}
			} else if len(bkps) == 1 && len(errors) != 1 {
				t.Errorf("expected one backup and one error, got %d backups and %d errors", len(bkps), len(errors))
			}
		})

		t.Run("RunWithMultipleFiles", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Insert some data to create actual database content
			for i := range 1000 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-data-%d", i),
						},
					},
				)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			// Compact the page logger to move data from page logs to range files
			err = app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).PageLogger().Compact(
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Errorf("expected no error compacting page logger, got %v", err)
			}

			dfs := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem()

			// Get the snapshots and find a restore point
			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()
			snapshotLogger.GetSnapshots()
			snapshotKeys := snapshotLogger.Keys()

			if len(snapshotKeys) == 0 {
				t.Fatalf("Expected at least one snapshot, got %d", len(snapshotKeys))
			}

			snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

			if err != nil {
				t.Fatalf("Expected no error getting snapshot, got %v", err)
			}

			if len(snapshot.RestorePoints.Data) == 0 {
				t.Fatalf("Expected at least one restore point, got %d", len(snapshot.RestorePoints.Data))
			}

			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				dfs,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
				func(backup *backups.Backup) {
					backup.SetMaxPartSize(1)
				},
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			if backup.DatabaseID != mock.DatabaseID {
				t.Errorf("expected %s, got %s", mock.DatabaseID, backup.DatabaseID)
			}

			if backup.DatabaseBranchID != mock.DatabaseBranchID {
				t.Errorf("expected %s, got %s", mock.DatabaseBranchID, backup.DatabaseBranchID)
			}

			// Check if the backup files exist
			entries, err := dfs.FileSystem().ReadDir(backup.DirectoryPath())
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			entryCount := len(entries)

			if entryCount < 2 {
				t.Fatalf("expected at least 2 backup files, got %d", entryCount)
			}
		})

		t.Run("Rolling", func(t *testing.T) {
			source := test.MockDatabase(app)

			sourceDB, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseID, source.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDB)

			snapshotLogger := app.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).SnapshotLogger()

			// Create an initial checkpoint before creating the table (this ensures we have a baseline restore point)
			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a test table
			_, err = sourceDB.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			totalCount := 1000

			// Insert 1000 rows at a time
			for range 4 {
				// Begin a transaction
				err = sourceDB.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
					for j := range 1000 {
						_, err = db.Exec(
							"INSERT INTO test (name) VALUES (?)",
							[]sqlite3.StatementParameter{
								{
									Type:  sqlite3.ParameterTypeText,
									Value: fmt.Appendf(nil, "test-%d", j),
								},
							},
						)

						if err != nil {
							return err
						}
					}

					return nil
				})

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseID, source.DatabaseBranchID)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				// Create a backup with the selected restore point
				backup, err := backups.Run(
					app.Config,
					app.Cluster.ObjectFS(),
					source.DatabaseID,
					source.DatabaseBranchID,
					snapshotLogger,
					app.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).FileSystem(),
					app.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).RollbackLogger(),
				)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				// Create a new mock database for the target
				target := test.MockDatabase(app)

				// Restore the database to the selected restore point
				err = backups.RestoreFromBackup(
					backup.RestorePoint.Timestamp,
					source.DatabaseID,
					source.DatabaseBranchID,
					target.DatabaseID,
					target.DatabaseBranchID,
					app.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).FileSystem(),
					app.DatabaseManager.Resources(target.DatabaseID, target.DatabaseBranchID).FileSystem(),
				)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				db, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseID, target.DatabaseBranchID)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				// Check if the test table exists
				results, err := db.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				if results != nil && len(results.Rows) > 0 {
					count := results.Rows[0][0].Int64()
					// We're using second-to-last restore point, so expect totalCount (previous state)
					expectedCount := int64(totalCount)

					if count != expectedCount {
						t.Errorf("expected %d, got %d", expectedCount, count)
					}
				}

				// Update total count for next iteration
				totalCount += 1000

				app.DatabaseManager.ConnectionManager().Remove(target.DatabaseID, target.DatabaseBranchID, db)

				// Verify the backup file content
				oldData, _ := app.DatabaseManager.Resources(source.DatabaseID, source.DatabaseBranchID).FileSystem().FileSystem().ReadFile(
					file.GetDatabaseFileBaseDir(source.DatabaseID, source.DatabaseBranchID) + "/0000000001",
				)

				newData, _ := app.DatabaseManager.Resources(target.DatabaseID, target.DatabaseBranchID).FileSystem().FileSystem().ReadFile(
					file.GetDatabaseFileBaseDir(target.DatabaseID, target.DatabaseBranchID) + "/0000000001",
				)

				if !bytes.Equal(oldData, newData) {
					t.Errorf("expected %s, got %s", oldData, newData)
				}

				time.Sleep(1 * time.Millisecond)
			}
		})

		t.Run("RunWithEmptyDatabase", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err == nil {
				t.Fatal("expected error, got nil")
			}

			if err != backups.ErrBackupNoRestorePoint {
				t.Fatalf("expected %v, got %v", backups.ErrBackupNoRestorePoint, err)
			}

			if backup != nil {
				t.Fatalf("expected nil, got %v", backup)
			}
		})

		t.Run("BackupSize", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			size := backup.GetSize()

			if size <= 0 {
				t.Errorf("expected backup size to be greater than 0, got %d", size)
			}
		})

		t.Run("ListBackups", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(db)

			snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger()

			// Create an initial checkpoint before creating the table
			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a test table
			_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			var createdBackups []*backups.Backup

			// Create 10 different backups by inserting data and creating backups in each iteration
			for i := range 10 {
				// Insert some test data to ensure the database has actual content
				for j := range 100 {
					_, err = db.GetConnection().Exec(
						"INSERT INTO test (name) VALUES (?)",
						[]sqlite3.StatementParameter{
							{
								Type:  sqlite3.ParameterTypeText,
								Value: fmt.Appendf(nil, "test-data-backup-%d-record-%d", i, j),
							},
						},
					)

					if err != nil {
						t.Fatalf("expected no error inserting data, got %v", err)
					}
				}

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				// Create a backup
				backup, err := backups.Run(
					app.Config,
					app.Cluster.ObjectFS(),
					mock.DatabaseID,
					mock.DatabaseBranchID,
					snapshotLogger,
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
					app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
				)

				if err != nil {
					t.Fatalf("expected no error creating backup %d, got %v", i, err)
				}

				createdBackups = append(createdBackups, backup)

				// Add a small delay to ensure different timestamps
				time.Sleep(10 * time.Millisecond)
			}

			// Now test ListBackups
			listedBackups, err := backups.ListBackups(
				app.Config,
				app.Cluster.ObjectFS(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			if err != nil {
				t.Fatalf("expected no error listing backups, got %v", err)
			}

			// Verify we have exactly 10 backups
			if len(listedBackups) != 10 {
				t.Errorf("expected 10 backups, got %d", len(listedBackups))
			}

			// Verify each listed backup matches the created backups
			for i, listedBackup := range listedBackups {
				if listedBackup.DatabaseID != mock.DatabaseID {
					t.Errorf("expected %s, got %s for backup %d", mock.DatabaseID, listedBackup.DatabaseID, i)
				}

				if listedBackup.DatabaseBranchID != mock.DatabaseBranchID {
					t.Errorf("expected %s, got %s for backup %d", mock.DatabaseBranchID, listedBackup.DatabaseBranchID, i)
				}

				if listedBackup.RestorePoint.Timestamp != createdBackups[i].RestorePoint.Timestamp {
					t.Errorf("expected restore point timestamp %d, got %d for backup %d",
						createdBackups[i].RestorePoint.Timestamp, listedBackup.RestorePoint.Timestamp, i)
				}

				if listedBackup.GetSize() <= 0 {
					t.Errorf("expected backup size to be greater than 0 for backup %d, got %d", i, listedBackup.GetSize())
				}
			}
		})
	})
}
