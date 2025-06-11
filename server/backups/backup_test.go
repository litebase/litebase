package backups_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/backups"
	"github.com/litebase/litebase/server/database"
	"github.com/litebase/litebase/server/file"
	"github.com/litebase/litebase/server/sqlite3"
	"github.com/litebase/litebase/server/storage"
)

func TestGetBackup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().UnixNano(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if backup == nil {
			t.Fatal("expected backup to be non-nil")
		}

		if backup.DatabaseId != mock.DatabaseId {
			t.Errorf("expected %s, got %s", mock.DatabaseId, backup.DatabaseId)
		}

		if backup.BranchId != mock.BranchId {
			t.Errorf("expected %s, got %s", mock.BranchId, backup.BranchId)
		}
	})
}

func TestGetNextBackup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table and insert data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Insert some test data to ensure the database has actual content
		for i := 0; i < 10; i++ {
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

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Compact the page logger to move data from page logs to range files
		err = app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger().Compact(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
		)

		if err != nil {
			t.Errorf("expected no error compacting page logger, got %v", err)
		}

		// Get the naturally created snapshots from the checkpoint process
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
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

		restorePointTimestamp := latestSnapshot.RestorePoints.Data[0]

		// Create a backup using an actual restore point timestamp
		_, error := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			restorePointTimestamp,
			snapshotLogger,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if error != nil {
			t.Errorf("expected no error, got %v", error)
		}

		nextBackup, err := backups.GetNextBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(-time.Duration(10)*time.Second).UnixNano(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if nextBackup == nil {
			t.Fatal("expected nextBackup to be non-nil")
		}

		if nextBackup.DatabaseId != mock.DatabaseId {
			t.Errorf("expected %s, got %s", mock.DatabaseId, nextBackup.DatabaseId)
		}

		if nextBackup.BranchId != mock.BranchId {
			t.Errorf("expected %s, got %s", mock.BranchId, nextBackup.BranchId)
		}
	})
}

func TestBackupDelete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table and insert data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Insert some test data to ensure the database has actual content
		for i := 0; i < 10; i++ {
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

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Compact the page logger to move data from page logs to range files
		err = app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger().Compact(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
		)

		if err != nil {
			t.Errorf("expected no error compacting page logger, got %v", err)
		}

		// Get the naturally created snapshots from the checkpoint process
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
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

		restorePointTimestamp := latestSnapshot.RestorePoints.Data[0]

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			restorePointTimestamp,
			snapshotLogger,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
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
}

func TestBackupDirectoryPath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId, time.Now().UnixNano(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expectedPath := fmt.Sprintf(
			"%s%d/",
			file.GetDatabaseBackupsDirectory(backup.DatabaseId, backup.BranchId),
			backup.RestorePoint.Timestamp,
		)

		if backup.DirectoryPath() != expectedPath {
			t.Errorf("expected %s, got %s", expectedPath, backup.DirectoryPath())
		}
	})
}

func TestBackupFilePath(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().UnixNano(),
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
}

func TestBackGetAndSetMaxPartSize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId, time.Now().UnixNano(),
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
}

func TestBackupKey(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().UnixNano(),
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
}

func TestBackupRun(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table and insert data
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

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

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Compact the page logger to move data from page logs to range files
		err = app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger().Compact(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
		)

		if err != nil {
			t.Errorf("expected no error compacting page logger, got %v", err)
		}

		// Get the snapshots and find a restore point
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()

		// Test that backup fails with current timestamp (no restore point exists)
		_, err = backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().UnixNano(),
			snapshotLogger,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err == nil {
			t.Errorf("expected error, got nil")
		}

		snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

		if err != nil {
			t.Fatalf("Expected no error getting snapshots, got %v", err)
		}

		// Get the latest snapshot
		keys := snapshotLogger.Keys()
		latestSnapshot := snapshots[keys[len(keys)-1]]

		if len(latestSnapshot.RestorePoints.Data) == 0 {
			t.Fatalf("Expected at least one restore point, got %d", len(latestSnapshot.RestorePoints.Data))
		}

		restorePointTimestamp := latestSnapshot.RestorePoints.Data[0]

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			restorePointTimestamp,
			snapshotLogger,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if backup.DatabaseId != mock.DatabaseId {
			t.Errorf("expected %s, got %s", mock.DatabaseId, backup.DatabaseId)
		}

		if backup.BranchId != mock.BranchId {
			t.Errorf("expected %s, got %s", mock.BranchId, backup.BranchId)
		}
	})
}

func TestBackupRunOnlyOneBackupAtATime(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Insert some data to create actual database content
		for i := range 100 {
			_, err = db.GetConnection().SqliteConnection().Exec(
				context.Background(),
				[]byte("INSERT INTO test (name) VALUES (?)"),
				sqlite3.StatementParameter{
					Type:  sqlite3.ParameterTypeText,
					Value: fmt.Appendf(nil, "test-data-%d", i),
				},
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Compact the page logger to move data from page logs to range files
		err = app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger().Compact(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
		)

		if err != nil {
			t.Errorf("expected no error compacting page logger, got %v", err)
		}

		// Get the naturally created snapshots from the checkpoint process
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
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

		restorePointTimestamp := latestSnapshot.RestorePoints.Data[0]

		wg := sync.WaitGroup{}

		var errors []error

		wg.Add(2)
		go func() {
			defer wg.Done()

			_, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseId,
				mock.BranchId,
				restorePointTimestamp,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				errors = append(errors, err)
			}
		}()

		go func() {
			defer wg.Done()
			// time.Sleep(1 * time.Millisecond)

			_, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				mock.DatabaseId,
				mock.BranchId,
				restorePointTimestamp,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				errors = append(errors, err)
			}
		}()

		wg.Wait()

		if len(errors) != 1 {
			t.Errorf("expected one error, got %d", len(errors))
		}
	})
}

func TestBackupRunWithMultipleFiles(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

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

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Compact the page logger to move data from page logs to range files
		err = app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).PageLogger().Compact(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
		)

		if err != nil {
			t.Errorf("expected no error compacting page logger, got %v", err)
		}

		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()

		// Get the snapshots and find a restore point
		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
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

		restorePointTimestamp := snapshot.RestorePoints.Data[0]

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			restorePointTimestamp,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			dfs,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			func(backup *backups.Backup) {
				backup.SetMaxPartSize(1)
			},
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if backup.DatabaseId != mock.DatabaseId {
			t.Errorf("expected %s, got %s", mock.DatabaseId, backup.DatabaseId)
		}

		if backup.BranchId != mock.BranchId {
			t.Errorf("expected %s, got %s", mock.BranchId, backup.BranchId)
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
}

func TestBackup_Rolling(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		source := test.MockDatabase(app)

		sourceDB, err := app.DatabaseManager.ConnectionManager().Get(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(source.DatabaseId, source.BranchId, sourceDB)

		snapshotLogger := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).SnapshotLogger()

		// Create an initial checkpoint before creating the table (this ensures we have a baseline restore point)
		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = sourceDB.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalCount := 0

		// Insert 1000 rows at a time
		for range 4 {
			// Begin a transaction
			err = sourceDB.GetConnection().Transaction(false, func(db *database.DatabaseConnection) error {
				for j := range 1000 {
					_, err = db.SqliteConnection().Exec(
						context.Background(),
						[]byte("INSERT INTO test (name) VALUES (?)"),
						sqlite3.StatementParameter{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-%d", j),
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

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(source.DatabaseId, source.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Get the snapshots
			snapshotLogger.GetSnapshots()

			// Get the lastest snapshot timestamp
			snapshotKeys := snapshotLogger.Keys()

			snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if err := snapshot.Load(); err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			restorePoints := snapshot.RestorePoints.Data

			// Use the second-to-last restore point if available, but ensure we never use the initial empty database state
			var restoreTimestamp int64

			if len(restorePoints) >= 4 {
				// Use second-to-last restore point for rolling backup, but ensure it's not the empty database state
				// Skip index 0 (empty database) and use index len-2 (previous state with table)
				restoreTimestamp = restorePoints[len(restorePoints)-2]
			} else if len(restorePoints) >= 3 {
				// If we have 3 restore points, use index 2 (latest with table + data)
				restoreTimestamp = restorePoints[2]
			} else if len(restorePoints) >= 2 {
				// If we only have 2 restore points, use index 1 (table created state)
				restoreTimestamp = restorePoints[1]
			}

			// Create a backup with the selected restore point
			backup, err := backups.Run(
				app.Config,
				app.Cluster.ObjectFS(),
				source.DatabaseId,
				source.BranchId,
				restoreTimestamp,
				snapshotLogger,
				app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem(),
				app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new mock database for the target
			target := test.MockDatabase(app)

			// Restore the database to the selected restore point
			err = backups.RestoreFromBackup(
				backup.RestorePoint.Timestamp,
				source.DatabaseId,
				source.BranchId,
				target.DatabaseId,
				target.BranchId,
				app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem(),
				app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem(),
			)
			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.ConnectionManager().Get(target.DatabaseId, target.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Check if the test table exists
			results, err := db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("SELECT COUNT(*) FROM test"))

			if len(restorePoints) < 4 {
				// Less than 4 restore points, so we're using a recent restore point that should have the table
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
				if results != nil && len(results.Rows) > 0 {
					count := results.Rows[0][0].Int64()
					// If we're using the latest restore point, expect current + new batch
					// If we're using table created state, expect 0
					var expectedCount int64
					if len(restorePoints) == 3 {
						expectedCount = int64(totalCount + 1000) // Latest state
					} else {
						expectedCount = 0 // Table exists but no data
					}
					if count != expectedCount {
						t.Fatalf("expected %d, got %d", expectedCount, count)
					}
				}
			} else {
				// Multiple restore points (4+), using second-to-last restore point for rolling backup
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
			}

			// Update total count for next iteration
			totalCount += 1000

			app.DatabaseManager.ConnectionManager().Remove(target.DatabaseId, target.BranchId, db)

			// Verify the backup file content
			oldData, _ := app.DatabaseManager.Resources(source.DatabaseId, source.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(source.DatabaseId, source.BranchId) + "/0000000001",
			)

			newData, _ := app.DatabaseManager.Resources(target.DatabaseId, target.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(target.DatabaseId, target.BranchId) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}

			time.Sleep(1 * time.Millisecond)
		}
	})
}

func TestBackupRunWithInvalidFutureRestorePoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(time.Hour).UnixNano(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
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
}

func TestBackupRunWithInvalidPastRestorePoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(-time.Hour).UnixNano(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
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
}

func TestBackupSize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()

		// Get the snapshots
		snapshotLogger.GetSnapshots()

		// Get the lastest snapshot timestamp
		snapshotKeys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		backup, err := backups.Run(
			app.Config,
			app.Cluster.ObjectFS(),
			mock.DatabaseId,
			mock.BranchId,
			snapshot.RestorePoints.End,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		size := backup.Size()

		if size <= 0 {
			t.Errorf("expected backup size to be greater than 0, got %d", size)
		}
	})
}

func TestBackupToMap(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), []byte("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)"))

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.Config,
			app.Cluster.ObjectFS(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().UnixNano(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backupMap := backup.ToMap()

		if backupMap["database_id"] != backup.DatabaseId {
			t.Errorf("expected %s, got %s", backup.DatabaseId, backupMap["database_id"])
		}

		if backupMap["branch_id"] != backup.BranchId {
			t.Errorf("expected %s, got %s", backup.BranchId, backupMap["branch_id"])
		}
	})
}
