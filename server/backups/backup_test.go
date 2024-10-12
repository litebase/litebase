package backups_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"litebase/internal/config"
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/backups"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"sort"
	"sync"
	"testing"
	"time"
)

func TestGetBackup(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if backup == nil {
			t.Error("expected backup to be non-nil")
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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Create a backup
		_, error := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if error != nil {
			t.Errorf("expected no error, got %v", error)
		}

		nextBackup, err := backups.GetNextBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(-time.Duration(10)*time.Second).Unix(),
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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		path := backup.FilePath(1)

		// Check if the backup file exists
		if _, err := storage.ObjectFS().Stat(path); os.IsNotExist(err) {
			t.Errorf("expected backup file to exist at %s", path)
		}

		err = backup.Delete()

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Check if the backup file was deleted
		if _, err := storage.ObjectFS().Stat(path); !os.IsNotExist(err) {
			t.Errorf("expected backup file to be deleted at %s", path)
		}
	})
}

func TestBackupDirectoryPath(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId, time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expectedPath := fmt.Sprintf(
			"%s/%d",
			file.GetDatabaseBackupsDirectory(backup.DatabaseId, backup.BranchId),
			backup.RestorePoint.Timestamp,
		)

		if backup.DirectoryPath() != expectedPath {
			t.Errorf("expected %s, got %s", expectedPath, backup.DirectoryPath())
		}
	})
}

func TestBackupFilePath(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expectedPath := fmt.Sprintf(
			"%s/%s",
			backup.DirectoryPath(),
			backup.Key(1),
		)

		if backup.FilePath(1) != expectedPath {
			t.Errorf("expected %s, got %s", expectedPath, backup.FilePath(1))
		}
	})
}

func TestBackGetAndSetMaxPartSize(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId, time.Now().Unix(),
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

func TestBackupHash(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId, time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseId))
		hash.Write([]byte(backup.BranchId))
		hash.Write([]byte(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)))
		expectedHash := fmt.Sprintf("%x", hash.Sum(nil))

		if backup.Hash() != expectedHash {
			t.Errorf("expected %s, got %s", expectedHash, backup.Hash())
		}
	})
}

func TestBackupKey(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseId))
		hash.Write([]byte(backup.BranchId))
		hash.Write([]byte(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)))
		expectedKey := fmt.Sprintf("%s-1.tar.gz", hex.EncodeToString(hash.Sum(nil)))

		key := backup.Key(1)

		if key != expectedKey {
			t.Errorf("expected %s, got %s", expectedKey, key)
		}
	})
}

func TestBackupRun(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err == nil {
			t.Errorf("expected error, got nil")
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err = backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
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
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		wg := sync.WaitGroup{}

		wg.Add(2)
		go func() {
			defer wg.Done()

			_, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				time.Now().Unix(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

		}()

		go func() {
			defer wg.Done()
			time.Sleep(250 * time.Nanosecond)

			_, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				time.Now().Unix(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err == nil {
				t.Errorf("expected error, got nil")
			}
		}()

		wg.Wait()
	})
}

func TestBackupRunWithMultipleFiles(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()
		data := make([]byte, 1024)

		// Create the test files
		for i := 1; i <= 10; i++ {
			err := dfs.FileSystem().WriteFile(
				fmt.Sprintf("%s/%010d", file.GetDatabaseFileDir(mock.DatabaseId, mock.BranchId), i),
				data,
				0644,
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			dfs,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			func(backup *backups.Backup) {
				backup.SetMaxPartSize(1024)
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
		for i := 1; i <= int(backup.RestorePoint.PageCount); i++ {
			if _, err := dfs.FileSystem().Stat(backup.FilePath(i)); os.IsNotExist(err) {
				t.Errorf("expected backup file to exist at %s", backup.FilePath(i))
			}
		}
	})
}

func TestBackupRunWith1HourRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(1) * time.Hour).Unix())

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Insert 1000 rows at a time
		for i := 4; i > 0; i-- {
			// Begin a transaction
			err = db.GetConnection().SqliteConnection().Begin()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			for j := 0; j < 1000; j++ {
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", fmt.Sprintf("test-%d", j))

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			err = db.GetConnection().SqliteConnection().Commit()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(i*10) * time.Minute).Unix())

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalRestorePoints := 0

		for _, snapshot := range snapshots {
			totalRestorePoints += snapshot.RestorePoints.Total
		}

		if totalRestorePoints != 5 {
			t.Fatalf("expected 4 restore points, got %d", totalRestorePoints)
		}

		timestamps := make([]int64, 0)

		for _, s := range snapshots {
			log.Println("Timestamp", s.Timestamp)
			snapshot, err := snapshotLogger.GetSnapshot(s.Timestamp)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			timestamps = append(timestamps, snapshot.RestorePoints.Data...)
		}

		if len(timestamps) != 5 {
			t.Fatalf("expected %d restore points, got %d", 5, len(timestamps))
		}

		totalCount := 0

		// Sort the timestamps from oldest to newest
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		// Create a backup for each restore point
		for i, timestamp := range timestamps {
			backup, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				timestamp,
				snapshotLogger,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase(app)

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseId,
				mock.BranchId,
				newMock.DatabaseId,
				newMock.BranchId,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.ConnectionManager().Get(newMock.DatabaseId, newMock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(newMock.DatabaseId, newMock.BranchId, db)

			// Check if the test table exists
			results, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

			if i == 0 && err == nil {
				t.Errorf("expected error SQLite3 Error[1]: no such table: test, got nil")
			} else if i != 0 && err != nil {
				log.Println("Error", i, err)
				t.Errorf("expected no error, got %v", err)
			}

			if i != 0 && len(results.Rows) > 0 {
				count, _ := results.Rows[0][0].Int64()

				if count != int64(totalCount) {
					t.Errorf("expected %d, got %d", totalCount, count)
				}

				totalCount += 1000
			}

			oldData, _ := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseId, mock.BranchId) + "/0000000001",
			)
			newData, _ := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseId, newMock.BranchId) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith3HourRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(3) * time.Hour).Unix())

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalCount := 0

		// Insert 1000 rows at a time
		for i := 17; i > 0; i-- {
			// Begin a transaction
			err = db.GetConnection().SqliteConnection().Begin()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			for j := 0; j < 1000; j++ {
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", fmt.Sprintf("test-%d", j))

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			err = db.GetConnection().SqliteConnection().Commit()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(i*10) * time.Minute).Unix())

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalRestorePoints := 0

		for _, snapshot := range snapshots {
			totalRestorePoints += snapshot.RestorePoints.Total
		}

		if totalRestorePoints != 18 {
			t.Errorf("expected 18 restore points, got %d", totalRestorePoints)
		}

		keys := snapshotLogger.Keys()

		snapshot, err := snapshotLogger.GetSnapshot(snapshots[keys[0]].Timestamp)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a backup for each restore point
		for i, timestamp := range snapshot.RestorePoints.Data {
			backup, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				timestamp,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase(app)

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseId,
				mock.BranchId,
				newMock.DatabaseId,
				newMock.BranchId,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.ConnectionManager().Get(newMock.DatabaseId, newMock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(newMock.DatabaseId, newMock.BranchId, db)

			// Check if the test table exists
			results, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

			if i == 0 && err == nil {
				t.Errorf("expected error SQLite3 Error[1]: no such table: test, got nil")
			} else if i != 0 && err != nil {
				log.Println("Error", i, err)
				t.Errorf("expected no error, got %v", err)
			}

			if i != 0 {
				count, _ := results.Rows[0][0].Int64()

				if count != int64(totalCount) {
					t.Errorf("expected %d, got %d", totalCount, count)
				}

				totalCount += 1000
			}

			oldData, _ := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseId, mock.BranchId) + "/0000000001",
			)

			newData, _ := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseId, newMock.BranchId) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith24HourRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(24) * time.Hour).Unix())

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Insert 1000 rows at a time
		for i := (24 - 1); i > 0; i-- {
			// Begin a transaction
			err = db.GetConnection().SqliteConnection().Begin()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			for j := 0; j < 1000; j++ {
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", fmt.Sprintf("test-%d", j))

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			err = db.GetConnection().SqliteConnection().Commit()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(i) * time.Hour).Unix())

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := snapshotLogger.GetSnapshots()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalRestorePoints := 0

		for _, snapshot := range snapshots {
			totalRestorePoints += snapshot.RestorePoints.Total
		}

		if totalRestorePoints != 24 {
			t.Errorf("expected %d restore points, got %d", 24, totalRestorePoints)
		}

		timestamps := make([]int64, 0)

		for _, s := range snapshots {
			snapshot, err := snapshotLogger.GetSnapshot(s.Timestamp)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			timestamps = append(timestamps, snapshot.RestorePoints.Data...)
		}

		totalCount := 0

		// Create a backup for each restore point
		for i, timestamp := range timestamps {
			backup, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				timestamp,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase(app)

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseId,
				mock.BranchId,
				newMock.DatabaseId,
				newMock.BranchId,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.ConnectionManager().Get(newMock.DatabaseId, newMock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(newMock.DatabaseId, newMock.BranchId, db)

			// Check if the test table exists
			results, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

			if i == 0 && err == nil {
				t.Errorf("expected error SQLite3 Error[1]: no such table: test, got nil")
			} else if i != 0 && err != nil {
				log.Println("Error", i, err)
				t.Errorf("expected no error, got %v", err)
			}

			if i != 0 && len(results.Rows) > 0 {
				count, _ := results.Rows[0][0].Int64()

				if count != int64(totalCount) {
					t.Errorf("expected %d, got %d", totalCount, count)
				}

				totalCount += 1000
			}

			oldData, _ := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseId, mock.BranchId) + "/0000000001",
			)

			newData, _ := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseId, newMock.BranchId) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith7DayRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)
		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		snapshotLogger := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger()
		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Set the timestamp to 7 days ago
		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(24*7) * time.Hour).Unix())

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Insert 1000 rows at a time
		for i := ((24 * 7) - 1); i > 0; i-- {
			// Begin a transaction
			err = db.GetConnection().SqliteConnection().Begin()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			for j := 0; j < 1000; j++ {
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", fmt.Sprintf("test-%d", j))

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			err = db.GetConnection().SqliteConnection().Commit()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(i) * time.Hour).Unix())

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := snapshotLogger.GetSnapshotsWithRestorePoints()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalRestorePoints := 0

		for _, snapshot := range snapshots {
			totalRestorePoints += snapshot.RestorePoints.Total
		}

		if totalRestorePoints != (24 * 7) {
			t.Fatalf("expected %d restore points, got %d", (24 * 7), totalRestorePoints)
		}

		timestamps := make([]int64, 0)

		for _, s := range snapshots {
			snapshot, err := snapshotLogger.GetSnapshot(s.Timestamp)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			timestamps = append(timestamps, snapshot.RestorePoints.Data...)
		}

		if len(timestamps) != (24 * 7) {
			t.Fatalf("expected %d restore points, got %d", (24 * 7), len(timestamps))
		}

		totalCount := 0

		// Sort the timestamps from oldest to newest
		sort.Slice(timestamps, func(i, j int) bool {
			return timestamps[i] < timestamps[j]
		})

		// Create a backup for each restore point
		for i, timestamp := range timestamps {
			backup, err := backups.Run(
				mock.DatabaseId,
				mock.BranchId,
				timestamp,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase(app)

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseId,
				mock.BranchId,
				newMock.DatabaseId,
				newMock.BranchId,
				app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
				app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.ConnectionManager().Get(newMock.DatabaseId, newMock.BranchId)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(newMock.DatabaseId, newMock.BranchId, db)

			// Check if the test table exists
			results, err := db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

			if i == 0 && err == nil {
				t.Errorf("expected error SQLite3 Error[1]: no such table: test, got nil")
			} else if i != 0 && err != nil {
				log.Println("Error", i, err)
				t.Fatalf("expected no error, got %v", err)
			}

			if i != 0 && len(results.Rows) > 0 {
				count, _ := results.Rows[0][0].Int64()

				if count != int64(totalCount) {
					t.Fatalf("expected %d, got %d", totalCount, count)
				}

				totalCount += 1000
			}

			oldData, _ := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseId, mock.BranchId) + "/0000000001",
			)

			newData, _ := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseId, newMock.BranchId) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWithInvalidFutureRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(time.Hour).Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if err != backups.BackupErrorNoRestorePoint {
			t.Fatalf("expected %v, got %v", backups.BackupErrorNoRestorePoint, err)
		}

		if backup != nil {
			t.Fatalf("expected nil, got %v", backup)
		}
	})
}

func TestBackupRunWithInvalidPastRestorePoint(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Add(-time.Hour).Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err == nil {
			t.Fatal("expected error, got nil")
		}

		if err != backups.BackupErrorNoRestorePoint {
			t.Fatalf("expected %v, got %v", backups.BackupErrorNoRestorePoint, err)
		}

		if backup != nil {
			t.Fatalf("expected nil, got %v", backup)
		}
	})
}

func TestBackupRunWithConcurrentWrites(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(2) * time.Hour).Unix())

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create timestamps for the restore points that are one minue intervals
		// that go back 60 minutes
		timestamps := make([]int64, 0)

		for i := 60; i > 0; i-- {
			timestamps = append(timestamps, time.Now().Truncate(time.Hour).Add(-time.Duration(i)*time.Minute).Unix())
		}

		// var timestampChan = make(chan int64, len(timestamps))
		var timestampChan = make(chan int64, 1)
		var wg sync.WaitGroup
		wg.Add(2)

		// Run inserts into the database
		go func() {
			defer wg.Done()

			for _, timestamp := range timestamps {
				err = db.GetConnection().SqliteConnection().Begin()

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				for j := 0; j < 1000; j++ {
					_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "INSERT INTO test (name) VALUES (?)", fmt.Sprintf("test-%d", j))

					if err != nil {
						t.Errorf("expected no error, got %v", err)
					}
				}

				err = db.GetConnection().SqliteConnection().Commit()

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				checkpointer.SetTimestamp(timestamp)

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				timestampChan <- timestamp
			}

			close(timestampChan)
		}()

		// Run backups and restores
		go func() {
			defer wg.Done()

			for timestamp := range timestampChan {
				backup, err := backups.Run(
					mock.DatabaseId,
					mock.BranchId,
					timestamp,
					app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
					app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
					app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
				)

				if err != nil {
					log.Println("Backup Timestmap", timestamp)
					t.Errorf("expected no error, got %v", err)
				}

				if backup == nil {
					continue
				}

				// Create a new database
				newMock := test.MockDatabase(app)

				// Restore the database
				err = backups.RestoreFromBackup(
					timestamp,
					backup.Hash(),
					mock.DatabaseId,
					mock.BranchId,
					newMock.DatabaseId,
					newMock.BranchId,
					app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
					app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem(),
				)

				if err != nil {
					if err == backups.BackupErrorNoRestorePoint {
						continue
					}

					if err == backups.ErrorBackupRangeFileEmpty {
						continue
					}

					t.Errorf("expected no error, got %v", err)
				}

				db, err := app.DatabaseManager.ConnectionManager().Get(newMock.DatabaseId, newMock.BranchId)

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				// Check if the test table exists
				_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "SELECT COUNT(*) FROM test")

				if err != nil {
					t.Errorf("expected no error, got %v", err)
				}

				oldData, _ := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem().FileSystem().ReadFile(
					file.GetDatabaseFileBaseDir(mock.DatabaseId, mock.BranchId) + "/0000000001",
				)

				newData, _ := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem().FileSystem().ReadFile(
					file.GetDatabaseFileBaseDir(newMock.DatabaseId, newMock.BranchId) + "/0000000001",
				)

				if !bytes.Equal(oldData, newData) {
					t.Errorf("expected %s, got %s", oldData, newData)
				}

				app.DatabaseManager.ConnectionManager().Release(newMock.DatabaseId, newMock.BranchId, db)
			}
		}()

		wg.Wait()
	})
}

func TestBackupRunContents(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		dfs := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem()
		checkpointer, err := app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		testCases := []struct {
			timestamp int64
			pages     []int64
		}{
			{
				timestamp: time.Now().Truncate(time.Hour).Add(-time.Duration(5) * time.Minute).Unix(),
				pages:     []int64{1, 2, 3},
			},
			{
				timestamp: time.Now().Truncate(time.Hour).Add(-time.Duration(4) * time.Minute).Unix(),
				pages:     []int64{1, 2, 3, 4, 5},
			},
			{
				timestamp: time.Now().Truncate(time.Hour).Add(-time.Duration(3) * time.Minute).Unix(),
				pages:     []int64{1, 3},
			},
			{
				timestamp: time.Now().Truncate(time.Hour).Add(-time.Duration(2) * time.Minute).Unix(),
				pages:     []int64{1, 2},
			},
			{
				timestamp: time.Now().Truncate(time.Hour).Add(-time.Duration(1) * time.Minute).Unix(),
				pages:     []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
			},
		}

		for _, testCase := range testCases {
			for _, page := range testCase.pages {
				_, err := dfs.WriteAt(
					[]byte(fmt.Sprintf("page-%d", page)),
					file.PageOffset(page, config.Get().PageSize),
				)

				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}
			}

			checkpointer.SetTimestamp(testCase.timestamp)

			err = checkpointer.Begin()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			for _, page := range testCase.pages {
				checkpointer.CheckpointPage(
					page,
					[]byte(fmt.Sprintf("page-%d", page)),
				)
			}

			err = checkpointer.Commit()

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Truncate(time.Hour).Add(-time.Duration(1)*time.Minute).Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if backup == nil {
			t.Fatalf("expected nil, got %v", backup)
		}

		// Create a new database
		newMock := test.MockDatabase(app)
		targetDfs := app.DatabaseManager.Resources(newMock.DatabaseId, newMock.BranchId).FileSystem()

		// Restore the database
		err = backups.RestoreFromBackup(
			time.Now().Truncate(time.Hour).Add(-time.Duration(1)*time.Minute).Unix(),
			backup.Hash(),
			mock.DatabaseId,
			mock.BranchId,
			newMock.DatabaseId,
			newMock.BranchId,
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			targetDfs,
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Open the backup file in the new database
		if targetDfs.Metadata().PageCount != 9 {
			t.Fatalf("expected 9, got %d", targetDfs.Metadata().PageCount)
		}
	})
}

func TestBackupSize(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		size := backup.Size()

		if size <= 0 {
			t.Errorf("expected backup size to be greater than 0, got %d", size)
		}
	})
}

func TestBackupToMap(t *testing.T) {
	test.Run(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(mock.DatabaseId, mock.BranchId, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseId, mock.BranchId)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).SnapshotLogger(),
			app.DatabaseManager.Resources(mock.DatabaseId, mock.BranchId).FileSystem(),
			mock.DatabaseId,
			mock.BranchId,
			time.Now().Unix(),
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
