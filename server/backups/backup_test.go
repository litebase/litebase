package backups_test

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"litebase/internal/test"
	"litebase/server/backups"
	"litebase/server/database"
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
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if backup == nil {
			t.Error("expected backup to be non-nil")
		}

		if backup.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("expected %s, got %s", mock.DatabaseUuid, backup.DatabaseUuid)
		}

		if backup.BranchUuid != mock.BranchUuid {
			t.Errorf("expected %s, got %s", mock.BranchUuid, backup.BranchUuid)
		}
	})
}

func TestGetNextBackup(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		// Create a backup
		_, error := backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
		)

		if error != nil {
			t.Errorf("expected no error, got %v", error)
		}

		nextBackup, err := backups.GetNextBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Add(-time.Duration(10)*time.Second).Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if nextBackup == nil {
			t.Fatal("expected nextBackup to be non-nil")
		}

		if nextBackup.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("expected %s, got %s", mock.DatabaseUuid, nextBackup.DatabaseUuid)
		}

		if nextBackup.BranchUuid != mock.BranchUuid {
			t.Errorf("expected %s, got %s", mock.BranchUuid, nextBackup.BranchUuid)
		}
	})
}

func TestBackupDelete(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
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
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		expectedPath := fmt.Sprintf(
			"%s/%d",
			file.GetDatabaseBackupsDirectory(backup.DatabaseUuid, backup.BranchUuid),
			backup.RestorePoint.Timestamp,
		)

		if backup.DirectoryPath() != expectedPath {
			t.Errorf("expected %s, got %s", expectedPath, backup.DirectoryPath())
		}
	})
}

func TestBackupFilePath(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
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
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
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
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseUuid))
		hash.Write([]byte(backup.BranchUuid))
		hash.Write([]byte(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)))
		expectedHash := fmt.Sprintf("%x", hash.Sum(nil))

		if backup.Hash() != expectedHash {
			t.Errorf("expected %s, got %s", expectedHash, backup.Hash())
		}
	})
}

func TestBackupKey(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseUuid))
		hash.Write([]byte(backup.BranchUuid))
		hash.Write([]byte(fmt.Sprintf("%d", backup.RestorePoint.Timestamp)))
		expectedKey := fmt.Sprintf("%s-1.tar.gz", hex.EncodeToString(hash.Sum(nil)))

		key := backup.Key(1)

		if key != expectedKey {
			t.Errorf("expected %s, got %s", expectedKey, key)
		}
	})
}

func TestBackupRun(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
		)

		if err == nil {
			t.Errorf("expected error, got nil")
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err = backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		if backup.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("expected %s, got %s", mock.DatabaseUuid, backup.DatabaseUuid)
		}

		if backup.BranchUuid != mock.BranchUuid {
			t.Errorf("expected %s, got %s", mock.BranchUuid, backup.BranchUuid)
		}
	})
}

func TestBackupRunOnlyOneBackupAtATime(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		wg := sync.WaitGroup{}

		wg.Add(2)
		go func() {
			defer wg.Done()

			_, err := backups.Run(
				mock.DatabaseUuid,
				mock.BranchUuid,
				time.Now().Unix(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

		}()

		go func() {
			defer wg.Done()
			time.Sleep(100 * time.Nanosecond)

			_, err := backups.Run(
				mock.DatabaseUuid,
				mock.BranchUuid,
				time.Now().Unix(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err == nil {
				t.Errorf("expected error, got nil")
			}
		}()

		wg.Wait()
	})
}

func TestBackupRunWithMultipleFiles(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		dfs := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem()
		data := make([]byte, 1024)

		// Create the test files
		for i := 1; i <= 10; i++ {
			err := dfs.FileSystem().WriteFile(
				fmt.Sprintf("%s/%010d", file.GetDatabaseFileDir(mock.DatabaseUuid, mock.BranchUuid), i),
				data,
				0644,
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		}

		backup, err := backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			dfs,
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			func(backup *backups.Backup) {
				backup.SetMaxPartSize(1024)
			},
		)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		if backup.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("expected %s, got %s", mock.DatabaseUuid, backup.DatabaseUuid)
		}

		if backup.BranchUuid != mock.BranchUuid {
			t.Errorf("expected %s, got %s", mock.BranchUuid, backup.BranchUuid)
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
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		checkpointer, err := database.Resources(mock.DatabaseUuid, mock.BranchUuid).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(1) * time.Hour).Unix())

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

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

			err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalRestorePoints := 0

		for _, snapshot := range snapshots {
			totalRestorePoints += snapshot.RestorePoints.Total
		}

		if totalRestorePoints != 5 {
			t.Errorf("expected 4 restore points, got %d", totalRestorePoints)
		}

		snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, snapshots[0].Timestamp)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		totalCount := 0

		// Create a backup for each restore point
		for i, timestamp := range snapshot.RestorePoints.Data {
			backup, err := backups.Run(
				mock.DatabaseUuid,
				mock.BranchUuid,
				timestamp,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase()

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseUuid,
				mock.BranchUuid,
				newMock.DatabaseUuid,
				newMock.BranchUuid,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := database.ConnectionManager().Get(newMock.DatabaseUuid, newMock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer database.ConnectionManager().Release(newMock.DatabaseUuid, newMock.BranchUuid, db)

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

			oldData, _ := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseUuid, mock.BranchUuid) + "/0000000001",
			)
			newData, _ := database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseUuid, newMock.BranchUuid) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith3HourRestorePoint(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		checkpointer, err := database.Resources(mock.DatabaseUuid, mock.BranchUuid).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(3) * time.Hour).Unix())

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

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

			err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

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

		snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, snapshots[0].Timestamp)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a backup for each restore point
		for i, timestamp := range snapshot.RestorePoints.Data {
			backup, err := backups.Run(
				mock.DatabaseUuid,
				mock.BranchUuid,
				timestamp,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase()

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseUuid,
				mock.BranchUuid,
				newMock.DatabaseUuid,
				newMock.BranchUuid,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := database.ConnectionManager().Get(newMock.DatabaseUuid, newMock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer database.ConnectionManager().Release(newMock.DatabaseUuid, newMock.BranchUuid, db)

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

			oldData, _ := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseUuid, mock.BranchUuid) + "/0000000001",
			)

			newData, _ := database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseUuid, newMock.BranchUuid) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith24HourRestorePoint(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		checkpointer, err := database.Resources(mock.DatabaseUuid, mock.BranchUuid).Checkpointer()

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		checkpointer.SetTimestamp(time.Now().Truncate(time.Hour).Add(-time.Duration(24) * time.Hour).Unix())

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

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

			err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

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
			snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, s.Timestamp)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			timestamps = append(timestamps, snapshot.RestorePoints.Data...)
		}

		totalCount := 0

		// Create a backup for each restore point
		for i, timestamp := range timestamps {
			backup, err := backups.Run(
				mock.DatabaseUuid,
				mock.BranchUuid,
				timestamp,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase()

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseUuid,
				mock.BranchUuid,
				newMock.DatabaseUuid,
				newMock.BranchUuid,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := database.ConnectionManager().Get(newMock.DatabaseUuid, newMock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer database.ConnectionManager().Release(newMock.DatabaseUuid, newMock.BranchUuid, db)

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

			oldData, _ := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseUuid, mock.BranchUuid) + "/0000000001",
			)

			newData, _ := database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseUuid, newMock.BranchUuid) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

func TestBackupRunWith7DayRestorePoint(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		checkpointer, err := database.Resources(mock.DatabaseUuid, mock.BranchUuid).Checkpointer()

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

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

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

			err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		}

		snapshots, err := backups.GetSnapshots(mock.DatabaseUuid, mock.BranchUuid)

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
			snapshot, err := backups.GetSnapshot(mock.DatabaseUuid, mock.BranchUuid, s.Timestamp)

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
				mock.DatabaseUuid,
				mock.BranchUuid,
				timestamp,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a new database
			newMock := test.MockDatabase()

			// Restore the database
			err = backups.RestoreFromBackup(
				timestamp,
				backup.Hash(),
				mock.DatabaseUuid,
				mock.BranchUuid,
				newMock.DatabaseUuid,
				newMock.BranchUuid,
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem(),
			)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			db, err := database.ConnectionManager().Get(newMock.DatabaseUuid, newMock.BranchUuid)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			defer database.ConnectionManager().Release(newMock.DatabaseUuid, newMock.BranchUuid, db)

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

			oldData, _ := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(mock.DatabaseUuid, mock.BranchUuid) + "/0000000001",
			)

			newData, _ := database.Resources(newMock.DatabaseUuid, newMock.BranchUuid).FileSystem().FileSystem().ReadFile(
				file.GetDatabaseFileBaseDir(newMock.DatabaseUuid, newMock.BranchUuid) + "/0000000001",
			)

			if !bytes.Equal(oldData, newData) {
				t.Errorf("expected %s, got %s", oldData, newData)
			}
		}
	})
}

// TODO: Test trying to backup before an actual restore point, this should fail

// TODO: Test writing to the database while a backup is running

func TestBackupSize(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.Run(
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).RollbackLogger(),
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
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		defer database.ConnectionManager().Release(mock.DatabaseUuid, mock.BranchUuid, db)

		// Create a test table
		_, err = db.GetConnection().SqliteConnection().Exec(context.Background(), "CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		err = database.ConnectionManager().ForceCheckpoint(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backup, err := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

		backupMap := backup.ToMap()

		if backupMap["database_id"] != backup.DatabaseUuid {
			t.Errorf("expected %s, got %s", backup.DatabaseUuid, backupMap["database_id"])
		}

		if backupMap["branch_id"] != backup.BranchUuid {
			t.Errorf("expected %s, got %s", backup.BranchUuid, backupMap["branch_id"])
		}
	})
}
