package backups_test

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"litebase/internal/test"
	"litebase/server/backups"
	"litebase/server/database"
	"litebase/server/file"
	"litebase/server/storage"
	"os"
	"sync"
	"testing"
	"time"
)

func TestGetBackup(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
		)

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
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
		)

		if error != nil {
			t.Errorf("expected no error, got %v", error)
		}

		nextBackup := backups.GetNextBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Add(-time.Duration(10)*time.Second).Unix(),
		)

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
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
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

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

		expectedPath := fmt.Sprintf(
			"%s/%d",
			file.GetDatabaseBackupsDirectory(backup.DatabaseUuid, backup.BranchUuid),
			backup.SnapshotTimestamp,
		)

		if backup.DirectoryPath() != expectedPath {
			t.Errorf("expected %s, got %s", expectedPath, backup.DirectoryPath())
		}
	})
}

func TestBackupFilePath(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

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

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

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

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseUuid))
		hash.Write([]byte(backup.BranchUuid))
		hash.Write([]byte(fmt.Sprintf("%d", backup.SnapshotTimestamp)))
		expectedHash := fmt.Sprintf("%x", hash.Sum(nil))

		if backup.Hash() != expectedHash {
			t.Errorf("expected %s, got %s", expectedHash, backup.Hash())
		}
	})
}

func TestBackupKey(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid, time.Now().Unix(),
		)

		hash := sha1.New()
		hash.Write([]byte(backup.DatabaseUuid))
		hash.Write([]byte(backup.BranchUuid))
		hash.Write([]byte(fmt.Sprintf("%d", backup.SnapshotTimestamp)))
		expectedKey := fmt.Sprintf("%s-1.zip", hex.EncodeToString(hash.Sum(nil)))

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
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
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
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				mock.DatabaseUuid,
				mock.BranchUuid,
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}

		}()

		go func() {
			defer wg.Done()
			time.Sleep(100 * time.Nanosecond)

			_, err := backups.Run(
				database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
				mock.DatabaseUuid,
				mock.BranchUuid,
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
		dfs := database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem()
		data := make([]byte, 1024)

		// Create the test files
		for i := 0; i < 10; i++ {
			err := dfs.FileSystem().WriteFile(
				fmt.Sprintf("%s/%d.txt", file.GetDatabaseFileDir(mock.DatabaseUuid, mock.BranchUuid), i),
				data,
				0644,
			)

			if err != nil {
				t.Errorf("expected no error, got %v", err)
			}
		}

		backup, err := backups.Run(
			dfs,
			mock.DatabaseUuid,
			mock.BranchUuid,
			func(backup *backups.Backup) {
				backup.SetMaxPartSize(1024)
			},
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

		// Check if the backup files exist
		for i := 1; i <= 10; i++ {
			if _, err := dfs.FileSystem().Stat(backup.FilePath(i)); os.IsNotExist(err) {
				t.Errorf("expected backup file to exist at %s", backup.FilePath(i))
			}
		}
	})
}

func TestBackupSize(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		db, err := database.ConnectionManager().Get(mock.DatabaseUuid, mock.BranchUuid)

		if err != nil {
			t.Errorf("expected no error, got %v", err)
		}

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
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
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

		backup := backups.GetBackup(
			database.Resources(mock.DatabaseUuid, mock.BranchUuid).FileSystem(),
			mock.DatabaseUuid,
			mock.BranchUuid,
			time.Now().Unix(),
		)

		backupMap := backup.ToMap()

		if backupMap["database_id"] != backup.DatabaseUuid {
			t.Errorf("expected %s, got %s", backup.DatabaseUuid, backupMap["database_id"])
		}

		if backupMap["branch_id"] != backup.BranchUuid {
			t.Errorf("expected %s, got %s", backup.BranchUuid, backupMap["branch_id"])
		}
	})
}
