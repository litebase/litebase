package backups_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/server"
)

func TestRollbackLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {

		t.Run("NewRollbackLogger", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			// Check if the page logger is not nil
			if rollbackLogger == nil {
				t.Fatal("Expected page logger to be not nil")
			}

			// Check if the page logger has the correct DatabaseID and BranchID
			if rollbackLogger.DatabaseID != mock.DatabaseID {
				t.Errorf("Expected DatabaseID to be %s, got %s", mock.DatabaseID, rollbackLogger.DatabaseID)
			}

			if rollbackLogger.BranchID != mock.DatabaseBranchID {
				t.Errorf("Expected BranchID to be %s, got %s", mock.DatabaseBranchID, rollbackLogger.BranchID)
			}
		})

		t.Run("Close", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			// Close the page logger
			err := rollbackLogger.Close()

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})

		t.Run("Commit", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			offset, _, err := rollbackLogger.StartFrame(1234567890)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			size, err := rollbackLogger.Log(1, 1234567890, []byte("test data 1"))

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Commit a log
			err = rollbackLogger.Commit(1234567890, offset, size)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})

		t.Run("TestPageLoggerGetLog", func(t *testing.T) {
			mock := test.MockDatabase(app)

			timestamp := time.Now().UTC().UnixNano()

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			// Get a log
			rollbackLog, err := rollbackLogger.GetLog(timestamp)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if rollbackLog == nil {
				t.Fatal("Expected rollback log to be not nil")
			}

			startOfHourTimestamp := time.Now().UTC().Truncate(time.Hour).UnixNano()

			if rollbackLog.Timestamp != startOfHourTimestamp {
				t.Errorf("Expected Timestamp %d, got %d", startOfHourTimestamp, rollbackLog.Timestamp)
			}
		})

		t.Run("Log", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			// Log a page
			size, err := rollbackLogger.Log(1, 1234567890, []byte("test data 1"))

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if size <= 0 {
				t.Errorf("Expected size to be greater than 0, got %d", size)
			}

			size, err = rollbackLogger.Log(1, 1234567891, []byte("test data 2"))

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if size <= 0 {
				t.Errorf("Expected size to be greater than 0, got %d", size)
			}
		})

		t.Run("Rollback", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			offset, size, err := rollbackLogger.StartFrame(1234567890)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			// Log a page
			s, err := rollbackLogger.Log(1, 1234567890, []byte("test data 1"))

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			size += s

			// Rollback
			err = rollbackLogger.Rollback(1234567890, offset, size)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}
		})

		t.Run("StartFrame", func(t *testing.T) {
			mock := test.MockDatabase(app)

			// Create a new page logger
			rollbackLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
			)

			offset, size, err := rollbackLogger.StartFrame(1234567890)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if offset < 0 {
				t.Errorf("Expected offset to be >= 0, got %d", offset)
			}

			if size < 0 {
				t.Errorf("Expected size to be >= 0, got %d", size)
			}
		})
	})
}
