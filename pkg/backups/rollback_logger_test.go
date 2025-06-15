package backups_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/server"
)

func TestNewRollbackLogger(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)

		// Check if the page logger is not nil
		if rollbackLogger == nil {
			t.Fatal("Expected page logger to be not nil")
		}

		// Check if the page logger has the correct DatabaseId and BranchId
		if rollbackLogger.DatabaseId != mock.DatabaseId {
			t.Errorf("Expected DatabaseId to be %s, got %s", mock.DatabaseId, rollbackLogger.DatabaseId)
		}

		if rollbackLogger.BranchId != mock.BranchId {
			t.Errorf("Expected BranchId to be %s, got %s", mock.BranchId, rollbackLogger.BranchId)
		}
	})
}

func TestPageLoggerClose(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)

		// Close the page logger
		err := rollbackLogger.Close()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestRollbackLoggerCommit(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
		)

		offset, size, err := rollbackLogger.StartFrame(1234567890)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		size, err = rollbackLogger.Log(1, 1234567890, []byte("test data 1"))

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Commit a log
		err = rollbackLogger.Commit(1234567890, offset, size)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestPageLoggerGetLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		timestamp := time.Now().UTC().UnixNano()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
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
}

func TestRollbackLoggerLog(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
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
}

func TestRollbackLoggerRollback(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
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
}

func TestRollbackLoggerStartFrame(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(
			app.Cluster.TieredFS(),
			mock.DatabaseId,
			mock.BranchId,
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
}
