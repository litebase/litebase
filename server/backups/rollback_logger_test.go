package backups_test

import (
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
	"time"
)

func TestNewRollbackLogger(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Check if the page logger is not nil
		if rollbackLogger == nil {
			t.Error("Expected page logger to be not nil")
		}

		// Check if the page logger has the correct DatabaseUuid and BranchUuid
		if rollbackLogger.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("Expected DatabaseUuid to be %s, got %s", mock.DatabaseUuid, rollbackLogger.DatabaseUuid)
		}

		if rollbackLogger.BranchUuid != mock.BranchUuid {
			t.Errorf("Expected BranchUuid to be %s, got %s", mock.BranchUuid, rollbackLogger.BranchUuid)
		}
	})
}

func TestPageLoggerClose(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Close the page logger
		err := rollbackLogger.Close()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestRollbackLoggerCommit(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Commit a log
		err := rollbackLogger.Commit(1234567890, 1, 100)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestPageLoggerGetLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		timestamp := time.Now().Unix()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Get a log
		rollbackLog, err := rollbackLogger.GetLog(timestamp)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if rollbackLog == nil {
			t.Error("Expected rollback log to be not nil")
		}

		startOfHourTimestamp := time.Now().Truncate(time.Hour).Unix()

		if rollbackLog.Timestamp != startOfHourTimestamp {
			t.Errorf("Expected Timestamp %d, got %d", startOfHourTimestamp, rollbackLog.Timestamp)
		}
	})
}

func TestPageLoggerLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

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
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

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
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		rollbackLogger := backups.NewRollbackLogger(mock.DatabaseUuid, mock.BranchUuid)

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
