package backups_test

import (
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
)

func TestNewPageLogger(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		pageLogger := backups.NewPageLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Check if the page logger is not nil
		if pageLogger == nil {
			t.Error("Expected page logger to be not nil")
		}

		// Check if the page logger has the correct DatabaseUuid and BranchUuid
		if pageLogger.DatabaseUuid != mock.DatabaseUuid {
			t.Errorf("Expected DatabaseUuid to be %s, got %s", mock.DatabaseUuid, pageLogger.DatabaseUuid)
		}

		if pageLogger.BranchUuid != mock.BranchUuid {
			t.Errorf("Expected BranchUuid to be %s, got %s", mock.BranchUuid, pageLogger.BranchUuid)
		}
	})
}

func TestPageLoggerClose(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		pageLogger := backups.NewPageLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Close the page logger
		err := pageLogger.Close()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}

func TestPageLoggerLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		// Create a new page logger
		pageLogger := backups.NewPageLogger(mock.DatabaseUuid, mock.BranchUuid)

		// Log a page
		err := pageLogger.Log(1, 1234567890, []byte("test data 1"))

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		err = pageLogger.Log(1, 1234567891, []byte("test data 2"))

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}
	})
}
