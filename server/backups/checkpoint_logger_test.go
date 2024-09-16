package backups_test

import (
	"encoding/binary"
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
	"time"
)

func TestNewCheckpointLogger(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)

		if logger == nil {
			t.Fatal("Expected logger to be created, got nil")
		}

		if logger.DatabaseUuid != mock.DatabaseUuid {
			t.Fatalf("Expected databaseUuid %s, got %s", mock.DatabaseUuid, logger.DatabaseUuid)
		}

		if logger.BranchUuid != mock.BranchUuid {
			t.Fatalf("Expected branchUuid %s, got %s", mock.BranchUuid, logger.BranchUuid)
		}
	})
}

func TestCheckpointLoggerClose(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)

		if err := logger.Close(); err != nil {
			t.Fatalf("Expected no error on close, got %v", err)
		}
	})
}

func TestCheckpointLoggerFile(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)

		file, err := logger.File()

		if err != nil {
			t.Fatalf("Expected no error on File(), got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be created, got nil")
		}
	})
}

func TestCheckpointLoggerFileAlreadyOpened(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)

		_, err := logger.File()
		if err != nil {
			t.Fatalf("Expected no error on first File() call, got %v", err)
		}

		file, err := logger.File()
		if err != nil {
			t.Fatalf("Expected no error on second File() call, got %v", err)
		}

		if file == nil {
			t.Fatal("Expected file to be created, got nil")
		}
	})
}

func TestCheckpointLoggerLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		logger := backups.NewCheckpointLogger(mock.DatabaseUuid, mock.BranchUuid)
		timestamps := make([]uint64, 0)

		for i := 0; i < 10; i++ {
			// Timestamps sub seconds to avoid collisions
			timestamp := uint64(time.Now().Add(time.Duration(10-i) * time.Second).UnixNano())
			timestamps = append(timestamps, timestamp)
			err := logger.Log(timestamp, uint32(i))

			if err != nil {
				t.Fatalf("Expected no error on File(), got %v", err)
			}
		}

		// read the file to verify the logs were written
		file, err := logger.File()

		if err != nil {
			t.Fatalf("Expected no error on File(), got %v", err)
		}

		entry := make([]byte, 64)

		for i, timestamp := range timestamps {
			_, err := file.Read(entry)

			if err != nil {
				break
			}

			if len(entry) == 0 {
				break
			}

			entryTimestamp := binary.LittleEndian.Uint64(entry[0:8])

			if entryTimestamp != timestamp {
				t.Fatal("Expected valid log entry, got nil")
			}

			pageCount := binary.LittleEndian.Uint32(entry[8:12])

			if pageCount != uint32(i) {
				t.Fatal("Expected valid log entry, got nil")
			}
		}
	})
}
