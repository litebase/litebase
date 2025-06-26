package backups_test

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/server"
)

func TestRollbackLogNew(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("OpenRollbackLog", func(t *testing.T) {
			mock := test.MockDatabase(app)

			timestampAtHour := time.Now().UTC().Truncate(time.Hour).UnixNano()

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestampAtHour,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			if rollbackLog.Timestamp != timestampAtHour {
				t.Fatalf("Expected Timestamp %d, got %d", timestampAtHour, rollbackLog.Timestamp)
			}

			if rollbackLog.File == nil {
				t.Fatalf("Expected File to be initialized")
			}
		})

		t.Run("AppendFrame", func(t *testing.T) {
			mock := test.MockDatabase(app)
			timestamp := time.Now().UTC().UnixNano()

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestamp,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			offset, size, err := rollbackLog.AppendFrame(timestamp)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			if offset < 0 {
				t.Fatalf("Expected offset to be greater than 0, got %d", offset)
			}

			if size <= 0 {
				t.Fatalf("Expected size to be greater than 0, got %d", size)
			}
		})

		t.Run("AppendLog", func(t *testing.T) {
			mock := test.MockDatabase(app)
			timestamp := time.Now().UTC().UnixNano()
			pageNumber := int64(1)

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestamp,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			entry := backups.NewRollbackLogEntry(pageNumber, time.Now().UTC().UnixNano(), []byte("test data"))

			size, err := rollbackLog.AppendLog(bytes.NewBuffer([]byte{}), entry)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			if size <= 0 {
				t.Fatalf("Expected size to be greater than 0, got %d", size)
			}
		})

		t.Run("Close", func(t *testing.T) {
			mock := test.MockDatabase(app)
			timestamp := time.Now().UTC().UnixNano()

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestamp,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			err = rollbackLog.Close()

			if err != nil {
				t.Fatalf("Failed to close RollbackLog: %v", err)
			}
		})

		t.Run("Commit", func(t *testing.T) {
			mock := test.MockDatabase(app)
			timestamp := time.Now().UTC().UnixNano()
			pageNumber := int64(1)

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestamp,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			offset, size, err := rollbackLog.AppendFrame(timestamp)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			entry := backups.NewRollbackLogEntry(pageNumber, time.Now().UTC().UnixNano(), []byte("test data"))

			s, err := rollbackLog.AppendLog(bytes.NewBuffer([]byte{}), entry)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			size += int64(s)

			err = rollbackLog.Commit(offset, size)

			if err != nil {
				t.Fatalf("Failed to commit RollbackLog: %v", err)
			}

			frameHeader := make([]byte, backups.RollbackFrameHeaderSize)

			// Check the rollback log to see if the frame has been marked as committed
			n, err := rollbackLog.File.ReadAt(frameHeader, offset)

			if err != nil {
				t.Fatalf("Failed to read RollbackLog: %v", err)
			}

			if n != backups.RollbackFrameHeaderSize {
				t.Fatalf("Expected to read %d bytes, got %d", backups.RollbackFrameHeaderSize, n)
			}

			committed := binary.LittleEndian.Uint32(frameHeader[4:8])

			// Check if the frame is marked as committed
			if committed != 1 {
				t.Fatalf("Expected frame to be committed, but it is not")
			}
		})

		t.Run("ReadAfter", func(t *testing.T) {
			mock := test.MockDatabase(app)
			startOfHour := time.Now().UTC().Truncate(time.Hour)

			testCases := []struct {
				timestamp int64
				pages     []int64
			}{
				{startOfHour.Add(time.Duration(0) * time.Minute).UnixNano(), []int64{1, 2, 3}},
				{startOfHour.Add(time.Duration(1) * time.Minute).UnixNano(), []int64{1, 3, 5}},
				{startOfHour.Add(time.Duration(2) * time.Minute).UnixNano(), []int64{1, 4, 5, 6}},
				{startOfHour.Add(time.Duration(3) * time.Minute).UnixNano(), []int64{1, 6, 7, 8}},
			}

			backupLogger := backups.NewRollbackLogger(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
			)

			for _, tc := range testCases {
				offset, size, err := backupLogger.StartFrame(tc.timestamp)

				if err != nil {
					t.Fatalf("Failed to open RollbackLog: %v", err)
				}

				for _, page := range tc.pages {
					s, err := backupLogger.Log(page, tc.timestamp, []byte(fmt.Sprintf("data for page %d", page)))

					if err != nil {
						t.Fatalf("Failed to append RollbackLogEntry: %v", err)
					}

					size += int64(s)
				}

				err = backupLogger.Commit(tc.timestamp, offset, size)

				if err != nil {
					t.Fatalf("Failed to commit RollbackLog: %v", err)
				}
			}

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				startOfHour.UnixNano(),
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			for _, tc := range testCases {
				var entries []*backups.RollbackLogEntry
				rollbackLogEntriesChannel, doneChannel, errorChannel := rollbackLog.ReadForTimestamp(tc.timestamp)

			outerLoop:
				for {
					select {
					case <-doneChannel:
						break outerLoop
					case err := <-errorChannel:
						t.Fatalf("Failed to read RollbackLog: %v", err)
						break outerLoop
					case e := <-rollbackLogEntriesChannel:
						entries = append(entries, e...)
					}
				}
				afterTestCaseCount := 0

				for _, ftc := range testCases {
					if ftc.timestamp >= tc.timestamp {
						afterTestCaseCount += len(ftc.pages)
					}
				}

				if afterTestCaseCount != len(entries) {
					t.Errorf("Expected %d entries after timestamp %d, got %d", afterTestCaseCount, tc.timestamp, len(entries))
				}
			}
		})

		t.Run("Rollback", func(t *testing.T) {
			mock := test.MockDatabase(app)
			timestamp := time.Now().UTC().UnixNano()
			pageNumber := int64(1)

			rollbackLog, err := backups.OpenRollbackLog(
				app.Cluster.TieredFS(),
				mock.DatabaseID,
				mock.BranchID,
				timestamp,
			)

			if err != nil {
				t.Fatalf("Failed to open RollbackLog: %v", err)
			}

			defer rollbackLog.Close()

			offset, size, err := rollbackLog.AppendFrame(timestamp)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			entry := backups.NewRollbackLogEntry(pageNumber, time.Now().UTC().UnixNano(), []byte("test data"))

			s, err := rollbackLog.AppendLog(bytes.NewBuffer([]byte{}), entry)

			if err != nil {
				t.Fatalf("Failed to append RollbackLogEntry: %v", err)
			}

			size += int64(s)

			err = rollbackLog.Rollback(offset, size)

			if err != nil {
				t.Fatalf("Failed to rollback RollbackLog: %v", err)
			}

			// Verify that the rollback was successful
			fileinfo, err := rollbackLog.File.Stat()

			if err != nil {
				t.Fatalf("Failed to get file info: %v", err)
			}

			if fileinfo.Size() != 0 {
				t.Fatalf("Expected file size to be 0 after rollback, but got %d", fileinfo.Size())
			}
		})
	})
}
