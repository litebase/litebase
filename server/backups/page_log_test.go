package backups_test

import (
	"bytes"
	"litebase/internal/test"
	"litebase/server/backups"
	"testing"
	"time"
)

func TestOpenPageLog(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()

		pageNumber := uint32(1)

		pageLog, err := backups.OpenPageLog(mock.DatabaseUuid, mock.BranchUuid, pageNumber)

		if err != nil {
			t.Fatalf("Failed to open PageLog: %v", err)
		}

		defer pageLog.Close()

		if pageLog.PageNumber != pageNumber {
			t.Fatalf("Expected PageNumber %d, got %d", pageNumber, pageLog.PageNumber)
		}
	})
}

func TestPageLogAppend(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		pageNumber := uint32(1)

		pageLog, err := backups.OpenPageLog(mock.DatabaseUuid, mock.BranchUuid, pageNumber)

		if err != nil {
			t.Fatalf("Failed to open PageLog: %v", err)
		}

		defer pageLog.Close()

		entry := backups.NewPageLogEntry(pageNumber, uint64(time.Now().Unix()), []byte("test data"))

		err = pageLog.Append(bytes.NewBuffer([]byte{}), entry)

		if err != nil {
			t.Fatalf("Failed to append PageLogEntry: %v", err)
		}
	})
}

func TestPageLogClose(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		pageNumber := uint32(1)

		pageLog, err := backups.OpenPageLog(mock.DatabaseUuid, mock.BranchUuid, pageNumber)

		if err != nil {
			t.Fatalf("Failed to open PageLog: %v", err)
		}

		err = pageLog.Close()

		if err != nil {
			t.Fatalf("Failed to close PageLog: %v", err)
		}
	})
}

func TestPageLogReader(t *testing.T) {
	test.Run(t, func() {
		mock := test.MockDatabase()
		pageNumber := uint32(1)

		pageLog, err := backups.OpenPageLog(mock.DatabaseUuid, mock.BranchUuid, pageNumber)

		if err != nil {
			t.Fatalf("Failed to open PageLog: %v", err)
		}

		defer pageLog.Close()

		entries := make([]*backups.PageLogEntry, 0)

		for i := 0; i < 5; i++ {
			entry := backups.NewPageLogEntry(pageNumber, uint64(time.Now().Unix()), []byte("test data"))
			entries = append(entries, entry)

			err = pageLog.Append(bytes.NewBuffer([]byte{}), entry)

			if err != nil {
				t.Fatalf("Failed to append PageLogEntry: %v", err)
			}
		}

		readerEntries, readerError := pageLog.Reader()

		if readerError != nil {
			t.Fatalf("Error reading PageLog: %v", readerError)
		}

		for i, entry := range readerEntries {
			if entry.PageNumber != entries[i].PageNumber {
				t.Fatalf("Expected PageNumber %d, got %d", entries[i].PageNumber, entry.PageNumber)
			}
		}
	})
}
