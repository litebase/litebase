package backups_test

import (
	"bytes"
	"crypto/sha1"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewRollbackLogEntry(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		timestamp := time.Now().UTC().UnixNano()
		data := []byte("test data")

		hash := sha1.New()
		hash.Write(data)
		sha1 := hash.Sum(nil)

		entry := backups.NewRollbackLogEntry(1, timestamp, data)

		if entry == nil {
			t.Fatal("PageLogEntry is nil")
		}

		if entry.Version != backups.RollbackLogVersion {
			t.Fatalf("Expected Version %d, got %d", backups.RollbackLogVersion, entry.Version)
		}

		if len(entry.Data) == 0 {
			t.Fatal("Data is nil or empty")
		}

		if entry.PageNumber != 1 {
			t.Fatalf("Expected PageNumber 1, got %d", entry.PageNumber)
		}

		if entry.Timestamp != timestamp {
			t.Fatalf("Expected Timestamp %d, got %d", timestamp, entry.Timestamp)
		}

		if !bytes.Equal(entry.SHA1, []byte(sha1)) {
			t.Fatalf("Expected SHA1 %x, got %x", sha1, entry.SHA1)
		}
	})
}

func TestPageLogEntrySerialize(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		timestamp := time.Now().UTC().UnixNano()
		data := []byte("test data")
		entry := backups.NewRollbackLogEntry(1, timestamp, data)

		// Serialize the entry
		buf := bytes.NewBuffer(make([]byte, 1024))

		serialized, err := entry.Serialize(buf)

		if err != nil {
			t.Fatalf("Failed to serialize PageLogEntry: %v", err)
		}

		// Deserialize the entry
		deserializedEntry, err := backups.DeserializeRollbackLogEntry(bytes.NewReader(serialized))

		if err != nil {
			t.Fatalf("Failed to deserialize PageLogEntry: %v", err)
		}

		// Check if the original entry and deserialized entry are the same
		if entry.PageNumber != deserializedEntry.PageNumber {
			t.Fatalf("Expected PageNumber %d, got %d", entry.PageNumber, deserializedEntry.PageNumber)
		}

		if entry.Timestamp != deserializedEntry.Timestamp {
			t.Fatalf("Expected Timestamp %d, got %d", entry.Timestamp, deserializedEntry.Timestamp)
		}

		if !bytes.Equal(entry.Data, deserializedEntry.Data) {
			t.Fatalf("Expected Data %x, got %x", entry.Data, deserializedEntry.Data)
		}

		if entry.SizeCompressed != deserializedEntry.SizeCompressed {
			t.Fatalf("Expected SizeCompressed %d, got %d", entry.SizeCompressed, deserializedEntry.SizeCompressed)
		}

		if entry.SizeDecompressed != deserializedEntry.SizeDecompressed {
			t.Fatalf("Expected SizeDecompressed %d, got %d", len(data), deserializedEntry.SizeDecompressed)
		}
	})
}
