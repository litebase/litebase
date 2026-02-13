package database_test

import (
	"bytes"
	"crypto/rand"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/memory"
	"github.com/litebase/litebase/pkg/server"
)

// TestDatabaseWAL_CacheEvictionWithTxnBuffer verifies behavior when the WAL
// cache is much smaller than the number of pages written inside a transaction.
// It ensures writes buffered in the transaction are flushed to the WAL file
// and remain readable even when the in-memory WAL cache must evict pages.
func TestDatabaseWAL_CacheEvictionWithTxnBuffer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock := test.MockDatabase(app)

		walManager, _ := app.DatabaseManager.Resources(mock.Branch).DatabaseWALManager()

		wal := database.NewDatabaseWAL(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			mock.DatabaseID,
			mock.DatabaseBranchID,
			app.Cluster.LocalFS(),
			walManager,
			time.Now().UTC().UnixNano(),
		)

		// Replace the WAL in-memory cache with a tiny one to force eviction.
		// This simulates a low-memory/capacity scenario. Use the package helper
		// so tests don't need to access unexported fields.
		small := memory.NewManagedLRUCache(memory.ManagedLRUCacheConfig{
			Capacity:    2, // only 2 pages
			Manager:     app.Cluster.MemoryManager,
			DefaultSize: 4096,
			Owner:       "test-wal-small-cache",
		})

		database.SetWALCacheForTest(wal, small)

		// Begin an explicit transaction so writes go into the TransactionBuffer.
		connID := "tx-conn"

		if err := wal.Begin(connID); err != nil {
			t.Fatalf("Begin failed: %v", err)
		}

		// Prepare and write multiple pages (more than cache capacity)
		pages := 10
		pageSize := 4096
		written := make([][]byte, pages)

		for i := 0; i < pages; i++ {
			b := make([]byte, pageSize)
			if _, err := rand.Read(b); err != nil {
				t.Fatalf("rand.Read failed: %v", err)
			}

			written[i] = make([]byte, pageSize)
			copy(written[i], b)

			n, err := wal.WriteAt(connID, b, int64(i*pageSize))

			if err != nil {
				t.Fatalf("WriteAt failed for page %d: %v", i, err)
			}
			if n != pageSize {
				t.Fatalf("expected %d bytes written, got %d", pageSize, n)
			}
		}

		// End transaction to flush buffered writes to the WAL (and into cache)
		if err := wal.End(connID); err != nil {
			t.Fatalf("End failed: %v", err)
		}

		// Ensure sync to push to underlying storage
		if err := wal.Sync(); err != nil {
			t.Fatalf("Sync failed: %v", err)
		}

		// Now read back every page via the WAL ReadAt path which consults cache/file.
		readBuf := make([]byte, pageSize)

		for i := 0; i < pages; i++ {
			// Use a different connection id to ensure reads don't hit txnBuffer
			n, err := wal.ReadAt("reader", readBuf, int64(i*pageSize))

			if err != nil {
				t.Fatalf("ReadAt failed for page %d: %v", i, err)
			}

			if n != pageSize {
				t.Fatalf("expected %d bytes read, got %d for page %d", pageSize, n, i)
			}
			if !bytes.Equal(readBuf, written[i]) {
				t.Fatalf("data mismatch for page %d", i)
			}
		}

		// Also verify underlying file contents directly
		file, err := wal.File()

		if err != nil {
			t.Fatalf("File() failed: %v", err)
		}

		// Read sequentially from file to ensure no corruption
		for i := 0; i < pages; i++ {
			buf := make([]byte, pageSize)

			if _, err := file.ReadAt(buf, int64(i*pageSize)); err != nil {
				t.Fatalf("file ReadAt failed for page %d: %v", i, err)
			}

			if !bytes.Equal(buf, written[i]) {
				t.Fatalf("file data mismatch for page %d", i)
			}
		}
	})
}
