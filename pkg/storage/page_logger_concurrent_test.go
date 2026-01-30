package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	internalStorage "github.com/litebase/litebase/internal/test/storage"
	"github.com/litebase/litebase/pkg/server"
)

func TestPageLogger_ConcurrentCompactionBarriers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("MultipleConcurrentReads", func(t *testing.T) {
			db := test.MockDatabase(app)

			pageLogger, err := internalStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			const numReaders = 50
			readersStarted := make(chan bool, numReaders)
			readersCompleted := make(chan bool, numReaders)

			// Start multiple concurrent readers
			for i := range numReaders {
				go func(id int) {
					err := pageLogger.CompactionPassiveBarrierRead(func() error {
						readersStarted <- true
						// Simulate some read work
						time.Sleep(10 * time.Millisecond)
						return nil
					})

					if err != nil {
						t.Errorf("Reader %d failed: %v", id, err)
					}

					readersCompleted <- true
				}(i)
			}

			// Wait for all readers to start (proves they're concurrent)
			timeout := time.After(2 * time.Second)
			startedCount := 0

			for startedCount < numReaders {
				select {
				case <-readersStarted:
					startedCount++
				case <-timeout:
					t.Fatalf("Timeout waiting for readers to start. Only %d/%d started", startedCount, numReaders)
				}
			}

			// Wait for all readers to complete
			for range numReaders {
				select {
				case <-readersCompleted:
				case <-time.After(3 * time.Second):
					t.Fatalf("Timeout waiting for readers to complete")
				}
			}

			t.Logf("Successfully ran %d concurrent readers", numReaders)
		})

		t.Run("CompactionBlocksReads", func(t *testing.T) {
			db := test.MockDatabase(app)

			pageLogger, err := internalStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			compactionStarted := make(chan bool)
			compactionHoldingLock := make(chan bool)
			readerBlocked := make(chan bool)
			compactionDone := make(chan bool)

			// Start a compaction that holds the lock
			go func() {
				err := pageLogger.CompactionPassiveBarrier(func() error {
					compactionStarted <- true
					<-compactionHoldingLock
					time.Sleep(100 * time.Millisecond)
					return nil
				})

				if err != nil {
					t.Errorf("Compaction failed: %v", err)
				}

				compactionDone <- true
			}()

			// Wait for compaction to acquire lock
			<-compactionStarted

			// Try to acquire read lock (should block)
			go func() {
				// Give compaction time to actually acquire the lock
				time.Sleep(10 * time.Millisecond)
				readerBlocked <- true

				err := pageLogger.CompactionPassiveBarrierRead(func() error {
					return nil
				})

				if err != nil {
					t.Errorf("Reader failed: %v", err)
				}
			}()

			// Verify reader is blocked
			select {
			case <-readerBlocked:
				// Reader started trying to acquire lock
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Reader didn't attempt to acquire lock")
			}

			// Release compaction lock
			close(compactionHoldingLock)

			// Wait for compaction to complete
			select {
			case <-compactionDone:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Compaction didn't complete")
			}

			t.Log("Successfully verified compaction blocks reads")
		})

		t.Run("NestedReadsWork", func(t *testing.T) {
			db := test.MockDatabase(app)

			pageLogger, err := internalStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			// Simulate nested read operations (like vector_search calling nested queries)
			err = pageLogger.CompactionPassiveBarrierRead(func() error {
				// This is the outer read
				return pageLogger.CompactionPassiveBarrierRead(func() error {
					// This is the nested/inner read
					// With RWMutex, this should work fine
					return nil
				})
			})

			if err != nil {
				t.Fatalf("Nested reads failed: %v", err)
			}

			t.Log("Successfully performed nested reads (RWMutex allows this)")
		})
	})
}
