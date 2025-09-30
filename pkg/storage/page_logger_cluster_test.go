package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	testingStorage "github.com/litebase/litebase/internal/test/storage"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestPageLoggerClusterAwareCompaction(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("CompactionWithReplicaCoordination", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a page logger with a mock node publisher
			mockNodePublisher := testingStorage.NewMockNodePublisher()
			pageLogger, err := storage.NewPageLogger(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
				mockNodePublisher,
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			// Acquire a timestamp locally to create an in-use log
			localTimestamp := time.Now().UTC().UnixNano()
			pageLogger.Acquire(localTimestamp)

			// Simulate a replica response with an older timestamp that's still in use
			replicaTimestamp := localTimestamp - 1000000000 // 1 second earlier
			replicaResponse := messages.PageLoggerVersionUsageResponse{
				BranchID:   db.DatabaseBranchID,
				DatabaseID: db.DatabaseID,
				Versions:   []int64{replicaTimestamp},
			}

			// Set up the mock to return this response
			mockNodePublisher.SetResponse("replica1", replicaResponse)

			// Write some test data to a page that should be protected by the
			// replica timestamp
			testData := make([]byte, 4096)

			for i := range testData {
				testData[i] = byte(i % 256)
			}

			// Write with the replica timestamp (should be protected)
			_, err = pageLogger.Write(1, replicaTimestamp, testData)

			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}

			// Write with an even older timestamp (should be eligible for compaction)
			olderTimestamp := replicaTimestamp - 1000000000

			_, err = pageLogger.Write(2, olderTimestamp, testData)

			if err != nil {
				t.Fatalf("Failed to write older page: %v", err)
			}

			// Try to compact - the page with replicaTimestamp should be protected
			err = pageLogger.Compact(
				app.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Fatalf("Failed to compact page logger: %v", err)
			}

			// Verify that the replica-protected data is still readable
			readData := make([]byte, 4096)
			found, _, err := pageLogger.Read(1, replicaTimestamp, readData)

			if err != nil {
				t.Fatalf("Failed to read protected page: %v", err)
			}

			if !found {
				t.Fatal("Expected replica-protected page to still exist after compaction")
			}

			// Verify the data is correct
			for i := range readData {
				if readData[i] != testData[i] {
					t.Fatalf("Data corruption detected at byte %d: expected %d, got %d", i, testData[i], readData[i])
				}
			}

			// Clean up
			pageLogger.Release(localTimestamp)
		})

		t.Run("ReplicaNodeSkipsCoordination", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a page logger with a mock node publisher configured as replica
			mockNodePublisher := testingStorage.NewMockNodePublisher()
			mockNodePublisher.SetReplica(true) // This is a replica node

			pageLogger, err := storage.NewPageLogger(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
				mockNodePublisher,
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			// Write test data
			testData := make([]byte, 4096)
			timestamp := time.Now().UTC().UnixNano()

			_, err = pageLogger.Write(1, timestamp, testData)
			if err != nil {
				t.Fatalf("Failed to write page: %v", err)
			}

			// Compaction should work normally on replica (no coordination)
			err = pageLogger.Compact(
				app.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem(),
			)

			if err != nil {
				t.Fatalf("Failed to compact page logger on replica: %v", err)
			}
		})

		t.Run("GetInUseVersions", func(t *testing.T) {
			db := test.MockDatabase(app)

			pageLogger, err := testingStorage.NewPageLoggerForTesting(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err != nil {
				t.Fatalf("Failed to create page logger: %v", err)
			}

			// Initially no versions should be in use
			inUseVersions := pageLogger.GetInUseVersions()
			if len(inUseVersions) != 0 {
				t.Fatalf("Expected 0 in-use versions, got %d", len(inUseVersions))
			}

			// Acquire some timestamps
			timestamp1 := time.Now().UTC().UnixNano()
			timestamp2 := timestamp1 + 1000000
			timestamp3 := timestamp1 + 2000000

			pageLogger.Acquire(timestamp1)
			pageLogger.Acquire(timestamp2)
			pageLogger.Acquire(timestamp3)

			// Should have 3 in-use versions
			inUseVersions = pageLogger.GetInUseVersions()
			if len(inUseVersions) != 3 {
				t.Fatalf("Expected 3 in-use versions, got %d", len(inUseVersions))
			}

			// Release one
			pageLogger.Release(timestamp2)

			// Should have 2 in-use versions
			inUseVersions = pageLogger.GetInUseVersions()
			if len(inUseVersions) != 2 {
				t.Fatalf("Expected 2 in-use versions, got %d", len(inUseVersions))
			}

			// Release the rest
			pageLogger.Release(timestamp1)
			pageLogger.Release(timestamp3)

			// Should have no in-use versions
			inUseVersions = pageLogger.GetInUseVersions()
			if len(inUseVersions) != 0 {
				t.Fatalf("Expected 0 in-use versions, got %d", len(inUseVersions))
			}
		})
	})
}
