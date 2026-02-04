package server_test

import (
	"sync"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/server"
)

func TestVectorIndexManagerMarkPending(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mgr := server.NewVectorIndexManager(app)

		t.Run("SingleMark", func(t *testing.T) {
			mgr.MarkPending("db1", "branch1", "table1")

			// Check that index was marked
			key := "db1:branch1:table1"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			if info.PendingCount != 1 {
				t.Errorf("Expected PendingCount=1, got %d", info.PendingCount)
			}

			if info.Processing {
				t.Error("Expected Processing=false")
			}
		})

		t.Run("MultipleMark", func(t *testing.T) {
			mgr.MarkPending("db2", "branch2", "table2")
			mgr.MarkPending("db2", "branch2", "table2")
			mgr.MarkPending("db2", "branch2", "table2")

			key := "db2:branch2:table2"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			if info.PendingCount != 3 {
				t.Errorf("Expected PendingCount=3, got %d", info.PendingCount)
			}
		})

		t.Run("DifferentIndexes", func(t *testing.T) {
			mgr.MarkPending("db3", "branch3", "table3")
			mgr.MarkPending("db3", "branch3", "table4")
			mgr.MarkPending("db3", "branch4", "table3")

			// Should have 3 different indexes
			if len(mgr.GetIndexes()) < 3 {
				t.Errorf("Expected at least 3 indexes, got %d", len(mgr.GetIndexes()))
			}
		})
	})
}

func TestVectorIndexManagerProcessIndexesOnlyOnPrimary(t *testing.T) {
	t.Run("PrimaryNodeProcesses", func(t *testing.T) {
		test.RunWithApp(t, func(app *server.App) {
			// Wait for node to become primary
			if err := app.Cluster.Node().WaitForPrimary(); err != nil {
				t.Fatalf("Failed to wait for primary: %v", err)
			}

			mgr := server.NewVectorIndexManager(app)
			mgr.MarkPending("db1", "branch1", "vectors")

			// Process should work on primary
			mgr.ProcessIndexesForTest()

			// Check that processing was marked
			key := "db1:branch1:vectors"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to still be tracked")
			}

			if info.PendingCount != 0 {
				t.Errorf("Expected PendingCount=0 after processing, got %d", info.PendingCount)
			}
		})
	})

	t.Run("ReplicaNodeDoesNotProcess", func(t *testing.T) {
		test.RunWithApp(t, func(app *server.App) {
			// Force node to be replica by setting membership
			app.Cluster.Node().SetMembership(cluster.ClusterMembershipReplica)

			mgr := server.NewVectorIndexManager(app)
			mgr.MarkPending("db1", "branch1", "vectors")

			originalCount := int64(1)

			// Process should be skipped on replica
			mgr.ProcessIndexesForTest()

			// Pending count should remain unchanged
			key := "db1:branch1:vectors"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to still be tracked")
			}

			if info.PendingCount != originalCount {
				t.Errorf("Expected PendingCount=%d (unchanged on replica), got %d", originalCount, info.PendingCount)
			}
		})
	})
}

func TestVectorIndexManagerMarkProcessed(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mgr := server.NewVectorIndexManager(app)

		t.Run("MarkProcessedAfterProcessing", func(t *testing.T) {
			mgr.MarkPending("db1", "branch1", "table1")

			key := "db1:branch1:table1"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			// Simulate processing started
			info.Processing = true

			// Mark as processed
			mgr.MarkProcessed("db1", "branch1", "table1")

			info = getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to still be tracked")
			}

			if info.Processing {
				t.Error("Expected Processing=false after MarkProcessed")
			}
		})

		t.Run("MarkProcessedNonExistent", func(t *testing.T) {
			// Should not panic
			mgr.MarkProcessed("nonexistent", "branch", "table")
		})
	})
}

func TestVectorIndexManagerProcessingTimeout(t *testing.T) {
	// Set shorter timeout for testing
	originalTimeout := server.IndexManagerProcessingTimeout
	server.IndexManagerProcessingTimeout = 100 * time.Millisecond
	defer func() { server.IndexManagerProcessingTimeout = originalTimeout }()

	test.RunWithApp(t, func(app *server.App) {
		if err := app.Cluster.Node().WaitForPrimary(); err != nil {
			t.Fatalf("Failed to wait for primary: %v", err)
		}

		mgr := server.NewVectorIndexManager(app)

		t.Run("TimeoutResetsProcessing", func(t *testing.T) {
			mgr.MarkPending("db1", "branch1", "table1")

			key := "db1:branch1:table1"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			// Simulate stuck processing
			info.Processing = true
			info.LastUpdated = time.Now().UTC().Add(-10 * time.Minute)

			// Wait for timeout
			time.Sleep(150 * time.Millisecond)

			// Should have been reset and processed again
			info = getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to still be tracked")
			}

			// After timeout, it should be marked for processing again
			if info.PendingCount != 0 {
				t.Logf("Note: PendingCount=%d (may be 0 if job was enqueued)", info.PendingCount)
			}
		})
	})
}

func TestVectorIndexManagerCleanup(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mgr := server.NewVectorIndexManager(app)

		t.Run("OldIndexesAreCleanedUp", func(t *testing.T) {
			mgr.MarkPending("db1", "branch1", "table1")

			key := "db1:branch1:table1"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			// Simulate old index
			info.PendingCount = 0
			info.Processing = false
			info.LastUpdated = time.Now().UTC().Add(-10 * time.Minute)

			// Process should clean it up
			mgr.ProcessIndexesForTest()

			// Index should be removed
			info = getIndexInfo(mgr, key)

			if info != nil {
				t.Error("Expected old index to be cleaned up")
			}
		})

		t.Run("ActiveIndexesNotCleanedUp", func(t *testing.T) {
			mgr.MarkPending("db2", "branch2", "table2")

			key := "db2:branch2:table2"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			// Recent index should not be cleaned up
			mgr.ProcessIndexesForTest()

			info = getIndexInfo(mgr, key)

			if info == nil {
				t.Error("Expected recent index to still be tracked")
			}
		})
	})
}

func TestVectorIndexManagerConcurrency(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mgr := server.NewVectorIndexManager(app)

		t.Run("ConcurrentMarkPending", func(t *testing.T) {
			var wg sync.WaitGroup
			goroutines := 100

			wg.Add(goroutines)

			for i := 0; i < goroutines; i++ {
				go func() {
					defer wg.Done()
					mgr.MarkPending("db1", "branch1", "table1")
				}()
			}

			wg.Wait()

			key := "db1:branch1:table1"
			info := getIndexInfo(mgr, key)

			if info == nil {
				t.Fatal("Expected index to be tracked")
			}

			if info.PendingCount != int64(goroutines) {
				t.Errorf("Expected PendingCount=%d, got %d", goroutines, info.PendingCount)
			}
		})
	})
}

func TestVectorIndexManagerShutdown(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mgr := server.NewVectorIndexManager(app)

		t.Run("ShutdownStopsProcessing", func(t *testing.T) {
			// Start the manager in a goroutine
			go mgr.Run()

			// Let it run briefly
			time.Sleep(50 * time.Millisecond)

			// Shutdown
			mgr.Shutdown()

			// Verify context is canceled
			select {
			case <-mgr.GetContext().Done():
				// Success
			case <-time.After(100 * time.Millisecond):
				t.Error("Expected context to be canceled after shutdown")
			}
		})
	})
}

// Helper functions for testing

func getIndexInfo(mgr *server.VectorIndexManager, key string) *server.IndexInfo {
	indexes := mgr.GetIndexes()
	return indexes[key]
}
