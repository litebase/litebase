package database_test

import (
	"slices"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewDatabaseWAlManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if walm == nil {
			t.Fail()
		}

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}
	})
}

func TestDatabaseWALManager_Create(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersion, err := walm.Create()

		if err != nil {
			t.Errorf("Error creating new WAL version: %v", err)
		}

		if walVersion == nil {
			t.Fail()
		}
	})
}

func TestDatabaseWALManager_CreateFailsOnReplica(t *testing.T) {
	test.Run(t, func() {
		primary := test.NewTestServer(t)
		defer primary.Shutdown()

		replica := test.NewTestServer(t)
		defer replica.Shutdown()

		walm, err := database.NewDatabaseWALManager(
			replica.App.Cluster.Node(),
			replica.App.DatabaseManager.ConnectionManager(),
			replica.App.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			replica.App.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		_, err = walm.Create()

		if err == nil || err != database.ErrCreateWALVersionOnReplica {
			t.Errorf("Expected error creating new WAL version on a replica")
		}
	})
}

func TestDatabaseWALManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersion, err := walm.Create()

		if err != nil {
			t.Errorf("Error creating new WAL version: %v", err)
		}

		if walVersion == nil {
			t.Fatal()
		}

		walVersion2, err := walm.Get(walVersion.Timestamp())

		if err != nil {
			t.Errorf("Error getting WAL version: %v", err)
		}

		if walVersion2 == nil {
			t.Fatal()
		}

		if walVersion2.Timestamp() != walVersion.Timestamp() {
			t.Errorf("Expected WAL version %d, got %d", walVersion.Timestamp(), walVersion2.Timestamp())
		}
	})
}

func TestDatabaseWALManager_InUse(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersion, err := walm.Acquire()

		if err != nil {
			t.Errorf("Error creating new WAL version: %v", err)
		}

		if !walm.InUse(walVersion) {
			t.Errorf("Expected WAL version %d to be in use", walVersion)
		}

		walm.Release(walVersion)

		if walm.InUse(walVersion) {
			t.Errorf("Expected WAL version %d to not be in use", walVersion)
		}
	})
}

func TestDatabaseWALManager_InUseVersions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersions := make([]*database.DatabaseWAL, 4)

		for i := range 4 {
			walVersion, err := walm.Create()

			if err != nil {
				t.Errorf("Error creating new WAL version: %v", err)
			}

			if _, err := walm.Acquire(); err != nil {
				t.Errorf("Error acquiring WAL version: %v", err)
			}

			walVersions[i] = walVersion
		}

		inUseVersions := walm.InUseVersions()

		if len(inUseVersions) != 4 {
			t.Errorf("Expected 4 in use versions, got %d", len(inUseVersions))
		}

		for _, walVersion := range walVersions {
			if !slices.Contains(inUseVersions, walVersion.Timestamp()) {
				t.Errorf("Expected WAL version %d to be in use", walVersion.Timestamp())
			}

			walm.Release(walVersion.Timestamp())
		}

		inUseVersions = walm.InUseVersions()

		if len(inUseVersions) != 0 {
			t.Errorf("Expected 0 in use versions, got %d", len(inUseVersions))
		}
	})
}

func TestDatabaseWALManager_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersion, err := walm.Acquire()

		if err != nil {
			t.Errorf("Error creating new WAL version: %v", err)
		}

		if !walm.InUse(walVersion) {
			t.Errorf("Expected WAL version %d to be in use", walVersion)
		}

		walm.Release(walVersion)

		if walm.InUse(walVersion) {
			t.Errorf("Expected WAL version %d to not be in use", walVersion)
		}
	})
}

func TestDatabaseWALManager_RunGarbageCollection(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
			app.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			app.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		walVersions := make([]*database.DatabaseWAL, 5)

		for i := range 5 {
			walVersion, err := walm.Create()

			if err != nil {
				t.Errorf("Error creating new WAL version: %v", err)
			}

			if _, err := walVersion.Size(); err != nil {
				t.Fatalf("Error getting WAL version size: %v", err)
			}

			if _, err := walm.Acquire(); err != nil {
				t.Fatalf("Error acquiring WAL version: %v", err)
			}

			if i == 0 {
				walm.Release(walVersion.Timestamp())
			}

			walVersions[i] = walVersion
		}

		if err != nil {
			t.Errorf("Error creating new WAL version: %v", err)
		}

		err = walm.RunGarbageCollection()

		if err != nil {
			t.Fatalf("Error running garbage collection: %v", err)
		}

		for i := range 5 {
			_, err := app.Cluster.NetworkFS().Stat(walVersions[i].Path)

			if i == 0 {
				if err == nil {
					t.Errorf("File should not exist for index %d, error: %v", i, err)
				}
			} else if err != nil {
				t.Errorf("File should exist still still in use for index %d, error: %v", i, err)
			}
		}

		for i := 1; i < 5; i++ {
			walm.Release(walVersions[i].Timestamp())
		}

		err = walm.RunGarbageCollection()

		if err != nil {
			t.Fatalf("Error running garbage collection: %v", err)
		}

		for i := 1; i < 5; i++ {
			_, err := app.Cluster.NetworkFS().Stat(walVersions[i].Path)

			if err == nil {
				t.Error("File should not exist")
			}
		}
	})
}

func TestDatabaseWALManager_RunGarbageCollectionFailsOnReplica(t *testing.T) {
	test.Run(t, func() {
		primary := test.NewTestServer(t)
		defer primary.Shutdown()

		replica := test.NewTestServer(t)
		defer replica.Shutdown()

		walm, err := database.NewDatabaseWALManager(
			replica.App.Cluster.Node(),
			replica.App.DatabaseManager.ConnectionManager(),
			replica.App.Cluster.MemoryManager,
			"databaseId",
			"branchId",
			replica.App.Cluster.NetworkFS(),
		)

		if err != nil {
			t.Errorf("Error creating WAL manager: %v", err)
		}

		err = walm.RunGarbageCollection()

		if err == nil || err != database.ErrRunWALGarbageCollectionOnReplica {
			t.Errorf("Expected error running garbage collection on a replica")
		}
	})
}

func TestDatabaseWALManager_RunGarbageCollectionWithReplicas(t *testing.T) {
	test.Run(t, func() {
		primary := test.NewTestServer(t)
		defer primary.Shutdown()

		// Create the database on the primary before replicas are started
		db := test.MockDatabase(primary.App)

		replica1 := test.NewTestServer(t)
		defer replica1.Shutdown()

		replica2 := test.NewTestServer(t)
		defer replica2.Shutdown()

		// Create three different WAL versions
		walm, err := primary.App.DatabaseManager.Resources(db.Branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		walVersions := make([]*database.DatabaseWAL, 3)

		for i := range 3 {
			walVersion, err := walm.Create()

			if err != nil {
				t.Fatalf("Error creating new WAL version: %v", err)
			}

			if _, err := walVersion.Size(); err != nil {
				t.Fatalf("Error getting WAL version size: %v", err)
			}

			if _, err := walm.Acquire(); err != nil {
				t.Fatalf("Error acquiring WAL version: %v", err)
			}

			walVersions[i] = walVersion
		}

		// Ensure the WAL versions are in use
		replica1WALManager, err := replica1.App.DatabaseManager.Resources(db.Branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		replica2WALManager, err := replica2.App.DatabaseManager.Resources(db.Branch).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		if _, err := replica1WALManager.Get(walVersions[1].Timestamp()); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if _, err := replica2WALManager.Get(walVersions[1].Timestamp()); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		// Run garbage collection on the primary
		err = walm.RunGarbageCollection()

		if err != nil {
			t.Fatalf("Error running garbage collection: %v", err)
		}

		for i := range 3 {
			_, err := primary.App.Cluster.NetworkFS().Stat(walVersions[i].Path)

			if err != nil {
				t.Error("File should exist")
			}
		}
	})
}

func TestDatabaseWALManager_ConcurrentBarriers(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("MultipleConcurrentReads", func(t *testing.T) {
			walm, err := database.NewDatabaseWALManager(
				app.Cluster.Node(),
				app.DatabaseManager.ConnectionManager(),
				app.Cluster.MemoryManager,
				"testdb",
				"testbranch",
				app.Cluster.NetworkFS(),
			)

			if err != nil {
				t.Fatalf("Error creating WAL manager: %v", err)
			}

			const numReaders = 50
			readersStarted := make(chan bool, numReaders)
			readersCompleted := make(chan bool, numReaders)

			// Start multiple concurrent readers
			for i := range numReaders {
				go func(id int) {
					err := walm.CheckpointBarrierRead(func() error {
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

		t.Run("WriteBlocksReads", func(t *testing.T) {
			walm, err := database.NewDatabaseWALManager(
				app.Cluster.Node(),
				app.DatabaseManager.ConnectionManager(),
				app.Cluster.MemoryManager,
				"testdb2",
				"testbranch2",
				app.Cluster.NetworkFS(),
			)

			if err != nil {
				t.Fatalf("Error creating WAL manager: %v", err)
			}

			writerStarted := make(chan bool)
			writerHoldingLock := make(chan bool)
			readerBlocked := make(chan bool)
			writerDone := make(chan bool)

			// Start a writer that holds the lock
			go func() {
				err := walm.CheckpointBarrier(func() error {
					writerStarted <- true
					<-writerHoldingLock
					time.Sleep(100 * time.Millisecond)
					return nil
				})

				if err != nil {
					t.Errorf("Writer failed: %v", err)
				}

				writerDone <- true
			}()

			// Wait for writer to acquire lock
			<-writerStarted

			// Try to acquire read lock (should block)
			go func() {
				// Give writer time to actually acquire the lock
				time.Sleep(10 * time.Millisecond)
				readerBlocked <- true

				err := walm.CheckpointBarrierRead(func() error {
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

			// Release writer lock
			close(writerHoldingLock)

			// Wait for writer to complete
			select {
			case <-writerDone:
			case <-time.After(500 * time.Millisecond):
				t.Fatal("Writer didn't complete")
			}

			t.Log("Successfully verified write blocks reads")
		})

		t.Run("NestedReadsWork", func(t *testing.T) {
			walm, err := database.NewDatabaseWALManager(
				app.Cluster.Node(),
				app.DatabaseManager.ConnectionManager(),
				app.Cluster.MemoryManager,
				"testdb3",
				"testbranch3",
				app.Cluster.NetworkFS(),
			)

			if err != nil {
				t.Fatalf("Error creating WAL manager: %v", err)
			}

			// Simulate the original deadlock scenario: outer read calls inner read
			err = walm.CheckpointBarrierRead(func() error {
				// This is the outer read
				return walm.CheckpointBarrierRead(func() error {
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
