package database_test

import (
	"slices"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewDatabaseWAlManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		walm, err := database.NewDatabaseWALManager(
			app.Cluster.Node(),
			app.DatabaseManager.ConnectionManager(),
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

			walm.Acquire()

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

			walVersion.Size()

			walm.Acquire()

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
					t.Error("File should not exist", err)
				}
			} else if err != nil {
				t.Error("File should exist still still in use", err)
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
			if i == 0 {
				continue
			}

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
		walm, err := primary.App.DatabaseManager.Resources(
			db.DatabaseID,
			db.DatabaseBranchID,
		).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		walVersions := make([]*database.DatabaseWAL, 3)

		for i := range 3 {
			walVersion, err := walm.Create()

			if err != nil {
				t.Fatalf("Error creating new WAL version: %v", err)
			}

			walVersion.Size()

			walm.Acquire()

			walVersions[i] = walVersion
		}

		// Ensure the WAL versions are in use
		replica1WALManager, err := replica1.App.DatabaseManager.Resources(
			db.DatabaseID,
			db.DatabaseBranchID,
		).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		replica2WALManager, err := replica2.App.DatabaseManager.Resources(
			db.DatabaseID,
			db.DatabaseBranchID,
		).DatabaseWALManager()

		if err != nil {
			t.Fatalf("Error creating WAL manager: %v", err)
		}

		replica1WALManager.Get(walVersions[1].Timestamp())
		replica2WALManager.Get(walVersions[1].Timestamp())

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
