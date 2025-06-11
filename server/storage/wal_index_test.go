package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/storage"
)

func TestNewWALIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		// Check if the WALIndex is initialized correctly
		if walIndex == nil {
			t.Fatal("Expected WALIndex to be initialized, but got nil")
		}
	})
}

func TestWALIndex_Close(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		// Close the WALIndex
		err := walIndex.Close()

		// Check if there was an error closing the WALIndex
		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}
	})
}

func TestWALIndex_File(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		// Get the file for the WALIndex
		file, err := walIndex.File()

		// Check if there was an error getting the file
		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		// Check if the file is not nil
		if file == nil {
			t.Fatal("Expected file to be initialized, but got nil")
		}
	})
}

func TestWALIndex_GetClosestVersion(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		past := time.Now().Add(-time.Second).UnixNano()
		present := time.Now().UnixNano()
		future := time.Now().Add(time.Second).UnixNano()

		walIndex.SetVersions([]int64{
			past,
			present,
			future,
		})

		// Get the closest version
		version := walIndex.GetClosestVersion(time.Now().Local().UnixNano())

		if version != present {
			t.Fatalf("Expected version to be %d, but got: %d", present, version)
		}

		version = walIndex.GetClosestVersion(past)

		if version != past {
			t.Fatalf("Expected version to be %d, but got: %d", past, version)
		}

		version = walIndex.GetClosestVersion(future)

		if version != future {
			t.Fatalf("Expected version to be %d, but got: %d", future, version)
		}
	})
}

func TestWALIndex_GetClosestVersion_MicroSeconds(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		past := time.Now().Add(-time.Microsecond).UnixNano()
		present := time.Now().UnixNano()
		future := time.Now().Add(time.Microsecond).UnixNano()

		walIndex.SetVersions([]int64{
			past,
			present,
			future,
		})

		// Get the closest version
		version := walIndex.GetClosestVersion(present)

		if version != present {
			t.Fatalf("Expected version to be %d, but got: %d", present, version)
		}

		version = walIndex.GetClosestVersion(past)

		if version != past {
			t.Fatalf("Expected version to be %d, but got: %d", past, version)
		}

		version = walIndex.GetClosestVersion(future)

		if version != future {
			t.Fatalf("Expected version to be %d, but got: %d", future, version)
		}
	})
}

func TestWALIndex_GetVersions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		walIndex.SetVersions([]int64{
			1,
			2,
			3,
		})

		// Get the versions
		versions, err := walIndex.GetVersions()

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		if len(versions) != 3 {
			t.Fatalf("Expected 3 versions, but got: %d", len(versions))
		}

		if versions[0] != 1 {
			t.Fatalf("Expected version 1, but got: %d", versions[0])
		}

		if versions[1] != 2 {
			t.Fatalf("Expected version 2, but got: %d", versions[1])
		}

		if versions[2] != 3 {
			t.Fatalf("Expected version 3, but got: %d", versions[2])
		}
	})
}

func TestWALIndex_RemoveVersionsFrom(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		past := time.Now().Add(-time.Second).UnixNano()
		present := time.Now().UnixNano()
		future := time.Now().Add(time.Second).UnixNano()

		walIndex.SetVersions([]int64{
			past,
			present,
			future,
		})

		removed, err := walIndex.RemoveVersionsFrom(present)

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		if len(removed) != 2 {
			t.Fatalf("Expected 2 versions, but got: %d", len(removed))
		}

		if removed[0] != past {
			t.Fatalf("Expected version %d, but got: %d", past, removed[0])
		}

		versions, err := walIndex.GetVersions()

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		if len(versions) != 1 {
			t.Fatalf("Expected 1 version, but got: %d", len(versions))
		}

		if versions[0] != future {
			t.Fatalf("Expected version %d, but got: %d", future, versions[0])
		}
	})
}

func TestWALIndex_SetVersions(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		// Set versions
		err := walIndex.SetVersions([]int64{
			1,
			2,
			3,
		})

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		// Get the versions
		versions, err := walIndex.GetVersions()

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		if len(versions) != 3 {
			t.Fatalf("Expected 3 versions, but got: %d", len(versions))
		}

		if versions[0] != 1 {
			t.Fatalf("Expected version 1, but got: %d", versions[0])
		}

		if versions[1] != 2 {
			t.Fatalf("Expected version 2, but got: %d", versions[1])
		}

		if versions[2] != 3 {
			t.Fatalf("Expected version 3, but got: %d", versions[2])
		}
	})
}

func TestWALIndex_Truncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		db := test.MockDatabase(app)

		// Create a new WALIndex instance
		walIndex := storage.NewWALIndex(
			db.DatabaseId,
			db.BranchId,
			app.Cluster.LocalFS(),
		)

		past1 := time.Now().Add(-time.Hour * 26).UnixNano()
		past2 := time.Now().Add(-time.Hour * 25).UnixNano()
		present := time.Now().UnixNano()

		walIndex.SetVersions([]int64{
			past1,
			past2,
			present,
		})

		err := walIndex.Truncate()

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		versions, err := walIndex.GetVersions()

		if err != nil {
			t.Fatalf("Expected no error, but got: %v", err)
		}

		if len(versions) != 1 {
			t.Fatalf("Expected 1 version, but got: %d", len(versions))
		}

		if versions[0] != present {
			t.Fatalf("Expected version %d, but got: %d", present, versions[0])
		}
	})
}
