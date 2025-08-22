package storage_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/storage"
)

func TestWALIndex(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("New", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			// Check if the WALIndex is initialized correctly
			if walIndex == nil {
				t.Fatal("Expected WALIndex to be initialized, but got nil")
			}
		})

		t.Run("Close", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			// Close the WALIndex
			err := walIndex.Close()

			// Check if there was an error closing the WALIndex
			if err != nil {
				t.Fatalf("Expected no error, but got: %v", err)
			}
		})

		t.Run("File", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
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

		t.Run("GetClosestVersion", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			past := time.Now().UTC().Add(-time.Second).UnixNano()
			present := time.Now().UTC().UnixNano()
			future := time.Now().UTC().Add(time.Second).UnixNano()

			if err := walIndex.SetVersions([]int64{
				past,
				present,
				future,
			}); err != nil {
				t.Fatalf("Expected no error, but got: %v", err)
			}

			// Get the closest version
			version := walIndex.GetClosestVersion(time.Now().UTC().UnixNano())

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

		t.Run("GetClosestVersion_MicroSeconds", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			past := time.Now().UTC().Add(-time.Microsecond).UnixNano()
			present := time.Now().UTC().UnixNano()
			future := time.Now().UTC().Add(time.Microsecond).UnixNano()

			if err := walIndex.SetVersions([]int64{
				past,
				present,
				future,
			}); err != nil {
				t.Fatalf("Expected no error, but got: %v", err)
			}

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

		t.Run("GetVersions", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			if err := walIndex.SetVersions([]int64{
				1,
				2,
				3,
			}); err != nil {
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

		t.Run("RemoveVersionsFrom", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			past := time.Now().UTC().Add(-time.Second).UnixNano()
			present := time.Now().UTC().UnixNano()
			future := time.Now().UTC().Add(time.Second).UnixNano()

			if err := walIndex.SetVersions([]int64{
				past,
				present,
				future,
			}); err != nil {
				t.Fatalf("Expected no error, but got: %v", err)
			}

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

		t.Run("SetVersions", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
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

		t.Run("Truncate", func(t *testing.T) {
			db := test.MockDatabase(app)

			// Create a new WALIndex instance
			walIndex := storage.NewWALIndex(
				db.DatabaseID,
				db.DatabaseBranchID,
				app.Cluster.LocalFS(),
			)

			past1 := time.Now().UTC().Add(-time.Hour * 26).UnixNano()
			past2 := time.Now().UTC().Add(-time.Hour * 25).UnixNano()
			present := time.Now().UTC().UnixNano()

			if err := walIndex.SetVersions([]int64{
				past1,
				past2,
				present,
			}); err != nil {
				t.Fatalf("Expected no error, but got: %v", err)
			}

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
	})
}
