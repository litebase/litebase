package database_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabase(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabase", func(t *testing.T) {
			db := database.NewDatabase(app.DatabaseManager, "test")

			if db == nil {
				t.Fatal("Failed to create new database")
			}
		})

		t.Run("CreateDatabase", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_CreateDatabase", "main")

			if err != nil {
				t.Error(err)
			}

			if db == nil {
				t.Fatal("Failed to create new database")
			}
		})

		t.Run("InsertDatabase", func(t *testing.T) {
			db := database.NewDatabase(app.DatabaseManager, "test_InsertDatabase")

			err := database.InsertDatabase(db)

			if err != nil {
				t.Error(err)
			}
		})

		t.Run("UpdateDatabase", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_UpdateDatabase", "main")

			if err != nil {
				t.Fatal(err)
			}

			db.Name = "test2"

			err = database.UpdateDatabase(db)

			if err != nil {
				t.Error(err)
			}

			db2, err := app.DatabaseManager.Get(db.DatabaseID)

			if err != nil {
				t.Error(err)
			}

			if db2.Name != "test_UpdateDatabase" {
				t.Errorf("Expected name to be 'test_UpdateDatabase', got '%s'", db2.Name)
			}
		})

		t.Run("Database_Branches", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Branches", "main")

			if err != nil {
				t.Fatal(err)
			}

			branches, err := db.Branches()
			if err != nil {
				t.Fatal(err)
			}

			if len(branches) == 0 {
				t.Fatal("Expected at least one branch, but got none")
			}

			if branches[0].Name != "main" {
				t.Errorf("Expected primary branch name to be 'main', got '%s'", branches[0].Name)
			}
		})

		t.Run("Database_Branch", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Database_Branch", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			branch, err := db.Branch(primaryBranch.DatabaseBranchID)

			if err != nil {
				t.Fatal(err)
			}

			if branch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			if branch.Name != "main" {
				t.Errorf("Expected primary branch name to be 'main', got '%s'", branch.Name)
			}
		})

		t.Run("Database_CreateBranch", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_CreateBranch", "main")

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.CreateBranch("test_branch", "")

			if err != nil {
				t.Fatal(err)
			}

			if branch == nil {
				t.Fatal("Expected branch to be created, but got nil")
			}
		})

		t.Run("Database_CreateBranchFromParentWithNoSnapshots", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_CreateBranchFromParent", "main")

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.CreateBranch("test_branch", "main")

			if err != nil {
				t.Fatal(err)
			}

			if branch == nil {
				t.Fatal("Expected branch to be created, but got nil")
			}
		})

		t.Run("Database_CreateBranchFromParentWith1Snapshot", func(t *testing.T) {
			mock := test.MockDatabase(app)

			sourceDb, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

			// Create an initial checkpoint before creating the table (this will be restore point 0)
			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Create a test table and insert some data
			_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Create the new branch from the primary branch
			branch, err := db.CreateBranch("test_branch", "main")

			if err != nil {
				t.Fatal(err)
			}

			if branch == nil {
				t.Fatal("Expected branch to be created, but got nil")
			}

			targetDB, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(targetDB)

			// Check if the table exists in the new branch
			_, err = targetDB.GetConnection().Exec("SELECT * FROM test", nil)

			if err != nil {
				t.Fatalf("Expected table 'test' to exist in new branch, got error: %v", err)
			}
		})

		t.Run("Database_CreateBranchFromParentWithMultipleSnapshots", func(t *testing.T) {
			mock := test.MockDatabase(app)

			sourceDb, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(sourceDb)

			// Create an initial checkpoint before creating the table (this will be restore point 0)
			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Create a test table and insert some data
			_, err = sourceDb.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT)", nil)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.BranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			for range 10 {
				// Insert some data into the table
				_, err = sourceDb.GetConnection().Exec("INSERT INTO test (value) VALUES('test_value')", nil)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}

				err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.BranchID)

				if err != nil {
					t.Fatalf("Expected no error, got %v", err)
				}
			}

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			// Create the new branch from the primary branch
			branch, err := db.CreateBranch("test_branch", "main")

			if err != nil {
				t.Fatal(err)
			}

			if branch == nil {
				t.Fatal("Expected branch to be created, but got nil")
			}

			targetDB, err := app.DatabaseManager.ConnectionManager().Get(db.DatabaseID, branch.DatabaseBranchID)

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(targetDB)

			// Check if the table exists in the new branch
			res, err := targetDB.GetConnection().Exec("SELECT COUNT(*) FROM test", nil)

			if err != nil {
				t.Fatalf("Expected table 'test' to exist in new branch, got error: %v", err)
			}

			if res == nil {
				t.Fatal("Expected result set to be non-nil, got nil")
			}

			if res.Rows[0][0].Int64() != 10 {
				t.Errorf("Expected 10 rows in 'test' table, got %d", res.Rows[0][0].Int64())
			}
		})

		t.Run("Database_HasBranch", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_HasBranch", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			hasBranch := db.HasBranch(primaryBranch.DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to have branch 'main'")
			}
		})

		t.Run("Database_UpdateBranchCache", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_UpdateBranchCache", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			// Test updating cache with a branch that exists
			db.UpdateBranchCache(primaryBranch.DatabaseBranchID, true)

			// Verify the cache was updated by checking HasBranch
			hasBranch := db.HasBranch(primaryBranch.DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to have cached branch after UpdateBranchCache")
			}

			// Test updating cache with a non-existent branch
			nonExistentBranchID := "non-existent-branch-id"

			db.UpdateBranchCache(nonExistentBranchID, false)

			// Verify the cache was updated
			hasBranch = db.HasBranch(nonExistentBranchID)

			if hasBranch {
				t.Error("Expected database to not have non-existent branch after UpdateBranchCache")
			}
		})

		t.Run("Database_InvalidateBranchCache", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_InvalidateBranchCache", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			branchID := primaryBranch.DatabaseBranchID

			// First, ensure the branch is in cache by calling HasBranch
			hasBranch := db.HasBranch(branchID)

			if !hasBranch {
				t.Fatal("Expected database to have branch before invalidation test")
			}

			// Invalidate the cache entry
			db.InvalidateBranchCache(branchID)

			// The branch should still exist in the database, but the cache should be cleared
			// We can verify this by checking HasBranch again - it should hit the database
			hasBranch = db.HasBranch(branchID)

			if !hasBranch {
				t.Error("Expected database to still have branch after cache invalidation")
			}

			// Test invalidating a non-existent cache entry (should not cause errors)
			db.InvalidateBranchCache("non-existent-branch-id")
		})

		t.Run("Database_CacheConsistency", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_CacheConsistency", "main")

			if err != nil {
				t.Fatal(err)
			}

			// Create a new branch and verify cache is updated
			newBranch, err := db.CreateBranch("test_cache_branch", "")
			if err != nil {
				t.Fatal(err)
			}

			// The cache should be updated when the branch is created
			hasBranch := db.HasBranch(newBranch.DatabaseBranchID)
			if !hasBranch {
				t.Error("Expected database to have newly created branch in cache")
			}

			// Note: We can't test branch deletion here because it requires the branch
			// to not be the primary branch, and the Delete method has protection
			// against deleting the primary branch. Instead, we'll test cache invalidation
			// directly by invalidating and then checking that it gets reloaded from DB.

			// Manually invalidate the cache
			db.InvalidateBranchCache(newBranch.DatabaseBranchID)

			// The branch should still exist in the database after cache invalidation
			hasBranch = db.HasBranch(newBranch.DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to still have branch after cache invalidation")
			}
		})

		t.Run("Database_BranchDeletionCacheInvalidation", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_BranchDeletion", "main")

			if err != nil {
				t.Fatal(err)
			}

			// Create a secondary branch (non-primary)
			secondaryBranch, err := db.CreateBranch("secondary", "")

			if err != nil {
				t.Fatal(err)
			}

			// Verify the branch exists in cache
			hasBranch := db.HasBranch(secondaryBranch.DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to have newly created secondary branch")
			}

			// Test cache invalidation mechanism - this should clear the cache entry
			// but the branch should still be reloaded from DB when checked again
			db.InvalidateBranchCache(secondaryBranch.DatabaseBranchID)

			// The branch should still exist in DB after cache invalidation (reload from DB)
			hasBranch = db.HasBranch(secondaryBranch.DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to still have branch after cache invalidation (should reload from DB)")
			}

			// Test actual branch deletion
			err = secondaryBranch.Delete()

			if err != nil {
				t.Fatalf("Failed to delete secondary branch: %v", err)
			}

			// After deletion, the branch should no longer exist in database
			hasBranch = db.HasBranch(secondaryBranch.DatabaseBranchID)

			if hasBranch {
				t.Error("Expected branch to not exist after deletion")
			}

			// Verify that we can create a branch with the same name again
			newBranch, err := db.CreateBranch("secondary", "")
			if err != nil {
				t.Fatalf("Failed to create new branch with same name: %v", err)
			}

			// This should be a different branch with a different ID
			if newBranch.DatabaseBranchID == secondaryBranch.DatabaseBranchID {
				t.Error("Expected new branch to have different ID than deleted branch")
			}
		})

		t.Run("Key", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Key", "main")

			if err != nil {
				t.Fatal(err)
			}
			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			k := primaryBranch.Key

			key := db.Key(primaryBranch.DatabaseBranchID)

			if key != k {
				t.Errorf("Expected key to be '%s', got '%s'", k, key)
			}
		})

		t.Run("PrimaryBranch", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_PrimaryBranch", "main")

			if err != nil {
				t.Fatal(err)
			}

			branch := db.PrimaryBranch()

			if branch == nil {
				t.Fatal("Expected database to have a primary branch")
			}

			if branch.Name != "main" {
				t.Errorf("Expected primary branch ID to be 'main', got '%s'", branch.DatabaseBranchID)
			}
		})

		t.Run("Save", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Save", "main")

			if err != nil {
				t.Fatal(err)
			}

			err = db.Save()

			if err != nil {
				t.Error(err)
			}
		})

		t.Run("Url", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Url", "main")

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			url := db.Url(primaryBranch.DatabaseBranchID)

			port := app.Config.Port

			expected := fmt.Sprintf("http://localhost:%s/%s", port, primaryBranch.Key)

			if url != expected {
				t.Errorf("Expected URL to be '%s', got '%s'", expected, url)
			}
		})
	})
}
