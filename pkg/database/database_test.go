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

			if db2.Name != "test2" {
				t.Errorf("Expected name to be 'test2', got '%s'", db2.Name)
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
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Branches", "main")

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.Branch(db.PrimaryBranch().DatabaseBranchID)
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

			hasBranch := db.HasBranch(db.PrimaryBranch().DatabaseBranchID)

			if !hasBranch {
				t.Error("Expected database to have branch 'main'")
			}
		})

		t.Run("Key", func(t *testing.T) {
			db, err := database.CreateDatabase(app.DatabaseManager, "test_Key", "main")

			if err != nil {
				t.Fatal(err)
			}

			k := db.PrimaryBranch().Key

			key := db.Key(db.PrimaryBranch().DatabaseBranchID)

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

			url := db.Url(db.PrimaryBranch().DatabaseBranchID)

			port := app.Config.Port

			expected := fmt.Sprintf("localhost:%s/%s", port, db.PrimaryBranch().Key)

			if url != expected {
				t.Errorf("Expected URL to be '%s', got '%s'", expected, url)
			}
		})
	})
}
