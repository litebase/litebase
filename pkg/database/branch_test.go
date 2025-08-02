package database_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestBranch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a test database first
		testDB, err := database.CreateDatabase(app.DatabaseManager, "test_NewBranch", "main")

		if err != nil {
			t.Fatal(err)
		}

		t.Run("NewBranch", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"Test Branch",
			)

			if err != nil {
				t.Fatal(err)
			}

			if branch.Name != "Test Branch" {
				t.Fatal("Branch name is not correct")
			}

			if branch.DatabaseBranchID == "" {
				t.Fatal("Branch ID is empty")
			}
		})

		t.Run("InsertBranch", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"Test Insert Branch",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			if branch.ID == 0 {
				t.Fatal("Branch ID is not set after insertion")
			}

			if !branch.Exists {
				t.Fatal("Branch exists flag is not set after insertion")
			}
		})

		t.Run("UpdateBranch", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"Test Update Branch",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			branch.Name = "Updated Branch Name"

			err = database.UpdateBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			// Reload the branch from the database to verify the update
			db, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var updatedName string

			err = db.QueryRow(
				`SELECT name FROM database_branches WHERE id = ?`,
				branch.ID,
			).Scan(&updatedName)

			if err != nil {
				t.Fatal(err)
			}

			if updatedName != "Updated Branch Name" {
				t.Fatalf("Expected branch name to be 'Updated Branch Name', got '%s'", updatedName)
			}
		})

		t.Run("Branch_Database", func(t *testing.T) {
			mock := test.MockDatabase(app)

			branch, err := database.NewBranch(
				app.DatabaseManager,
				mock.ID,
				"",
				"test_Branch_Database",
			)

			if err != nil {
				t.Fatal(err)
			}

			branch.DatabaseID = mock.DatabaseID

			err = branch.Save()

			if err != nil {
				t.Fatal(err)
			}

			db, err := branch.Database()

			if err != nil {
				t.Fatal(err)
			}

			if db == nil {
				t.Fatal("Expected database to be set")
			}
		})

		t.Run("Branch_Delete", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			branch, err := db.CreateBranch("test", "main")

			if err != nil {
				t.Fatal(err)
			}

			err = branch.Delete()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("Branch_DeleteFailsOnPrimaryBranch", func(t *testing.T) {
			mock := test.MockDatabase(app)

			db, err := app.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatal(err)
			}

			primaryBranch := db.PrimaryBranch()

			if primaryBranch == nil {
				t.Fatal("Expected primary branch to be found, but got nil")
			}

			err = primaryBranch.Delete()

			if err == nil {
				t.Fatal("Expected error when deleting primary branch, but got none")
			}

			branch, err := db.Branch(primaryBranch.Name)

			if err != nil {
				t.Fatal(err)
			}

			err = branch.Delete()

			if err == nil {
				t.Fatal("Expected error when deleting the primary branch, but got none")
			}
		})

		t.Run("Branch_ParentBranch", func(t *testing.T) {
			db := test.MockDatabase(app)

			branch, err := database.NewBranch(
				app.DatabaseManager,
				db.ID,
				"main",
				"TestParentBranch",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			parentBranch := branch.ParentBranch()

			if parentBranch == nil {
				t.Fatal("Expected parent branch to not be nil")
			}

			if parentBranch.Name != "main" {
				t.Fatalf("Expected parent branch name to be 'main', got '%s'", parentBranch.Name)
			}
		})

		t.Run("Branch_Save", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"Test Save Branch",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			branch.DatabaseID = "test_database_id"

			err = branch.Save()

			if err != nil {
				t.Fatal(err)
			}
		})

		t.Run("NewBranchDuplicateName", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"duplicate_branch_name",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			// Attempt to create a branch with the same name as an existing one
			duplicateBranch, err := database.NewBranch(
				app.DatabaseManager,
				testDB.ID,
				"",
				"duplicate_branch_name",
			)

			if err == nil {
				t.Fatal("Expected error when creating branch with duplicate name, but got none")
			}

			if err.Error() != fmt.Errorf("branch with name '%s' already exists in this database", "duplicate_branch_name").Error() {
				t.Fatalf("Expected specific error message, got: %v", err)
			}

			if duplicateBranch != nil {
				t.Fatal("Expected duplicate branch to be nil, but it is not")
			}
		})

		t.Run("NewBranchDuplicateNameDifferentParent", func(t *testing.T) {
			testDB1 := test.MockDatabase(app)

			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB1.ID,
				"",
				"duplicate_branch_name",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			// Attempt to create a branch with the same name as an existing one
			duplicateBranch, err := database.NewBranch(
				app.DatabaseManager,
				testDB1.ID,
				"main",
				"duplicate_branch_name",
			)

			if err == nil {
				t.Fatal("Expected error when creating branch with duplicate name in different parent, but got none")
			}

			if duplicateBranch != nil {
				t.Fatal("Expected duplicate branch to be nil, but it is not")
			}
		})

		t.Run("NewBranchDuplicateNameDifferentDatabase", func(t *testing.T) {
			testDB1 := test.MockDatabase(app)

			testDB2 := test.MockDatabase(app)

			branch, err := database.NewBranch(
				app.DatabaseManager,
				testDB1.ID,
				"",
				"duplicate_branch_name",
			)

			if err != nil {
				t.Fatal(err)
			}

			err = database.InsertBranch(branch)

			if err != nil {
				t.Fatal(err)
			}

			// Attempt to create a branch with the same name as an existing one
			duplicateBranch, err := database.NewBranch(
				app.DatabaseManager,
				testDB2.ID,
				"",
				"duplicate_branch_name",
			)

			if err != nil {
				t.Fatal("Expected no error when creating branch with duplicate name in different database, but got:", err)
			}

			if duplicateBranch == nil {
				t.Fatal("Expected duplicate branch to be not nil, but it is")
			}
		})
	})
}
