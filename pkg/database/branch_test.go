package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestNewBranch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewBranch", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
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

			if branch.Key == "" {
				t.Fatal("Branch key is empty")
			}
		})

		t.Run("InsertBranch", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
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

		t.Run("Save", func(t *testing.T) {
			branch, err := database.NewBranch(
				app.DatabaseManager,
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
	})
}
