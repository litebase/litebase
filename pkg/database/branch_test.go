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

			// Verify that branch settings were created
			db, err := app.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				t.Fatal(err)
			}

			var (
				backupsEnabled                  int
				backupsInterval                 string
				backupsRetentionDays            int
				errorLogsEnabled                int
				errorLogsRetentionDays          int
				incrementalBackupsEnabled       int
				incrementalBackupsRetentionDays int
				queryLogsEnabled                int
				queryLogsRetentionDays          int
				defaultPragmasJSON              string
			)

			err = db.QueryRow(`
				SELECT 
					backups_enabled,
					backups_interval,
					backups_retention_days,
					error_logs_enabled,
					error_logs_retention_days,
					incremental_backups_enabled,
					incremental_backups_retention_days,
					query_logs_enabled,
					query_logs_retention_days,
					default_pragmas_json
				FROM database_branch_settings 
				WHERE database_branch_reference_id = ?
			`, branch.ID).Scan(
				&backupsEnabled,
				&backupsInterval,
				&backupsRetentionDays,
				&errorLogsEnabled,
				&errorLogsRetentionDays,
				&incrementalBackupsEnabled,
				&incrementalBackupsRetentionDays,
				&queryLogsEnabled,
				&queryLogsRetentionDays,
				&defaultPragmasJSON,
			)

			if err != nil {
				t.Fatalf("Failed to query branch settings: %v", err)
			}

			// Verify default values
			if backupsEnabled != 1 {
				t.Errorf("Expected backups_enabled to be 1, got %d", backupsEnabled)
			}

			if backupsInterval != "24h" {
				t.Errorf("Expected backups_interval to be '24h', got '%s'", backupsInterval)
			}

			if backupsRetentionDays != 30 {
				t.Errorf("Expected backups_retention_days to be 30, got %d", backupsRetentionDays)
			}

			if errorLogsEnabled != 1 {
				t.Errorf("Expected error_logs_enabled to be 1, got %d", errorLogsEnabled)
			}

			if errorLogsRetentionDays != 15 {
				t.Errorf("Expected error_logs_retention_days to be 15, got %d", errorLogsRetentionDays)
			}

			if incrementalBackupsEnabled != 1 {
				t.Errorf("Expected incremental_backups_enabled to be 1, got %d", incrementalBackupsEnabled)
			}

			if incrementalBackupsRetentionDays != 7 {
				t.Errorf("Expected incremental_backups_retention_days to be 7, got %d", incrementalBackupsRetentionDays)
			}

			if queryLogsEnabled != 1 {
				t.Errorf("Expected query_logs_enabled to be 1, got %d", queryLogsEnabled)
			}

			if queryLogsRetentionDays != 15 {
				t.Errorf("Expected query_logs_retention_days to be 15, got %d", queryLogsRetentionDays)
			}

			if defaultPragmasJSON == "" {
				t.Error("Expected default_pragmas_json to be set, got empty string")
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
				t.Fatalf("failed to get mock database: %v", err)
			}

			con, err := app.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database connection: %v", err)
			}

			defer app.DatabaseManager.ConnectionManager().Release(con)

			if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			if err := con.Checkpoint(); err != nil {
				t.Fatalf("failed to create checkpoint: %v", err)
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

			primaryBranch, err := db.PrimaryBranch()

			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
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
