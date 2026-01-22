package http_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

func TestDatabaseBranchControllerIndex(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		testDatabase := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(testDatabase.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(testDatabase.DatabaseID, testDatabase.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		for i := range 3 {
			if _, err := db.CreateBranch(fmt.Sprintf("branch-%d", i), "main"); err != nil {
				t.Fatalf("failed to create branch: %v", err)
			}
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: auth.Resource(fmt.Sprintf("database:%s", db.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeList},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", db.Name), "GET", nil)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("failed to get databases expected status code 200, got %d", statusCode)
		}

		if resp == nil {
			t.Fatalf("response is nil")
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}

		data, ok := resp["data"].([]any)

		if !ok {
			t.Fatalf("expected data to be an array, got %T", resp["data"])
		}

		if len(data) < 3 {
			t.Fatalf("expected at least 3 database branches, got %d", len(data))
		}
	})
}

func TestDatabaseBranchControllerShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		database := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(database.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: auth.Resource(fmt.Sprintf("database:%s:branch:*", db.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabasePrivilegeShow},
		}})

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches/%s", database.DatabaseName, primaryBranch.Name), "GET", nil)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("failed to get database expected status code 200, got %d", statusCode)
		}

		if resp == nil {
			t.Fatalf("response is nil")
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}

		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		primaryBranch, err = db.PrimaryBranch()

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		if data["databaseBranchId"] != primaryBranch.DatabaseBranchID {
			t.Fatalf("expected database branch id to be %s, got %v", primaryBranch.DatabaseBranchID, data["databaseBranchId"])
		}
	})
}

func TestDatabaseBranchControllerStore(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: auth.Resource(fmt.Sprintf("database:%s", mock.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name": "test_branch",
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Errorf("expected status code 200, got %d", statusCode)
		}

		// Check the response data
		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Logf("response: %v", resp)
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		if data["name"] != "test_branch" {
			t.Fatalf("expected database name to be 'test_branch', got %v", data["name"])
		}

		branch, err := db.Branch(data["name"].(string))

		if err != nil {
			t.Fatalf("failed to get branch: %v", err)
		}

		if branch.Name != "test_branch" {
			t.Fatalf("expected branch name to be 'test_branch', got %v", branch.Name)
		}
	})
}

func TestDatabaseBranchControllerStore_WithSameNameFails(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		// database, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		// if err != nil {
		// 	t.Fatalf("failed to get mock database: %v", err)
		// }

		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		if err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name": "main",
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 400 {
			t.Log(resp)
			t.Fatalf("expected status code 400, got %d", statusCode)
		}

		if resp["status"] != "error" {
			t.Fatalf("expected error status, got %v", resp["status"])
		}

		if !strings.Contains(resp["message"].(string), "already exists") {
			t.Log(resp)
			t.Fatalf("expected error message to contain 'already exists', got %v", resp["message"])
		}

		databases, err := server.App.DatabaseManager.All()

		if err != nil {
			t.Fatalf("failed to get databases: %v", err)
		}

		if len(databases) != 1 {
			t.Fatalf("expected exactly 1 database, got %d", len(databases))
		}
	})
}

func TestDatabaseBranchControllerStore_CanCreateEmptyBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		// Create a branch from a database that hasn't been checkpointed
		// This should succeed and create an empty branch
		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name": "empty_branch",
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d: %v", statusCode, resp)
		}

		if resp["status"] != "success" {
			t.Fatalf("expected success status, got %v", resp["status"])
		}
	})
}

func TestDatabaseBranchControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		t.Run("ExistingDatabase", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			mock := test.MockDatabase(server.App)

			// Get the database and create a non-primary branch to delete
			db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

			if err != nil {
				t.Fatalf("failed to get mock database: %v", err)
			}

			con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("failed to get database connection: %v", err)
			}

			defer server.App.DatabaseManager.ConnectionManager().Release(con)

			if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			if err := con.Checkpoint(); err != nil {
				t.Fatalf("failed to create checkpoint: %v", err)
			}

			if err != nil {
				t.Fatalf("failed to create test table: %v", err)
			}

			// Create a test branch to delete (not the primary branch)
			testBranch, err := db.CreateBranch("test-branch", "main")

			if err != nil {
				t.Fatalf("failed to create test branch: %v", err)
			}

			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
			}})

			resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches/%s", mock.DatabaseName, testBranch.Name), "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 200 {
				t.Fatalf("expected status code 200, got %d", statusCode)
			}

			if resp == nil {
				t.Fatalf("response is nil")
			}

			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}

			if resp["message"] != "Database branch deleted successfully" {
				t.Fatalf("expected message to be 'Database branch deleted successfully.', got %v", resp["message"])
			}
		})

		t.Run("PrimaryBranchCannotBeDeleted", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			mock := test.MockDatabase(server.App)

			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
			}})

			// Try to delete the primary branch (should fail)
			resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches/%s", mock.DatabaseName, mock.BranchName), "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 500 {
				t.Fatalf("expected status code 500, got %d", statusCode)
			}

			if resp == nil {
				t.Fatalf("response is nil")
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			// The exact error message may vary, but it should indicate the primary branch cannot be deleted
			if resp["message"] == nil {
				t.Fatalf("expected error message, got nil")
			}
		})

		t.Run("NonExistingDatabase", func(t *testing.T) {
			server := test.NewTestServer(t)
			defer server.Shutdown()

			client := server.WithAccessKeyClient([]auth.Statement{{
				Effect:   auth.StatementEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
			}})

			resp, statusCode, err := client.Send("/v1/databases/non-existing-name/branches/main", "DELETE", nil)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 404 {
				t.Fatalf("expected status code 404, got %d", statusCode)
			}

			if resp == nil {
				t.Fatalf("response is nil")
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			if resp["message"] != "Error: database not found" {
				t.Fatalf("expected message to be 'Error: database not found', got %v", resp["message"])
			}
		})
	})
}

func TestDatabaseBranchControllerStore_CopiesSettingsFromParent(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		// Get the parent branch
		parentBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch: %v", err)
		}

		// Update the parent branch settings to custom values
		customSettings := &database.DatabaseBranchSettings{
			BackupsEnabled:                  true,
			BackupInterval:                  "48h",
			BackupsRetentionDays:            14,
			IncrementalBackupsEnabled:       true,
			IncrementalBackupsRetentionDays: 5,
			QueryLogsEnabled:                true,
			QueryLogsRetentionDays:          20,
			ErrorLogsEnabled:                true,
			ErrorLogsRetentionDays:          25,
			DefaultPragmas:                  &database.DatabaseDefaultPragmaSettings{},
		}

		err = parentBranch.UpdateBranchSettings(customSettings)

		if err != nil {
			t.Fatalf("failed to update parent branch settings: %v", err)
		}

		// Create a checkpoint so we can create a child branch
		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		// Create a child branch
		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name":       "child-branch",
			"parentName": parentBranch.Name,
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d: %v", statusCode, resp)
		}

		// Get the child branch and verify its settings match the parent
		childBranch, err := db.Branch("child-branch")

		if err != nil {
			t.Fatalf("failed to get child branch: %v", err)
		}

		childSettings, err := childBranch.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get child branch settings: %v", err)
		}

		// Verify all settings were copied from the parent
		if childSettings.BackupsEnabled != customSettings.BackupsEnabled {
			t.Errorf("expected BackupsEnabled to be %v, got %v", customSettings.BackupsEnabled, childSettings.BackupsEnabled)
		}

		if childSettings.BackupInterval != customSettings.BackupInterval {
			t.Errorf("expected BackupInterval to be %v, got %v", customSettings.BackupInterval, childSettings.BackupInterval)
		}

		if childSettings.BackupsRetentionDays != customSettings.BackupsRetentionDays {
			t.Errorf("expected BackupsRetentionDays to be %v, got %v", customSettings.BackupsRetentionDays, childSettings.BackupsRetentionDays)
		}

		if childSettings.IncrementalBackupsEnabled != customSettings.IncrementalBackupsEnabled {
			t.Errorf("expected IncrementalBackupsEnabled to be %v, got %v", customSettings.IncrementalBackupsEnabled, childSettings.IncrementalBackupsEnabled)
		}

		if childSettings.IncrementalBackupsRetentionDays != customSettings.IncrementalBackupsRetentionDays {
			t.Errorf("expected IncrementalBackupsRetentionDays to be %v, got %v", customSettings.IncrementalBackupsRetentionDays, childSettings.IncrementalBackupsRetentionDays)
		}

		if childSettings.QueryLogsEnabled != customSettings.QueryLogsEnabled {
			t.Errorf("expected QueryLogsEnabled to be %v, got %v", customSettings.QueryLogsEnabled, childSettings.QueryLogsEnabled)
		}

		if childSettings.QueryLogsRetentionDays != customSettings.QueryLogsRetentionDays {
			t.Errorf("expected QueryLogsRetentionDays to be %v, got %v", customSettings.QueryLogsRetentionDays, childSettings.QueryLogsRetentionDays)
		}

		if childSettings.ErrorLogsEnabled != customSettings.ErrorLogsEnabled {
			t.Errorf("expected ErrorLogsEnabled to be %v, got %v", customSettings.ErrorLogsEnabled, childSettings.ErrorLogsEnabled)
		}

		if childSettings.ErrorLogsRetentionDays != customSettings.ErrorLogsRetentionDays {
			t.Errorf("expected ErrorLogsRetentionDays to be %v, got %v", customSettings.ErrorLogsRetentionDays, childSettings.ErrorLogsRetentionDays)
		}
	})
}
func TestDatabaseBranchControllerStore_WithEncryption(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServerWithEncryption(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: auth.Resource(fmt.Sprintf("database:%s", mock.DatabaseID)),
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name":      "encrypted_branch",
			"encrypted": true,
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		if data["name"] != "encrypted_branch" {
			t.Fatalf("expected branch name to be 'encrypted_branch', got %v", data["name"])
		}

		// Verify encryption is actually configured
		branch, err := db.Branch("encrypted_branch")

		if err != nil {
			t.Fatalf("failed to get branch: %v", err)
		}

		// Use cached settings (UpdateBranchSettings updates the cache)
		settings := branch.Settings

		// if !settings.Encrypted {
		// 	t.Errorf("expected branch to be encrypted")
		// }

		// // Direct query AFTER reload
		// sysDb, _ := server.App.DatabaseManager.SystemDatabase().DB()
		// var dbEnc int
		// var dbHash sql.NullString
		// sysDb.QueryRow("SELECT encrypted, data_encryption_key_hash FROM database_branch_settings WHERE database_branch_reference_id = ?", branch.ID).Scan(&dbEnc, &dbHash)
		// t.Logf("DB has: encrypted=%d, hash=%s (Valid=%v)", dbEnc, dbHash.String, dbHash.Valid)

		// // Check if migration has run
		// var migrationCount int
		// sysDb.QueryRow("SELECT COUNT(*) FROM migrations WHERE id >= 5").Scan(&migrationCount)
		// t.Logf("Migrations >= 5: %d", migrationCount)

		// // Get column list
		// rows, _ := sysDb.Query("PRAGMA table_info(database_branch_settings)")
		// t.Log("All columns:")
		// for rows.Next() {
		// 	var cid int
		// 	var name string
		// 	var typ string
		// 	var notnull int
		// 	var dflt sql.NullString
		// 	var pk int
		// 	rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk)
		// 	t.Logf("  %d: %s %s", cid, name, typ)
		// }

		// rows.Close()

		t.Logf("Encrypted=%v, KeyHash=%s, ConfigHash=%s", settings.Encrypted, settings.DataEncryptionKeyHash, server.App.Config.DataEncryptionKeyHash)

		if settings.DataEncryptionKeyHash != server.App.Config.DataEncryptionKeyHash {
			t.Errorf("expected key hash %s to match server config %s", settings.DataEncryptionKeyHash, server.App.Config.DataEncryptionKeyHash)
		}
	})
}

func TestDatabaseBranchControllerStore_WithEncryptionButNoKey(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		if _, err := con.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil); err != nil {
			t.Fatalf("failed to create test table: %v", err)
		}

		if err := con.Checkpoint(); err != nil {
			t.Fatalf("failed to create checkpoint: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
		}})

		resp, statusCode, err := client.Send(fmt.Sprintf("/v1/databases/%s/branches", mock.DatabaseName), "POST", map[string]any{
			"name":      "encrypted_branch",
			"encrypted": true,
		})

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 400 {
			t.Fatalf("expected status code 400, got %d", statusCode)
		}

		if resp["status"] != "error" {
			t.Fatalf("expected error status, got %v", resp["status"])
		}

		message := resp["message"].(string)
		if !strings.Contains(message, "LITEBASE_DATA_ENCRYPTION_KEY") {
			t.Fatalf("expected error message about encryption key, got %v", message)
		}
	})
}
