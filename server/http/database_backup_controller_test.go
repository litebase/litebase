package http_test

import (
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server/auth"
	"github.com/litebase/litebase/server/backups"
	appHttp "github.com/litebase/litebase/server/http"
	"github.com/litebase/litebase/server/storage"
)

func TestDatabaseBackupStoreController(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		response, statusCode, err := client.SendToDatabase(db, "/backups", "POST", appHttp.DatabaseBackupStoreRequest{})

		if err != nil {
			t.Fatalf("failed to send backup request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if response["status"] != "success" {
			t.Fatalf("expected status 'success', got %s", response["status"])
		}

		if response["message"] != "Database backup created successfully" {
			t.Fatalf("expected message 'Database backup created successfully', got %s", response["message"])
		}
	})
}

func TestDatabaseBackupShowController(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		backup, err := backups.Run(
			server.App.Config,
			server.App.Cluster.ObjectFS(),
			db.DatabaseId,
			db.BranchId,
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).SnapshotLogger(),
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("failed to create backup: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		response, statusCode, err := client.SendToDatabase(db, fmt.Sprintf("/backups/%d", backup.RestorePoint.Timestamp), "GET", nil)

		if err != nil {
			t.Fatalf("failed to send backup request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if response["status"] != "success" {
			t.Fatalf("expected status 'success', got %s", response["status"])
		}

		if response["data"] == nil {
			t.Fatalf("expected data to be present, got %v", response["data"])
		}

		if response["data"].(map[string]any)["timestamp"] != float64(backup.RestorePoint.Timestamp) {
			t.Errorf("expected timestamp %d, got %v", backup.RestorePoint.Timestamp, response["data"].(map[string]any)["timestamp"])
		}

		if response["data"].(map[string]any)["size"] == nil {
			t.Fatalf("expected size to be present, got %v", response["data"].(map[string]any)["size"])
		}
		if response["data"].(map[string]any)["size"].(float64) <= 0 {
			t.Fatalf("expected size to be greater than 0, got %v", response["data"].(map[string]any)["size"])
		}

		if response["data"].(map[string]any)["database_id"] != db.DatabaseId {
			t.Fatalf("expected database_id to be present, got %v", response["data"].(map[string]any)["database_id"])
		}

		if response["data"].(map[string]any)["branch_id"] != db.BranchId {
			t.Fatalf("expected branch_id to be present, got %v", response["data"].(map[string]any)["branch_id"])
		}
	})
}

func TestDatabaseBackupControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		// Force immediate compaction for testing
		originalInterval := storage.PageLoggerCompactInterval
		storage.PageLoggerCompactInterval = 0
		defer func() {
			storage.PageLoggerCompactInterval = originalInterval
		}()

		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db.DatabaseId, db.BranchId, con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseId, db.BranchId)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		backup, err := backups.Run(
			server.App.Config,
			server.App.Cluster.ObjectFS(),
			db.DatabaseId,
			db.BranchId,
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).SnapshotLogger(),
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).FileSystem(),
			server.App.DatabaseManager.Resources(db.DatabaseId, db.BranchId).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("failed to create backup: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		response, statusCode, err := client.SendToDatabase(db, fmt.Sprintf("/backups/%d", backup.RestorePoint.Timestamp), "DELETE", appHttp.DatabaseBackupStoreRequest{})

		if err != nil {
			t.Fatalf("failed to send backup request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if response["status"] != "success" {
			t.Fatalf("expected status 'success', got %s", response["status"])
		}

		if response["message"] != "Database backup deleted successfully" {
			t.Fatalf("expected message 'Database backup deleted successfully', got %s", response["message"])
		}

		response, statusCode, err = client.SendToDatabase(db, fmt.Sprintf("/backups/%d", backup.RestorePoint.Timestamp), "DELETE", nil)

		if err != nil {
			t.Fatalf("failed to send delete backup request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if response["status"] != "success" {
			t.Fatalf("expected status 'success', got %s", response["status"])
		}
	})
}
