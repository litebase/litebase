package http_test

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	appHttp "github.com/litebase/litebase/pkg/http"
	"github.com/litebase/litebase/pkg/sqlite3"
)

func TestDatabaseBackupIndexController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.ConnectionManager().Get(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(db)

		_, err = db.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		// Create a test table
		_, err = db.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

		if err != nil {
			t.Fatalf("expected no error, got %v", err)
		}

		var createdBackups []*backups.Backup

		// Create 10 different backups by inserting data and creating backups in each iteration
		for i := range 10 {
			// Insert some test data to ensure the database has actual content
			for j := range 100 {
				_, err = db.GetConnection().Exec(
					"INSERT INTO test (name) VALUES (?)",
					[]sqlite3.StatementParameter{
						{
							Type:  sqlite3.ParameterTypeText,
							Value: fmt.Appendf(nil, "test-data-backup-%d-record-%d", i, j),
						},
					},
				)

				if err != nil {
					t.Fatalf("expected no error inserting data, got %v", err)
				}
			}

			err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(mock.DatabaseID, mock.DatabaseBranchID)

			if err != nil {
				t.Fatalf("expected no error, got %v", err)
			}

			// Create a backup
			backup, err := backups.Run(
				server.App.Config,
				server.App.Cluster.ObjectFS(),
				mock.DatabaseID,
				mock.DatabaseBranchID,
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).SnapshotLogger(),
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).FileSystem(),
				server.App.DatabaseManager.Resources(mock.DatabaseID, mock.DatabaseBranchID).RollbackLogger(),
			)

			if err != nil {
				t.Fatalf("expected no error creating backup %d, got %v", i, err)
			}

			err = server.App.DatabaseManager.SystemDatabase().StoreDatabaseBackup(
				mock.ID,
				mock.BranchID,
				mock.DatabaseID,
				mock.DatabaseBranchID,
				backup.RestorePoint.Timestamp,
				backup.RestorePoint.PageCount,
				backup.GetSize(),
			)

			if err != nil {
				t.Fatalf("expected no error storing backup %d, got %v", i, err)
			}

			createdBackups = append(createdBackups, backup)

			// Add a small delay to ensure different timestamps
			time.Sleep(10 * time.Millisecond)
		}

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		response, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/%s/backups", mock.DatabaseName, mock.BranchName),
			"GET",
			nil,
		)

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

		if len(response["data"].([]any)) == 0 {
			t.Fatalf("expected at least one backup, got none")
		}

		for _, backup := range createdBackups {
			timestamp := backup.RestorePoint.Timestamp
			size := backup.GetSize()

			found := false
			for _, item := range response["data"].([]any) {
				backupData, ok := item.(map[string]any)

				if !ok {
					continue
				}

				timestampInt64, err := strconv.ParseInt(backupData["restore_point"].(map[string]any)["timestamp"].(string), 10, 64)

				if err != nil {
					continue
				}

				if timestampInt64 == timestamp && int64(backupData["size"].(float64)) == size {
					found = true
					break
				}
			}

			if !found {
				t.Errorf("expected to find backup with timestamp %d and size %d, but it was not found", timestamp, size)
			}
		}
	})
}

func TestDatabaseBackupStoreController(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

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

		response, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/%s/backups",
				db.DatabaseName,
				db.BranchName,
			),
			"POST",
			appHttp.DatabaseBackupStoreRequest{},
		)

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
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		backup, err := backups.Run(
			server.App.Config,
			server.App.Cluster.ObjectFS(),
			db.DatabaseID,
			db.DatabaseBranchID,
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).SnapshotLogger(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).RollbackLogger(),
		)

		if err != nil {
			t.Fatalf("failed to create backup: %v", err)
		}

		server.App.DatabaseManager.SystemDatabase().StoreDatabaseBackup(
			db.ID,
			db.BranchID,
			db.DatabaseID,
			db.DatabaseBranchID,
			backup.RestorePoint.Timestamp,
			backup.RestorePoint.PageCount,
			backup.GetSize(),
		)

		client := server.WithAccessKeyClient([]auth.AccessKeyStatement{
			{
				Effect:   "Allow",
				Resource: "*",
				Actions:  []auth.Privilege{auth.DatabasePrivilegeBackup},
			},
		})

		response, statusCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/backups/%d",
				db.DatabaseName,
				db.BranchName,
				backup.RestorePoint.Timestamp,
			),
			"GET",
			nil,
		)

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

		timestamp, err := strconv.ParseInt(response["data"].(map[string]any)["restore_point"].(map[string]any)["timestamp"].(string), 10, 64)

		if err != nil {
			t.Fatalf("failed to parse timestamp: %v", err)
		}

		if timestamp != backup.RestorePoint.Timestamp {
			t.Errorf("expected timestamp %d, got %v", backup.RestorePoint.Timestamp, timestamp)
		}

		if response["data"].(map[string]any)["size"] == nil {
			t.Fatalf("expected size to be present, got %v", response["data"].(map[string]any)["size"])
		}

		if response["data"].(map[string]any)["size"].(float64) <= 0 {
			t.Fatalf("expected size to be greater than 0, got %v", response["data"].(map[string]any)["size"])
		}

		if response["data"].(map[string]any)["database_id"] != db.DatabaseID {
			t.Fatalf("expected database_id to be present, got %v", response["data"].(map[string]any)["database_id"])
		}

		if response["data"].(map[string]any)["database_branch_id"] != db.DatabaseBranchID {
			t.Fatalf("expected database_branch_id to be present, got %v", response["data"].(map[string]any)["database_branch_id"])
		}
	})
}

func TestDatabaseBackupControllerDestroy(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := test.MockDatabase(server.App)

		con, err := server.App.DatabaseManager.ConnectionManager().Get(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get database connection: %v", err)
		}

		defer server.App.DatabaseManager.ConnectionManager().Release(con)

		_, err = con.GetConnection().Exec("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)", nil)

		if err != nil {
			t.Fatalf("failed to create table: %v", err)
		}

		err = server.App.DatabaseManager.ConnectionManager().ForceCheckpoint(db.DatabaseID, db.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to checkpoint database: %v", err)
		}

		backup, err := backups.Run(
			server.App.Config,
			server.App.Cluster.ObjectFS(),
			db.DatabaseID,
			db.DatabaseBranchID,
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).SnapshotLogger(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).FileSystem(),
			server.App.DatabaseManager.Resources(db.DatabaseID, db.DatabaseBranchID).RollbackLogger(),
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

		response, statusCode, err := client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/backups/%d",
				db.DatabaseName,
				db.BranchName,
				backup.RestorePoint.Timestamp,
			),
			"DELETE",
			appHttp.DatabaseBackupStoreRequest{},
		)

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

		response, statusCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/backups/%d",
				db.DatabaseName,
				db.BranchName,
				backup.RestorePoint.Timestamp,
			),
			"DELETE",
			nil,
		)

		if err != nil {
			t.Fatalf("failed to send delete backup request: %v", err)
		}

		if statusCode != 200 {
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		if response["status"] != "success" {
			t.Fatalf("expected status 'success', got %s", response["status"])
		}

		// Try to retrieve the deleted backup
		response, statusCode, err = client.Send(
			fmt.Sprintf(
				"/v1/databases/%s/%s/backups/%d",
				db.DatabaseName,
				db.BranchName,
				backup.RestorePoint.Timestamp,
			),
			"GET",
			nil,
		)

		if err != nil {
			t.Fatalf("failed to send get backup request: %v", err)
		}

		if statusCode != 404 {
			t.Fatalf("expected status code 404, got %d", statusCode)
		}

		if response["status"] != "error" {
			t.Fatalf("expected status 'error', got %s", response["status"])
		}

		if response["message"] != "Error: backup not found" {
			t.Fatalf("expected message 'Error: backup not found', got %s", response["message"])
		}
	})
}
