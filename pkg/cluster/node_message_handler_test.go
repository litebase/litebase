package cluster_test

import (
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/cluster/messages"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestHandleMessage(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Test a known message type.
		resp, err := app.Cluster.Node().HandleMessage(messages.NodeMessage{
			Data: messages.HeartbeatMessage{
				Time: time.Now().UTC().Unix(),
			},
		})

		if err != nil {
			t.Error(err)
		}

		if _, ok := resp.Data.(messages.ErrorMessage); ok {
			t.Error("Expected heartbeat response")
		}

		// Test an unknown message type.
		resp, err = app.Cluster.Node().HandleMessage(messages.NodeMessage{
			Data: "unknown message type",
		})

		if err != nil {
			t.Error(err)
		}

		if _, ok := resp.Data.(messages.ErrorMessage); !ok {
			t.Error("Expected error response")
		}
	})
}

func TestHandleDatabaseBranchSettingsUpdated(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a mock database
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		branch, err := db.Branch(mock.BranchName)

		if err != nil {
			t.Fatalf("failed to get branch: %v", err)
		}

		// Get initial settings
		initialSettings, err := branch.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get initial settings: %v", err)
		}

		// Verify initial settings
		if !initialSettings.QueryLogsEnabled {
			t.Fatal("expected query logs to be enabled initially")
		}

		// Update settings directly in the database
		newSettings := &database.DatabaseBranchSettings{
			BackupsEnabled:                  false,
			BackupInterval:                  "12h",
			BackupsRetentionDays:            15,
			DefaultPragmas:                  initialSettings.DefaultPragmas,
			ErrorLogsEnabled:                false,
			ErrorLogsRetentionDays:          10,
			IncrementalBackupsEnabled:       false,
			IncrementalBackupsRetentionDays: 3,
			QueryLogsEnabled:                false,
			QueryLogsRetentionDays:          7,
		}

		err = branch.UpdateBranchSettings(newSettings)

		if err != nil {
			t.Fatalf("failed to update settings: %v", err)
		}

		// Verify the in-memory settings haven't changed yet
		if !branch.Settings.QueryLogsEnabled {
			t.Fatal("in-memory settings should still have query logs enabled")
		}

		// Send the DatabaseBranchSettingsUpdated message
		message := messages.NodeMessage{
			Data: messages.DatabaseBranchSettingsUpdated{
				DatabaseID:       db.DatabaseID,
				DatabaseBranchID: branch.DatabaseBranchID,
			},
		}

		_, err = app.Cluster.Node().HandleMessage(message)

		if err != nil {
			t.Fatalf("failed to handle message: %v", err)
		}

		// Get the branch again to check if settings were reloaded
		updatedBranch, err := db.BranchByID(branch.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get updated branch: %v", err)
		}

		// Verify the settings were reloaded
		if updatedBranch.Settings.QueryLogsEnabled {
			t.Error("expected query logs to be disabled after settings update")
		}

		if updatedBranch.Settings.BackupsEnabled {
			t.Error("expected backups to be disabled after settings update")
		}

		if updatedBranch.Settings.ErrorLogsEnabled {
			t.Error("expected error logs to be disabled after settings update")
		}

		if updatedBranch.Settings.IncrementalBackupsEnabled {
			t.Error("expected incremental backups to be disabled after settings update")
		}

		if updatedBranch.Settings.BackupInterval != "12h" {
			t.Errorf("expected backup interval to be '12h', got %s", updatedBranch.Settings.BackupInterval)
		}

		if updatedBranch.Settings.BackupsRetentionDays != 15 {
			t.Errorf("expected backups retention days to be 15, got %d", updatedBranch.Settings.BackupsRetentionDays)
		}

		if updatedBranch.Settings.QueryLogsRetentionDays != 7 {
			t.Errorf("expected query logs retention days to be 7, got %d", updatedBranch.Settings.QueryLogsRetentionDays)
		}

		if updatedBranch.Settings.ErrorLogsRetentionDays != 10 {
			t.Errorf("expected error logs retention days to be 10, got %d", updatedBranch.Settings.ErrorLogsRetentionDays)
		}

		if updatedBranch.Settings.IncrementalBackupsRetentionDays != 3 {
			t.Errorf("expected incremental backups retention days to be 3, got %d", updatedBranch.Settings.IncrementalBackupsRetentionDays)
		}
	})
}

func TestHandleDatabaseBranchSettingsUpdated_InvalidDatabase(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Send message with non-existent database ID
		message := messages.NodeMessage{
			Data: messages.DatabaseBranchSettingsUpdated{
				DatabaseID:       "nonexistent",
				DatabaseBranchID: "nonexistent",
			},
		}

		resp, err := app.Cluster.Node().HandleMessage(message)

		if err != nil {
			t.Fatalf("unexpected error from HandleMessage: %v", err)
		}

		// Check if response contains an error message
		if errMsg, ok := resp.Data.(messages.ErrorMessage); !ok {
			t.Fatal("expected error message response when handling message with invalid database ID")
		} else if errMsg.Message == "" {
			t.Fatal("expected error message to contain text")
		}
	})
}

func TestHandleDatabaseBranchSettingsUpdated_InvalidBranch(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		// Create a mock database
		mock := test.MockDatabase(app)

		db, err := app.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get database: %v", err)
		}

		// Send message with non-existent branch ID
		message := messages.NodeMessage{
			Data: messages.DatabaseBranchSettingsUpdated{
				DatabaseID:       db.DatabaseID,
				DatabaseBranchID: "nonexistent",
			},
		}

		resp, err := app.Cluster.Node().HandleMessage(message)

		if err != nil {
			t.Fatalf("unexpected error from HandleMessage: %v", err)
		}

		// Check if response contains an error message
		if errMsg, ok := resp.Data.(messages.ErrorMessage); !ok {
			t.Fatal("expected error message response when handling message with invalid branch ID")
		} else if errMsg.Message == "" {
			t.Fatal("expected error message to contain text")
		}
	})
}
