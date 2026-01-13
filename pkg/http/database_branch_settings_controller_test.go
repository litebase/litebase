package http_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/auth"
)

func TestDatabaseBranchSettingsControllerShow(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: auth.Resource(fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, primaryBranch.DatabaseBranchID)),
			Actions:  []auth.Privilege{auth.DatabasePrivilegeShow},
		}})

		resp, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
			"GET",
			nil,
		)

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

		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		// Verify default settings
		if data["backupsEnabled"] != true {
			t.Errorf("expected backupsEnabled to be false, got %v", data["backupsEnabled"])
		}

		if data["backupInterval"] != "24h" {
			t.Errorf("expected backupInterval to be '24h', got %v", data["backupInterval"])
		}

		if data["backupsRetentionDays"] != float64(30) {
			t.Errorf("expected backupsRetentionDays to be 30, got %v", data["backupsRetentionDays"])
		}

		if data["errorLogsEnabled"] != true {
			t.Errorf("expected errorLogsEnabled to be true, got %v", data["errorLogsEnabled"])
		}

		if data["errorLogsRetentionDays"] != float64(15) {
			t.Errorf("expected errorLogsRetentionDays to be 15, got %v", data["errorLogsRetentionDays"])
		}

		if data["incrementalBackupsEnabled"] != true {
			t.Errorf("expected incrementalBackupsEnabled to be false, got %v", data["incrementalBackupsEnabled"])
		}

		if data["incrementalBackupsRetentionDays"] != float64(7) {
			t.Errorf("expected incrementalBackupsRetentionDays to be 7, got %v", data["incrementalBackupsRetentionDays"])
		}

		if data["queryLogsEnabled"] != true {
			t.Errorf("expected queryLogsEnabled to be true, got %v", data["queryLogsEnabled"])
		}

		if data["queryLogsRetentionDays"] != float64(15) {
			t.Errorf("expected queryLogsRetentionDays to be 15, got %v", data["queryLogsRetentionDays"])
		}

		defaultPragmas, ok := data["defaultPragmas"].(map[string]any)

		if !ok {
			t.Fatalf("expected defaultPragmas to be an object, got %T", data["defaultPragmas"])
		}

		if defaultPragmas["foreignKeys"] != "ON" {
			t.Errorf("expected defaultPragmas.foreignKeys to be 'ON', got %v", defaultPragmas["foreignKeys"])
		}
	})
}

func TestDatabaseBranchSettingsControllerUpdate(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
		}})

		// Update settings
		resp, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
			"PUT",
			map[string]any{
				"backupsEnabled":                  true,
				"backupInterval":                  "48h",
				"backupsRetentionDays":            60,
				"errorLogsEnabled":                true,
				"errorLogsRetentionDays":          15,
				"incrementalBackupsEnabled":       true,
				"incrementalBackupsRetentionDays": 14,
				"queryLogsEnabled":                true,
				"queryLogsRetentionDays":          30,
			},
		)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Logf("response: %v", resp)
			t.Fatalf("expected status code 200, got %d", statusCode)
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

		// Verify updated settings
		if data["backupsEnabled"] != true {
			t.Errorf("expected backupsEnabled to be true, got %v", data["backupsEnabled"])
		}

		if data["backupInterval"] != "48h" {
			t.Errorf("expected backupInterval to be '48h', got %v", data["backupInterval"])
		}

		if data["backupsRetentionDays"] != float64(60) {
			t.Errorf("expected backupsRetentionDays to be 60, got %v", data["backupsRetentionDays"])
		}

		if data["incrementalBackupsEnabled"] != true {
			t.Errorf("expected incrementalBackupsEnabled to be true, got %v", data["incrementalBackupsEnabled"])
		}

		if data["incrementalBackupsRetentionDays"] != float64(14) {
			t.Errorf("expected incrementalBackupsRetentionDays to be 14, got %v", data["incrementalBackupsRetentionDays"])
		}

		if data["queryLogsRetentionDays"] != float64(30) {
			t.Errorf("expected queryLogsRetentionDays to be 30, got %v", data["queryLogsRetentionDays"])
		}

		// Verify settings weren't changed for fields we didn't update
		if data["errorLogsEnabled"] != true {
			t.Errorf("expected errorLogsEnabled to remain true, got %v", data["errorLogsEnabled"])
		}

		if data["errorLogsRetentionDays"] != float64(15) {
			t.Errorf("expected errorLogsRetentionDays to remain 15, got %v", data["errorLogsRetentionDays"])
		}
	})
}

func TestDatabaseBranchSettingsControllerUpdate_NonExistentBranch(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
		}})

		resp, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/branches/nonexistent/settings", mock.DatabaseName),
			"PUT",
			map[string]any{
				"backupsEnabled":                  true,
				"backupInterval":                  "24h",
				"backupsRetentionDays":            30,
				"errorLogsEnabled":                true,
				"errorLogsRetentionDays":          15,
				"incrementalBackupsEnabled":       false,
				"incrementalBackupsRetentionDays": 7,
				"queryLogsEnabled":                true,
				"queryLogsRetentionDays":          15,
			},
		)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 404 {
			t.Fatalf("expected status code 404, got %d", statusCode)
		}

		if resp["status"] != "error" {
			t.Fatalf("expected error status, got %v", resp["status"])
		}
	})
}

func TestDatabaseBranchSettingsControllerUpdate_Validation(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		mock := test.MockDatabase(server.App)

		db, err := server.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database: %v", err)
		}

		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch: %v", err)
		}

		client := server.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
		}})

		t.Run("InvalidForeignKeysValue", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
					"defaultPragmas": map[string]any{
						"foreignKeys": "INVALID",
					},
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 for invalid foreignKeys, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			errors, ok := resp["errors"].(map[string]any)
			if !ok {
				t.Fatalf("expected errors to be an object, got %T", resp["errors"])
			}

			if _, hasError := errors["foreignKeys"]; !hasError {
				t.Logf("Errors received: %+v", errors)
				t.Fatalf("expected foreignKeys validation error")
			}
		})

		t.Run("MissingForeignKeysWhenDefaultPragmasProvided", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
					"defaultPragmas":                  map[string]any{},
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 for missing foreignKeys, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			errors, ok := resp["errors"].(map[string]any)
			if !ok {
				t.Fatalf("expected errors to be an object, got %T", resp["errors"])
			}

			if _, hasError := errors["foreignKeys"]; !hasError {
				t.Logf("Errors received: %+v", errors)
				t.Fatalf("expected foreignKeys validation error")
			}
		})

		t.Run("InvalidRetentionDays_Zero", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            0,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 for zero retention days, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}
		})

		t.Run("InvalidRetentionDays_Negative", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          -5,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 for negative retention days, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}
		})

		t.Run("ValidDefaultPragmas_ON", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
					"defaultPragmas": map[string]any{
						"foreignKeys": "ON",
					},
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 200 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 200, got %d", statusCode)
			}

			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}
		})

		t.Run("ValidDefaultPragmas_OFF", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
					"defaultPragmas": map[string]any{
						"foreignKeys": "OFF",
					},
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 200 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 200, got %d", statusCode)
			}

			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}
		})

		t.Run("OmittedDefaultPragmas_Valid", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "24h",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 200 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 200 when omitting defaultPragmas, got %d", statusCode)
			}

			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}
		})

		t.Run("InvalidBackupInterval", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled":                  true,
					"backupInterval":                  "invalid",
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 for invalid backup interval, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}
		})

		t.Run("BackupIntervalRequiredWhenBackupsEnabled", func(t *testing.T) {
			// Try to enable backups without providing backupInterval
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled": true,
					// backupInterval intentionally omitted
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 422 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 422 when backupInterval is missing and backupsEnabled is true, got %d", statusCode)
			}

			if resp["status"] != "error" {
				t.Fatalf("expected error status, got %v", resp["status"])
			}

			errors, ok := resp["errors"].(map[string]any)
			if !ok {
				t.Fatalf("expected errors to be an object, got %T", resp["errors"])
			}

			if _, hasError := errors["backupInterval"]; !hasError {
				t.Fatalf("expected backupInterval validation error")
			}
		})

		t.Run("BackupIntervalNotRequiredWhenBackupsDisabled", func(t *testing.T) {
			resp, statusCode, err := client.Send(
				fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch.Name),
				"PUT",
				map[string]any{
					"backupsEnabled": false,
					// backupInterval intentionally omitted
					"backupsRetentionDays":            30,
					"errorLogsEnabled":                true,
					"errorLogsRetentionDays":          15,
					"incrementalBackupsEnabled":       false,
					"incrementalBackupsRetentionDays": 7,
					"queryLogsEnabled":                true,
					"queryLogsRetentionDays":          15,
				},
			)

			if err != nil {
				t.Fatalf("failed to send request: %v", err)
			}

			if statusCode != 200 {
				t.Logf("response: %v", resp)
				t.Fatalf("expected status code 200 when backupInterval is omitted and backupsEnabled is false, got %d", statusCode)
			}

			if resp["status"] != "success" {
				t.Fatalf("expected success status, got %v", resp["status"])
			}
		})
	})
}

func TestDatabaseBranchSettingsControllerUpdate_PublishesMessage(t *testing.T) {
	test.Run(t, func() {
		// Create two servers to test cluster message publishing
		server1 := test.NewTestServer(t)
		defer server1.Shutdown()
		server2 := test.NewTestServer(t)
		defer server2.Shutdown()

		mock := test.MockDatabase(server1.App)

		// Get the database on server1
		db1, err := server1.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database on server1: %v", err)
		}

		primaryBranch1, err := db1.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch on server1: %v", err)
		}

		// Get the database on server2 (should share the same system database)
		db2, err := server2.App.DatabaseManager.Get(mock.DatabaseID)

		if err != nil {
			t.Fatalf("failed to get mock database on server2: %v", err)
		}

		primaryBranch2, err := db2.PrimaryBranch()

		if err != nil {
			t.Fatalf("failed to get primary branch on server2: %v", err)
		}

		client := server1.WithAccessKeyClient([]auth.Statement{{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{auth.DatabasePrivilegeManage},
		}})

		// Get the initial settings from both servers
		initialSettings1, err := primaryBranch1.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get initial settings on server1: %v", err)
		}

		initialSettings2, err := primaryBranch2.GetBranchSettings()

		if err != nil {
			t.Fatalf("failed to get initial settings on server2: %v", err)
		}

		// Verify both servers start with the same settings
		if initialSettings1.QueryLogsEnabled != initialSettings2.QueryLogsEnabled {
			t.Fatal("servers have different initial query logs settings")
		}

		// Update settings on server1
		resp, statusCode, err := client.Send(
			fmt.Sprintf("/v1/databases/%s/branches/%s/settings", mock.DatabaseName, primaryBranch1.Name),
			"PUT",
			map[string]any{
				"backupsEnabled":                  false,
				"backupInterval":                  "24h",
				"backupsRetentionDays":            20,
				"errorLogsEnabled":                false,
				"errorLogsRetentionDays":          10,
				"incrementalBackupsEnabled":       false,
				"incrementalBackupsRetentionDays": 5,
				"queryLogsEnabled":                false,
				"queryLogsRetentionDays":          10,
			},
		)

		if err != nil {
			t.Fatalf("failed to send request: %v", err)
		}

		if statusCode != 200 {
			t.Logf("response: %v", resp)
			t.Fatalf("expected status code 200, got %d", statusCode)
		}

		// Get the branch again on server1 to verify settings were updated in memory
		updatedBranch1, err := db1.BranchByID(primaryBranch1.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get updated branch on server1: %v", err)
		}

		// Verify the settings were updated on server1
		if updatedBranch1.Settings.QueryLogsEnabled {
			t.Error("expected query logs to be disabled on server1")
		}

		if updatedBranch1.Settings.BackupsEnabled {
			t.Error("expected backups to be disabled on server1")
		}

		if updatedBranch1.Settings.ErrorLogsEnabled {
			t.Error("expected error logs to be disabled on server1")
		}

		if updatedBranch1.Settings.IncrementalBackupsEnabled {
			t.Error("expected incremental backups to be disabled on server1")
		}

		if updatedBranch1.Settings.BackupInterval != "24h" {
			t.Errorf("expected backup interval to be '24h' on server1, got %s", updatedBranch1.Settings.BackupInterval)
		}

		if updatedBranch1.Settings.BackupsRetentionDays != 20 {
			t.Errorf("expected backups retention days to be 20 on server1, got %d", updatedBranch1.Settings.BackupsRetentionDays)
		}

		if updatedBranch1.Settings.QueryLogsRetentionDays != 10 {
			t.Errorf("expected query logs retention days to be 10 on server1, got %d", updatedBranch1.Settings.QueryLogsRetentionDays)
		}

		if updatedBranch1.Settings.ErrorLogsRetentionDays != 10 {
			t.Errorf("expected error logs retention days to be 10 on server1, got %d", updatedBranch1.Settings.ErrorLogsRetentionDays)
		}

		if updatedBranch1.Settings.IncrementalBackupsRetentionDays != 5 {
			t.Errorf("expected incremental backups retention days to be 5 on server1, got %d", updatedBranch1.Settings.IncrementalBackupsRetentionDays)
		}

		// Give a small amount of time for the cluster message to propagate
		// In production, this happens asynchronously via the cluster messaging system
		time.Sleep(100 * time.Millisecond)

		// Get the branch on server2 to verify settings were updated via cluster message
		updatedBranch2, err := db2.BranchByID(primaryBranch2.DatabaseBranchID)

		if err != nil {
			t.Fatalf("failed to get updated branch on server2: %v", err)
		}

		// Verify server2 received the message and updated its in-memory settings
		if updatedBranch2.Settings.QueryLogsEnabled {
			t.Error("expected query logs to be disabled on server2 after cluster message")
		}

		if updatedBranch2.Settings.BackupsEnabled {
			t.Error("expected backups to be disabled on server2 after cluster message")
		}

		if updatedBranch2.Settings.ErrorLogsEnabled {
			t.Error("expected error logs to be disabled on server2 after cluster message")
		}

		if updatedBranch2.Settings.IncrementalBackupsEnabled {
			t.Error("expected incremental backups to be disabled on server2 after cluster message")
		}

		if updatedBranch2.Settings.BackupInterval != "24h" {
			t.Errorf("expected backup interval to be '24h' on server2, got %s", updatedBranch2.Settings.BackupInterval)
		}

		if updatedBranch2.Settings.BackupsRetentionDays != 20 {
			t.Errorf("expected backups retention days to be 20 on server2, got %d", updatedBranch2.Settings.BackupsRetentionDays)
		}

		if updatedBranch2.Settings.QueryLogsRetentionDays != 10 {
			t.Errorf("expected query logs retention days to be 10 on server2, got %d", updatedBranch2.Settings.QueryLogsRetentionDays)
		}

		// Verify that the response returns the reloaded settings
		data, ok := resp["data"].(map[string]any)

		if !ok {
			t.Fatalf("expected data to be an object, got %T", resp["data"])
		}

		if data["queryLogsEnabled"] != false {
			t.Error("expected response queryLogsEnabled to be false")
		}

		if data["backupsEnabled"] != false {
			t.Error("expected response backupsEnabled to be false")
		}

		// Ensure settings were different initially to verify the update actually happened
		if !initialSettings1.QueryLogsEnabled {
			t.Error("initial settings should have had query logs enabled")
		}

		if !initialSettings1.BackupsEnabled {
			t.Error("initial settings should have had backups enabled")
		}
	})
}
