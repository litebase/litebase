package http_test

import (
	"fmt"
	"testing"

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
