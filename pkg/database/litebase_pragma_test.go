package database

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/litebase/litebase/internal/utils"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLitebasePragmaHandler_ParsePragma(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		wantMatch bool
		wantName  string
		wantValue string
	}{
		{
			name:      "GET pragma",
			sql:       "PRAGMA litebase_backups_enabled",
			wantMatch: true,
			wantName:  "backups_enabled",
			wantValue: "",
		},
		{
			name:      "SET pragma with equals",
			sql:       "PRAGMA litebase_backups_enabled = true",
			wantMatch: true,
			wantName:  "backups_enabled",
			wantValue: "true",
		},
		{
			name:      "SET pragma with 0",
			sql:       "PRAGMA litebase_backups_enabled = 0",
			wantMatch: true,
			wantName:  "backups_enabled",
			wantValue: "0",
		},
		{
			name:      "SET pragma with interval",
			sql:       "PRAGMA litebase_backup_interval = 24h",
			wantMatch: true,
			wantName:  "backup_interval",
			wantValue: "24h",
		},
		{
			name:      "SET pragma with integer",
			sql:       "PRAGMA litebase_backups_retention_days = 7",
			wantMatch: true,
			wantName:  "backups_retention_days",
			wantValue: "7",
		},
		{
			name:      "Case insensitive",
			sql:       "pragma LITEBASE_backups_enabled = FALSE",
			wantMatch: true,
			wantName:  "backups_enabled",
			wantValue: "FALSE",
		},
		{
			name:      "With extra whitespace",
			sql:       "  PRAGMA   litebase_backups_enabled   =   true  ",
			wantMatch: true,
			wantName:  "backups_enabled",
			wantValue: "true", // Should be trimmed by ParseLitebasePragma
		},
		{
			name:      "Not a litebase PRAGMA",
			sql:       "PRAGMA foreign_keys = ON",
			wantMatch: false,
		},
		{
			name:      "Regular SQL",
			sql:       "SELECT * FROM users",
			wantMatch: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			isMatch := IsLitebasePragma(tt.sql)

			if tt.wantMatch {
				require.True(t, isMatch, "Expected to match PRAGMA pattern")

				name, value, isSet, err := ParseLitebasePragma(tt.sql)
				require.NoError(t, err)

				assert.Equal(t, tt.wantName, name, "PRAGMA name mismatch")
				assert.Equal(t, tt.wantValue, value, "PRAGMA value mismatch")

				if tt.wantValue != "" {
					assert.True(t, isSet, "Expected isSet to be true")
				} else {
					assert.False(t, isSet, "Expected isSet to be false")
				}
			} else {
				assert.False(t, isMatch, "Expected not to match PRAGMA pattern")
			}
		})
	}
}

func TestLitebasePragmaHandler_ParseBool(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    bool
		wantErr bool
	}{
		{name: "true", value: "true", want: true, wantErr: false},
		{name: "TRUE", value: "TRUE", want: true, wantErr: false},
		{name: "1", value: "1", want: true, wantErr: false},
		{name: "on", value: "on", want: true, wantErr: false},
		{name: "ON", value: "ON", want: true, wantErr: false},
		{name: "yes", value: "yes", want: true, wantErr: false},
		{name: "YES", value: "YES", want: true, wantErr: false},
		{name: "false", value: "false", want: false, wantErr: false},
		{name: "FALSE", value: "FALSE", want: false, wantErr: false},
		{name: "0", value: "0", want: false, wantErr: false},
		{name: "off", value: "off", want: false, wantErr: false},
		{name: "OFF", value: "OFF", want: false, wantErr: false},
		{name: "no", value: "no", want: false, wantErr: false},
		{name: "NO", value: "NO", want: false, wantErr: false},
		{name: "invalid", value: "invalid", want: false, wantErr: true},
		{name: "empty", value: "", want: false, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseBool(tt.value)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestLitebasePragmaHandler_BoolToInt(t *testing.T) {
	assert.Equal(t, 1, utils.BoolToInt(true))
	assert.Equal(t, 0, utils.BoolToInt(false))
}

func TestLitebasePragmaHandler_GetPragma(t *testing.T) {
	// Just test that we can call getPragma with different settings
	settings := &DatabaseBranchSettings{
		BackupsEnabled:                  true,
		BackupInterval:                  "24h",
		BackupsRetentionDays:            7,
		IncrementalBackupsEnabled:       false,
		IncrementalBackupsRetentionDays: 3,
		QueryLogsEnabled:                true,
		QueryLogsRetentionDays:          14,
		ErrorLogsEnabled:                true,
		ErrorLogsRetentionDays:          30,
	}

	handler := &LitebasePragmaHandler{}

	tests := []struct {
		name     string
		pragma   string
		wantType string // "int" or "string"
	}{
		{
			name:     "backups_enabled",
			pragma:   "backups_enabled",
			wantType: "int",
		},
		{
			name:     "backup_interval",
			pragma:   "backup_interval",
			wantType: "string",
		},
		{
			name:     "backups_retention_days",
			pragma:   "backups_retention_days",
			wantType: "int",
		},
		{
			name:     "incremental_backups_enabled",
			pragma:   "incremental_backups_enabled",
			wantType: "int",
		},
		{
			name:     "query_logs_enabled",
			pragma:   "query_logs_enabled",
			wantType: "int",
		},
		{
			name:     "error_logs_enabled",
			pragma:   "error_logs_enabled",
			wantType: "int",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			value, err := handler.getPragma(tt.pragma, settings)
			require.NoError(t, err)
			require.NotNil(t, value)

			// Check that we got the expected type
			if tt.wantType == "int" {
				_, ok := value.(int)
				assert.True(t, ok, "Expected int type")
			} else {
				_, ok := value.(string)
				assert.True(t, ok, "Expected string type")
			}
		})
	}
}

func TestLitebasePragmaHandler_SetPragma(t *testing.T) {
	handler := &LitebasePragmaHandler{}

	tests := []struct {
		name      string
		setting   string
		value     string
		verify    func(t *testing.T, settings *DatabaseBranchSettings)
		wantErr   bool
		errString string
	}{
		{
			name:    "set backups_enabled to false",
			setting: "backups_enabled",
			value:   "false",
			verify: func(t *testing.T, settings *DatabaseBranchSettings) {
				assert.False(t, settings.BackupsEnabled)
			},
		},
		{
			name:    "set backups_enabled to true",
			setting: "backups_enabled",
			value:   "1",
			verify: func(t *testing.T, settings *DatabaseBranchSettings) {
				assert.True(t, settings.BackupsEnabled)
			},
		},
		{
			name:    "set backup_interval",
			setting: "backup_interval",
			value:   "48h",
			verify: func(t *testing.T, settings *DatabaseBranchSettings) {
				assert.Equal(t, DatabaseBranchBackupInterval("48h"), settings.BackupInterval)
			},
		},
		{
			name:    "set backups_retention_days",
			setting: "backups_retention_days",
			value:   "14",
			verify: func(t *testing.T, settings *DatabaseBranchSettings) {
				assert.Equal(t, 14, settings.BackupsRetentionDays)
			},
		},
		{
			name:    "set incremental_backups_enabled",
			setting: "incremental_backups_enabled",
			value:   "on",
			verify: func(t *testing.T, settings *DatabaseBranchSettings) {
				assert.True(t, settings.IncrementalBackupsEnabled)
			},
		},
		{
			name:      "invalid backup_interval",
			setting:   "backup_interval",
			value:     "12h",
			wantErr:   true,
			errString: "invalid backup interval",
		},
		{
			name:      "invalid boolean value",
			setting:   "backups_enabled",
			value:     "maybe",
			wantErr:   true,
			errString: "invalid boolean value",
		},
		{
			name:      "invalid retention days",
			setting:   "backups_retention_days",
			value:     "-1",
			wantErr:   true,
			errString: "must be a positive integer",
		},
		{
			name:      "unknown pragma",
			setting:   "unknown_setting",
			value:     "value",
			wantErr:   true,
			errString: "unknown litebase PRAGMA",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create settings with valid defaults
			settings := &DatabaseBranchSettings{
				BackupsEnabled:                  true,
				BackupInterval:                  "24h",
				BackupsRetentionDays:            7,
				IncrementalBackupsEnabled:       false,
				IncrementalBackupsRetentionDays: 3,
				QueryLogsEnabled:                true,
				QueryLogsRetentionDays:          14,
				ErrorLogsEnabled:                true,
				ErrorLogsRetentionDays:          30,
			}

			// For testing purposes, create a mock branch
			// In real implementation, this would update the database
			err := handler.setPragmaForTest(tt.setting, tt.value, settings)

			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errString)
			} else {
				require.NoError(t, err)

				// Verify the setting was updated
				if tt.verify != nil {
					tt.verify(t, settings)
				}
			}
		})
	}
}

// Helper method for testing setPragma without database dependency
func (h *LitebasePragmaHandler) setPragmaForTest(name, value string, settings *DatabaseBranchSettings) error {
	// This is the same logic as setPragma but without the database update
	switch name {
	case "backups_enabled":
		boolVal, err := parseBool(value)
		if err != nil {
			return err
		}
		settings.BackupsEnabled = boolVal

	case "backup_interval":
		interval := DatabaseBranchBackupInterval(value)

		if !interval.IsValid() {
			return fmt.Errorf("invalid backup interval format")
		}

		settings.BackupInterval = interval
	case "backups_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return fmt.Errorf("backups_retention_days must be a positive integer")
		}

		settings.BackupsRetentionDays = days
	case "incremental_backups_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return err
		}

		settings.IncrementalBackupsEnabled = boolVal
	case "incremental_backups_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return fmt.Errorf("incremental_backups_retention_days must be a positive integer")
		}

		settings.IncrementalBackupsRetentionDays = days
	case "query_logs_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return err
		}

		settings.QueryLogsEnabled = boolVal
	case "query_logs_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return fmt.Errorf("query_logs_retention_days must be a positive integer")
		}

		settings.QueryLogsRetentionDays = days
	case "error_logs_enabled":
		boolVal, err := parseBool(value)

		if err != nil {
			return err
		}

		settings.ErrorLogsEnabled = boolVal
	case "error_logs_retention_days":
		days, err := strconv.Atoi(value)

		if err != nil || days < 1 {
			return fmt.Errorf("error_logs_retention_days must be a positive integer")
		}

		settings.ErrorLogsRetentionDays = days
	default:
		return fmt.Errorf("unknown litebase PRAGMA: %s", name)
	}

	return nil
}

func TestLitebasePragmaHandler_GetAfterSet(t *testing.T) {
	handler := &LitebasePragmaHandler{}

	settings := &DatabaseBranchSettings{
		BackupsEnabled:                  true,
		BackupInterval:                  "24h",
		BackupsRetentionDays:            7,
		IncrementalBackupsEnabled:       false,
		IncrementalBackupsRetentionDays: 3,
		QueryLogsEnabled:                true,
		QueryLogsRetentionDays:          14,
		ErrorLogsEnabled:                true,
		ErrorLogsRetentionDays:          30,
	}

	// Set a value
	err := handler.setPragmaForTest("backups_enabled", "false", settings)
	require.NoError(t, err)

	// Get the value
	value, err := handler.getPragma("backups_enabled", settings)
	require.NoError(t, err)
	require.NotNil(t, value)

	// Should be 0 (false)
	assert.Equal(t, 0, value)

	// Set to true
	err = handler.setPragmaForTest("backups_enabled", "true", settings)
	require.NoError(t, err)

	// Get the value again
	value, err = handler.getPragma("backups_enabled", settings)
	require.NoError(t, err)
	require.NotNil(t, value)

	// Should be 1 (true)
	assert.Equal(t, 1, value)
}
