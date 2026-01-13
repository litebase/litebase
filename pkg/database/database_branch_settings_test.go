package database

import (
	"testing"
)

func TestDatabaseBranchBackupInterval_IsValid(t *testing.T) {
	tests := []struct {
		name     string
		interval DatabaseBranchBackupInterval
		want     bool
		reason   string
	}{
		{
			name:     "valid 1 day",
			interval: "24h",
			want:     true,
			reason:   "24h is exactly 1 day",
		},
		{
			name:     "valid 2 days",
			interval: "48h",
			want:     true,
			reason:   "48h is exactly 2 days",
		},
		{
			name:     "valid 7 days",
			interval: "168h",
			want:     true,
			reason:   "168h is exactly 7 days",
		},
		{
			name:     "valid 30 days",
			interval: "720h",
			want:     true,
			reason:   "720h is exactly 30 days",
		},
		{
			name:     "7d format",
			interval: "7d",
			want:     false,
			reason:   "Go's time.ParseDuration doesn't support 'd' suffix",
		},
		{
			name:     "30d format",
			interval: "30d",
			want:     false,
			reason:   "Go's time.ParseDuration doesn't support 'd' suffix",
		},
		{
			name:     "2w format",
			interval: "2w",
			want:     false,
			reason:   "Go's time.ParseDuration doesn't support 'w' suffix",
		},
		{
			name:     "too short - 1 hour",
			interval: "1h",
			want:     false,
			reason:   "1h is less than minimum 24h",
		},
		{
			name:     "too short - 12 hours",
			interval: "12h",
			want:     false,
			reason:   "12h is less than minimum 24h",
		},
		{
			name:     "too short - 23 hours",
			interval: "23h",
			want:     false,
			reason:   "23h is less than minimum 24h",
		},
		{
			name:     "not day increment - 25 hours",
			interval: "25h",
			want:     false,
			reason:   "25h is not an increment of 24h",
		},
		{
			name:     "not day increment - 36 hours",
			interval: "36h",
			want:     false,
			reason:   "36h is not an increment of 24h",
		},
		{
			name:     "not day increment - 50 hours",
			interval: "50h",
			want:     false,
			reason:   "50h is not an increment of 24h",
		},
		{
			name:     "invalid format",
			interval: "invalid",
			want:     false,
			reason:   "Cannot parse as duration",
		},
		{
			name:     "empty string",
			interval: "",
			want:     false,
			reason:   "Empty string is not a valid duration",
		},
		{
			name:     "negative duration",
			interval: "-24h",
			want:     false,
			reason:   "Negative duration is less than minimum",
		},
		{
			name:     "zero duration",
			interval: "0h",
			want:     false,
			reason:   "Zero is less than minimum 24h",
		},
		{
			name:     "valid large interval - 90 days",
			interval: "2160h",
			want:     true,
			reason:   "2160h is exactly 90 days",
		},
		{
			name:     "valid with minutes that equal full days",
			interval: "2880m",
			want:     true,
			reason:   "2880m equals 48h (2 days)",
		},
		{
			name:     "invalid with extra minute",
			interval: "1441m",
			want:     false,
			reason:   "1441m is 24h1m, not an increment of 24h",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.interval.IsValid()

			if got != tt.want {
				t.Errorf("IsValid() = %v, want %v (reason: %s)", got, tt.want, tt.reason)
			}
		})
	}
}

func TestNewDefaultBranchSettings(t *testing.T) {
	settings := NewDefaultBranchSettings()

	if settings == nil {
		t.Fatal("Expected non-nil settings")
	}

	// Test default values
	if !settings.BackupsEnabled {
		t.Error("Expected BackupsEnabled to be true by default")
	}

	if settings.BackupInterval != "24h" {
		t.Errorf("Expected BackupInterval to be '24h', got '%s'", settings.BackupInterval)
	}

	if settings.BackupsRetentionDays != 30 {
		t.Errorf("Expected BackupsRetentionDays to be 30, got %d", settings.BackupsRetentionDays)
	}

	if !settings.IncrementalBackupsEnabled {
		t.Error("Expected IncrementalBackupsEnabled to be true by default")
	}

	if settings.IncrementalBackupsRetentionDays != 7 {
		t.Errorf("Expected IncrementalBackupsRetentionDays to be 7, got %d", settings.IncrementalBackupsRetentionDays)
	}

	if !settings.QueryLogsEnabled {
		t.Error("Expected QueryLogsEnabled to be true by default")
	}

	if settings.QueryLogsRetentionDays != 15 {
		t.Errorf("Expected QueryLogsRetentionDays to be 15, got %d", settings.QueryLogsRetentionDays)
	}

	if !settings.ErrorLogsEnabled {
		t.Error("Expected ErrorLogsEnabled to be true by default")
	}

	if settings.ErrorLogsRetentionDays != 15 {
		t.Errorf("Expected ErrorLogsRetentionDays to be 15, got %d", settings.ErrorLogsRetentionDays)
	}

	if settings.DefaultPragmas == nil {
		t.Fatal("Expected non-nil DefaultPragmas")
	}

	if settings.DefaultPragmas.ForeignKeys != "ON" {
		t.Errorf("Expected ForeignKeys to be 'ON', got '%s'", settings.DefaultPragmas.ForeignKeys)
	}

	// Verify that the default BackupInterval is valid
	if !settings.BackupInterval.IsValid() {
		t.Error("Expected default BackupInterval to be valid")
	}
}
