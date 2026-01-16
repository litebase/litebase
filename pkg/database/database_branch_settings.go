package database

import (
	"database/sql"
	"time"
)

type DatabaseBranchBackupInterval string

func (d DatabaseBranchBackupInterval) IsValid() bool {
	// Parse the interval as a Go duration string
	duration, err := time.ParseDuration(string(d))

	if err != nil {
		return false
	}

	// Ensure the interval is at least 1 day (24 hours)
	minDuration := 24 * time.Hour

	if duration < minDuration {
		return false
	}

	// Ensure the interval is an increment of 1 day
	return duration%(24*time.Hour) == 0
}

type DatabaseBranchSettings struct {
	BackupsCleanedAt                sql.NullInt64                  `json:"backupsCleanedAt"`
	BackupsEnabled                  bool                           `json:"backupsEnabled"`
	BackupInterval                  DatabaseBranchBackupInterval   `json:"backupInterval"`
	BackupNextAt                    sql.NullInt64                  `json:"backupNextAt"`
	BackupsRetentionDays            int                            `json:"backupsRetentionDays"`
	DefaultPragmas                  *DatabaseDefaultPragmaSettings `json:"defaultPragmas"`
	ErrorLogsCleanedAt              sql.NullInt64                  `json:"errorLogsCleanedAt"`
	ErrorLogsEnabled                bool                           `json:"errorLogsEnabled"`
	ErrorLogsRetentionDays          int                            `json:"errorLogsRetentionDays"`
	IncrementalBackupsCleanedAt     sql.NullInt64                  `json:"incrementalBackupsCleanedAt"`
	IncrementalBackupsEnabled       bool                           `json:"incrementalBackupsEnabled"`
	IncrementalBackupsRetentionDays int                            `json:"incrementalBackupsRetentionDays"`
	QueryLogsCleanedAt              sql.NullInt64                  `json:"queryLogsCleanedAt"`
	QueryLogsEnabled                bool                           `json:"queryLogsEnabled"`
	QueryLogsRetentionDays          int                            `json:"queryLogsRetentionDays"`
}

type DatabaseDefaultPragmaSettings struct {
	ForeignKeys string `json:"foreignKeys" validate:"required,oneof=ON OFF"`
}

// NewDefaultBranchSettings creates default settings for a new branch.
func NewDefaultBranchSettings() *DatabaseBranchSettings {
	return &DatabaseBranchSettings{
		BackupsEnabled:                  true,
		BackupInterval:                  "24h",
		BackupNextAt:                    sql.NullInt64{Valid: true, Int64: time.Now().Add(24 * time.Hour).Unix()},
		BackupsRetentionDays:            30,
		IncrementalBackupsEnabled:       true,
		IncrementalBackupsRetentionDays: 7,
		QueryLogsEnabled:                true,
		QueryLogsRetentionDays:          15,
		ErrorLogsEnabled:                true,
		ErrorLogsRetentionDays:          15,
		DefaultPragmas: &DatabaseDefaultPragmaSettings{
			ForeignKeys: "ON",
		},
	}
}
