package database

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type DatabaseBranchSettings struct {
	BackupsCleanedAt                int64                          `json:"backupsCleanedAt"`
	BackupsEnabled                  bool                           `json:"backupsEnabled"`
	BackupInterval                  string                         `json:"backupInterval"`
	BackupNextAt                    int64                          `json:"backupNextAt"`
	BackupsRetentionDays            int                            `json:"backupsRetentionDays"`
	DefaultPragmas                  *DatabaseDefaultPragmaSettings `json:"defaultPragmas"`
	ErrorLogsCleanedAt              int64                          `json:"errorLogsCleanedAt"`
	ErrorLogsEnabled                bool                           `json:"errorLogsEnabled"`
	ErrorLogsRetentionDays          int                            `json:"errorLogsRetentionDays"`
	IncrementalBackupsCleanedAt     int64                          `json:"incrementalBackupsCleanedAt"`
	IncrementalBackupsEnabled       bool                           `json:"incrementalBackupsEnabled"`
	IncrementalBackupsRetentionDays int                            `json:"incrementalBackupsRetentionDays"`
	QueryLogsCleanedAt              int64                          `json:"queryLogsCleanedAt"`
	QueryLogsEnabled                bool                           `json:"queryLogsEnabled"`
	QueryLogsRetentionDays          int                            `json:"queryLogsRetentionDays"`
}

type DatabaseDefaultPragmaSettings struct {
	ForeignKeys string `json:"foreignKeys"`
}

// Implement sql.Scanner interface for reading JSON from database
func (ds *DatabaseDefaultPragmaSettings) Scan(value any) error {
	if value == nil {
		return nil
	}

	var jsonData []byte
	switch v := value.(type) {
	case string:
		jsonData = []byte(v)
	case []byte:
		jsonData = v
	default:
		return fmt.Errorf("cannot scan %T into DatabaseSettings", value)
	}

	return json.Unmarshal(jsonData, ds)
}

// Implement driver.Valuer interface for storing JSON to database
func (ds DatabaseDefaultPragmaSettings) Value() (driver.Value, error) {
	return json.Marshal(ds)
}
