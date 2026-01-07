package database

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type DatabaseSettings struct {
	Backups        DatabaseBackupSettings        `json:"backups"`
	DefaultPragmas DatabaseDefaultPragmaSettings `json:"default_pragmas"`
	Observability  DatabaseObservabilitySettings `json:"observability"`
}

// Implement sql.Scanner interface for reading JSON from database
func (ds *DatabaseSettings) Scan(value any) error {
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
func (ds DatabaseSettings) Value() (driver.Value, error) {
	return json.Marshal(ds)
}

type DatabaseBackupSettings struct {
	Enabled            bool                              `json:"enabled"`
	Interval           string                            `json:"interval"`
	IncrementalBackups DatabaseIncrementalBackupSettings `json:"incremental"`
	RetentionDays      int                               `json:"retention_days"`
}

type DatabaseDefaultPragmaSettings struct {
	ForeignKeys string `json:"foreign_keys"`
}

type DatabaseIncrementalBackupSettings struct {
	Enabled       bool `json:"enabled"`
	RetentionDays int  `json:"retention_days"`
}

type DatabaseObservabilitySettings struct {
	Logs DatabaseObservabilityLogSettings `json:"logs"`
}

type DatabaseObservabilityLogSettings struct {
	Enabled       bool `json:"enabled"`
	RetentionDays int  `json:"retention_days"`
}
