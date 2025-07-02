package database

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
)

type DatabaseSettings struct {
	Backups DatabaseBackupSettings `json:"backups"`
}

// Implement sql.Scanner interface for reading JSON from database
func (ds *DatabaseSettings) Scan(value interface{}) error {
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
	IncrementalBackups DatabaseIncrementalBackupSettings `json:"incremental"`
}

type DatabaseIncrementalBackupSettings struct {
	Enabled bool `json:"enabled"`
}
