package database

type DatabaseSettings struct {
	Backups DatabaseBackupSettings `json:"backups"`
}

type DatabaseBackupSettings struct {
	Enabled            bool                              `json:"enabled"`
	IncrementalBackups DatabaseIncrementalBackupSettings `json:"incremental"`
}

type DatabaseIncrementalBackupSettings struct {
	Enabled bool `json:"enabled"`
}
