package migrations

import "database/sql"

// Migration0000000002DatabaseBranchSettings creates the database_branch_settings table.
func Migration0000000002DatabaseBranchSettings(db *sql.DB) error {
	// Create the database_branch_settings table
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS database_branch_settings (
			id INTEGER PRIMARY KEY,
			database_branch_reference_id INTEGER NOT NULL UNIQUE,
			
			backups_enabled INTEGER DEFAULT 0,
			backups_interval TEXT DEFAULT '24h',
			backups_retention_days INTEGER DEFAULT 30,
			backups_cleaned_at INTEGER,
			backup_next_at INTEGER,

			default_pragmas_json TEXT,

			error_logs_enabled INTEGER DEFAULT 1,
			error_logs_retention_days INTEGER DEFAULT 15,
			error_logs_cleaned_at INTEGER,
		
			incremental_backups_enabled INTEGER DEFAULT 0,
			incremental_backups_retention_days INTEGER DEFAULT 7,
			incremental_backups_cleaned_at INTEGER,
		
			query_logs_enabled INTEGER DEFAULT 1,
			query_logs_retention_days INTEGER DEFAULT 15,
			query_logs_cleaned_at INTEGER,
		
			created_at INTEGER NOT NULL,
			updated_at INTEGER NOT NULL,
		
			FOREIGN KEY (database_branch_reference_id) REFERENCES database_branches(id) ON DELETE CASCADE
		) STRICT
	`)

	if err != nil {
		return err
	}

	// Create index for finding branches that need backups
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_branch_settings_next_backup
		ON database_branch_settings(backup_next_at)
		WHERE backups_enabled = 1 AND backup_next_at IS NOT NULL
	`)

	if err != nil {
		return err
	}

	// Create index for finding branches that need backup cleanup
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_branch_settings_backup_cleanup
		ON database_branch_settings(backups_cleaned_at)
		WHERE backups_enabled = 1
	`)

	if err != nil {
		return err
	}

	// Create index for finding branches that need incremental backup cleanup
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_branch_settings_incremental_cleanup
		ON database_branch_settings(incremental_backups_cleaned_at)
		WHERE incremental_backups_enabled = 1
	`)

	if err != nil {
		return err
	}

	// Create index for finding branches that need query log cleanup
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_branch_settings_query_log_cleanup
		ON database_branch_settings(query_logs_cleaned_at)
		WHERE query_logs_enabled = 1
	`)

	if err != nil {
		return err
	}

	// Create index for finding branches that need error log cleanup
	_, err = db.Exec(`
		CREATE INDEX IF NOT EXISTS idx_branch_settings_error_log_cleanup
		ON database_branch_settings(error_logs_cleaned_at)
		WHERE error_logs_enabled = 1
	`)

	if err != nil {
		return err
	}

	return nil
}
