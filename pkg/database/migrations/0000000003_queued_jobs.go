package migrations

import "database/sql"

// Migration0000000003QueuedJobs creates the queued_jobs table for the job queue system.
func Migration0000000003QueuedJobs(db *sql.DB) error {
	// Create the queued_jobs table
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS queued_jobs
		(
			id INTEGER PRIMARY KEY,
			queue_name TEXT NOT NULL,
			name TEXT NOT NULL,
			key TEXT NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			data TEXT,
			attempts INTEGER NOT NULL DEFAULT 0,
			max_attempts INTEGER NOT NULL DEFAULT 3,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			available_at TEXT NOT NULL,
			reserved_at TEXT,
			reserved_by TEXT,
			completed_at TEXT,
			error_log TEXT
		) STRICT`,
	)

	if err != nil {
		return err
	}

	// Create index on queue_name and status for efficient job retrieval
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_queued_jobs_queue_status 
		ON queued_jobs (queue_name, status, available_at)`,
	)

	if err != nil {
		return err
	}

	// Create index on key for efficient lookups when checking for existing unprocessed jobs
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_queued_jobs_key 
		ON queued_jobs (key)`,
	)

	if err != nil {
		return err
	}

	// Create index on status for monitoring
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_queued_jobs_status 
		ON queued_jobs (status)`,
	)

	if err != nil {
		return err
	}

	return nil
}
