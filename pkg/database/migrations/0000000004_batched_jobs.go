package migrations

import "database/sql"

// Migration0000000004BatchedJobs creates the job_batches and batched_jobs tables for batch job tracking.
func Migration0000000004BatchedJobs(db *sql.DB) error {
	// Create the job_batches table
	_, err := db.Exec(
		`CREATE TABLE IF NOT EXISTS job_batches
		(
			id INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			total_jobs INTEGER NOT NULL DEFAULT 0,
			pending_jobs INTEGER NOT NULL DEFAULT 0,
			failed_jobs INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			finished_at TEXT
		) STRICT`,
	)

	if err != nil {
		return err
	}

	// Create index on created_at for efficient cleanup queries
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_job_batches_created_at 
		ON job_batches (created_at)`,
	)

	if err != nil {
		return err
	}

	// Create the batched_jobs table
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS batched_jobs
		(
			id INTEGER PRIMARY KEY,
			batch_id INTEGER NOT NULL,
			queue_id INTEGER NOT NULL,
			status TEXT NOT NULL DEFAULT 'pending',
			progress INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			FOREIGN KEY (batch_id) REFERENCES job_batches(id) ON DELETE CASCADE,
			FOREIGN KEY (queue_id) REFERENCES queued_jobs(id) ON DELETE CASCADE
		) STRICT`,
	)

	if err != nil {
		return err
	}

	// Create index on batch_id for efficient batch status queries
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_batched_jobs_batch_id 
		ON batched_jobs (batch_id)`,
	)

	if err != nil {
		return err
	}

	// Create index on queue_id for efficient job lookup
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_batched_jobs_queue_id 
		ON batched_jobs (queue_id)`,
	)

	if err != nil {
		return err
	}

	// Create index on batch_id and status for efficient progress tracking
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_batched_jobs_batch_status 
		ON batched_jobs (batch_id, status)`,
	)

	if err != nil {
		return err
	}

	return nil
}
