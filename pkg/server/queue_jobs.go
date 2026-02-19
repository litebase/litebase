package server

import (
	"time"

	"github.com/litebase/litebase/pkg/queue"
)

func (app *App) InitQueueJobs() {
	// Register backup job with a wrapper that updates backup_next_at on success.
	err := app.QueueWorkerPool.RegisterJob(
		"BackupJob",
		app.BackupJob,
		queue.WithRetries(5, 5*time.Minute),
	)

	if err != nil {
		panic(err)
	}

	// Register backup cleanup job with retries and timeout for file operations.
	err = app.QueueWorkerPool.RegisterJob(
		"CleanupBackupJob",
		app.CleanupBackupJob,
		queue.WithRetries(3, 5*time.Minute),
		queue.WithTimeout(30*time.Minute),
	)

	if err != nil {
		panic(err)
	}

	// Register incremental backup cleanup job with retries and timeout for file operations.
	err = app.QueueWorkerPool.RegisterJob(
		"CleanupIncrementalBackupJob",
		app.CleanupIncrementalBackupJob,
		queue.WithRetries(3, 5*time.Minute),
		queue.WithTimeout(30*time.Minute),
	)

	if err != nil {
		panic(err)
	}

	// Register query log cleanup job with retries and timeout for file operations.
	err = app.QueueWorkerPool.RegisterJob(
		"CleanupQueryLogJob",
		app.CleanupQueryLogJob,
		queue.WithRetries(3, 5*time.Minute),
		queue.WithTimeout(30*time.Minute),
	)

	if err != nil {
		panic(err)
	}

	// Register error log cleanup job with retries and timeout for file operations.
	err = app.QueueWorkerPool.RegisterJob(
		"CleanupErrorLogJob",
		app.CleanupErrorLogJob,
		queue.WithRetries(3, 5*time.Minute),
		queue.WithTimeout(30*time.Minute),
	)

	if err != nil {
		panic(err)
	}
}
