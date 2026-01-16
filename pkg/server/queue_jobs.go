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
}
