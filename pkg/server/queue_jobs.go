package server

import (
	"time"

	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/queue"
)

func (app *App) InitQueueJobs() {
	// Register backup job
	err := app.QueueWorkerPool.RegisterJob(
		"BackupJob",
		backups.BackupJob,
		queue.WithRetries(5, 5*time.Minute),
	)

	if err != nil {
		panic(err)
	}
}
