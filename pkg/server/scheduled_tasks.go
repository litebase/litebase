package server

import (
	"context"
	"log/slog"

	"github.com/litebase/litebase/pkg/scheduler"
)

// InitScheduledTasks registers all scheduled tasks with the scheduler.
func (app *App) InitScheduledTasks() {
	// Register your scheduled tasks here

	// CRITICAL TASKS - These will catch up after downtime:
	// Critical tasks are important for data integrity and should run even if missed.
	// If the cluster was down for 5 days, each critical task runs once on startup.

	// Enqueue backups every 15 minutes:
	// Checks for branches with backup_next_at <= now and enqueues backup jobs.
	// Running every 15 min ensures backups execute within 15 min of scheduled time.
	// Natural distribution: backups spread across 24h based on branch creation time.
	// Note: NOT critical - if we miss enqueueing during downtime, backups will
	// catch up naturally when the scheduler resumes (backup_next_at is preserved).
	err := app.Scheduler.RegisterTask(
		"EnqueueBackups",
		func(ctx context.Context) error { return app.EnqueueBackupJobs(ctx) },
		scheduler.WithCron("*/15 * * * *"), // Every 15 minutes
		scheduler.WithoutOverlap(),         // Don't run concurrent enqueue operations
	)

	if err != nil {
		slog.Error("Failed to register EnqueueBackups task", "error", err)
	}

	// Cleanup old database backups (critical - prevents storage bloat):
	// Runs daily at 04:00 to find and cleanup expired backups.
	// Backups older than retention_days are deleted from storage and database.
	// Daily execution ensures short retention periods (1-7 days) are honored promptly.
	// Critical because storage bloat affects system operation and costs.
	err = app.Scheduler.RegisterTask(
		"CleanupExpiredBackups",
		func(ctx context.Context) error { return app.EnqueueBackupCleanupJobs(ctx) },
		scheduler.WithSchedule(scheduler.Daily),
		scheduler.WithTime("04:00"),
		scheduler.WithCritical(),   // Will catch up after downtime to prevent storage bloat
		scheduler.WithoutOverlap(), // Don't run concurrent cleanup operations
	)

	if err != nil {
		slog.Error("Failed to register CleanupExpiredBackups task", "error", err)
	}

	// Cleanup old incremental backups (critical - prevents storage bloat):
	// Runs daily at 04:30 to find and cleanup expired snapshots and rollback logs.
	// Incremental backups (snapshots/rollback logs) older than retention_days are deleted.
	// Daily execution ensures short retention periods (1-7 days) are honored promptly.
	// Critical because incremental backup storage can grow quickly and affect costs.
	err = app.Scheduler.RegisterTask(
		"CleanupExpiredIncrementalBackups",
		func(ctx context.Context) error { return app.EnqueueIncrementalBackupCleanupJobs(ctx) },
		scheduler.WithSchedule(scheduler.Daily),
		scheduler.WithTime("04:30"),
		scheduler.WithCritical(),   // Will catch up after downtime to prevent storage bloat
		scheduler.WithoutOverlap(), // Don't run concurrent cleanup operations
	)

	if err != nil {
		slog.Error("Failed to register CleanupExpiredIncrementalBackups task", "error", err)
	}

	// Prune old queued jobs (critical - prevents table growth):
	// app.Scheduler.RegisterTask(
	// 	"PruneQueuedJobs",
	// 	queue.PruneQueuedJobsTask,
	// 	scheduler.WithSchedule(scheduler.Daily),
	// 	scheduler.WithTime("01:00"),
	// 	scheduler.WithCritical(), // Will catch up after downtime
	// 	scheduler.WithoutOverlap(),
	// )

	// NON-CRITICAL TASKS - These will be skipped if missed during downtime:
	// Informational or monitoring tasks that don't affect data integrity.

	// Hourly status report (informational - okay to skip if missed):
	// app.Scheduler.RegisterTask(
	// 	"HourlyStatusReport",
	// 	statusReportHandler,
	// 	scheduler.WithSchedule(scheduler.Hourly),
	// 	// No WithCritical() - will be skipped if missed
	// )

	// Every 10 minutes health check (monitoring - okay to skip):
	// app.Scheduler.RegisterTask(
	// 	"HealthCheck",
	// 	healthCheckHandler,
	// 	scheduler.WithCron("*/10 * * * *"),
	// 	// No WithCritical() - will resume on schedule after startup
	// )

	// Twice daily metrics collection (analytics - okay to skip):
	// app.Scheduler.RegisterTask(
	// 	"MetricsCollection",
	// 	metricsHandler,
	// 	scheduler.WithCron("0 2,14 * * *"),
	// 	// No WithCritical() - missed data points are acceptable
	// )

	slog.Info("Scheduled tasks initialized")
}
