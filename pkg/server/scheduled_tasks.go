package server

import (
	"context"
	"log/slog"
	// Uncomment when registering tasks:
	// "github.com/litebase/litebase/pkg/scheduler"
)

// InitScheduledTasks registers all scheduled tasks with the scheduler.
func (app *App) InitScheduledTasks() {
	// Register your scheduled tasks here

	// CRITICAL TASKS - These will catch up after downtime:
	// Critical tasks are important for data integrity and should run even if missed.
	// If the cluster was down for 5 days, each critical task runs once on startup.

	// Daily backup (critical - must not be skipped):
	// app.Scheduler.RegisterTask(
	// 	"DailyBackup",
	// 	dailyBackupHandler,
	// 	scheduler.WithSchedule(scheduler.Daily),
	// 	scheduler.WithTime("03:00"),  // 3 AM UTC
	// 	scheduler.WithCritical(),     // Will catch up if missed during downtime
	// 	scheduler.WithoutOverlap(),   // Don't run concurrent backups
	// )

	// Cleanup old database backups (critical - prevents storage bloat):
	// app.Scheduler.RegisterTask(
	// 	"CleanupOldBackups",
	// 	cleanupBackupsHandler,
	// 	scheduler.WithSchedule(scheduler.Weekly),
	// 	scheduler.WithWeekday("Sunday"),
	// 	scheduler.WithTime("04:00"),
	// 	scheduler.WithCritical(),     // Will catch up after downtime
	// 	scheduler.WithoutOverlap(),
	// )

	// Prune old queued jobs (critical - prevents table growth):
	// app.Scheduler.RegisterTask(
	// 	"PruneQueuedJobs",
	// 	queue.PruneQueuedJobsTask,
	// 	scheduler.WithSchedule(scheduler.Daily),
	// 	scheduler.WithTime("01:00"),
	// 	scheduler.WithCritical(),     // Will catch up after downtime
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

// Example task handlers (implement as needed):

func exampleHandler(ctx context.Context) error {
	slog.Info("Example task executing")
	// Add your task logic here
	return nil
}
