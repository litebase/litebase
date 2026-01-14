package server

import (
	"context"
	"log/slog"
	// Uncomment when registering tasks:
	// "github.com/litebase/litebase/pkg/scheduler"
)

// InitScheduledTasks registers all scheduled tasks with the scheduler.
func (app *App) InitScheduledTasks() {
	// Example tasks demonstrating different scheduling options
	// Uncomment and modify as needed

	// Simple schedules:
	// app.Scheduler.RegisterTask(
	// 	"DailyBackup",
	// 	dailyBackupHandler,
	// 	scheduler.WithSchedule(scheduler.Daily),
	// 	scheduler.WithTime("03:00"),  // 3 AM UTC
	// )

	// Weekly maintenance on Sundays:
	// app.Scheduler.RegisterTask(
	// 	"WeeklyMaintenance",
	// 	maintenanceHandler,
	// 	scheduler.WithSchedule(scheduler.Weekly),
	// 	scheduler.WithWeekday("Sunday"),
	// 	scheduler.WithTime("02:00"),
	// )

	// Cron expressions for complex schedules:

	// Every 5 minutes:
	// app.Scheduler.RegisterTask(
	// 	"FrequentCheck",
	// 	checkHandler,
	// 	scheduler.WithCron("*/5 * * * *"),
	// 	scheduler.WithoutOverlap(),
	// )

	// Twice daily (2am and 2pm):
	// app.Scheduler.RegisterTask(
	// 	"TwiceDaily",
	// 	reportHandler,
	// 	scheduler.WithCron("0 2,14 * * *"),
	// )

	// Every 10 minutes:
	// app.Scheduler.RegisterTask(
	// 	"StatusCheck",
	// 	statusHandler,
	// 	scheduler.WithCron("*/10 * * * *"),
	// )

	// Four times daily (every 6 hours):
	// app.Scheduler.RegisterTask(
	// 	"QuarterlySync",
	// 	syncHandler,
	// 	scheduler.WithCron("0 */6 * * *"),
	// )

	slog.Info("Scheduled tasks initialized")
}

// Example task handlers (implement as needed):

func exampleHandler(ctx context.Context) error {
	slog.Info("Example task executing")
	// Add your task logic here
	return nil
}
