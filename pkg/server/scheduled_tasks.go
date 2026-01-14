package server

import (
	"log/slog"
)

// InitScheduledTasks registers all scheduled tasks with the scheduler.
func (app *App) InitScheduledTasks() {
	// Example task - replace with actual tasks as needed
	// err := app.Scheduler.RegisterTask(
	// 	"ExampleTask",
	// 	exampleTaskHandler,
	// 	scheduler.WithSchedule(scheduler.Daily),
	// 	scheduler.WithTime("02:00"),
	// )
	// if err != nil {
	// 	slog.Error("Failed to register example task", "error", err)
	// }

	slog.Info("Scheduled tasks initialized")
}
