package cluster_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/server"
)

func TestEventsManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		eventsManager := app.Cluster.EventsManager()

		if eventsManager == nil {
			t.Error("EventsManager() returned nil")
		}
	})
}

func TestEventsManagerHook(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		eventsManager := app.Cluster.EventsManager()

		hook := eventsManager.Hook()

		if hook == nil {
			t.Error("EventsManagerHook() returned nil")
		}
	})
}

func TestEventsManagerInit(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		eventsManager := app.Cluster.EventsManager()

		eventsManager.Init()
	})
}
