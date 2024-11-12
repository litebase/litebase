package database_test

import (
	"litebase/internal/test"
	"litebase/server"
	"litebase/server/database"
	"testing"
	"time"
)

func TestNewDatabaseWALSynchronizer(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)

		if synchronizer == nil {
			t.Error("expected synchronizer to not be nil")
		}
	})
}

func TestDatabaseWALSynchronizeTruncate(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)
		db := test.MockDatabase(app)

		err := synchronizer.Truncate(
			db.DatabaseId,
			db.BranchId,
			0,
			0,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected error to be nil, got %v", err)
		}
	})
}

func TestDatabaseWALSynchronizeWriteAt(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		synchronizer := database.NewDatabaseWALSynchronizer(app.DatabaseManager)
		db := test.MockDatabase(app)

		err := synchronizer.WriteAt(
			db.DatabaseId,
			db.BranchId,
			[]byte("hello"),
			0,
			1,
			time.Now().Unix(),
		)

		if err != nil {
			t.Errorf("expected error to be nil, got %v", err)
		}
	})
}
