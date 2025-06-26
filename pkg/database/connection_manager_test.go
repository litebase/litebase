package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestConnectionManager_CheckpointAll(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		test.MockDatabase(app)
		test.MockDatabase(app)

		connectionManager := app.DatabaseManager.ConnectionManager()
		connectionManager.CheckpointAll()
	})
}

func TestConnectionManager_CloseDatabaseConnections(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		app.DatabaseManager.ConnectionManager().CloseDatabaseConnections(mock1.DatabaseID)

		_, err = con1.GetConnection().Exec("SELECT 1", nil)

		if err != database.ErrDatabaseConnectionClosed {
			t.Fatalf("Expected database connection to be closed, got %v", err)
		}
	})
}

func TestConnectionManager_CloseDatabaseBranchConnections(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		app.DatabaseManager.ConnectionManager().CloseDatabaseBranchConnections(mock1.DatabaseID, mock1.BranchID)

		_, err = con1.GetConnection().Exec("SELECT 1", nil)

		if err != database.ErrDatabaseConnectionClosed {
			t.Fatalf("Expected database connection to be closed, got %v", err)
		}
	})
}

func TestConnectionManager_Drain(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		defaultConnectionDrainingWaitTime := database.ConnectionDrainingWaitTime
		database.ConnectionDrainingWaitTime = 0

		defer func() {
			database.ConnectionDrainingWaitTime = defaultConnectionDrainingWaitTime
		}()

		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		err = app.DatabaseManager.ConnectionManager().Drain(mock1.DatabaseID, mock1.BranchID, func() error {
			return nil
		})

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestConnectionManager_ForceCheckpoint(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		err = app.DatabaseManager.ConnectionManager().ForceCheckpoint(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestConnectionManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		// Simulate some work with the connection
		_, err = con1.GetConnection().Exec("SELECT 1", nil)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}
	})
}

func TestConnectionManager_Release(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)
	})
}

func TestConnectionManager_Shutdown(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		defaultConnectionDrainingWaitTime := database.ConnectionDrainingWaitTime
		database.ConnectionDrainingWaitTime = 0

		defer func() {
			database.ConnectionDrainingWaitTime = defaultConnectionDrainingWaitTime
		}()

		mock1 := test.MockDatabase(app)

		con1, err := app.DatabaseManager.ConnectionManager().Get(mock1.DatabaseID, mock1.BranchID)

		if err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		defer app.DatabaseManager.ConnectionManager().Release(con1.DatabaseID, con1.BranchID, con1)

		app.DatabaseManager.ConnectionManager().Shutdown()

		if !con1.GetConnection().Closed() {
			t.Fatalf("Expected connection to be closed")
		}
	})
}

func TestConnectionManager_StateError(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		app.DatabaseManager.ConnectionManager().Shutdown()

		if app.DatabaseManager.ConnectionManager().StateError() != database.ErrorConnectionManagerShutdown {
			t.Fatalf("Expected StateError to be ErrorConnectionManagerShutdown, got %v", app.DatabaseManager.ConnectionManager().StateError())
		}
	})
}

func TestConnectionManager_Tick(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		app.DatabaseManager.ConnectionManager().Tick()
	})
}
