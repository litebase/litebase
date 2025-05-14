package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/server"
	"github.com/litebase/litebase/server/database"
)

func TestNewDatabaseManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		if dm == nil {
			t.Errorf("Expected non-nil DatabaseManager")
		}
	})
}

func TestDatabaseManager_All(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		databases, err := dm.All()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(databases) != 0 {
			t.Errorf("Expected 0 databases, got %d", len(databases))
		}
	})
}

func TestDatabaseManager_ConnectionManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		if dm.ConnectionManager() == nil {
			t.Errorf("Expected non-nil ConnectionManager")
		}
	})
}

func TestDatabaseManager_Create(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if database == nil {
			t.Fatal("Expected non-nil DatabaseKey")
		}

		if database.Name != "test" {
			t.Errorf("Expected DatabaseId to be 'test', got %s", database.Id)
		}
	})
}

func TestDatabaseManager_Delete(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		fileSystem := dm.Resources(database.Id, database.PrimaryBranchId).FileSystem()

		// Ensure the database directory exists
		if !fileSystem.Exists() {
			t.Errorf("Expected database directory to exist")
		}

		err = dm.Delete(database)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		// Ensure the database directory does not exist
		if fileSystem.Exists() {
			t.Errorf("Expected database directory to not exist")
		}

		_, err = dm.Get(database.Id)

		if err == nil {
			t.Errorf("Expected error, got nil")
		}
	})
}

func TestDatabaseManager_Exists(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		exists, err := dm.Exists("nonexistent")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if exists {
			t.Errorf("Expected false, got true")
		}

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		exists, err = dm.Exists(database.Name)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if !exists {
			t.Errorf("Expected true, got false")
		}
	})
}

func TestDatabaseManager_Get(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		db, err := dm.Get(database.Id)

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if db == nil {
			t.Fatal("Expected non-nil Database")
		}

		if db.Id != database.Id {
			t.Errorf("Expected DatabaseId to be %s, got %s", database.Id, db.Id)
		}
	})
}

func TestDatabaseManager_PageLogManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		if dm.PageLogManager() == nil {
			t.Errorf("Expected non-nil PageLogManager")
		}
	})
}

func TestDatabaseManager_Resources(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		resources := dm.Resources(database.Id, database.PrimaryBranchId)

		if resources == nil {
			t.Errorf("Expected non-nil Resources")
		}
	})
}

func TestDatabaseManager_ShutdownResources(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

		database, err := dm.Create("test", "main")

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		err = dm.ShutdownResources()

		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		resources := dm.Resources(database.Id, database.PrimaryBranchId)

		if resources == nil {
			t.Errorf("Expected non-nil Resources")
		}
	})
}
