package database_test

import (
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/server"
)

func TestDatabaseManager(t *testing.T) {
	test.RunWithApp(t, func(app *server.App) {
		t.Run("NewDatabaseManager", func(t *testing.T) {
			dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

			if dm == nil {
				t.Errorf("Expected non-nil DatabaseManager")
			}
		})

		t.Run("All", func(t *testing.T) {
			dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

			databases, err := dm.All()

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			if len(databases) != 0 {
				t.Errorf("Expected 0 databases, got %d", len(databases))
			}
		})

		t.Run("ConnectionManager", func(t *testing.T) {
			dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

			if dm.ConnectionManager() == nil {
				t.Errorf("Expected non-nil ConnectionManager")
			}
		})

		t.Run("Create", func(t *testing.T) {
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

		t.Run("Delete", func(t *testing.T) {
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

		t.Run("Delete_ActiveDatabase", func(t *testing.T) {
			dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

			db, err := dm.Create("test", "main")

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			fileSystem := dm.Resources(db.Id, db.PrimaryBranchId).FileSystem()

			// Ensure the database directory exists
			if !fileSystem.Exists() {
				t.Errorf("Expected database directory to exist")
			}

			con1, err := dm.ConnectionManager().Get(db.Id, db.PrimaryBranchId)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			defer dm.ConnectionManager().Release(db.Id, db.PrimaryBranchId, con1)

			_, err = con1.GetConnection().Exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);", nil)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			err = dm.Delete(db)

			if err != nil {
				t.Errorf("Expected no error, got %v", err)
			}

			_, err = con1.GetConnection().Exec("INSERT INTO test (value) VALUES ('Hello, World!');", nil)

			if err != database.ErrDatabaseConnectionClosed {
				t.Errorf("Expected database connection to be closed, got %v", err)
			}

			con2, err := dm.ConnectionManager().Get(db.Id, db.PrimaryBranchId)

			if err == nil {
				t.Errorf("Expected error, got nil")
			}

			dm.ConnectionManager().Release(db.Id, db.PrimaryBranchId, con2)
		})

		t.Run("Exists", func(t *testing.T) {
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

		t.Run("Get", func(t *testing.T) {
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

		t.Run("PageLogManager", func(t *testing.T) {
			dm := database.NewDatabaseManager(app.Cluster, app.Auth.SecretsManager)

			if dm.PageLogManager() == nil {
				t.Errorf("Expected non-nil PageLogManager")
			}
		})

		t.Run("Resources", func(t *testing.T) {
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

		t.Run("ShutdownResources", func(t *testing.T) {
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

		t.Run("SystemDatabase", func(t *testing.T) {
			systemDB := app.DatabaseManager.SystemDatabase()
			defer systemDB.Close()

			if systemDB == nil {
				t.Fatal("Expected non-nil SystemDatabase")
			}
		})
	})
}
