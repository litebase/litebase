package database_test

import (
	"database/sql"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/database/migrations"
)

func TestMigrationStructure(t *testing.T) {
	// Test that migrations are properly defined
	allMigrations := migrations.GetAllMigrations()

	if len(allMigrations) == 0 {
		t.Fatal("No migrations found")
	}

	// Verify first migration is the initial schema
	if allMigrations[0].Name != "001_initial_schema" {
		t.Errorf("Expected first migration to be '001_initial_schema', got '%s'", allMigrations[0].Name)
	}

	// Verify all migrations have names and up functions
	for i, m := range allMigrations {
		if m.Name == "" {
			t.Errorf("Migration %d has empty name", i)
		}

		if m.Up == nil {
			t.Errorf("Migration %s has nil Up function", m.Name)
		}
	}
}

func TestMigrationRunner(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		// Verify migrations table exists
		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count)

		if err != nil {
			t.Fatalf("Migrations table not created: %v", err)
		}

		// Verify all migrations were recorded
		allMigrations := migrations.GetAllMigrations()

		if count != len(allMigrations) {
			t.Errorf("Expected %d migrations to be recorded, got %d", len(allMigrations), count)
		}

		// Verify specific tables from initial schema exist
		tables := []string{
			"migrations",
			"metadata",
			"databases",
			"database_branches",
			"database_backups",
			"access_keys",
			"tokens",
			"users",
			"database_imports",
			"database_import_chunks",
		}

		for _, tableName := range tables {
			var tableExists bool

			err = db.QueryRow(
				"SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name=?",
				tableName,
			).Scan(&tableExists)

			if err != nil {
				t.Fatalf("Failed to check for table %s: %v", tableName, err)
			}

			if !tableExists {
				t.Errorf("Table %s was not created", tableName)
			}
		}

		// Verify migration names are recorded correctly
		rows, err := db.Query("SELECT name FROM migrations ORDER BY id")

		if err != nil {
			t.Fatalf("Failed to query migrations: %v", err)
		}

		defer func() {
			err := rows.Close()

			if err != nil {
				t.Fatalf("Failed to close rows: %v", err)
			}
		}()

		recordedMigrations := []string{}

		for rows.Next() {
			var name string

			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan migration name: %v", err)
			}

			recordedMigrations = append(recordedMigrations, name)
		}

		if len(recordedMigrations) != len(allMigrations) {
			t.Errorf("Expected %d migrations, got %d", len(allMigrations), len(recordedMigrations))
		}

		for i, expected := range allMigrations {
			if i >= len(recordedMigrations) {
				t.Errorf("Missing migration at index %d: %s", i, expected.Name)
			} else if recordedMigrations[i] != expected.Name {
				t.Errorf("Migration %d: expected %s, got %s", i, expected.Name, recordedMigrations[i])
			}
		}
	})
}

func TestMigrationRunnerOrder(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		// Track order of execution by creating test tables
		executionOrder := []string{}

		// Create test migrations with names that sort alphabetically but
		// should execute in numerical order
		testMigrations := []migrations.Migration{
			{
				Name: "test_003_third",
				Up: func(db *sql.DB) error {
					_, err := db.Exec("CREATE TABLE IF NOT EXISTS test_migration_third (id INTEGER, execution_order INTEGER)")

					if err != nil {
						return err
					}

					_, err = db.Exec("INSERT INTO test_migration_third (id, execution_order) VALUES (1, (SELECT COUNT(*) FROM migrations))")

					return err
				},
			},
			{
				Name: "test_001_first",
				Up: func(db *sql.DB) error {
					_, err := db.Exec("CREATE TABLE IF NOT EXISTS test_migration_first (id INTEGER, execution_order INTEGER)")

					if err != nil {
						return err
					}

					_, err = db.Exec("INSERT INTO test_migration_first (id, execution_order) VALUES (1, (SELECT COUNT(*) FROM migrations))")

					return err
				},
			},
			{
				Name: "test_002_second",
				Up: func(db *sql.DB) error {
					_, err := db.Exec("CREATE TABLE IF NOT EXISTS test_migration_second (id INTEGER, execution_order INTEGER)")

					if err != nil {
						return err
					}

					_, err = db.Exec("INSERT INTO test_migration_second (id, execution_order) VALUES (1, (SELECT COUNT(*) FROM migrations))")

					return err
				},
			},
		}

		// Run test migrations
		runner := database.NewMigrationRunner(db, testMigrations)

		err = runner.Run()

		if err != nil {
			t.Fatalf("Failed to run test migrations: %v", err)
		}

		// Verify migrations were applied in sorted order
		rows, err := db.Query("SELECT name FROM migrations WHERE name LIKE 'test_%' ORDER BY id")

		if err != nil {
			t.Fatalf("Failed to query test migrations: %v", err)
		}

		defer func() {
			err := rows.Close()
			if err != nil {
				t.Fatalf("Failed to close rows: %v", err)
			}
		}()

		for rows.Next() {
			var name string

			if err := rows.Scan(&name); err != nil {
				t.Fatalf("Failed to scan migration name: %v", err)
			}

			executionOrder = append(executionOrder, name)
		}

		expectedOrder := []string{"test_001_first", "test_002_second", "test_003_third"}

		if len(executionOrder) != len(expectedOrder) {
			t.Errorf("Expected %d migrations, got %d", len(expectedOrder), len(executionOrder))
		}

		for i, expected := range expectedOrder {
			if i >= len(executionOrder) {
				t.Errorf("Missing migration at index %d: %s", i, expected)
			} else if executionOrder[i] != expected {
				t.Errorf("Migration %d: expected %s, got %s", i, expected, executionOrder[i])
			}
		}

		// Verify execution order by checking the execution_order column
		var firstOrder, secondOrder, thirdOrder int

		err = db.QueryRow("SELECT execution_order FROM test_migration_first WHERE id = 1").Scan(&firstOrder)

		if err != nil {
			t.Fatalf("Failed to get execution order for first migration: %v", err)
		}

		err = db.QueryRow("SELECT execution_order FROM test_migration_second WHERE id = 1").Scan(&secondOrder)

		if err != nil {
			t.Fatalf("Failed to get execution order for second migration: %v", err)
		}

		err = db.QueryRow("SELECT execution_order FROM test_migration_third WHERE id = 1").Scan(&thirdOrder)

		if err != nil {
			t.Fatalf("Failed to get execution order for third migration: %v", err)
		}

		// First should execute before second, second before third
		if firstOrder >= secondOrder {
			t.Errorf("First migration should execute before second: first=%d, second=%d", firstOrder, secondOrder)
		}

		if secondOrder >= thirdOrder {
			t.Errorf("Second migration should execute before third: second=%d, third=%d", secondOrder, thirdOrder)
		}
	})
}

func TestMigrationIdempotency(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get system database: %v", err)
		}

		// Get initial migration count
		var initialCount int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&initialCount)

		if err != nil {
			t.Fatalf("Failed to get initial migration count: %v", err)
		}

		// Run migrations again
		allMigrations := migrations.GetAllMigrations()
		runner := database.NewMigrationRunner(db, allMigrations)
		err = runner.Run()

		if err != nil {
			t.Fatalf("Failed to run migrations second time: %v", err)
		}

		// Verify migration count hasn't changed
		var finalCount int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&finalCount)

		if err != nil {
			t.Fatalf("Failed to get final migration count: %v", err)
		}

		if initialCount != finalCount {
			t.Errorf("Migration count changed on second run: initial=%d, final=%d", initialCount, finalCount)
		}
	})
}
