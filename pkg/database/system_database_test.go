package database_test

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/litebase/litebase/internal/test"
	"github.com/litebase/litebase/pkg/database/migrations"
)

func TestSystemDatabase(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		db := server.App.DatabaseManager.SystemDatabase()

		if db == nil {
			t.Fatal("expected system database to be initialized")
		}

		// _, err := db.Exec("CREATE TABLE IF NOT EXISTS test (id INTEGER PRIMARY KEY, name TEXT)", nil)

		// if err != nil {
		// 	t.Fatalf("expected no error, got %v", err)
		// }
	})
}

func TestSystemDatabase_CheckAndRunMigrations(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		// After initial setup, migrations should be up to date
		err := systemDB.CheckAndRunMigrations()

		if err != nil {
			t.Fatalf("CheckAndRunMigrations failed: %v", err)
		}

		// Verify migrations ran successfully by checking metadata
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var hashExists bool
		err = db.QueryRow("SELECT COUNT(*) > 0 FROM metadata WHERE key = 'migrations_hash'").Scan(&hashExists)

		if err != nil {
			t.Fatalf("Failed to check migrations hash: %v", err)
		}

		if !hashExists {
			t.Error("migrations_hash not found in metadata after CheckAndRunMigrations")
		}
	})
}

func TestSystemDatabase_MigrationsHashCheck(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		// Get the stored hash
		var storedHash string

		err = db.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&storedHash)

		if err != nil {
			t.Fatalf("Failed to get stored hash: %v", err)
		}

		// Calculate expected hash
		allMigrations := migrations.GetAllMigrations()

		if len(allMigrations) == 0 {
			t.Fatal("No migrations found")
		}

		latestMigration := allMigrations[len(allMigrations)-1]
		expectedHash := sha256.Sum256([]byte(latestMigration.Name))
		expectedHashStr := fmt.Sprintf("%x", expectedHash)

		if storedHash != expectedHashStr {
			t.Errorf("Hash mismatch: stored=%s, expected=%s", storedHash, expectedHashStr)
		}
	})
}

func TestSystemDatabase_CheckAndRunMigrations_Idempotent(t *testing.T) {
	test.Run(t, func() {
		server := test.NewTestServer(t)
		defer server.Shutdown()

		systemDB := server.App.DatabaseManager.SystemDatabase()

		// Run migrations multiple times
		for i := range 3 {
			err := systemDB.CheckAndRunMigrations()

			if err != nil {
				t.Fatalf("CheckAndRunMigrations failed on iteration %d: %v", i, err)
			}
		}

		// Verify migrations table still has correct data
		db, err := systemDB.DB()

		if err != nil {
			t.Fatalf("Failed to get database: %v", err)
		}

		var count int
		err = db.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&count)

		if err != nil {
			t.Fatalf("Failed to count migrations: %v", err)
		}

		expectedCount := len(migrations.GetAllMigrations())

		if count != expectedCount {
			t.Errorf("Expected %d migrations, got %d", expectedCount, count)
		}
	})
}
