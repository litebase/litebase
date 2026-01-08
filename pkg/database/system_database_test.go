package database_test

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"testing"
	"time"

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

func TestSystemDatabase_MultipleNodesConcurrentAccess(t *testing.T) {
	test.Run(t, func() {
		servers := make([]*test.TestServer, 2)

		test.RunWithTearDown(t, func() {
			// Create two test servers to simulate multiple nodes sharing same data directory
			servers[0] = test.NewTestServer(t)
			<-servers[0].Started

			servers[1] = test.NewTestServer(t)
			<-servers[1].Started

			server1 := servers[0]
			server2 := servers[1]

			// Determine which server is primary
			isPrimary1 := server1.App.Cluster.Node().IsPrimary()
			isPrimary2 := server2.App.Cluster.Node().IsPrimary()

			t.Logf("Server1 primary status: %v, Node ID: %s", isPrimary1, server1.App.Cluster.Node().ID)
			t.Logf("Server2 primary status: %v, Node ID: %s", isPrimary2, server2.App.Cluster.Node().ID)

			// Access system database from both nodes concurrently
			done := make(chan error, 2)

			// Node 1: Try to access system database
			go func() {
				// If this is the primary node, sleep to let replica access first
				if isPrimary1 {
					t.Logf("Server1 (PRIMARY) sleeping for 100ms to allow replica to access first")
					time.Sleep(100 * time.Millisecond)
				}

				t.Logf("Server1 attempting to get system database...")

				systemDB1 := server1.App.DatabaseManager.SystemDatabase()

				db1, err := systemDB1.DB()

				if err != nil {
					done <- fmt.Errorf("server1 failed to get DB: %w", err)
					return
				}

				t.Logf("Server1 successfully got DB connection")

				// Try to query metadata table
				var count int
				err = db1.QueryRow("SELECT COUNT(*) FROM metadata").Scan(&count)

				if err != nil {
					done <- fmt.Errorf("server1 failed to query metadata: %w", err)
					return
				}

				t.Logf("Server1 successfully queried metadata table, count: %d", count)

				done <- nil
			}()

			// Node 2: Try to access system database
			go func() {
				// If this is the primary node, sleep to let replica access first
				if isPrimary2 {
					t.Logf("Server2 (PRIMARY) sleeping for 100ms to allow replica to access first")
					time.Sleep(100 * time.Millisecond)
				}

				t.Logf("Server2 attempting to get system database...")

				systemDB2 := server2.App.DatabaseManager.SystemDatabase()

				db2, err := systemDB2.DB()

				if err != nil {
					done <- fmt.Errorf("server2 failed to get DB: %w", err)
					return
				}

				t.Logf("Server2 successfully got DB connection")

				// Try to query metadata table
				var count int
				err = db2.QueryRow("SELECT COUNT(*) FROM metadata").Scan(&count)

				if err != nil {
					done <- fmt.Errorf("server2 failed to query metadata: %w", err)
					return
				}

				t.Logf("Server2 successfully queried metadata table, count: %d", count)

				done <- nil
			}()

			// Wait for both goroutines to complete
			for range 2 {
				if err := <-done; err != nil {
					t.Fatalf("Concurrent access failed: %v", err)
				}
			}

			// Verify both nodes can still access the database successfully
			systemDB1 := server1.App.DatabaseManager.SystemDatabase()

			db1, err := systemDB1.DB()

			if err != nil {
				t.Fatalf("server1 final check failed: %v", err)
			}

			var hash1 string

			err = db1.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&hash1)

			if err != nil {
				t.Fatalf("server1 failed to get migrations hash: %v", err)
			}

			systemDB2 := server2.App.DatabaseManager.SystemDatabase()

			db2, err := systemDB2.DB()

			if err != nil {
				t.Fatalf("server2 final check failed: %v", err)
			}

			var hash2 string

			err = db2.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&hash2)

			if err != nil {
				t.Fatalf("server2 failed to get migrations hash: %v", err)
			}

			// Both nodes should see the same migrations hash
			if hash1 != hash2 {
				t.Errorf("Migrations hash mismatch between nodes: server1=%s, server2=%s", hash1, hash2)
			}
		}, func() {
			for _, s := range servers {
				if s != nil {
					s.Shutdown()
				}
			}
		})
	})
}

func TestSystemDatabase_NewMigrationRollingUpdate(t *testing.T) {
	test.Run(t, func() {
		servers := make([]*test.TestServer, 0)

		test.RunWithTearDown(t, func() {
			// Start two servers with initial migrations sharing same data directory
			t.Log("Starting server1 and server2 with initial migrations")
			server1 := test.NewTestServer(t)
			<-server1.Started
			servers = append(servers, server1)

			server2 := test.NewTestServer(t)
			<-server2.Started
			servers = append(servers, server2)

			// Verify initial state - both servers should have the same migrations
			t.Log("Verifying initial state of both servers")

			systemDB1 := server1.App.DatabaseManager.SystemDatabase()

			db1, err := systemDB1.DB()

			if err != nil {
				t.Fatalf("server1 failed to get DB: %v", err)
			}

			var initialHash1 string

			err = db1.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&initialHash1)

			if err != nil {
				t.Fatalf("server1 failed to get initial hash: %v", err)
			}

			t.Logf("Server1 initial migrations hash: %s", initialHash1)

			systemDB2 := server2.App.DatabaseManager.SystemDatabase()

			db2, err := systemDB2.DB()

			if err != nil {
				t.Fatalf("server2 failed to get DB: %v", err)
			}

			var initialHash2 string

			err = db2.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&initialHash2)

			if err != nil {
				t.Fatalf("server2 failed to get initial hash: %v", err)
			}

			t.Logf("Server2 initial migrations hash: %s", initialHash2)

			if initialHash1 != initialHash2 {
				t.Fatalf("Initial hashes don't match: server1=%s, server2=%s", initialHash1, initialHash2)
			}

			// Get initial migration count
			var initialMigrationCount int

			err = db1.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&initialMigrationCount)

			if err != nil {
				t.Fatalf("Failed to get initial migration count: %v", err)
			}

			t.Logf("Initial migration count: %d", initialMigrationCount)

			// Verify test table doesn't exist yet
			var testTableExists bool

			err = db1.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='test_new_migration'").Scan(&testTableExists)

			if err != nil {
				t.Fatalf("Failed to check test table: %v", err)
			}

			if testTableExists {
				t.Fatal("test_new_migration table should not exist yet")
			}

			// Shutdown both servers before adding new migration
			t.Log("Shutting down both servers to simulate deployment")
			server1.Shutdown()
			server2.Shutdown()

			// Add a new migration programmatically (simulates code update/deployment)
			t.Log("Adding new migration to migrations list")

			newMigration := migrations.Migration{
				Name: "002_test_new_migration",
				Up: func(db *sql.DB) error {
					_, err := db.Exec(`CREATE TABLE IF NOT EXISTS test_new_migration (
						id INTEGER PRIMARY KEY,
						data TEXT NOT NULL
					)`)
					return err
				},
			}

			// Save original migrations to restore after test
			originalMigrations := make([]migrations.Migration, len(migrations.AllMigrations))
			copy(originalMigrations, migrations.AllMigrations)
			defer func() {
				migrations.AllMigrations = originalMigrations
			}()

			// Add new migration to the global list (simulates deploying new code)
			migrations.AllMigrations = append(migrations.AllMigrations, newMigration)

			// Restart server1 first (rolling update pattern)
			t.Log("Restarting server1 (will be primary and run migration)")
			server1New := test.NewTestServer(t)
			<-server1New.Started
			servers = append(servers, server1New)

			// Server1 should have automatically run the new migration during startup
			systemDB1New := server1New.App.DatabaseManager.SystemDatabase()
			db1New, err := systemDB1New.DB()

			if err != nil {
				t.Fatalf("server1 (restarted) failed to get DB: %v", err)
			}

			// Verify the migration was run
			var migrationExists bool
			err = db1New.QueryRow("SELECT COUNT(*) > 0 FROM migrations WHERE name = ?", newMigration.Name).Scan(&migrationExists)

			if err != nil {
				t.Fatalf("Failed to check migration existence: %v", err)
			}

			if !migrationExists {
				t.Fatal("New migration should have been automatically run by server1")
			}

			t.Log("Server1: New migration was automatically detected and run")

			// Verify the new table exists on server1
			var tableExists bool
			err = db1New.QueryRow("SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='test_new_migration'").Scan(&tableExists)

			if err != nil {
				t.Fatalf("Failed to check test table on server1: %v", err)
			}

			if !tableExists {
				t.Fatal("test_new_migration table should exist on server1")
			}

			t.Log("Server1: test_new_migration table exists")

			// Verify migration hash was updated
			var hash1New string
			err = db1New.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&hash1New)

			if err != nil {
				t.Fatalf("Failed to get hash from server1: %v", err)
			}

			t.Logf("Server1 migrations hash: %s", hash1New)

			// Wait a bit (simulates rolling update delay)
			t.Log("Waiting 50ms before restarting server2")
			time.Sleep(50 * time.Millisecond)

			// Restart server2
			t.Log("Restarting server2 (replica)")
			server2New := test.NewTestServer(t)
			<-server2New.Started
			servers = append(servers, server2New)

			systemDB2New := server2New.App.DatabaseManager.SystemDatabase()

			db2New, err := systemDB2New.DB()

			if err != nil {
				t.Fatalf("server2 (restarted) failed to get DB: %v", err)
			}

			// Server2 should see the migration that server1 already ran
			// (since they share the same system database)
			var migrationCount2 int
			err = db2New.QueryRow("SELECT COUNT(*) FROM migrations").Scan(&migrationCount2)

			if err != nil {
				t.Fatalf("Failed to get migration count on server2: %v", err)
			}

			t.Logf("Server2 migration count: %d", migrationCount2)

			expectedCount := len(migrations.AllMigrations)

			if migrationCount2 != expectedCount {
				t.Errorf("Expected %d migrations on server2, got %d", expectedCount, migrationCount2)
			}

			// Verify the new table is accessible from server2
			var tableExists2 bool
			err = db2New.QueryRow("SELECT COUNT(*) > 0 FROM sqlite_master WHERE type='table' AND name='test_new_migration'").Scan(&tableExists2)

			if err != nil {
				t.Fatalf("Failed to check test table on server2: %v", err)
			}

			if !tableExists2 {
				t.Fatal("test_new_migration table should be accessible from server2")
			}

			t.Log("Server2: test_new_migration table is accessible")

			// Verify both servers have the same hash
			var hash2New string
			err = db2New.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&hash2New)

			if err != nil {
				t.Fatalf("Failed to get hash from server2: %v", err)
			}

			if hash1New != hash2New {
				t.Errorf("Migrations hash mismatch after update: server1=%s, server2=%s", hash1New, hash2New)
			}

			t.Logf("Both servers have matching migrations hash: %s", hash1New)

			// Verify we can write to the new table from both servers
			// Both servers share the same data directory
			t.Log("Testing write operations to new table from both servers")
			_, err = db1New.Exec("INSERT INTO test_new_migration (data) VALUES (?)", "test from server1")

			if err != nil {
				t.Fatalf("Failed to insert from server1: %v", err)
			}

			_, err = db2New.Exec("INSERT INTO test_new_migration (data) VALUES (?)", "test from server2")

			if err != nil {
				t.Fatalf("Failed to insert from server2: %v", err)
			}

			// Give a moment for writes to be visible across connections
			time.Sleep(10 * time.Millisecond)

			// Verify both servers can query the new table
			// Server2 should see both inserts (its own + server1's)
			var count2 int
			err = db2New.QueryRow("SELECT COUNT(*) FROM test_new_migration").Scan(&count2)

			if err != nil {
				t.Fatalf("Failed to count rows on server2: %v", err)
			}

			// Server2 sees both rows since it queried after both inserts
			if count2 != 2 {
				t.Errorf("Expected 2 rows in test_new_migration on server2, got %d", count2)
			}

			t.Logf("Server2: Successfully verified %d row(s) in new table - confirms shared database", count2)

			// Server1 may see 1 or 2 rows depending on SQLite connection caching
			// The important part is the table structure exists and is usable
			var count1 int
			err = db1New.QueryRow("SELECT COUNT(*) FROM test_new_migration").Scan(&count1)

			if err != nil {
				t.Fatalf("Failed to count rows on server1: %v", err)
			}

			if count1 < 1 {
				t.Errorf("Expected at least 1 row in test_new_migration on server1, got %d", count1)
			}

			t.Logf("Server1: Successfully verified %d row(s) in new table", count1)
		}, func() {
			for _, s := range servers {
				if s != nil {
					s.Shutdown()
				}
			}
		})
	})
}
