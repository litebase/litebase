package database

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/litebase/litebase/pkg/database/migrations"
)

// Constants and variables related to the system database.
const SystemDatabaseID = "system"
const SystemDatabaseBranchID = "system"
const SystemDatabaseName = "system"

// A static system database that can be used to new up a new reference.
var TheSystemDatabase Database = Database{
	DatabaseID: SystemDatabaseID,
	Name:       SystemDatabaseName,
}

// The system database structure that has a connection to the system database.
type SystemDatabase struct {
	databaseManager *DatabaseManager
	db              *sql.DB
	initialized     bool
	mutex           *sync.Mutex
}

// Create a new instance of the system database.
func NewSystemDatabase(databaseManager *DatabaseManager) *SystemDatabase {
	s := &SystemDatabase{
		databaseManager: databaseManager,
		mutex:           &sync.Mutex{},
	}

	return s
}

// Close the system database.
func (s *SystemDatabase) Close() error {
	if s.db != nil {
		err := s.db.Close()

		if err != nil {
			return err
		}

		s.db = nil
	}

	return nil
}

// Get a singleton instance of the system database.
func (s *SystemDatabase) DB() (*sql.DB, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Return existing connection if available
	if s.db != nil {
		return s.db, nil
	}

	// Open the connection that we will cache
	db, err := sql.Open("litebase-internal", "system/system")

	if err != nil {
		return nil, fmt.Errorf("failed to open system database: %w", err)
	}

	// Cache the connection immediately
	s.db = db

	// Always ensure migrations are checked/run on first access
	if !s.initialized {
		isPrimary := s.databaseManager.Cluster.Node().IsPrimary()

		// Only primary nodes should check and run migrations
		if isPrimary {
			needsMigrations, err := s.needsMigrations()

			if err == nil && needsMigrations {
				slog.Info("Running pending migrations on DB access")
				// Run migrations on the cached connection
				s.runMigrationsOnConnection(s.db)
			} else if err == nil {
				// Migrations already up to date
				slog.Debug("Migrations are up to date")
			} else {
				// Error checking migrations, run them to be safe
				slog.Info("Error checking migrations, running them", "error", err)
				// Run migrations on the cached connection
				s.runMigrationsOnConnection(s.db)
			}
		}

		// Mark as initialized after check (primary) or skip (non-primary)
		s.initialized = true
	}

	return s.db, nil
}

// CheckAndRunMigrations checks if migrations are up to date and runs them if needed.
// This should be called when a node becomes primary.
func (s *SystemDatabase) CheckAndRunMigrations() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	needsMigrations, err := s.needsMigrations()

	if err != nil {
		return fmt.Errorf("failed to check migrations status: %w", err)
	}

	if !needsMigrations {
		slog.Debug("Migrations are up to date")
		return nil
	}

	// Ensure we have a connection before running migrations
	if s.db == nil {
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			return fmt.Errorf("failed to open system database: %w", err)
		}

		s.db = db
	}

	slog.Info("Running pending migrations")
	s.runMigrationsOnConnection(s.db)
	s.initialized = true

	return nil
}

// needsMigrations checks if the database needs migrations by comparing hashes
func (s *SystemDatabase) needsMigrations() (bool, error) {
	// Get all migrations
	allMigrations := migrations.GetAllMigrations()
	if len(allMigrations) == 0 {
		return false, nil
	}

	// Calculate expected hash from latest migration name
	latestMigration := allMigrations[len(allMigrations)-1]
	expectedHash := sha256.Sum256([]byte(latestMigration.Name))
	expectedHashStr := fmt.Sprintf("%x", expectedHash)

	// Try to open database to check current hash
	db, err := sql.Open("litebase-internal", "system/system")

	if err != nil {
		// If we can't open, assume migrations needed
		return true, nil
	}

	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("Error closing database connection", "error", err)
		}
	}()

	// Check if metadata table exists
	var tableName string
	err = db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'").Scan(&tableName)
	if err != nil {
		// Table doesn't exist, migrations needed
		return true, nil
	}

	// Get current hash from metadata
	var currentHash string
	err = db.QueryRow("SELECT value FROM metadata WHERE key = ?", "migrations_hash").Scan(&currentHash)
	if err != nil {
		// Hash not found, migrations needed
		return true, nil
	}

	// Compare hashes
	if currentHash != expectedHashStr {
		slog.Debug("Migrations hash mismatch", "current", currentHash, "expected", expectedHashStr)
		return true, nil
	}

	return false, nil
}

// runMigrationsOnConnection executes the migration runner on a specific connection
func (s *SystemDatabase) runMigrationsOnConnection(db *sql.DB) {
	// Get all migrations and run them
	allMigrations := migrations.GetAllMigrations()

	runner := NewMigrationRunner(db, allMigrations)

	if err := runner.Run(); err != nil {
		panic(fmt.Errorf("failed to run migrations: %w", err))
	}
}
