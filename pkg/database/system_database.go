package database

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/litebase/litebase/pkg/cluster/messages"
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
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.db != nil {
		err := s.db.Close()

		if err != nil {
			return err
		}

		s.db = nil
		s.initialized = false // Reset initialized flag so migrations can run on next access
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

	// Configure connection pool to prevent "driver: bad connection" errors
	// under high concurrency. Set generous limits to handle parallel operations.
	db.SetMaxOpenConns(25)   // Allow enough concurrent connections for parallel operations
	db.SetMaxIdleConns(10)   // Keep more idle connections ready
	db.SetConnMaxLifetime(0) // Connections don't expire
	db.SetConnMaxIdleTime(0) // Idle connections don't expire

	// Cache the connection immediately
	s.db = db

	// Always ensure migrations are checked/run on first access
	if !s.initialized {
		isPrimary := s.databaseManager.Cluster.Node().IsPrimary()

		// Only primary nodes should run migrations, but all nodes should verify
		if isPrimary {
			needsMigrations, err := s.needsMigrationsOnConnection(s.db)

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
		} else {
			// Replicas should verify migrations but never write to the database
			// The primary is responsible for applying migrations
			needsMigrations, err := s.needsMigrationsOnConnection(s.db)

			if err == nil && !needsMigrations {
				slog.Debug("Migrations verified on replica")
			} else if err == nil && needsMigrations {
				// Migrations are missing - this is expected during rolling updates
				// The primary will apply them. We just need to be aware they exist in code
				// but may not be in the database yet.
				slog.Debug("Replica detected code has newer migrations than database", "note", "Primary will apply them")
			}
		}

		// Mark as initialized after check
		s.initialized = true
	}

	return s.db, nil
}

// CheckAndRunMigrations checks if migrations are up to date and runs them if needed.
// This should be called when a node becomes primary.
func (s *SystemDatabase) CheckAndRunMigrations() error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Ensure we have a connection before running migrations
	if s.db == nil {
		db, err := sql.Open("litebase-internal", "system/system")

		if err != nil {
			return fmt.Errorf("failed to open system database: %w", err)
		}

		s.db = db
	}

	needsMigrations, err := s.needsMigrationsOnConnection(s.db)

	if err != nil {
		return fmt.Errorf("failed to check migrations status: %w", err)
	}

	if !needsMigrations {
		slog.Debug("Migrations are up to date")
		return nil
	}

	slog.Info("Running pending migrations")
	s.runMigrationsOnConnection(s.db)
	s.initialized = true

	// Broadcast to cluster that migrations were updated
	s.broadcastMigrationsUpdated()

	return nil
}

// needsMigrationsOnConnection checks if the database needs migrations using an existing connection
func (s *SystemDatabase) needsMigrationsOnConnection(db *sql.DB) (bool, error) {
	// Get all migrations
	allMigrations := migrations.GetAllMigrations()
	if len(allMigrations) == 0 {
		return false, nil
	}

	// Calculate expected hash from latest migration name
	latestMigration := allMigrations[len(allMigrations)-1]
	expectedHash := sha256.Sum256([]byte(latestMigration.Name))
	expectedHashStr := fmt.Sprintf("%x", expectedHash)

	// Check if metadata table exists
	var tableName string

	err := db.QueryRow("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'").Scan(&tableName)

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

// broadcastMigrationsUpdated notifies all cluster nodes that migrations have been updated
func (s *SystemDatabase) broadcastMigrationsUpdated() {
	allMigrations := migrations.GetAllMigrations()
	if len(allMigrations) == 0 {
		return
	}

	latestMigration := allMigrations[len(allMigrations)-1]
	expectedHash := sha256.Sum256([]byte(latestMigration.Name))
	expectedHashStr := fmt.Sprintf("%x", expectedHash)

	msg := messages.MigrationsUpdatedMessage{
		LatestMigration: latestMigration.Name,
		MigrationsHash:  expectedHashStr,
	}

	slog.Debug("Broadcasting migrations update to cluster", "latest_migration", msg.LatestMigration, "hash", msg.MigrationsHash)

	// Broadcast to all nodes in the cluster
	if s.databaseManager != nil && s.databaseManager.Cluster != nil {
		node := s.databaseManager.Cluster.Node()
		if node != nil && !node.IsReplica() {
			primary := node.Primary()
			if primary != nil {
				_, errors := primary.Publish(messages.NodeMessage{
					Data: msg,
				})
				if len(errors) > 0 {
					slog.Error("Failed to broadcast migrations update to replicas", "errors", errors)
				}
			}
		}
	}
}

// OnMigrationsUpdated handles notification from primary that migrations were updated
// This should be called by replica nodes when they receive a migration update notification
func (s *SystemDatabase) OnMigrationsUpdated() {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	// Reset initialized flag to force migration recheck on next DB access
	s.initialized = false

	slog.Info("Received migrations update notification - will recheck on next access")
}
