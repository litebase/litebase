package database

import (
	"database/sql"
	"fmt"
	"log"
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

	err := s.databaseManager.Cluster.Node().WaitForPrimary()

	if err != nil {
		panic(err)
	}

	// Don't run init here - let DB() handle it with proper locking

	return s
}

// Close the system database.
func (s *SystemDatabase) Close() error {
	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

// Get a singleton instance of the system database.
func (s *SystemDatabase) DB() (*sql.DB, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	if s.db != nil {
		return s.db, nil
	}

	// Initialize the system database if this node should manage it and it hasn't been initialized yet
	if !s.initialized && (s.databaseManager.Cluster.Node().IsPrimary()) {
		s.init()
		s.initialized = true
	}

	db, err := sql.Open("litebase-internal", "system/system")

	if err != nil {
		return nil, fmt.Errorf("failed to open system database: %w", err)
	}

	s.db = db

	return s.db, nil
}

// Initialize the system database by running migrations.
func (s *SystemDatabase) init() {
	db, err := sql.Open("litebase-internal", "system/system")

	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		if err := db.Close(); err != nil {
			slog.Error("Error closing database connection", "error", err)
		}
	}()

	// Get all migrations and run them
	allMigrations := migrations.GetAllMigrations()

	runner := NewMigrationRunner(db, allMigrations)

	if err := runner.Run(); err != nil {
		panic(fmt.Errorf("failed to run migrations: %w", err))
	}
}
