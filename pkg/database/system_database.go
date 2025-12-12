package database

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"sync"
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

	if !s.initialized && (s.databaseManager.Cluster.Node().IsPrimary()) {
		s.init()
		s.initialized = true
	}

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

// Initialize the system database by creating necessary tables.
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

	// Create the metadata table if it doesn't exist.
	_, err = db.Exec(
		`
		CREATE TABLE IF NOT EXISTS metadata
		(
			id INTEGER PRIMARY KEY, 
			key TEXT UNIQUE, 
			value TEXT
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create the databases table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS databases
		(
			id INTEGER PRIMARY KEY, 
			database_id TEXT UNIQUE, 
			name TEXT UNIQUE,
			primary_branch_reference_id INTEGER,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create the branches table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_branches
		(
			id INTEGER PRIMARY KEY, 
			database_reference_id INTEGER,
			parent_database_branch_reference_id INTEGER,
			database_id TEXT,
			database_branch_id TEXT,
			name TEXT,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create index on database_reference_id and the branch name.
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_database_branches_database_reference_id_name 
		ON database_branches (database_reference_id, name)`,
	)

	if err != nil {
		panic(err)
	}

	// Create the database backups table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_backups
		(
			id INTEGER PRIMARY KEY, 
			database_reference_id INTEGER,
			database_branch_reference_id INTEGER,
			database_id TEXT,
			database_branch_id TEXT,
			restore_point_timestamp INTEGER,
			restore_point_page_count INTEGER,
			size INTEGER,
			created_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE,
			FOREIGN KEY (database_branch_reference_id) REFERENCES database_branches(id) ON DELETE CASCADE
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create a table for access keys
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS access_keys
		(
			id INTEGER PRIMARY KEY,
			access_key_id TEXT UNIQUE,
			access_key_secret TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create a table for tokens
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS tokens
		(
			id INTEGER PRIMARY KEY,
			token_id TEXT UNIQUE,
			token_hash TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create a table for users
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS users
		(
			id INTEGER PRIMARY KEY,
			username TEXT UNIQUE,
			password TEXT NOT NULL,
			description TEXT,
			statements TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create a table for database imports
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_imports
		(
			id INTEGER PRIMARY KEY,
			database_reference_id INTEGER,
			database_branch_reference_id INTEGER,
			status TEXT NOT NULL,
			total_size INTEGER,
			chunk_count INTEGER NOT NULL,
			error_message TEXT,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			completed_at TEXT,
			FOREIGN KEY (database_reference_id) REFERENCES databases(id) ON DELETE CASCADE,
			FOREIGN KEY (database_branch_reference_id) REFERENCES database_branches(id) ON DELETE CASCADE
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create a table for database import chunks
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_import_chunks
		(
			id INTEGER PRIMARY KEY,
			import_reference_id INTEGER NOT NULL,
			chunk_index INTEGER NOT NULL,
			chunk_size INTEGER NOT NULL,
			checksum TEXT,
			uploaded_at TEXT NOT NULL,
			FOREIGN KEY (import_reference_id) REFERENCES database_imports(id) ON DELETE CASCADE,
			UNIQUE(import_reference_id, chunk_index)
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create index on import_reference_id for database_import_chunks
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_database_import_chunks_import_reference_id 
		ON database_import_chunks (import_reference_id)`,
	)

	if err != nil {
		panic(err)
	}
}
