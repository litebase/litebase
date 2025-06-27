package database

import (
	"database/sql"
	"fmt"
	"log"
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
	// PrimaryBranchID:   SystemDatabaseBranchID,
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
	sd := &SystemDatabase{
		databaseManager: databaseManager,
		mutex:           &sync.Mutex{},
	}

	sd.init()

	sd.initialized = true

	return sd
}

func (s *SystemDatabase) Close() error {
	if s.db != nil {
		return s.db.Close()
	}

	return nil
}

func (s *SystemDatabase) DB() (*sql.DB, error) {
	if s.db != nil {
		return s.db, nil
	}

	db, err := sql.Open("litebase", "system/system")

	if err != nil {
		return nil, fmt.Errorf("failed to open system database: %w", err)
	}

	s.db = db

	return s.db, nil
}

// Initialize the system database by creating necessary tables.
func (s *SystemDatabase) init() {
	db, err := sql.Open("litebase", "system/system")

	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

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
			primary_branch_id INTEGER,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create the database keys table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_keys
		(
			id INTEGER PRIMARY KEY,
			database_id INTEGER,
			database_branch_id TEXT,
			key TEXT UNIQUE,
			FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
		)
		`,
	)

	if err != nil {
		panic(err)
	}

	// Create index for efficient lookups by database_id and database_branch_id
	_, err = db.Exec(
		`CREATE INDEX IF NOT EXISTS idx_database_keys_db_branch 
		ON database_keys(database_id, database_branch_id)`,
	)

	if err != nil {
		panic(err)
	}

	// Create the branches table if it doesn't exist.
	_, err = db.Exec(
		`CREATE TABLE IF NOT EXISTS database_branches
		(
			id INTEGER PRIMARY KEY, 
			database_id INTEGER,
			database_branch_id TEXT,
			name TEXT,
			key TEXT,
			settings TEXT,
			created_at TEXT,
			updated_at TEXT,
			FOREIGN KEY (database_id) REFERENCES databases(id) ON DELETE CASCADE
		)
		`,
	)

	if err != nil {
		panic(err)
	}
}
