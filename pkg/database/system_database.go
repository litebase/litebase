package database

import (
	"sync"

	"github.com/litebase/litebase/pkg/sqlite3"
)

// Constants and variables related to the system database.
const SystemDatabaseID = "system"
const SystemDatabaseBranchID = "system"
const SystemDatabaseName = "system"
const SystemDatabasePrimaryBranchName = "system"

// A static system database that can be used to new up a new reference.
var TheSystemDatabase Database = Database{
	DatabaseID: SystemDatabaseID,
	Name:       SystemDatabaseName,
	// PrimaryBranchID:   SystemDatabaseBranchID,
	PrimaryBranchName: SystemDatabasePrimaryBranchName,
}

// The system database structure that has a connection to the system database.
type SystemDatabase struct {
	clientConnection *ClientConnection
	databaseManager  *DatabaseManager
	mutex            *sync.Mutex
}

// Create a new instance of the system database.
func NewSystemDatabase(databaseManager *DatabaseManager) *SystemDatabase {
	sd := &SystemDatabase{
		databaseManager: databaseManager,
		mutex:           &sync.Mutex{},
	}

	sd.init()

	return sd
}

// Close the system database connection by removing it from the connection manager.
func (s *SystemDatabase) Close() bool {
	if s.clientConnection != nil {
		s.databaseManager.ConnectionManager().Remove(
			SystemDatabaseID,
			SystemDatabaseBranchID,
			s.clientConnection,
		)

		s.clientConnection = nil
	}

	return true
}

// Return the client connection to the system database.
func (s *SystemDatabase) connection() *ClientConnection {
	if s.clientConnection != nil && s.clientConnection.connection.Closed() {
		s.clientConnection = nil
	}

	if s.clientConnection == nil {
		databaseConnection, err := s.databaseManager.ConnectionManager().Get(SystemDatabaseID, SystemDatabaseBranchID)

		if err != nil {
			panic(err)
		}

		s.clientConnection = databaseConnection
	}

	return s.clientConnection
}

// Execute a SQL statement against the system database.
func (s *SystemDatabase) Exec(
	sql string,
	args []sqlite3.StatementParameter,
) (*sqlite3.Result, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	return s.connection().GetConnection().Exec(sql, args)
}

// Initialize the system database by creating necessary tables.
func (s *SystemDatabase) init() {
	// Create the metadata table if it doesn't exist.
	_, err := s.Exec(
		`
		CREATE TABLE IF NOT EXISTS metadata
		(
			id INTEGER PRIMARY KEY, 
			key TEXT UNIQUE, 
			value TEXT
		)
		`,
		nil,
	)

	if err != nil {
		panic(err)
	}

	// Create the users table if it doesn't exist.
	_, err = s.Exec(
		`
		CREATE TABLE IF NOT EXISTS users
		(
			id INTEGER PRIMARY KEY, 
			username TEXT UNIQUE, 
			password TEXT,
			statements TEXT,
			created_at TEXT,
			updated_at TEXT
		)	
		`,
		nil,
	)

	if err != nil {
		panic(err)
	}

	// Create the databases table if it doesn't exist.
	_, err = s.Exec(
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
		nil,
	)

	if err != nil {
		panic(err)
	}

	// Create the database keys table if it doesn't exist.
	_, err = s.Exec(
		`CREATE TABLE IF NOT EXISTS database_keys
		(
			id INTEGER PRIMARY KEY,
			database_id INTEGER,
			database_branch_id TEXT,
			key TEXT UNIQUE,
		)
		`,
		nil,
	)

	if err != nil {
		panic(err)
	}

	// Create the branches table if it doesn't exist.
	_, err = s.Exec(
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
			FOREIGN KEY (database_id) REFERENCES databases(id)
		)
		`,
		nil,
	)

	if err != nil {
		panic(err)
	}
}
