package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

type Database struct {
	ID                       int64             `json:"-"`
	DatabaseManager          *DatabaseManager  `json:"-"`
	Name                     string            `json:"name"`
	DatabaseID               string            `json:"database_id"`
	PrimaryBranchReferenceID sql.NullInt64     `json:"primary_branch_reference_id"`
	Settings                 *DatabaseSettings `json:"settings"`
	CreatedAt                time.Time         `json:"created_at"`
	UpdatedAt                time.Time         `json:"updated_at"`

	exists        bool
	primaryBranch *Branch
}

func NewDatabase(databaseManager *DatabaseManager, databaseName string) *Database {
	return &Database{
		DatabaseID:      uuid.New().String(),
		DatabaseManager: databaseManager,
		Name:            databaseName,
	}
}

func CreateDatabase(databaseManager *DatabaseManager, databaseName string, branchName string) (*Database, error) {
	database := NewDatabase(databaseManager, databaseName)

	database.Settings = &DatabaseSettings{
		Backups: DatabaseBackupSettings{
			Enabled: true,
			IncrementalBackups: DatabaseIncrementalBackupSettings{
				Enabled: true,
			},
		},
	}

	database.CreatedAt = time.Now().UTC()
	database.UpdatedAt = time.Now().UTC()

	err := database.Save()

	if err != nil {
		return nil, err
	}

	// Create the initial branch
	branch, err := NewBranch(databaseManager, branchName)

	if err != nil {
		return nil, fmt.Errorf("failed to create branch: %w", err)
	}

	branch.DatabaseID = database.DatabaseID
	branch.DatabaseReferenceID = sql.NullInt64{Int64: database.ID, Valid: true}

	err = branch.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save branch: %w", err)
	}

	// Update the database with the branch
	database.PrimaryBranchReferenceID = sql.NullInt64{Int64: branch.ID, Valid: true}

	err = database.Save()

	if err != nil {
		return nil, err
	}

	return database, nil
}

// Insert a new database into the system database.
func InsertDatabase(database *Database) error {
	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	var primaryBranchId sql.NullInt64
	if database.PrimaryBranchReferenceID.Valid {
		primaryBranchId = database.PrimaryBranchReferenceID
	}

	result, err := db.Exec(
		`INSERT INTO databases (
			database_id,
			primary_branch_reference_id, 
			name,
			settings,
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?)
		`,
		database.DatabaseID,
		primaryBranchId,
		database.Name,
		database.Settings,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	database.ID = id
	database.exists = true

	return nil
}

// Update an existing database in the system database.
func UpdateDatabase(database *Database) error {
	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	settingsJson, err := json.Marshal(database.Settings)

	if err != nil {
		return err
	}

	var primaryBranchId sql.NullInt64
	if database.PrimaryBranchReferenceID.Valid {
		primaryBranchId = database.PrimaryBranchReferenceID
	}

	_, err = db.Exec(
		`UPDATE databases 
		SET 
			name = ?,
			primary_branch_reference_id = ?,
			settings = ?,
			updated_at = ?
		WHERE database_id = ?
		`,
		database.Name,
		primaryBranchId,
		string(settingsJson),
		time.Now().UTC(),
		database.DatabaseID,
	)

	if err != nil {
		return err
	}

	return nil
}

// Check if a branch exists for the database.
func (database *Database) HasBranch(branchID string) bool {
	if database.DatabaseID == SystemDatabaseID && branchID == SystemDatabaseBranchID {
		return true
	}

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Error checking branch existence", "error", err)
		return false
	}

	var id int64

	err = db.QueryRow(
		`SELECT id FROM database_branches WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchID,
	).Scan(&id)

	if err != nil {
		slog.Error("Error checking branch existence", "error", err)

		return false
	}

	return true
}

// Get the key for a branch of the database.
func (database *Database) Key(branchID string) string {
	var key string

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return ""
	}

	err = db.QueryRow(
		`SELECT key FROM database_branches WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchID,
	).Scan(&key)

	if err != nil {
		return ""
	}

	return key
}

// Load and return the primary branch of the database.
func (database *Database) PrimaryBranch() *Branch {
	if database == nil {
		return nil
	}

	if database.primaryBranch == nil {
		// If no primary branch ID is set, return nil
		if !database.PrimaryBranchReferenceID.Valid || database.PrimaryBranchReferenceID.Int64 == 0 {
			return nil
		}

		// Load the primary branch from the system database using the foreign key
		if database.DatabaseManager != nil {
			db, err := database.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				return nil
			}

			var branch Branch

			err = db.QueryRow(
				`SELECT id, database_reference_id, database_id, database_branch_id, name, key, settings, created_at, updated_at FROM database_branches WHERE id = ?`,
				database.PrimaryBranchReferenceID.Int64,
			).Scan(
				&branch.ID,
				&branch.DatabaseReferenceID,
				&branch.DatabaseID,
				&branch.DatabaseBranchID,
				&branch.Name,
				&branch.Key,
				&branch.Settings,
				&branch.CreatedAt,
				&branch.UpdatedAt,
			)

			if err == nil {
				branch.DatabaseManager = database.DatabaseManager
				branch.Exists = true
				database.primaryBranch = &branch
			} else {
				log.Println("Error loading primary branch:", err)
			}
		}
	}

	return database.primaryBranch
}

func (database *Database) Save() error {
	if database.exists {
		return UpdateDatabase(database)
	} else {
		return InsertDatabase(database)
	}
}

func (database *Database) Url(branchID string) string {
	port := ""

	if database.DatabaseManager.Cluster.Config.Port != "80" {
		port = fmt.Sprintf(":%s", database.DatabaseManager.Cluster.Config.Port)
	}

	return fmt.Sprintf(
		"%s%s/%s",
		database.DatabaseManager.Cluster.Config.HostName,
		port,
		database.Key(branchID),
	)
}
