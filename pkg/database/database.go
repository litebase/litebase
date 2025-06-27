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
	ID              int64             `json:"-"`
	DatabaseManager *DatabaseManager  `json:"-"`
	Name            string            `json:"name"`
	DatabaseID      string            `json:"database_id"`
	PrimaryBranchID sql.NullInt64     `json:"primary_branch_id"`
	Settings        *DatabaseSettings `json:"settings"`
	CreatedAt       time.Time         `json:"created_at"`
	UpdatedAt       time.Time         `json:"updated_at"`

	exists        bool
	primaryBranch *Branch
}

func Directory() string {
	return "_databases/"
}

func TmpDirectory() string {
	return "_databases/"
}

func NewDatabase(databaseManager *DatabaseManager, name string) *Database {
	return &Database{
		DatabaseID:      uuid.New().String(),
		DatabaseManager: databaseManager,
		Name:            name,
	}
}

// Insert a new database into the system database.
func InsertDatabase(database *Database) error {
	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	result, err := db.Exec(
		`INSERT INTO databases (
			database_id,
			primary_branch_id, 
			name, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?)
		`,
		database.DatabaseID,
		database.PrimaryBranchID,
		database.Name,
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

	var primaryBranchId int64

	if database.PrimaryBranchID.Valid {
		primaryBranchId = database.PrimaryBranchID.Int64
	}

	_, err = db.Exec(
		`UPDATE databases 
		SET 
			name = ?,
			primary_branch_id = ?,
			settings = ?,
			updated_at = ?
		WHERE database_id = ?
		`,
		database.Name,
		primaryBranchId,
		string(settingsJson),
		time.Now().UTC(),
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
		slog.Debug("Error checking branch existence", "error", err)
		return false
	}

	var id int64

	err = db.QueryRow(
		`SELECT id FROM database_branches WHERE database_branch_id = ? AND database_id = ?`,
		branchID,
		database.ID,
	).Scan(&id)

	if err != nil {
		if err != sql.ErrNoRows {
			slog.Error("Error checking branch existence", "error", err)
		}
		return false
	}

	return true
}

func (database *Database) Key(branchID string) string {
	var branch *Branch

	// TODO: Load the branch from the database

	return branch.Key
}

func (database *Database) PrimaryBranch() *Branch {
	if database == nil {
		return nil
	}

	if database.primaryBranch == nil {
		// If no primary branch ID is set, return nil
		if !database.PrimaryBranchID.Valid || database.PrimaryBranchID.Int64 == 0 {
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
				`SELECT * FROM database_branches WHERE id = ?`,
				database.PrimaryBranchID.Int64,
			).Scan(
				&branch.ID,
				&branch.DatabaseID,
				&branch.BranchID,
				&branch.Name,
				&branch.Key,
				&branch.Settings,
				&branch.CreatedAt,
				&branch.UpdatedAt,
			)

			if err == nil {
				branch.DatabaseManager = database.DatabaseManager
				branch.exists = true
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

func (database *Database) BranchDirectory(branchID string) string {
	return fmt.Sprintf("%s%s/%s", Directory(), database.DatabaseID, branchID)
}

func (database *Database) Url(branchID string) string {
	port := ""

	if database.DatabaseManager.Cluster.Config.Port != "80" {
		port = fmt.Sprintf(":%s", database.DatabaseManager.Cluster.Config.Port)
	}

	return fmt.Sprintf(
		"http://%s%s/%s",
		database.DatabaseManager.Cluster.Config.HostName,
		port,
		database.Key(branchID),
	)
}
