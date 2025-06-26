package database

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/sqlite3"
)

type Database struct {
	ID                int64            `json:"-"`
	DatabaseManager   *DatabaseManager `json:"-"`
	Name              string           `json:"name"`
	Branches          []*Branch        `json:"branches"`
	DatabaseID        string           `json:"database_id"`
	PrimaryBranchID   int64            `json:"primary_branch_id"`
	PrimaryBranchName string           `json:"primary_branch_name"`
	Settings          DatabaseSettings `json:"settings"`
	CreatedAt         time.Time        `json:"created_at"`
	UpdatedAt         time.Time        `json:"updated_at"`

	exists         bool
	primaryBranch  *Branch
	systemDatabase *SystemDatabase `json:"-"`
}

func Directory() string {
	return "_databases/"
}

func TmpDirectory() string {
	return "_databases/"
}

func NewDatabase(databaseManager *DatabaseManager) *Database {
	return &Database{
		DatabaseManager: databaseManager,
	}
}

// Insert a new database into the system database.
func InsertDatabase(database *Database) error {
	systemDatabase := database.DatabaseManager.SystemDatabase()

	_, err := systemDatabase.Exec(
		`INSERT INTO databases (
			database_id, 
			name, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?)
		`,
		[]sqlite3.StatementParameter{
			{
				Type:  sqlite3.ParameterTypeText,
				Value: database.DatabaseID,
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: database.Name,
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: time.Now().UTC().Format(time.RFC3339),
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: time.Now().UTC().Format(time.RFC3339),
			},
		},
	)

	if err != nil {
		return err
	}

	database.exists = true

	return nil
}

// Update an existing database in the system database.
func UpdateDatabase(database *Database) error {
	systemDatabase := database.DatabaseManager.SystemDatabase()

	_, err := systemDatabase.Exec(
		`UPDATE databases 
		SET (
			name = ?,
			primary_branch_id = ?,
			settings = ?,
			updated_at = ?
		)
		WHERE database_id = ?
		`,
		[]sqlite3.StatementParameter{
			{
				Type:  sqlite3.ParameterTypeText,
				Value: database.Name,
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: database.PrimaryBranchID,
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: database.Settings,
			},
			{
				Type:  sqlite3.ParameterTypeText,
				Value: time.Now().UTC().Format(time.RFC3339),
			},
		},
	)

	if err != nil {
		return err
	}

	return nil
}

func (database *Database) HasBranch(branchID string) bool {
	if database.DatabaseID == SystemDatabaseID && branchID == SystemDatabaseBranchID {
		return true
	}

	// TODO: This needs to be an actualy lookup on the system database
	for _, branch := range database.Branches {
		if branch.BranchID == branchID {
			return true
		}
	}

	return false
}

func (database *Database) Key(branchID string) string {
	var branch *Branch

	for _, b := range database.Branches {
		if b.BranchID == branchID {
			branch = b
			break
		}
	}

	return branch.Key
}

func (database *Database) PrimaryBranch() *Branch {
	if database.primaryBranch == nil {
		// TODO: Select the branch from the system database
	}

	return database.primaryBranch
}

func (database *Database) Save() error {
	if database.exists {
		return InsertDatabase(database)
	} else {
		return UpdateDatabase(database)
	}
}

func (database *Database) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{
		"name":              database.Name,
		"branches":          database.Branches,
		"database_id":       database.DatabaseID,
		"primary_branch_id": database.PrimaryBranchID,
		"settings":          database.Settings,
		"url":               database.Url(database.PrimaryBranch().BranchID),
		"created_at":        database.CreatedAt,
		"updated_at":        database.UpdatedAt,
	})
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
