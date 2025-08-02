package database

import (
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/file"

	"github.com/google/uuid"
)

type Branch struct {
	ID                              int64 `json:"id"`
	database                        *Database
	DatabaseBranchID                string           `json:"database_branch_id"`
	DatabaseID                      string           `json:"database_id"`
	DatabaseManager                 *DatabaseManager `json:"-"`
	DatabaseReferenceID             sql.NullInt64    `json:"-"`
	Name                            string           `json:"name"`
	parentBranch                    *Branch          `json:"-"`
	ParentDatabaseBranchReferenceID sql.NullInt64    `json:"-"`
	Settings                        *BranchSettings  `json:"settings"`
	CreatedAt                       time.Time        `json:"created_at"`
	UpdatedAt                       time.Time        `json:"updated_at"`

	Exists bool `json:"-"`
}

func NewBranch(databaseManager *DatabaseManager, databaseReferenceID int64, parentName string, name string) (*Branch, error) {
	db, err := databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	// Ensure there is not a current branch with the same name and parent branch within this database
	var existingBranchCount int64
	var parentBranchID sql.NullInt64

	// Get the parent branch ID if parentName is provided
	if parentName != "" {
		err = db.QueryRow(
			`SELECT id FROM database_branches WHERE name = ? AND database_reference_id = ?`,
			parentName,
			databaseReferenceID,
		).Scan(&parentBranchID.Int64)

		if err != nil {
			return nil, fmt.Errorf("parent branch '%s' not found in this database", parentName)
		}

		parentBranchID.Valid = true
	}

	// Check for existing branch with same name within this database
	err = db.QueryRow(
		`SELECT COUNT(*) FROM database_branches 
			WHERE name = ? AND database_reference_id = ?`,
		name,
		databaseReferenceID,
	).Scan(&existingBranchCount)

	if err != nil {
		return nil, fmt.Errorf("error checking for existing branch: %w", err)
	}

	if existingBranchCount > 0 {
		return nil, fmt.Errorf("branch with name '%s' already exists in this database", name)
	}

	return &Branch{
		DatabaseBranchID:                uuid.New().String(),
		DatabaseManager:                 databaseManager,
		DatabaseReferenceID:             sql.NullInt64{Int64: databaseReferenceID, Valid: true},
		Name:                            name,
		ParentDatabaseBranchReferenceID: parentBranchID,
	}, nil
}

func InsertBranch(b *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	result, err := db.Exec(
		`INSERT INTO database_branches (
			database_reference_id,
			parent_database_branch_reference_id,
			database_id, 
			database_branch_id, 
			name, 
			settings, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`,
		b.DatabaseReferenceID,
		b.ParentDatabaseBranchReferenceID,
		b.DatabaseID,
		b.DatabaseBranchID,
		b.Name,
		b.Settings,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {

		log.Fatal(err)
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	b.ID = id
	b.Exists = true

	database, err := b.Database()

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Update the Database's branch cache
	if database != nil {
		database.UpdateBranchCache(b.DatabaseBranchID, true)
	}

	return nil
}

func UpdateBranch(b *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	_, err = db.Exec(
		`UPDATE database_branches
		SET
			name = ?,
			settings = ?,
			updated_at = ?
		WHERE database_branch_id = ?
		`,
		b.Name,
		b.Settings,
		time.Now().UTC(),
		b.DatabaseBranchID,
	)

	if err != nil {
		return err
	}

	database, err := b.Database()

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Update the Database's branch cache to ensure consistency
	if database != nil {
		database.UpdateBranchCache(b.DatabaseBranchID, true)
	}

	return nil
}

// Retrieve the database that the branch belongs to.
func (b *Branch) Database() (*Database, error) {
	if b.database != nil {
		return b.database, nil
	}

	db, err := b.DatabaseManager.Get(b.DatabaseID)

	if err != nil {
		return nil, err
	}

	b.database = db

	return b.database, nil
}

// Delete the branch
func (b *Branch) Delete() error {
	if b == nil || !b.Exists {
		return fmt.Errorf("branch does not exist or is nil")
	}

	database, err := b.Database()

	if err != nil {
		return fmt.Errorf("failed to load branch's database: %w", err)
	}

	primaryBranch := database.PrimaryBranch()

	if primaryBranch == nil {
		return fmt.Errorf("cannot delete branch: primary branch not found")
	}

	if primaryBranch.DatabaseBranchID == b.DatabaseBranchID {
		return fmt.Errorf("cannot delete the primary branch of a database")
	}

	resources := b.DatabaseManager.Resources(b.DatabaseID, b.DatabaseBranchID)

	// Close all database connections to the database before deleting it
	b.DatabaseManager.ConnectionManager().CloseDatabaseBranchConnections(b.DatabaseID, b.DatabaseBranchID)

	fileSystem := resources.FileSystem()

	// Delete from the system database
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	_, err = db.Exec(
		`DELETE FROM database_branches WHERE id = ?`,
		b.ID,
	)

	if err != nil {
		return fmt.Errorf("failed to delete database branch: %w", err)
	}

	database, err = b.Database()

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load branch's database: %w", err)
	}

	// Remove the branch from the database's branch cache
	if database != nil {
		database.branchCache.Delete(b.DatabaseBranchID)
		database.InvalidateBranchCache(b.DatabaseBranchID)
	}

	// Invalidate the database branch cache

	if b.DatabaseManager.databaseCache != nil {
		database, _ = b.DatabaseManager.Get(b.DatabaseID)

		database.branchCache.Delete(b.DatabaseBranchID)
	}

	// Delete the database storage.
	// TODO: Removing all database storage may require the removal of a lot of files.
	// How is this going to work with tiered storage? We also need to test that
	// removing a branch stops any operations to the database.
	err = fileSystem.FileSystem().RemoveAll(
		file.GetDatabaseBranchRootDir(
			b.DatabaseID,
			b.DatabaseBranchID,
		),
	)

	if err != nil {
		slog.Error("Error deleting database storage", "error", err)
		return err
	}

	resources.Remove()

	return nil
}

// Load and return the parent branch of the current branch
func (branch *Branch) ParentBranch() *Branch {
	if branch == nil {
		return nil
	}

	if branch.parentBranch == nil {
		// If no primary branch ID is set, return nil
		if !branch.ParentDatabaseBranchReferenceID.Valid || branch.ParentDatabaseBranchReferenceID.Int64 == 0 {
			return nil
		}

		// Load the primary branch from the system database using the foreign key
		if branch.DatabaseManager != nil {
			db, err := branch.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				return nil
			}

			var parentBranch Branch

			err = db.QueryRow(
				`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, settings, created_at, updated_at FROM database_branches WHERE id = ?`,
				branch.ParentDatabaseBranchReferenceID.Int64,
			).Scan(
				&parentBranch.ID,
				&parentBranch.DatabaseReferenceID,
				&parentBranch.ParentDatabaseBranchReferenceID,
				&parentBranch.DatabaseID,
				&parentBranch.DatabaseBranchID,
				&parentBranch.Name,
				&parentBranch.Settings,
				&parentBranch.CreatedAt,
				&parentBranch.UpdatedAt,
			)

			if err == nil {
				parentBranch.DatabaseManager = branch.DatabaseManager
				parentBranch.Exists = true
				branch.parentBranch = &parentBranch
			} else {
				log.Println("Error loading primary branch:", err)
			}
		}
	}

	return branch.parentBranch
}

// Save a database to the system database.
func (b *Branch) Save() error {
	if b.DatabaseID == "" || b.DatabaseBranchID == "" {
		return fmt.Errorf("branch is missing required fields")
	}

	if b.Exists {
		return UpdateBranch(b)
	} else {
		return InsertBranch(b)
	}
}
