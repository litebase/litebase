package database

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"math/big"
	"time"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/file"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	ID                              int64 `json:"id"`
	database                        *Database
	DatabaseBranchID                string           `json:"database_branch_id"`
	DatabaseID                      string           `json:"database_id"`
	DatabaseManager                 *DatabaseManager `json:"-"`
	DatabaseReferenceID             sql.NullInt64    `json:"-"`
	Key                             string           `json:"key"`
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

	var databaseKeyCount int64

	// Get the current count of branches to generate a unique key.
	err = db.QueryRow(`SELECT COUNT(*) FROM database_branches`).Scan(&databaseKeyCount)

	if err != nil {
		return nil, err
	}

	randInt64, err := rand.Int(rand.Reader, big.NewInt(100000))

	if err != nil {
		return nil, err
	}

	keyCount, err := utils.SafeInt64ToUint64(databaseKeyCount + time.Now().UTC().UnixNano() + randInt64.Int64())

	if err != nil {
		return nil, err
	}

	s, _ := sqids.New(sqids.Options{
		Alphabet:  "0123456789abcdefghijklmnopqrstuvwxyz",
		MinLength: 12,
	})

	key, err := s.Encode([]uint64{keyCount})

	if err != nil {
		return nil, err
	}

	return &Branch{
		DatabaseBranchID:                uuid.New().String(),
		DatabaseManager:                 databaseManager,
		DatabaseReferenceID:             sql.NullInt64{Int64: databaseReferenceID, Valid: true},
		Key:                             key,
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
			key, 
			name, 
			settings, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
		b.DatabaseReferenceID,
		b.ParentDatabaseBranchReferenceID,
		b.DatabaseID,
		b.DatabaseBranchID,
		b.Key,
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

	return nil
}

// Retrieve the database that the branch belongs to.
func (b *Branch) Database() *Database {
	if b.database != nil {
		return b.database
	}

	db, err := b.DatabaseManager.Get(b.DatabaseID)

	if err != nil {
		log.Println("Error getting database:", err)
		return nil
	}

	b.database = db

	return b.database
}

// Delete the branch
func (b *Branch) Delete() error {
	if b == nil || !b.Exists {
		return fmt.Errorf("branch does not exist or is nil")
	}

	if b.Database().PrimaryBranch().DatabaseBranchID == b.DatabaseBranchID {
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
				`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, key, settings, created_at, updated_at FROM database_branches WHERE id = ?`,
				branch.ParentDatabaseBranchReferenceID.Int64,
			).Scan(
				&parentBranch.ID,
				&parentBranch.DatabaseReferenceID,
				&parentBranch.ParentDatabaseBranchReferenceID,
				&parentBranch.DatabaseID,
				&parentBranch.DatabaseBranchID,
				&parentBranch.Name,
				&parentBranch.Key,
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
