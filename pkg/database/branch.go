package database

import (
	"crypto/rand"
	"database/sql"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/litebase/litebase/internal/utils"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	ID                              int64            `json:"id"`
	DatabaseBranchID                string           `json:"branch_id"`
	DatabaseID                      string           `json:"database_id"`
	DatabaseManager                 *DatabaseManager `json:"-"`
	DatabaseReferenceID             sql.NullInt64    `json:"database_reference_id"`
	Key                             string           `json:"key"`
	Name                            string           `json:"name"`
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
