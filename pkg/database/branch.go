package database

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"github.com/litebase/litebase/internal/utils"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	ID              int64 `json:"id"`
	DatabaseID      int64 `json:"database_id"`
	DatabaseManager *DatabaseManager
	BranchID        string          `json:"branch_id"`
	Key             string          `json:"key"`
	Name            string          `json:"name"`
	Settings        *BranchSettings `json:"settings"` // TODO: Need to make this a struct
	CreatedAt       time.Time
	UpdatedAt       time.Time

	exists bool
}

func NewBranch(databaseManager *DatabaseManager, name string) (*Branch, error) {
	randInt64, err := rand.Int(rand.Reader, big.NewInt(100000))

	if err != nil {
		return nil, err
	}

	db, err := databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	var databaseKeyCount int64

	err = db.QueryRow(`SELECT COUNT(*) FROM database_keys`).Scan(&databaseKeyCount)

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
		BranchID:        uuid.New().String(),
		DatabaseManager: databaseManager,
		Key:             key,
		Name:            name,
	}, nil
}

func InsertBranch(b *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	result, err := db.Exec(
		`INSERT INTO database_branches (
			database_id, 
			database_branch_id, 
			key, 
			name, 
			settings, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		`,
		b.DatabaseID,
		b.BranchID,
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
	b.exists = true

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
		b.BranchID,
	)

	if err != nil {
		return err
	}

	return nil
}

func (b *Branch) Save() error {
	if b.exists {
		return UpdateBranch(b)
	} else {
		return InsertBranch(b)
	}
}
