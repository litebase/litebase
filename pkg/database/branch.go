package database

import (
	"crypto/rand"
	"math/big"
	"time"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/config"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	ID         int64  `json:"id"`
	DatabaseID int64  `json:"database_id"`
	BranchID   string `json:"branch_id"`
	Key        string `json:"key"`
	Name       string `json:"name"`
	Settings   string `json:"settings"` // TODO: Need to make this a struct
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

func NewBranch(c *config.Config, dks *auth.DatabaseKeyStore, name string, isPrimary bool) (*Branch, error) {
	randInt64, err := rand.Int(rand.Reader, big.NewInt(100000))

	if err != nil {
		return nil, err
	}

	keyCount, err := utils.SafeInt64ToUint64(int64(dks.Len()) + time.Now().UTC().UnixNano() + randInt64.Int64())

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
		BranchID: uuid.New().String(),
		Key:      key,
		Name:     name,
	}, nil
}

func (b *Branch) Save() error {
	return nil
}
