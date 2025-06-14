package database

import (
	"math/rand/v2"
	"time"

	"github.com/litebase/litebase/common/config"
	"github.com/litebase/litebase/server/auth"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	Id        string `json:"id"`
	IsPrimary bool   `json:"is_primary"`
	Key       string `json:"key"`
	Name      string `json:"name"`
}

func NewBranch(c *config.Config, dks *auth.DatabaseKeyStore, name string, isPrimary bool) *Branch {
	randomFactor := rand.Int64N(100000)
	keyCount := uint64(int64(dks.Len()) + time.Now().UTC().UnixNano() + randomFactor)

	s, _ := sqids.New(sqids.Options{
		Alphabet:  "0123456789abcdefghijklmnopqrstuvwxyz",
		MinLength: 12,
	})

	// TODO: ensure that the key is unique in the database key store

	key, _ := s.Encode([]uint64{keyCount})

	return &Branch{
		Id:        uuid.New().String(),
		IsPrimary: isPrimary,
		Key:       key,
		Name:      name,
	}
}
