package database

import (
	"litebase/internal/config"
	"litebase/server/storage"
	"math/rand/v2"
	"time"

	"github.com/google/uuid"
	"github.com/sqids/sqids-go"
)

type Branch struct {
	Id        string `json:"id"`
	IsPrimary bool   `json:"is_primary"`
	Key       string `json:"key"`
	Name      string `json:"name"`
}

func NewBranch(c *config.Config, objectFS *storage.FileSystem, name string, isPrimary bool) *Branch {
	randomFactor := rand.Int64N(100000)
	keyCount := uint64(GetDatabaseKeyCount(c, objectFS) + time.Now().UnixMilli() + randomFactor)

	s, _ := sqids.New(sqids.Options{
		Alphabet:  "0123456789abcdefghijklmnopqrstuvwxyz",
		MinLength: 12,
	})

	key, _ := s.Encode([]uint64{keyCount})

	return &Branch{
		Id:        uuid.New().String(),
		IsPrimary: isPrimary,
		Key:       key,
		Name:      name,
	}
}
