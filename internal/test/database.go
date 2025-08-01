package test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"

	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
)

type TestDatabase struct {
	ID               int64
	BranchID         int64
	BranchName       string
	DatabaseID       string
	DatabaseBranchID string
	DatabaseKey      *auth.DatabaseKey
	DatabaseName     string
	AccessKey        *auth.AccessKey
}

type TestDatabaseAuthorizationCommand struct {
	SQL         string
	ExpectError bool
}

func CreateHash(length int) string {
	randomBytes := make([]byte, length)
	_, err := io.ReadFull(rand.Reader, randomBytes)

	if err != nil {
		log.Fatal(err)
	}
	hash := sha256.New()
	hash.Write(randomBytes)
	hashBytes := hash.Sum(nil)

	return fmt.Sprintf("%x", hashBytes)[:length]
}

func MockDatabase(app *server.App) TestDatabase {
	accessKeyId := CreateHash(32)

	accessKey := &auth.AccessKey{
		AccessKeyID:     accessKeyId,
		AccessKeySecret: "accessKeySecret",
		Statements: []auth.AccessKeyStatement{
			{
				Effect:   auth.AccessKeyEffectAllow,
				Resource: "*",
				Actions:  []auth.Privilege{"*"},
			},
		},
	}

	err := app.Auth.SecretsManager.StoreAccessKey(accessKey)

	if err != nil {
		log.Fatal(err)
	}

	randomDbName := "testdb_" + uuid.NewString()

	db, err := app.DatabaseManager.Create(randomDbName, "main")

	if err != nil {
		log.Fatal(err)
	}

	return TestDatabase{
		ID:               db.ID,
		BranchID:         db.PrimaryBranch().ID,
		BranchName:       db.PrimaryBranch().Name,
		DatabaseID:       db.DatabaseID,
		DatabaseBranchID: db.PrimaryBranch().DatabaseBranchID,
		DatabaseKey: &auth.DatabaseKey{
			DatabaseHash:       file.DatabaseHash(db.DatabaseID, db.PrimaryBranch().DatabaseBranchID),
			DatabaseID:         db.DatabaseID,
			DatabaseName:       db.Name,
			DatabaseBranchID:   db.PrimaryBranch().DatabaseBranchID,
			DatabaseBranchName: db.PrimaryBranch().Name,
		},
		DatabaseName: db.Name,

		AccessKey: accessKey,
	}
}
