package test

import (
	"crypto/rand"
	"encoding/hex"
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
	Credential       *auth.Credential
}

type TestDatabaseAuthorizationCommand struct {
	SQL         string
	ExpectError bool
}

func CreateHash(length int) string {
	randomBytes := make([]byte, (length+1)/2) // Ensure enough bytes for the desired length

	_, err := rand.Read(randomBytes)

	if err != nil {
		log.Fatal(err)
	}

	return hex.EncodeToString(randomBytes)[:length]
}

func MockDatabase(app *server.App) TestDatabase {
	accessKey, err := app.Auth.AccessKeyManager.Create("", []auth.Statement{
		{
			Effect:   auth.StatementEffectAllow,
			Resource: "*",
			Actions:  []auth.Privilege{"*"},
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	randomDbName := "testdb_" + uuid.NewString()

	db, err := app.DatabaseManager.Create(randomDbName, "main")

	if err != nil {
		log.Fatal(err)
	}

	credential := &auth.Credential{}
	credential.WithAccessKey(accessKey)

	primaryBranch, err := db.PrimaryBranch()

	if err != nil {
		log.Fatal(err)
	}

	return TestDatabase{
		ID:               db.ID,
		BranchID:         primaryBranch.ID,
		BranchName:       primaryBranch.Name,
		DatabaseID:       db.DatabaseID,
		DatabaseBranchID: primaryBranch.DatabaseBranchID,
		DatabaseKey: &auth.DatabaseKey{
			DatabaseHash:       file.DatabaseHash(db.DatabaseID, primaryBranch.DatabaseBranchID),
			DatabaseID:         db.DatabaseID,
			DatabaseName:       db.Name,
			DatabaseBranchID:   primaryBranch.DatabaseBranchID,
			DatabaseBranchName: primaryBranch.Name,
		},
		DatabaseName: db.Name,
		Credential:   credential,
	}
}
