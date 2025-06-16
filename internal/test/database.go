package test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"log"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/server"
	"github.com/litebase/litebase/pkg/sqlite3"
)

type TestDatabase struct {
	DatabaseId  string
	BranchId    string
	DatabaseKey *auth.DatabaseKey
	AccessKey   *auth.AccessKey
	Url         string
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
		AccessKeyId:     accessKeyId,
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

	db, err := app.DatabaseManager.Create("test-database", "main")

	if err != nil {
		log.Fatal(err)
	}

	url := fmt.Sprintf(
		"http://%s.%s.%s.%s:%s",
		db.Key(db.PrimaryBranchId),
		app.Config.ClusterId,
		app.Config.Region,
		app.Config.DomainName,
		app.Config.Port,
	)

	return TestDatabase{
		DatabaseId: db.Id,
		BranchId:   db.PrimaryBranchId,
		DatabaseKey: &auth.DatabaseKey{
			DatabaseHash: file.DatabaseHash(db.Id, db.PrimaryBranchId),
			DatabaseId:   db.Id,
			BranchId:     db.PrimaryBranchId,
			Key:          db.Key(db.PrimaryBranchId),
		},
		AccessKey: accessKey,
		Url:       url,
	}
}

func RunQuery(db *database.ClientConnection, statement []byte, parameters []sqlite3.StatementParameter) sqlite3.Result {
	s, err := db.GetConnection().Prepare(db.GetConnection().Context(), statement)

	if err != nil {
		log.Fatal(err)
	}

	result := sqlite3.NewResult()

	err = db.GetConnection().Query(
		result,
		s.Sqlite3Statement,
		parameters,
	)

	if err != nil {
		log.Fatal(err)
	}

	return *result
}
