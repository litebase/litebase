package test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"litebase/server"
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/file"
	"litebase/server/sqlite3"
	"log"
)

type TestDatabase struct {
	DatabaseId  string
	BranchId    string
	DatabaseKey *database.DatabaseKey
	AccessKey   *auth.AccessKey
}

func CreateHash(length int) string {
	randomBytes := make([]byte, length)
	io.ReadFull(rand.Reader, randomBytes)
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
		Permissions: []*auth.AccessKeyPermission{
			{
				Resource: "*",
				Actions:  []string{"*"},
			},
		},
	}

	app.Auth.SecretsManager.StoreAccessKey(accessKey)

	db, err := app.DatabaseManager.Create("test-database", "main")

	if err != nil {
		log.Fatal(err)
	}

	return TestDatabase{
		DatabaseId: db.Id,
		BranchId:   db.PrimaryBranchId,
		DatabaseKey: &database.DatabaseKey{
			DatabaseHash: file.DatabaseHash(db.Id, db.PrimaryBranchId),
			DatabaseId:   db.Id,
			BranchId:     db.PrimaryBranchId,
			Key:          db.Key(db.PrimaryBranchId),
		},
		AccessKey: accessKey,
	}
}

func RunQuery(db *database.ClientConnection, statement []byte, parameters []sqlite3.StatementParameter) sqlite3.Result {
	sqliteStatement, _, err := db.GetConnection().SqliteConnection().Prepare(db.GetConnection().Context(), statement)

	if err != nil {
		log.Fatal(err)
	}

	result := db.GetConnection().ResultPool().Get()
	defer db.GetConnection().ResultPool().Put(result)

	err = db.GetConnection().Query(
		result,
		sqliteStatement,
		parameters,
	)

	if err != nil {
		log.Fatal(err)
	}

	return *result
}
