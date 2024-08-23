package test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/file"
	"litebase/server/sqlite3"
	"litebase/server/storage"
	"log"
)

type TestDatabase struct {
	DatabaseUuid string
	BranchUuid   string
	DatabaseKey  *database.DatabaseKey
	AccessKey    *auth.AccessKey
}

func ClearDatabase() {
	database.ConnectionManager().Shutdown()
	storage.FS().RemoveAll("./.test")
}

func CreateHash(length int) string {
	randomBytes := make([]byte, length)
	io.ReadFull(rand.Reader, randomBytes)
	hash := sha256.New()
	hash.Write(randomBytes)
	hashBytes := hash.Sum(nil)

	return fmt.Sprintf("%x", hashBytes)
}

func MockDatabase() TestDatabase {
	accessKeyId := CreateHash(32)

	// accessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "accessKeySecret")
	// serverAccessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "serverAccessKeySecret")

	accessKey := &auth.AccessKey{
		AccessKeyId: accessKeyId,
	}

	auth.SecretsManager().StoreAccessKey(accessKey)

	db, err := database.Create("test-database", "main")

	if err != nil {
		log.Fatal(err)
	}

	return TestDatabase{
		DatabaseUuid: db.Id,
		BranchUuid:   db.PrimaryBranchId,
		DatabaseKey: &database.DatabaseKey{
			DatabaseHash: file.DatabaseHash(db.Id, db.PrimaryBranchId),
			DatabaseUuid: db.Id,
			BranchUuid:   db.PrimaryBranchId,
		},
		AccessKey: accessKey,
	}
}

func RunQuery(db *database.ClientConnection, statement string, parameters []interface{}) sqlite3.Result {
	sqliteStatement, err := db.GetConnection().SqliteConnection().Prepare(db.GetConnection().Context(), statement)

	if err != nil {
		log.Fatal(err)
	}

	result, err := db.GetConnection().Query(
		sqliteStatement,
		parameters...,
	)

	if err != nil {
		log.Fatal(err)
	}

	return result
}
