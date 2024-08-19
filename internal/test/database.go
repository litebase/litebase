package test

import (
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
	"litebase/server/auth"
	"litebase/server/database"
	"litebase/server/sqlite3"
	"litebase/server/storage"
	"log"
)

type TestDatabase struct {
	DatabaseUuid string
	BranchUuid   string
	DatabaseKey  string
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
	databaseUuid := CreateHash(32)
	branchUuid := CreateHash(32)
	databaseKey := CreateHash(32)
	accessKeyId := CreateHash(32)

	// accessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "accessKeySecret")
	// serverAccessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "serverAccessKeySecret")

	accessKey := &auth.AccessKey{
		AccessKeyId: accessKeyId,
	}

	auth.SecretsManager().StoreAccessKey(accessKey)

	_, err := database.Create(databaseUuid, branchUuid)

	if err != nil {
		log.Fatal(err)
	}

	return TestDatabase{
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		DatabaseKey:  databaseKey,
		AccessKey:    accessKey,
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

	db.Close()

	return result
}
