package test

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"litebasedb/server"
	"litebasedb/server/auth"
	"litebasedb/server/database"
	"litebasedb/server/sqlite3"
	"litebasedb/server/storage"
	"log"
)

type TestDatabase struct {
	DatabaseUuid    string
	BranchUuid      string
	DatabaseKey     string
	AccessKeyId     string
	AccessKeySecret string
}

func ClearDatabase() {
	database.ConnectionManager().Shutdown()
	storage.FS().RemoveAll("./data/_test")
}

func CreateHash(length int) string {
	randomBytes := make([]byte, length)
	io.ReadFull(rand.Reader, randomBytes)
	hash := sha1.New()
	hash.Write(randomBytes)
	hashBytes := hash.Sum(nil)

	return fmt.Sprintf("%x", hashBytes)
}

func MockDatabase() TestDatabase {
	databaseUuid := CreateHash(32)
	branchUuid := CreateHash(32)
	databaseKey := CreateHash(32)
	accessKeyId := CreateHash(32)

	config.Get().Signature = CreateHash(32)
	config.Get().SignatureNext = CreateHash(32)
	config.Get().DatabaseUuid = databaseUuid
	config.Get().BranchUuid = branchUuid

	server.NewApp(server.NewServer())

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
		DatabaseUuid:    databaseUuid,
		BranchUuid:      branchUuid,
		DatabaseKey:     databaseKey,
		AccessKeyId:     accessKeyId,
		AccessKeySecret: "accessKeySecret",
	}
}

func RunQuery(db *database.ClientConnection, statement string, parameters []interface{}) sqlite3.Result {
	sqliteStatement, err := db.GetConnection().SqliteConnection().Prepare(statement)

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
