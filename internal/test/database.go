package test

import (
	"crypto/rand"
	"crypto/sha1"
	"fmt"
	"io"
	"litebasedb/internal/config"
	"litebasedb/server/auth"
	"litebasedb/server/database"
	"litebasedb/server/sqlite3"
	"litebasedb/server/storage"
	"log"
)

func ClearDatabase() {
	database.ConnectionManager().Clear()
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

func MockDatabase() map[string]string {
	// prefix1 := CreateHash(32)
	// prefix2 := CreateHash(32)
	databaseUuid := CreateHash(32)
	branchUuid := CreateHash(32)
	databaseKey := CreateHash(32)
	accessKeyId := CreateHash(32)

	config.Get().DatabaseUuid = databaseUuid
	config.Get().BranchUuid = branchUuid

	// settings := map[string]interface{}{
	// 	"path": fmt.Sprintf("%s/%s/%s.db", prefix1, prefix2, databaseKey),
	// 	"branch_settings": map[string]interface{}{
	// 		"backups": map[string]interface{}{
	// 			"enabled": true,
	// 			"incremental_backups": map[string]interface{}{
	// 				"enabled": true,
	// 			},
	// 		},
	// 	},
	// }

	auth.SecretsManager().Init()

	// accessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "accessKeySecret")
	// serverAccessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get().Signature, "serverAccessKeySecret")

	accessKey := &auth.AccessKey{
		AccessKeyId: accessKeyId,
	}

	auth.SecretsManager().StoreAccessKey(accessKey)

	err := database.EnsureDatabaseExists(databaseUuid, branchUuid)

	if err != nil {
		log.Fatal(err)
	}

	return map[string]string{
		"accessKeyId":     accessKeyId,
		"accessKeySecret": "accessKeySecret",
		"branchUuid":      branchUuid,
		"databaseKey":     databaseKey,
		"databaseUuid":    databaseUuid,
	}
}

func EncryptQuery(statement string, parameters string, accessKeyId string, accessKeySecret string) map[string]string {
	encrypter := auth.NewEncrypter([]byte(accessKeyId))

	encryptedStatement, err := encrypter.Encrypt(statement)

	if err != nil {
		log.Fatal(err)
	}

	encrypter = auth.NewEncrypter([]byte(accessKeySecret))
	encryptedParameters, err := encrypter.Encrypt(parameters)

	if err != nil {
		log.Fatal(err)
	}

	return map[string]string{
		"statement":  encryptedStatement,
		"parameters": encryptedParameters,
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
