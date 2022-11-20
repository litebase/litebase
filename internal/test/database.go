package test

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"io"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/config"
	"litebasedb/runtime/database"
	"litebasedb/runtime/sqlite3"
	"log"
	"os"
)

func ClearDatabase() {
	database.ClearDatabases()
	os.RemoveAll("./data/_test")
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
	prefix1 := CreateHash(32)
	prefix2 := CreateHash(32)
	databaseUuid := CreateHash(32)
	branchUuid := CreateHash(32)
	databaseKey := CreateHash(32)
	accessKeyId := CreateHash(32)

	config.Set("database_uuid", databaseUuid)
	config.Set("branch_uuid", branchUuid)

	settings := map[string]interface{}{
		"path": fmt.Sprintf("%s/%s/%s.db", prefix1, prefix2, databaseKey),
		"branch_settings": map[string]interface{}{
			"backups": map[string]interface{}{
				"enabled": true,
				"incremental_backups": map[string]interface{}{
					"enabled": true,
				},
			},
		},
	}
	jsonSettings, _ := json.Marshal(settings)
	encryptedSettings, _ := auth.SecretsManager().Encrypt(config.Get("signature"), string(jsonSettings))

	data := map[string]interface{}{
		"database_uuid": databaseUuid,
		"branch_uuid":   branchUuid,
		"database_key":  databaseKey,
		"data":          encryptedSettings,
	}

	auth.SecretsManager().Init()

	auth.SecretsManager().StoreDatabaseSettings(
		databaseUuid,
		branchUuid,
		databaseKey,
		data["data"].(string),
	)

	// accessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get("signature"), "accessKeySecret")
	// serverAccessKeySecret, _ := auth.SecretsManager().Encrypt(config.Get("signature"), "serverAccessKeySecret")

	auth.SecretsManager().StoreAccessKey(
		databaseUuid,
		branchUuid,
		accessKeyId,
		"",
		// accessKeySecret,
		// serverAccessKeySecret,
		// map[string]interface{}{
		// 	"*": []string{"ALL"},
		// },
	)

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

func RunQuery(db *database.Database, statement string, parameters []interface{}) sqlite3.Result {
	sqliteStatement, err := db.GetConnection().Prepare(statement)

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

	db.GetConnection().Operator.Transmit()

	db.Close()

	return result
}
