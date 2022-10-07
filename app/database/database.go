package database

import (
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"litebasedb/runtime/app/secrets"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Database struct {
	connection   *Connection
	databaseUuid string
	branchUuid   string
	path         string
}

var databases = map[string]map[string]map[string]*Database{}

func ClearDatabases() {
	databases = map[string]map[string]map[string]*Database{}
}

func (d *Database) Close() {
	d.connection.Close()
}

func EnsureDatabaseExists(databaseUuid string, branchUuid string) {
	path := GetFilePath(databaseUuid, branchUuid)

	if path == "" {
		log.Fatalf("Database %s has not been configured.", databaseUuid)
	}

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		os.Mkdir(filepath.Dir(path), 0755)
	}

	if _, err := os.Stat(filepath.Dir(path) + "/commits"); os.IsNotExist(err) {
		os.Mkdir(filepath.Dir(path)+"/commits", 0755)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.WriteFile(path, []byte(""), 0644)
	}
}

func Get(databaseUuid string, branchUuid string, accessKey *auth.AccessKey, new bool) *Database {
	if accessKey != nil && accessKey.DatabaseUuid != databaseUuid {
		log.Fatal("Invalid access key.")
	}

	var key string

	if accessKey != nil {
		key = accessKey.AccessKeyId
	} else {
		key = "*"
	}

	database, hasDatabase := databases[databaseUuid][branchUuid][key]

	if !new && hasDatabase {
		if database.GetConnection().IsOpen() {
			return database
		} else {
			delete(databases[databaseUuid][branchUuid], key)
		}
	}

	path := GetFilePath(databaseUuid, branchUuid)

	if path == "" {
		return nil
	}

	databases[databaseUuid][branchUuid][key] = &Database{
		branchUuid:   branchUuid,
		connection:   NewConnection(path, accessKey),
		databaseUuid: databaseUuid,
		path:         path,
	}

	return databases[databaseUuid][branchUuid][key]
}

func (d *Database) GetConnection() *Connection {
	return d.connection
}

func GetFileDir(databaseUuid string, branchUuid string) string {
	return filepath.Dir(GetFilePath(databaseUuid, branchUuid))
}

func GetFilePath(databaseUuid string, branchUuid string) string {
	path := secrets.Manager().GetPath(databaseUuid, branchUuid)

	if path == "" {
		return ""
	}

	pathParts := strings.Split(path, "/")

	// Insert without replacing the branchuuid to the path before the last segement.
	pathParts = append(pathParts[:len(pathParts)-1], append([]string{branchUuid}, pathParts[len(pathParts)-1:]...)...)

	path = strings.Join(pathParts, "/")

	return strings.TrimRight(config.Get("data_path"), "/") + "/" + strings.TrimLeft(path, "/")

}

func Init() {
	Register()
}

func (d *Database) Path() string {
	return d.path
}
