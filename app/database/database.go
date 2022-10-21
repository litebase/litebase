package database

import (
	"fmt"
	"litebasedb/runtime/app/auth"
	"litebasedb/runtime/app/config"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type Database struct {
	branchUuid   string
	connection   *Connection
	databaseUuid string
	initialized  bool
	path         string
}

var databases = map[string]map[string]map[string]*Database{}

func ClearDatabases() {
	databases = map[string]map[string]map[string]*Database{}
}

func (d *Database) Close() {
	d.connection.Close()
}

func EnsureDatabaseExists(databaseUuid string, branchUuid string) error {
	path, err := GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return fmt.Errorf("Database %s has not been configured", databaseUuid)
	}

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
	}

	if _, err := os.Stat(path); os.IsNotExist(err) {
		os.WriteFile(path, []byte(""), 0644)
	}

	return nil
}

func Get(databaseUuid string, branchUuid string, accessKey *auth.AccessKey, new bool) (*Database, error) {
	if accessKey != nil && accessKey.DatabaseUuid != databaseUuid {
		return nil, fmt.Errorf("Access key is not valid for database %s", databaseUuid)
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
			return database, nil
		} else {
			delete(databases[databaseUuid][branchUuid], key)
		}
	}

	path, err := GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return nil, fmt.Errorf("Database %s has not been configured", databaseUuid)
	}

	if databases[databaseUuid] == nil {
		databases[databaseUuid] = map[string]map[string]*Database{}
	}

	if databases[databaseUuid][branchUuid] == nil {
		databases[databaseUuid][branchUuid] = map[string]*Database{}
	}

	databases[databaseUuid][branchUuid][key] = &Database{
		branchUuid:   branchUuid,
		connection:   NewConnection(path, accessKey),
		databaseUuid: databaseUuid,
		path:         path,
	}

	databases[databaseUuid][branchUuid][key].Init()
	databases[databaseUuid][branchUuid][key].connection.Open()

	return databases[databaseUuid][branchUuid][key], nil
}

func (d *Database) GetConnection() *Connection {
	return d.connection
}

func GetFileDir(databaseUuid string, branchUuid string) string {
	dir, err := GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return ""

	}
	return filepath.Dir(dir)
}

func GetFilePath(databaseUuid string, branchUuid string) (string, error) {
	path, err := auth.SecretsManager().GetPath(databaseUuid, branchUuid)

	if err != nil {
		log.Println("ERROr", err)
		return "", err
	}

	pathParts := strings.Split(path, "/")

	// Insert without replacing the branchuuid to the path before the last segement.
	pathParts = append(pathParts[:len(pathParts)-1], append([]string{branchUuid}, pathParts[len(pathParts)-1:]...)...)

	path = strings.Join(pathParts, "/")

	return strings.TrimRight(config.Get("data_path"), "/") + "/" + strings.TrimLeft(path, "/"), nil
}

func (d *Database) Init() {
	if d.initialized {
		return
	}

	Register(d.connection)
}

func (d *Database) Path() string {
	return d.path
}
