package database

import (
	"fmt"
	"litebasedb/runtime/auth"
	"litebasedb/runtime/file"
	"os"
	"path/filepath"
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
	path, err := file.GetFilePath(databaseUuid, branchUuid)

	if err != nil {
		return fmt.Errorf("Database %s has not been configured", databaseUuid)
	}

	if _, err := os.Stat(filepath.Dir(path)); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path), 0755)
	}

	if _, err := os.Stat(filepath.Dir(path) + "/backups"); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path)+"/backups", 0755)
	}

	if _, err := os.Stat(filepath.Dir(path) + "/restore_points"); os.IsNotExist(err) {
		os.MkdirAll(filepath.Dir(path)+"/restore_points", 0755)
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

	path, err := file.GetFilePath(databaseUuid, branchUuid)

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

func (d *Database) Init() {
	if d.initialized {
		return
	}

	Register(d.connection)
}

func (d *Database) Path() string {
	return d.path
}
