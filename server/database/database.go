package database

import (
	"encoding/json"
	"fmt"
	"litebase/internal/config"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/file"
	"litebase/server/storage"
	"log"
	"os"
	"sync"

	"github.com/google/uuid"
)

type Database struct {
	Name              string           `json:"name"`
	Branches          []*Branch        `json:"branches"`
	Id                string           `json:"id"`
	PrimaryBranchId   string           `json:"primary_branch_id"`
	PrimaryBranchName string           `json:"primary_branch_name"`
	Settings          DatabaseSettings `json:"settings"`
}

var databases map[string]*Database = make(map[string]*Database)
var databaseMutex = &sync.Mutex{}

func Create(databaseName, branchName string) (*Database, error) {
	branch := NewBranch(branchName, true)

	database := &Database{
		Name:              databaseName,
		Branches:          []*Branch{branch},
		Id:                uuid.New().String(),
		PrimaryBranchId:   branch.Id,
		PrimaryBranchName: branchName,
		Settings: DatabaseSettings{
			Backups: DatabaseBackupSettings{
				Enabled: true,
				IncrementalBackups: DatabaseIncrementalBackupSettings{
					Enabled: true,
				},
			},
		},
	}

	err := database.save()

	if err != nil {
		return nil, err
	}

	err = storage.ObjectFS().MkdirAll(database.BranchDirectory(branch.Id), 0755)

	if err != nil {
		log.Println("ERROR", err)
		return nil, err
	}

	return database, nil
}

func Init() {
	storage.ObjectFS().Mkdir(Directory(), 0755)
}

func All() ([]*Database, error) {
	var databases []*Database

	// Read all files in the databases directory
	entries, err := storage.ObjectFS().ReadDir(Directory())

	if err != nil {
		return nil, err
	}

	// TODO: High touch area, consider refactoring
	for _, entry := range entries {
		data, err := storage.ObjectFS().ReadFile(fmt.Sprintf("%s%s/settings.json", Directory(), entry.Name))

		if err != nil {
			return nil, err
		}

		datbase := &Database{}

		err = json.Unmarshal(data, datbase)

		if err != nil {
			return nil, err
		}

		databases = append(databases, datbase)
	}

	return databases, nil
}

func Delete(database *Database) error {
	path := fmt.Sprintf("%s%s", Directory(), database.Id)

	if _, err := storage.ObjectFS().Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", database.Id)
	}

	for _, branch := range database.Branches {
		auth.SecretsManager().DeleteDatabaseKey(
			database.Key(branch.Id),
		)
	}

	// TODO: Delete all branches
	// TODO: Delete all access keys
	// TODO: Delete all backups and storage

	return storage.ObjectFS().Remove(path)
}

func Directory() string {
	return "_databases/"
}

func Exists(name string) bool {
	databases, err := All()

	if err != nil {
		if os.IsNotExist(err) {
			return false
		}

		log.Fatal(err)
	}

	for _, database := range databases {
		if database.Name == name {
			return true
		}
	}

	return false
}

func TmpDirectory() string {
	return "_databases/"
}

func Get(databaseUuid string) (*Database, error) {
	databaseMutex.Lock()
	defer databaseMutex.Unlock()

	if databases[databaseUuid] != nil {
		return databases[databaseUuid], nil
	}

	path := fmt.Sprintf("%s%s/settings.json", file.DatabaseDirectory(), databaseUuid)
	file, err := storage.ObjectFS().Open(path)

	if err != nil {
		return nil, fmt.Errorf("database '%s' has not been configured", databaseUuid)
	}

	database := &Database{}

	err = json.NewDecoder(file).Decode(database)

	if err != nil {
		return nil, err
	}

	databases[databaseUuid] = database

	return database, nil
}

func (database *Database) Key(branchUuid string) string {
	var branch *Branch

	for _, b := range database.Branches {
		if b.Id == branchUuid {
			branch = b
			break
		}
	}

	return branch.Key
}

func (database *Database) save() error {
	storage.ObjectFS().MkdirAll(fmt.Sprintf("%s%s", Directory(), database.Id), 0755)

	jsonData, err := json.Marshal(database)

	if err != nil {
		return err
	}

	createError := storage.ObjectFS().WriteFile(fmt.Sprintf("%s%s/settings.json", Directory(), database.Id), jsonData, 0666)

	auth.SecretsManager().StoreDatabaseKey(
		database.Key(database.PrimaryBranchId),
		database.Id,
		database.PrimaryBranchId,
	)

	return createError
}

func (database *Database) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]interface{}{
		"name":              database.Name,
		"branches":          database.Branches,
		"id":                database.Id,
		"primary_branch_id": database.PrimaryBranchId,
		"settings":          database.Settings,
		"url":               database.Url(database.PrimaryBranchId),
	})
}

func (database *Database) BranchDirectory(branchUuid string) string {
	return fmt.Sprintf("%s%s/%s", Directory(), database.Id, branchUuid)
}

func (database *Database) Url(branchUuid string) string {
	return fmt.Sprintf(
		"http://%s.%s.%s.litebase.test:%s",
		database.Key(database.PrimaryBranchId),
		// TODO: Get the region
		"region",
		cluster.Get().Id,
		// TODO: Make optional for production
		config.Get().Port,
	)
}
