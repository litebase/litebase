package database

import (
	"encoding/json"
	"fmt"
	"litebase/server/auth"
	"litebase/server/cluster"
	"litebase/server/file"
	"log"
	"os"
	"sync"
	"time"

	"github.com/google/uuid"
)

type DatabaseManager struct {
	databases         map[string]*Database
	Cluster           *cluster.Cluster
	connectionManager *ConnectionManager
	mutex             *sync.Mutex
	resources         map[string]*DatabaseResources
	SecretsManager    *auth.SecretsManager
}

func NewDatabaseManager(cluster *cluster.Cluster, secretsManager *auth.SecretsManager) *DatabaseManager {
	return &DatabaseManager{
		databases:      make(map[string]*Database),
		Cluster:        cluster,
		mutex:          &sync.Mutex{},
		resources:      make(map[string]*DatabaseResources),
		SecretsManager: secretsManager,
	}
}

func (d *DatabaseManager) All() ([]*Database, error) {
	var databases []*Database

	// Read all files in the databases directory
	entries, err := d.Cluster.ObjectFS().ReadDir(Directory())

	if err != nil {
		return nil, err
	}

	// TODO: High touch area, consider refactoring
	for _, entry := range entries {
		data, err := d.Cluster.ObjectFS().ReadFile(fmt.Sprintf("%s%s/settings.json", Directory(), entry.Name()))

		if err != nil {
			return nil, err
		}

		database := &Database{}

		err = json.Unmarshal(data, database)

		if err != nil {
			return nil, err
		}

		database.DatabaseManager = d

		databases = append(databases, database)
	}

	return databases, nil
}

func (d *DatabaseManager) ConnectionManager() *ConnectionManager {
	if d.connectionManager != nil {
		return d.connectionManager
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.connectionManager == nil {
		d.connectionManager = &ConnectionManager{
			cluster:         d.Cluster,
			databaseManager: d,
			databases:       map[string]*DatabaseGroup{},
			mutex:           &sync.RWMutex{},
			state:           ConnectionManagerStateRunning,
		}

		// Start the connection ticker
		go func() {
			time.Sleep(1 * time.Second)
			d.connectionManager.connectionTicker = time.NewTicker(1 * time.Second)

			for {
				select {
				case <-d.Cluster.Node().Context().Done():
					return
				case <-d.connectionManager.connectionTicker.C:
					d.connectionManager.Tick()
				}
			}
		}()
	}

	return d.connectionManager
}

func (d *DatabaseManager) Create(databaseName, branchName string) (*Database, error) {
	branch := NewBranch(d.Cluster.Config, d.Cluster.ObjectFS(), branchName, true)

	database := &Database{
		DatabaseManager:   d,
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

	err = d.Cluster.ObjectFS().MkdirAll(database.BranchDirectory(branch.Id), 0755)

	if err != nil {
		log.Println("ERROR", err)
		return nil, err
	}

	return database, nil
}

func (d *DatabaseManager) Delete(database *Database) error {
	path := fmt.Sprintf("%s%s", Directory(), database.Id)

	if _, err := d.Cluster.ObjectFS().Stat(path); os.IsNotExist(err) {
		return fmt.Errorf("database '%s' does not exist", database.Id)
	}

	for _, branch := range database.Branches {
		database.DatabaseManager.SecretsManager.DeleteDatabaseKey(
			database.Key(branch.Id),
		)
	}

	// TODO: Delete all branches
	// TODO: Delete all access keys
	// TODO: Delete all backups and storage

	return d.Cluster.ObjectFS().Remove(path)
}

func (d *DatabaseManager) Exists(name string) bool {
	databases, err := d.All()

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

func (d *DatabaseManager) Get(databaseId string) (*Database, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.databases[databaseId] != nil {
		return d.databases[databaseId], nil
	}

	path := fmt.Sprintf("%s%s/settings.json", file.DatabaseDirectory(), databaseId)

	file, err := d.Cluster.ObjectFS().Open(path)

	if err != nil {
		return nil, fmt.Errorf("database '%s' has not been configured", databaseId)
	}

	database := &Database{}

	err = json.NewDecoder(file).Decode(database)

	if err != nil {
		return nil, err
	}

	d.databases[databaseId] = database

	return database, nil
}

/*
Get the resources for the given database and branch UUIDs. If the resources
have not been created, create them and store them in the resources map.
*/
func (d *DatabaseManager) Resources(databaseId, branchId string) *DatabaseResources {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if resource, ok := d.resources[file.DatabaseHash(databaseId, branchId)]; ok {
		return resource
	}

	resource := &DatabaseResources{
		BranchId:        branchId,
		config:          d.Cluster.Config,
		DatabaseId:      databaseId,
		databaseManager: d,
		DatabaseHash:    file.DatabaseHash(databaseId, branchId),
		mutex:           &sync.RWMutex{},
		tieredFS:        d.Cluster.TieredFS(),
		tmpFS:           d.Cluster.TmpFS(),
	}

	d.resources[file.DatabaseHash(databaseId, branchId)] = resource

	return resource
}

/*
Shutdown all of the database resources that have been created.
*/
func (d *DatabaseManager) ShutdownResources() {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, resource := range d.resources {
		resource.Remove()
	}

	d.resources = map[string]*DatabaseResources{}
}
