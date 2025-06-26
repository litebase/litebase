package database

import (
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"

	"github.com/google/uuid"
)

type DatabaseManager struct {
	Cluster                *cluster.Cluster
	databases              map[string]*Database
	connectionManager      *ConnectionManager
	connectionManagerMutex *sync.Mutex
	mutex                  *sync.Mutex
	pageLogManager         *storage.PageLogManager
	resources              map[string]*DatabaseResources
	SecretsManager         *auth.SecretsManager
	systemDatabase         *SystemDatabase
	systemDatabaseMutex    *sync.Mutex
	WriteQueueManager      *WriteQueueManager
}

// Create a new instance of the database manager.
func NewDatabaseManager(
	cluster *cluster.Cluster,
	secretsManager *auth.SecretsManager,
) *DatabaseManager {
	dbm := &DatabaseManager{
		Cluster:                cluster,
		connectionManagerMutex: &sync.Mutex{},
		databases:              make(map[string]*Database),
		mutex:                  &sync.Mutex{},
		resources:              make(map[string]*DatabaseResources),
		SecretsManager:         secretsManager,
		systemDatabaseMutex:    &sync.Mutex{},
		WriteQueueManager:      NewWriteQueueManager(cluster.Node().Context()),
	}

	dbm.pageLogManager = storage.NewPageLogManager(
		dbm.Cluster.Node().Context(),
	)

	dbm.pageLogManager.SetCompactionFn(dbm.compaction)

	return dbm
}

// Return all of the databases that have been configured in the system.
func (d *DatabaseManager) All() ([]*Database, error) {
	var databases []*Database

	// Read all files in the databases directory
tryRead:
	entries, err := d.Cluster.ObjectFS().ReadDir(Directory())

	if err != nil {
		if os.IsNotExist(err) {
			err := d.Cluster.ObjectFS().MkdirAll(Directory(), 0750)

			if err != nil {
				return nil, err
			}

			goto tryRead
		}

		return nil, err
	}

	// We need the database name, we need the id, we do not want to open each file (thinking)
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

// When the page log manager needs to compact the page logs, it will call this
// function. The database manager will call the compact function on each open
// database file system, while coordinating with the check pointer to ensure
// that pages are not being written to while the compaction is happening.
func (d *DatabaseManager) compaction() {
	for _, resource := range d.resources {
		walmanager, err := resource.DatabaseWALManager()

		if err != nil {
			log.Println("Error getting WAL manager:", err)
			continue
		}

		err = walmanager.CheckpointBarrier(func() error {
			checkpointer, err := resource.Checkpointer()

			if err != nil {
				log.Println("Error getting checkpointer:", err)
				return err
			}

			checkpointer.WithLock(func() {
				err := resource.FileSystem().Compact()

				if err != nil {
					slog.Error("Error compacting file system", "error", err)
				}
			})

			return nil
		})

		if err != nil {
			log.Println("Error creating checkpoint barrier:", err)
			continue
		}
	}
}

// Build the connection manager instance if it has not been created yet.
func (d *DatabaseManager) ConnectionManager() *ConnectionManager {
	d.connectionManagerMutex.Lock()
	defer d.connectionManagerMutex.Unlock()

	if d.connectionManager != nil {
		return d.connectionManager
	}

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

	return d.connectionManager
}

// Create a new instance of a database.
func (d *DatabaseManager) Create(databaseName, branchName string) (*Database, error) {
	dks, err := d.SecretsManager.DatabaseKeyStore(d.Cluster.Config.EncryptionKey)

	if err != nil {
		return nil, fmt.Errorf("failed to get database key store: %w", err)
	}

	branch, err := NewBranch(d.Cluster.Config, dks, branchName, true)

	if err != nil {
		return nil, fmt.Errorf("failed to create branch: %w", err)
	}

	// systemDB := d.SystemDatabase()

	// systemDB.Exec(
	// 	"INSERT INTO databases (name)"
	// )

	database := NewDatabase(d)
	database.DatabaseID = uuid.New().String()
	database.Name = databaseName

	database.PrimaryBranchID = branch.ID
	database.PrimaryBranchName = branchName
	database.Settings = DatabaseSettings{
		Backups: DatabaseBackupSettings{
			Enabled: true,
			IncrementalBackups: DatabaseIncrementalBackupSettings{
				Enabled: true,
			},
		},
	}

	database.CreatedAt = time.Now().UTC()
	database.UpdatedAt = time.Now().UTC()

	err = database.Save()

	if err != nil {
		return nil, err
	}

	// Create the initial branch
	branch.DatabaseID = database.ID

	err = branch.Save()

	if err != nil {
		return nil, err
	}

	// Update the database with the branch
	database.PrimaryBranchID = branch.ID

	err = database.Save()

	if err != nil {
		return nil, err
	}

	return database, nil
}

// Delete the given instance of the database.
func (d *DatabaseManager) Delete(database *Database) error {
	resources := d.Resources(database.DatabaseID, database.PrimaryBranch().BranchID)

	d.ConnectionManager().CloseDatabaseConnections(database.DatabaseID)

	fileSystem := resources.FileSystem()

	// Delete the database keys
	for _, branch := range database.Branches {
		err := d.SecretsManager.DeleteDatabaseKey(
			database.Key(branch.BranchID),
		)

		if err != nil {
			slog.Error("Error deleting database key", "error", err)
		}
	}

	// TODO: Removing all database storage may require the removal of a lot of files.
	// How is this going to work with tiered storage? We also need to test that
	// removing a database stops any opertaions to the database.
	err := fileSystem.FileSystem().RemoveAll(
		file.GetDatabaseRootDir(
			database.DatabaseID,
		),
	)

	if err != nil {
		log.Println("Error deleting database storage", err)
		return err
	}

	resources.Remove()

	delete(d.databases, database.DatabaseID)

	return nil
}

// Check if a database with the given name exists.
func (d *DatabaseManager) Exists(name string) (bool, error) {
	databases, err := d.All()

	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}

		return false, fmt.Errorf("failed to list databases: %w", err)
	}

	for _, database := range databases {
		if database.Name == name {
			return true, nil
		}
	}

	return false, nil
}

// Get a database instance by its ID.
func (d *DatabaseManager) Get(databaseId string) (*Database, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	if d.databases[databaseId] != nil {
		return d.databases[databaseId], nil
	}
	database := &Database{}

	if databaseId == SystemDatabaseID {
		database = &TheSystemDatabase
	} else {
		path := fmt.Sprintf("%s%s/settings.json", file.DatabaseDirectory(), databaseId)

		file, err := d.Cluster.ObjectFS().Open(path)

		if err != nil {
			return nil, fmt.Errorf("database '%s' has not been configured", databaseId)
		}

		defer file.Close()

		err = json.NewDecoder(file).Decode(database)

		if err != nil {
			return nil, err
		}
	}

	database.DatabaseManager = d
	d.databases[databaseId] = database

	return database, nil
}

// Return the page log manager instance.
func (d *DatabaseManager) PageLogManager() *storage.PageLogManager {
	return d.pageLogManager
}

// Get the resources for the given database and branch UUIDs. If the resources
// have not been created, create them and store them in the resources map.
func (d *DatabaseManager) Resources(databaseId, branchId string) *DatabaseResources {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseId, branchId)

	if resource, ok := d.resources[hash]; ok {
		return resource
	}

	resource := &DatabaseResources{
		BranchID:        branchId,
		config:          d.Cluster.Config,
		DatabaseID:      databaseId,
		databaseManager: d,
		DatabaseHash:    file.DatabaseHash(databaseId, branchId),
		mutex:           &sync.Mutex{},
		tieredFS:        d.Cluster.TieredFS(),
		tmpFS:           d.Cluster.TmpFS(),
	}

	d.resources[hash] = resource

	return d.resources[hash]
}

// Remove the resources for the given database from a running state.
func (d *DatabaseManager) Remove(databaseId, branchId string) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(databaseId, branchId)

	if resource, ok := d.resources[hash]; ok {
		resource.Remove()
		delete(d.resources, hash)
	}
}

// Shutdown all of the database resources that have been created.
func (d *DatabaseManager) ShutdownResources() error {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	for _, resource := range d.resources {
		resource.Remove()
	}

	d.resources = map[string]*DatabaseResources{}

	return nil
}

// Return the system database instance. If it has not been created yet, create it.
func (d *DatabaseManager) SystemDatabase() *SystemDatabase {
	// d.mutex.Lock()
	// defer d.mutex.Unlock()

	if d.systemDatabase != nil {
		return d.systemDatabase
	}

	d.systemDatabase = NewSystemDatabase(d)

	return d.systemDatabase
}
