package database

import (
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/cache"
	"github.com/litebase/litebase/pkg/cluster"
	"github.com/litebase/litebase/pkg/file"
	"github.com/litebase/litebase/pkg/storage"
)

type DatabaseManager struct {
	branchCache            *cache.LFUCache
	Cluster                *cluster.Cluster
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
		branchCache:            cache.NewLFUCache(100),
		Cluster:                cluster,
		connectionManagerMutex: &sync.Mutex{},
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

	RegisterDriver(
		fmt.Sprintf("litebase:%s", cluster.Node().ID),
		dbm.ConnectionManager(),
	)

	return dbm
}

// Return all of the databases that have been configured in the system.
func (d *DatabaseManager) All() ([]*Database, error) {
	db, err := d.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	var databases []*Database

	rows, err := db.Query(
		"SELECT * FROM databases",
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		database := &Database{}

		err := rows.Scan(
			&database.ID,
			&database.DatabaseID,
			&database.Name,
			&database.PrimaryBranchReferenceID,
			&database.Settings,
			&database.CreatedAt,
			&database.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		// // Parse timestamps if they exist
		// if database.CreatedAtRaw.Valid && database.CreatedAtRaw.String != "" {
		// 	if parsedTime, err := time.Parse(time.RFC3339, database.CreatedAtRaw.String); err == nil {
		// 		database.CreatedAt = parsedTime
		// 	}
		// }

		// if database.UpdatedAtRaw.Valid && database.UpdatedAtRaw.String != "" {
		// 	if parsedTime, err := time.Parse(time.RFC3339, database.UpdatedAtRaw.String); err == nil {
		// 		database.UpdatedAt = parsedTime
		// 	}
		// }

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
	return CreateDatabase(d, databaseName, branchName)
}

// Delete the given instance of the database.
func (d *DatabaseManager) Delete(database *Database) error {
	resources := d.Resources(database.DatabaseID, database.PrimaryBranch().DatabaseBranchID)

	// Close all database connections to the database before deleting it
	d.ConnectionManager().CloseDatabaseConnections(database.DatabaseID)

	fileSystem := resources.FileSystem()

	// Delete the database keys for all branches in the system database.
	db, err := d.SystemDatabase().DB()

	if err != nil {
		slog.Error("Error getting system database", "error", err)
		return err
	}

	// TODO: Removing all database storage may require the removal of a lot of files.
	// How is this going to work with tiered storage? We also need to test that
	// removing a database stops any opertaions to the database.
	err = fileSystem.FileSystem().RemoveAll(
		file.GetDatabaseRootDir(
			database.DatabaseID,
		),
	)

	if err != nil {
		slog.Error("Error deleting database storage", "error", err)
		return err
	}

	resources.Remove()

	_, err = db.Exec(
		"DELETE FROM databases WHERE id = ?",
		database.ID,
	)

	if err != nil {
		slog.Error("Error deleting database from system database", "error", err)
	}

	return nil
}

// Check if a database with the given name exists.
func (d *DatabaseManager) Exists(name string) (bool, error) {
	db, err := d.SystemDatabase().DB()

	if err != nil {
		return false, err
	}

	var count int64

	err = db.QueryRow(
		"SELECT COUNT(*) FROM databases WHERE name = ?",
		name,
	).Scan(&count)

	if err != nil {
		return false, fmt.Errorf("failed to check database existence: %w", err)
	}

	return count > 0, nil
}

// Get a database instance by its ID.
func (d *DatabaseManager) Get(databaseId string) (*Database, error) {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	database := &Database{}

	if databaseId == SystemDatabaseID {
		database = &TheSystemDatabase
	} else {
		db, err := d.SystemDatabase().DB()

		if err != nil {
			return nil, fmt.Errorf("failed to get system database: %w", err)
		}

		err = db.QueryRow(
			"SELECT id, database_id, name, primary_branch_reference_id, settings, created_at, updated_at FROM databases WHERE database_id = ?",
			databaseId,
		).Scan(
			&database.ID,
			&database.DatabaseID,
			&database.Name,
			&database.PrimaryBranchReferenceID,
			&database.Settings,
			&database.CreatedAt,
			&database.UpdatedAt,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to get database: %w", err)
		}
	}

	database.DatabaseManager = d

	return database, nil
}

// Get a database key by its key string.
func (d *DatabaseManager) GetKey(databaseKey string) (*Branch, error) {
	if value, ok := d.branchCache.Get(databaseKey); ok {
		if dbKey, valid := value.(*Branch); valid {
			return dbKey, nil
		}
	}

	db, err := d.SystemDatabase().DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database: %w", err)
	}

	branch := &Branch{}

	err = db.QueryRow(
		"SELECT id, database_id, database_branch_id, name, key, settings, created_at, updated_at FROM database_branches WHERE key = ?",
		databaseKey,
	).Scan(
		&branch.ID,
		&branch.DatabaseID,
		&branch.DatabaseBranchID,
		&branch.Name,
		&branch.Key,
		&branch.Settings,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get database key: %w", err)
	}

	if err := d.branchCache.Put(databaseKey, branch); err != nil {
		slog.Warn("Failed to cache database key", "error", err)
	}

	return branch, nil
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
	d.systemDatabaseMutex.Lock()
	defer d.systemDatabaseMutex.Unlock()

	if d.systemDatabase != nil {
		return d.systemDatabase
	}

	d.systemDatabase = NewSystemDatabase(d)

	return d.systemDatabase
}
