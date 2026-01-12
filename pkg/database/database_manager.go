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
	Cluster                *cluster.Cluster
	connectionManager      *ConnectionManager
	connectionManagerMutex *sync.Mutex
	databaseCache          *cache.LFUCache
	importManager          *DatabaseImportManager
	keyCache               *cache.LFUCache
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
		databaseCache:          cache.NewLFUCache(100),
		keyCache:               cache.NewLFUCache(100),
		mutex:                  &sync.Mutex{},
		resources:              make(map[string]*DatabaseResources),
		SecretsManager:         secretsManager,
		systemDatabaseMutex:    &sync.Mutex{},
		WriteQueueManager:      NewWriteQueueManager(cluster.Node().Context()),
	}

	dbm.pageLogManager = storage.NewPageLogManager(
		dbm.Cluster.Node().Context(),
		storage.WithNodePublisher(NewNodeAdapter(dbm.Cluster.Node())),
	)

	dbm.pageLogManager.SetCompactionFn(dbm.Compaction)

	RegisterDriver("litebase-internal", dbm.ConnectionManager())

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

	defer func() {
		if err := rows.Close(); err != nil {
			slog.Error("Error closing rows", "error", err)
		}
	}()

	for rows.Next() {
		database := &Database{}

		err := rows.Scan(
			&database.ID,
			&database.DatabaseID,
			&database.Name,
			&database.PrimaryBranchReferenceID,
			&database.CreatedAt,
			&database.UpdatedAt,
		)

		if err != nil {
			return nil, err
		}

		database.DatabaseManager = d
		database.branchCache = cache.NewLFUCache(100)
		databases = append(databases, database)
	}

	return databases, nil
}

// When the page log manager needs to compact the page logs, it will call this
// function. The database manager will call the compact function on each open
// database file system, while coordinating with the check pointer to ensure
// that pages are not being written to while the compaction is happening.
func (d *DatabaseManager) Compaction() {
	if !d.Cluster.Node().IsPrimary() {
		return
	}

	// Take a snapshot of resources while holding the lock, then release it
	// to avoid deadlock with other operations that need the lock
	d.mutex.Lock()

	resources := make([]*DatabaseResources, 0, len(d.resources))

	for _, resource := range d.resources {
		resources = append(resources, resource)
	}

	d.mutex.Unlock()

	// Process each resource without holding the manager mutex to avoid
	// lock inversion with CheckpointBarrier
	for _, resource := range resources {
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
					slog.Debug("Error in compaction barrier", "error", err)
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
		d.connectionManager.mutex.Lock()
		d.connectionManager.connectionTicker = time.NewTicker(1 * time.Second)
		d.connectionManager.mutex.Unlock()

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
	db, err := CreateDatabase(d, databaseName, branchName)

	if err != nil {
		slog.Error("Error creating database", "error", err, "name", databaseName, "branch", branchName)
		return nil, err
	}

	if err := d.databaseCache.Put(db.DatabaseID, db); err != nil {
		slog.Warn("Failed to cache database by ID", "error", err, "id", db.DatabaseID)
	}

	if err := d.keyCache.Put(db.Name, db.DatabaseID); err != nil {
		slog.Warn("Failed to cache database by name", "error", err, "name", db.Name)
	}

	return db, nil
}

// Delete the given instance of the database.
func (d *DatabaseManager) Delete(database *Database) error {
	primaryBranch, err := database.PrimaryBranch()

	if err != nil {
		return fmt.Errorf("cannot delete database: %w", err)
	}

	if primaryBranch == nil {
		return fmt.Errorf("cannot delete database: primary branch not found")
	}

	resources := d.Resources(primaryBranch)

	// Close all database connections to the database before deleting it
	d.ConnectionManager().CloseDatabaseConnections(database.DatabaseID)

	fileSystem := resources.FileSystem()

	// Delete from the system database
	db, err := d.SystemDatabase().DB()

	if err != nil {
		slog.Error("Error getting system database", "error", err)
		return err
	}

	_, err = db.Exec(
		"DELETE FROM databases WHERE id = ?",
		database.ID,
	)

	if err != nil {
		slog.Error("Error deleting database from system database", "error", err)
	}

	d.databaseCache.Delete(database.DatabaseID)
	d.keyCache.Delete(database.Name)

	// TODO: Removing all database storage may require the removal of a lot of files.
	// How is this going to work with tiered storage? We also need to test that
	// removing a database stops any operations to the database.
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
func (d *DatabaseManager) Get(databaseID string) (*Database, error) {
	if databaseID == SystemDatabaseID {
		d.mutex.Lock()
		defer d.mutex.Unlock()

		database := &TheSystemDatabase
		database.DatabaseManager = d

		if database.branchCache == nil {
			database.branchCache = cache.NewLFUCache(100)
		}

		return database, nil
	}

	// Check the database cache first
	if database, found := d.databaseCache.Get(databaseID); found {
		return database.(*Database), nil
	}

	// Get system database without holding the main mutex to avoid deadlock
	systemDB := d.SystemDatabase()
	db, err := systemDB.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database: %w", err)
	}

	database := NewDatabase(d, "")

	err = db.QueryRow(
		"SELECT id, database_id, name, primary_branch_reference_id, created_at, updated_at FROM databases WHERE database_id = ?",
		databaseID,
	).Scan(
		&database.ID,
		&database.DatabaseID,
		&database.Name,
		&database.PrimaryBranchReferenceID,
		&database.CreatedAt,
		&database.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	database.DatabaseManager = d
	database.branchCache = cache.NewLFUCache(100)
	d.mutex.Unlock()

	err = d.databaseCache.Put(database.DatabaseID, database)

	if err != nil {
		slog.Warn("Failed to cache database", "error", err, "databaseID", database.DatabaseID)
	}

	err = d.keyCache.Put(database.Name, database.DatabaseID)

	if err != nil {
		slog.Warn("Failed to cache database by name", "error", err, "name", database.Name)
	}

	return database, nil
}

// Get a database branch by the database and brancd ids.
func (d *DatabaseManager) GetBranch(databaseId, branchId string) (*Branch, error) {
	database, err := d.Get(databaseId)

	if err != nil {
		return nil, err
	}

	if databaseId == SystemDatabaseID {
		return &Branch{
			DatabaseBranchID: SystemDatabaseBranchID,
			DatabaseID:       SystemDatabaseID,
			Name:             "system",
			DatabaseManager:  d,
		}, nil
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()

	branch, err := database.BranchByID(branchId)

	if err != nil {
		return nil, err
	}

	return branch, nil
}

func (d *DatabaseManager) GetByName(name string) (*Database, error) {
	// Check the database cache first
	if databaseID, found := d.keyCache.Get(name); found {
		if database, found := d.databaseCache.Get(databaseID.(string)); found {
			return database.(*Database), nil
		}
	}

	// Get system database without holding the main mutex to avoid deadlock
	systemDB := d.SystemDatabase()
	db, err := systemDB.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database: %w", err)
	}

	var databaseID string

	err = db.QueryRow(
		"SELECT database_id FROM databases WHERE name = ?",
		name,
	).Scan(&databaseID)

	if err != nil {
		return nil, err
	}

	database := NewDatabase(d, "")

	err = db.QueryRow(
		"SELECT id, database_id, name, primary_branch_reference_id, created_at, updated_at FROM databases WHERE database_id = ?",
		databaseID,
	).Scan(
		&database.ID,
		&database.DatabaseID,
		&database.Name,
		&database.PrimaryBranchReferenceID,
		&database.CreatedAt,
		&database.UpdatedAt,
	)

	if err != nil {
		return nil, err
	}

	d.mutex.Lock()
	database.DatabaseManager = d
	database.branchCache = cache.NewLFUCache(100)
	d.mutex.Unlock()

	err = d.databaseCache.Put(database.DatabaseID, database)

	if err != nil {
		slog.Warn("Failed to cache database", "error", err, "databaseID", database.DatabaseID)
	}

	err = d.keyCache.Put(name, database.DatabaseID)

	if err != nil {
		slog.Warn("Failed to cache database by name", "error", err, "name", name)
	}

	return database, nil
}

// Return the database import manager instance. If it has not been created
// yet, create it and store it in the database manager.
func (d *DatabaseManager) ImportManager() *DatabaseImportManager {
	if d.importManager != nil {
		return d.importManager
	}

	return NewDatabaseImportManager(d)
}

// Return the page log manager instance.
func (d *DatabaseManager) PageLogManager() *storage.PageLogManager {
	return d.pageLogManager
}

// Get the resources for the given database and branch UUIDs. If the resources
// have not been created, create them and store them in the resources map.
func (d *DatabaseManager) Resources(branch *Branch) *DatabaseResources {
	d.mutex.Lock()
	defer d.mutex.Unlock()

	hash := file.DatabaseHash(branch.DatabaseID, branch.DatabaseBranchID)

	if resource, ok := d.resources[hash]; ok {
		return resource
	}

	resource := &DatabaseResources{
		Branch:          branch,
		config:          d.Cluster.Config,
		databaseManager: d,
		DatabaseHash:    file.DatabaseHash(branch.DatabaseID, branch.DatabaseBranchID),
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
