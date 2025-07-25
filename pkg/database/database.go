package database

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/cache"
)

type Database struct {
	ID                       int64             `json:"-"`
	DatabaseManager          *DatabaseManager  `json:"-"`
	Name                     string            `json:"name"`
	DatabaseID               string            `json:"database_id"`
	PrimaryBranchReferenceID sql.NullInt64     `json:"-"`
	Settings                 *DatabaseSettings `json:"settings"`
	CreatedAt                time.Time         `json:"created_at"`
	UpdatedAt                time.Time         `json:"updated_at"`

	exists        bool
	primaryBranch *Branch
	branchCache   *cache.LFUCache
	cacheMutex    sync.Mutex
}

func NewDatabase(databaseManager *DatabaseManager, databaseName string) *Database {
	return &Database{
		DatabaseID:      uuid.New().String(),
		DatabaseManager: databaseManager,
		Name:            databaseName,
		branchCache:     cache.NewLFUCache(100),
	}
}

func CreateDatabase(databaseManager *DatabaseManager, databaseName string, branchName string) (*Database, error) {
	database := NewDatabase(databaseManager, databaseName)

	database.Settings = &DatabaseSettings{
		Backups: DatabaseBackupSettings{
			Enabled: true,
			IncrementalBackups: DatabaseIncrementalBackupSettings{
				Enabled: true,
			},
		},
	}

	database.CreatedAt = time.Now().UTC()
	database.UpdatedAt = time.Now().UTC()

	err := database.Save()

	if err != nil {
		return nil, err
	}

	// Create the initial branch
	branch, err := database.CreateBranch(branchName, "")

	if err != nil {
		return nil, fmt.Errorf("failed to create branch: %w", err)
	}

	// Update the database with the branch
	database.PrimaryBranchReferenceID = sql.NullInt64{Int64: branch.ID, Valid: true}

	err = database.Save()

	if err != nil {
		return nil, err
	}

	return database, nil
}

// Insert a new database into the system database.
func InsertDatabase(database *Database) error {
	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	var primaryBranchId sql.NullInt64
	if database.PrimaryBranchReferenceID.Valid {
		primaryBranchId = database.PrimaryBranchReferenceID
	}

	result, err := db.Exec(
		`INSERT INTO databases (
			database_id,
			primary_branch_reference_id, 
			name,
			settings,
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?)
		`,
		database.DatabaseID,
		primaryBranchId,
		database.Name,
		database.Settings,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	database.ID = id
	database.exists = true

	return nil
}

// Update an existing database in the system database.
func UpdateDatabase(database *Database) error {
	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	settingsJson, err := json.Marshal(database.Settings)

	if err != nil {
		return err
	}

	var primaryBranchId sql.NullInt64

	if database.PrimaryBranchReferenceID.Valid {
		primaryBranchId = database.PrimaryBranchReferenceID
	}

	_, err = db.Exec(
		`UPDATE databases 
		SET 
			name = ?,
			primary_branch_reference_id = ?,
			settings = ?,
			updated_at = ?
		WHERE database_id = ?
		`,
		database.Name,
		primaryBranchId,
		string(settingsJson),
		time.Now().UTC(),
		database.DatabaseID,
	)

	if err != nil {
		return err
	}

	// Update the cached version's primary branch reference ID to ensure consistency
	// This is crucial for the PrimaryBranch() method to work correctly
	if cachedDb, found := database.DatabaseManager.databaseCache.Get(database.DatabaseID); found {
		cachedDatabase := cachedDb.(*Database)
		cachedDatabase.PrimaryBranchReferenceID = database.PrimaryBranchReferenceID

		// Clear the cached primary branch since the reference ID might have changed
		cachedDatabase.primaryBranch = nil
	}

	return nil
}

// Get a database branch by its ID.
func (database *Database) Branch(branchID string) (*Branch, error) {
	var branch Branch

	db, err := database.DatabaseManager.SystemDatabase().DB()
	if err != nil {
		return nil, err
	}

	err = db.QueryRow(
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, key, settings, created_at, updated_at 
		FROM database_branches 
		WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchID,
	).Scan(
		&branch.ID,
		&branch.DatabaseReferenceID,
		&branch.ParentDatabaseBranchReferenceID,
		&branch.DatabaseID,
		&branch.DatabaseBranchID,
		&branch.Name,
		&branch.Key,
		&branch.Settings,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("branch with ID %s not found", branchID)
		}
		return nil, fmt.Errorf("failed to query branch: %w", err)
	}

	return &branch, nil
}

// Retrieve all branches of the database.
func (database *Database) Branches() ([]*Branch, error) {
	var branches []*Branch

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	rows, err := db.Query(
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, key, settings, created_at, updated_at FROM database_branches
		WHERE database_reference_id = ?`,
		database.ID,
	)

	if err != nil {
		return nil, err
	}

	defer rows.Close()

	for rows.Next() {
		var branch Branch

		if err := rows.Scan(&branch.ID, &branch.DatabaseReferenceID, &branch.ParentDatabaseBranchReferenceID, &branch.DatabaseID, &branch.DatabaseBranchID, &branch.Name, &branch.Key, &branch.Settings, &branch.CreatedAt, &branch.UpdatedAt); err != nil {
			continue
		}

		branches = append(branches, &branch)
	}

	return branches, nil
}

// Copy the parent branch data to the new branch.
func (database *Database) copyBranchParentData(branch *Branch) error {
	parentBranchResources := database.DatabaseManager.Resources(
		database.DatabaseID,
		branch.ParentBranch().DatabaseBranchID,
	)

	parentDFS := parentBranchResources.FileSystem()

	branchDFS := database.DatabaseManager.Resources(
		database.DatabaseID,
		branch.DatabaseBranchID,
	).FileSystem()

	snapshotLogger := parentBranchResources.SnapshotLogger()
	checkpointer, err := parentBranchResources.Checkpointer()

	if err != nil {
		return fmt.Errorf("failed to get checkpointer: %w", err)
	}

	// Get the snapshots
	snapshotLogger.GetSnapshots()

	// Get the latest snapshot timestamp
	snapshotKeys := snapshotLogger.Keys()

	// Esnure there is a snapshot to restore from
	if len(snapshotKeys) > 0 {
		snapshot, err := snapshotLogger.GetSnapshot(snapshotKeys[len(snapshotKeys)-1])

		if err != nil {
			return fmt.Errorf("failed to get snapshot: %w", err)
		}

		return backups.RestoreFromTimestamp(
			database.DatabaseManager.Cluster.Config,
			database.DatabaseManager.Cluster.TieredFS(),
			database.DatabaseID,
			branch.ParentBranch().DatabaseBranchID,
			database.DatabaseID,
			branch.DatabaseBranchID,
			snapshot.RestorePoints.End,
			snapshotLogger,
			parentDFS,
			branchDFS,
			checkpointer,
			nil,
		)
	}

	return nil
}

// Create a new branch for the database.
func (database *Database) CreateBranch(name, parentBranchName string) (*Branch, error) {
	branch, err := NewBranch(database.DatabaseManager, database.ID, parentBranchName, name)

	if err != nil {
		return nil, fmt.Errorf("failed to create branch: %w", err)
	}

	branch.DatabaseID = database.DatabaseID

	err = branch.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save branch: %w", err)
	}

	// Update cache to reflect the new branch exists
	database.UpdateBranchCache(branch.DatabaseBranchID, true)

	// Copy the data from the parent branch if specified
	if parentBranchName != "" && branch.ParentBranch() != nil {
		err = database.copyBranchParentData(branch)

		if err != nil {
			return nil, fmt.Errorf("failed to copy parent branch data: %w", err)
		}
	}

	return branch, nil
}

// Check if a branch exists for the database.
func (database *Database) HasBranch(branchID string) bool {
	if database.DatabaseID == SystemDatabaseID && branchID == SystemDatabaseBranchID {
		return true
	}

	if found, exists := database.branchCache.Get(branchID); exists {
		return found.(bool)
	}

	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Error checking branch existence", "error", err)
		return false
	}

	var id int64

	err = db.QueryRow(
		`SELECT id FROM database_branches WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchID,
	).Scan(&id)

	exists := err == nil

	// Cache the result
	if err := database.branchCache.Put(branchID, exists); err != nil {
		slog.Warn("Failed to cache branch existence", "error", err)
	}

	if err != nil && err != sql.ErrNoRows {
		slog.Error("Error checking branch existence", "error", err)
	}

	return exists
}

// InvalidateBranchCache removes a branch from the cache
func (database *Database) InvalidateBranchCache(branchID string) {
	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	database.branchCache.Delete(branchID)
}

// Get the key for a branch of the database.
func (database *Database) Key(branchID string) string {
	var key string

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return ""
	}

	err = db.QueryRow(
		`SELECT key FROM database_branches WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchID,
	).Scan(&key)

	if err != nil {
		return ""
	}

	return key
}

// MarshalJSON customizes the JSON representation of the Database struct.
// It includes the URL for the primary branch.
func (database *Database) MarshalJSON() ([]byte, error) {
	type Alias Database

	primaryBranch := database.PrimaryBranch()

	if primaryBranch == nil {
		return nil, errors.New("primary branch not found")
	}

	return json.Marshal(&struct {
		*Alias
		Url string `json:"url"`
	}{
		Alias: (*Alias)(database),
		Url:   database.Url(primaryBranch.DatabaseBranchID),
	})
}

// Load and return the primary branch of the database
func (database *Database) PrimaryBranch() *Branch {
	if database == nil {
		return nil
	}

	if database.primaryBranch == nil {
		// If no primary branch ID is set, return nil
		if !database.PrimaryBranchReferenceID.Valid || database.PrimaryBranchReferenceID.Int64 == 0 {
			return nil
		}

		// Load the primary branch from the system database using the foreign key
		if database.DatabaseManager != nil {
			db, err := database.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				return nil
			}

			var branch Branch

			err = db.QueryRow(
				`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, key, settings, created_at, updated_at FROM database_branches WHERE id = ?`,
				database.PrimaryBranchReferenceID.Int64,
			).Scan(
				&branch.ID,
				&branch.DatabaseReferenceID,
				&branch.ParentDatabaseBranchReferenceID,
				&branch.DatabaseID,
				&branch.DatabaseBranchID,
				&branch.Name,
				&branch.Key,
				&branch.Settings,
				&branch.CreatedAt,
				&branch.UpdatedAt,
			)

			if err == nil {
				branch.DatabaseManager = database.DatabaseManager
				branch.Exists = true
				database.primaryBranch = &branch
			} else {
				log.Println("Error loading primary branch:", err)
			}
		}
	}

	return database.primaryBranch
}

// Save the database to the system database
func (database *Database) Save() error {
	if database.exists {
		return UpdateDatabase(database)
	} else {
		return InsertDatabase(database)
	}
}

// UpdateBranchCache updates the cache with branch existence information
func (database *Database) UpdateBranchCache(branchID string, exists bool) {
	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	if err := database.branchCache.Put(branchID, exists); err != nil {
		slog.Warn("Failed to update branch cache", "error", err)
	}
}

func (database *Database) Url(branchID string) string {
	protocol := "http://"
	port := ""

	if database.DatabaseManager.Cluster.Config.Port != "80" {
		port = fmt.Sprintf(":%s", database.DatabaseManager.Cluster.Config.Port)
	} else {
		protocol = "https://"
	}

	return fmt.Sprintf(
		"%s%s%s/%s",
		protocol,
		database.DatabaseManager.Cluster.Config.HostName,
		port,
		database.Key(branchID),
	)
}
