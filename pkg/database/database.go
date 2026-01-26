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
	"github.com/litebase/litebase/pkg/memory"
)

type Database struct {
	ID                       int64            `json:"-"`
	DatabaseManager          *DatabaseManager `json:"-"`
	Name                     string           `json:"name"`
	DatabaseID               string           `json:"databaseId"`
	PrimaryBranchReferenceID sql.NullInt64    `json:"-"`
	CreatedAt                time.Time        `json:"createdAt"`
	UpdatedAt                time.Time        `json:"updatedAt"`
	exists                   bool
	primaryBranch            *Branch
	branchCache              *memory.ManagedCache
	cacheMutex               sync.Mutex
}

func NewDatabase(databaseManager *DatabaseManager, databaseName string) *Database {
	return &Database{
		DatabaseID:      uuid.New().String(),
		DatabaseManager: databaseManager,
		Name:            databaseName,
		branchCache: memory.NewManagedCache(memory.ManagedCacheConfig{
			Capacity:    BranchCacheCapacity,
			Manager:     databaseManager.Cluster.MemoryManager,
			DefaultSize: BranchCacheDefaultSize,
			Owner:       fmt.Sprintf("branch-cache-%s", databaseName),
		}),
	}
}

func CreateDatabase(databaseManager *DatabaseManager, databaseName string, branchName string) (*Database, error) {
	database := NewDatabase(databaseManager, databaseName)
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
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?)
		`,
		database.DatabaseID,
		primaryBranchId,
		database.Name,
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

	var primaryBranchId sql.NullInt64

	if database.PrimaryBranchReferenceID.Valid {
		primaryBranchId = database.PrimaryBranchReferenceID
	}

	updatedAt := time.Now().UTC()

	// Update the database record
	_, err = db.Exec(
		`UPDATE databases 
		SET 
			name = ?,
			primary_branch_reference_id = ?,
			updated_at = ?
		WHERE database_id = ?
		`,
		database.Name,
		primaryBranchId,
		updatedAt,
		database.DatabaseID,
	)

	if err != nil {
		return err
	}

	// Update the cached version's primary branch reference ID to ensure consistency
	// This is crucial for the PrimaryBranch() method to work correctly
	if cachedDb, found := database.DatabaseManager.databaseCache.Get(database.DatabaseID); found {
		cachedDatabase := cachedDb.(*Database)

		cachedDatabase.Name = database.Name
		cachedDatabase.PrimaryBranchReferenceID = database.PrimaryBranchReferenceID
		cachedDatabase.UpdatedAt = updatedAt
		cachedDatabase.exists = true

		// Clear the cached primary branch since the reference ID might have changed
		cachedDatabase.primaryBranch = nil
	}

	return nil
}

// Get a database branch by its name.
func (database *Database) Branch(name string) (*Branch, error) {
	var branch Branch

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	err = db.QueryRow(
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at 
		FROM database_branches
		WHERE database_reference_id = ? AND name = ?`,
		database.ID,
		name,
	).Scan(
		&branch.ID,
		&branch.DatabaseReferenceID,
		&branch.ParentDatabaseBranchReferenceID,
		&branch.DatabaseID,
		&branch.DatabaseBranchID,
		&branch.Name,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}

		return nil, fmt.Errorf("failed to query branch: %w", err)
	}

	// Now that we have DatabaseBranchID, check if there's a cached version
	// This is critical: if UpdateBranchSettings was called, the cache has the latest settings
	if cached, exists := database.branchCache.Get(branch.DatabaseBranchID); exists {
		return cached.(*Branch), nil
	}

	branch.Exists = true
	branch.DatabaseManager = database.DatabaseManager

	if branch.Settings == nil {
		branch.Settings, err = branch.GetBranchSettings()

		if err != nil {
			return nil, fmt.Errorf("failed to load branch settings: %w", err)
		}
	}

	// Cache the branch object using DatabaseBranchID as key
	if err := database.branchCache.Put(branch.DatabaseBranchID, &branch); err != nil {
		slog.Warn("Failed to cache branch", "error", err)
	}

	return &branch, nil
}

// Retrieve a branch by its database_branch_id
func (database *Database) BranchByID(branchID string) (*Branch, error) {
	var branch Branch

	// Check cache first using DatabaseBranchID as key
	if cached, exists := database.branchCache.Get(branchID); exists {
		return cached.(*Branch), nil
	}

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	err = db.QueryRow(
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at 
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
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, err
		}

		return nil, fmt.Errorf("failed to query branch: %w", err)
	}

	branch.Exists = true
	branch.DatabaseManager = database.DatabaseManager

	if branch.Settings == nil {
		branch.Settings, err = branch.GetBranchSettings()

		if err != nil {
			return nil, fmt.Errorf("failed to load branch settings: %w", err)
		}
	}

	// Cache the branch object using DatabaseBranchID as key
	if err := database.branchCache.Put(branch.DatabaseBranchID, &branch); err != nil {
		slog.Warn("Failed to cache branch", "error", err)
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
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at FROM database_branches
		WHERE database_reference_id = ?`,
		database.ID,
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
		var branch Branch

		if err := rows.Scan(&branch.ID, &branch.DatabaseReferenceID, &branch.ParentDatabaseBranchReferenceID, &branch.DatabaseID, &branch.DatabaseBranchID, &branch.Name, &branch.CreatedAt, &branch.UpdatedAt); err != nil {
			continue
		}

		branch.Exists = true
		branch.DatabaseManager = database.DatabaseManager

		branches = append(branches, &branch)
	}

	return branches, nil
}

// Copy the parent branch data to the new branch.
func (database *Database) copyBranchParentData(branch *Branch) error {
	parentBranchResources := database.DatabaseManager.Resources(branch.ParentBranch())
	parentDFS := parentBranchResources.FileSystem()
	branchDFS := database.DatabaseManager.Resources(branch).FileSystem()
	snapshotLogger := parentBranchResources.SnapshotLogger()
	checkpointer, err := parentBranchResources.Checkpointer()

	if err != nil {
		return fmt.Errorf("failed to get checkpointer: %w", err)
	}

	// Force a checkpoint on the parent branch to move WAL pages to PageLogger
	// Then compact PageLogger to move pages into Range files
	// This is critical for encrypted databases because PageLog files use path-dependent IVs
	// and cannot be copied between branches (but Range files can be)
	parentDB, err := database.DatabaseManager.ConnectionManager().Get(database.DatabaseID, branch.ParentBranch().DatabaseBranchID)

	if err != nil {
		return fmt.Errorf("failed to get parent connection for checkpoint: %w", err)
	}

	err = parentDB.GetConnection().Checkpoint()

	if err != nil {
		database.DatabaseManager.ConnectionManager().Release(parentDB)
		return fmt.Errorf("failed to checkpoint parent branch before copy: %w", err)
	}

	database.DatabaseManager.ConnectionManager().Release(parentDB)

	// Compact PageLogger to move all pages into Range files
	err = parentDFS.ForceCompact()

	if err != nil {
		return fmt.Errorf("failed to compact parent branch PageLogger before copy: %w", err)
	}

	// Refresh snapshots after checkpoint and compact
	_, err = snapshotLogger.GetSnapshots()

	if err != nil {
		return fmt.Errorf("failed to get snapshots after checkpoint: %w", err)
	}

	// Get the latest snapshot timestamp
	snapshotKeys := snapshotLogger.Keys()

	// Ensure there is a snapshot to restore from
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
		return nil, err
	}

	branch.DatabaseID = database.DatabaseID

	err = branch.Save()

	if err != nil {
		return nil, fmt.Errorf("failed to save branch: %w", err)
	}

	branch.Settings, err = branch.GetBranchSettings()

	if err != nil {
		return nil, fmt.Errorf("failed to load branch settings: %w", err)
	}

	// Cache the newly created branch
	if err := database.branchCache.Put(branch.DatabaseBranchID, branch); err != nil {
		slog.Warn("Failed to cache new branch", "error", err)
	}

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
func (database *Database) HasBranch(branchId string) bool {
	if database.DatabaseID == SystemDatabaseID && branchId == SystemDatabaseBranchID {
		return true
	}

	if _, exists := database.branchCache.Get(branchId); exists {
		return true
	}

	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	db, err := database.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		slog.Error("Error checking branch existence", "error", err)

		return false
	}

	var branch Branch

	err = db.QueryRow(
		`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at 
		FROM database_branches
		WHERE database_reference_id = ? AND database_branch_id = ?`,
		database.ID,
		branchId,
	).Scan(
		&branch.ID,
		&branch.DatabaseReferenceID,
		&branch.ParentDatabaseBranchReferenceID,
		&branch.DatabaseID,
		&branch.DatabaseBranchID,
		&branch.Name,
		&branch.CreatedAt,
		&branch.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}

		slog.Error("Error checking branch existence", "error", err)
		return false
	}

	// Set required fields
	branch.Exists = true
	branch.DatabaseManager = database.DatabaseManager

	// Load branch settings
	if branch.Settings == nil {
		branch.Settings, err = branch.GetBranchSettings()

		if err != nil {
			slog.Error("Error loading branch settings", "error", err)
			return false
		}
	}

	// Cache the branch object
	if err := database.branchCache.Put(branchId, &branch); err != nil {
		slog.Warn("Failed to cache branch", "error", err)
	}

	return true
}

// InvalidateBranchCache removes a branch from the cache
func (database *Database) InvalidateBranchCache(branchId string) {
	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	database.branchCache.Delete(branchId)
}

// MarshalJSON customizes the JSON representation of the Database struct.
// It includes the URL for the primary branch.
func (database *Database) MarshalJSON() ([]byte, error) {
	type Alias Database

	primaryBranch, err := database.PrimaryBranch()

	if err != nil {
		return nil, err
	}

	if primaryBranch == nil {
		return nil, errors.New("primary branch not found")
	}

	return json.Marshal(&struct {
		*Alias
		Url string `json:"url"`
	}{
		Alias: (*Alias)(database),
		Url:   database.Url(primaryBranch.Name),
	})
}

// Load and return the primary branch of the database
func (database *Database) PrimaryBranch() (*Branch, error) {
	if database == nil {
		return nil, errors.New("database is nil")
	}

	if database.primaryBranch == nil {
		// If no primary branch ID is set, return nil
		if !database.PrimaryBranchReferenceID.Valid || database.PrimaryBranchReferenceID.Int64 == 0 {
			return nil, errors.New("primary branch not set")
		}

		// Load the primary branch from the system database using the foreign key
		if database.DatabaseManager != nil {
			db, err := database.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				return nil, err
			}

			var branch Branch

			err = db.QueryRow(
				`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at FROM database_branches WHERE id = ?`,
				database.PrimaryBranchReferenceID.Int64,
			).Scan(
				&branch.ID,
				&branch.DatabaseReferenceID,
				&branch.ParentDatabaseBranchReferenceID,
				&branch.DatabaseID,
				&branch.DatabaseBranchID,
				&branch.Name,
				&branch.CreatedAt,
				&branch.UpdatedAt,
			)

			if err == nil {
				branch.DatabaseManager = database.DatabaseManager
				branch.Exists = true
				database.primaryBranch = &branch

				if database.primaryBranch.Settings == nil {
					database.primaryBranch.Settings, err = database.primaryBranch.GetBranchSettings()

					if err != nil {
						return nil, fmt.Errorf("failed to load primary branch settings: %w", err)
					}
				}

				// Cache the primary branch in branchCache so BranchByID can find it
				if err := database.branchCache.Put(branch.DatabaseBranchID, database.primaryBranch); err != nil {
					slog.Warn("Failed to cache primary branch", "error", err)
				}
			} else {
				log.Println("Error loading primary branch:", err)
			}
		}
	}

	return database.primaryBranch, nil
}

// Save the database to the system database
func (database *Database) Save() error {
	if database.exists {
		return UpdateDatabase(database)
	} else {
		return InsertDatabase(database)
	}
}

// UpdateBranchCache updates the cache with branch information
func (database *Database) UpdateBranchCache(branchID string, branch *Branch) {
	database.cacheMutex.Lock()
	defer database.cacheMutex.Unlock()

	if _, exists := database.branchCache.Get(branchID); !exists {
		return
	}

	if err := database.branchCache.Put(branchID, branch); err != nil {
		slog.Warn("Failed to update branch cache", "error", err)
	}
}

func (database *Database) Url(branchName string) string {
	protocol := "http://"
	port := ""

	if database.DatabaseManager.Cluster.Config.Port != "80" {
		port = fmt.Sprintf(":%s", database.DatabaseManager.Cluster.Config.Port)
	} else {
		protocol = "https://"
	}

	branch, err := database.Branch(branchName)

	if err != nil {
		log.Println("Error getting branch:", err)
		return ""
	}

	return fmt.Sprintf(
		"%s%s%s/v1/databases/%s/branches/%s",
		protocol,
		database.DatabaseManager.Cluster.Config.HostName,
		port,
		database.Name,
		branch.Name,
	)
}
