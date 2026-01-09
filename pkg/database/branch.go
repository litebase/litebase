package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"time"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/file"

	"github.com/google/uuid"
)

var ErrBranchAlreadyExists = func(name string) error {
	return fmt.Errorf("branch with name '%s' already exists in this database", name)
}

type Branch struct {
	ID                              int64 `json:"id"`
	database                        *Database
	DatabaseBranchID                string                  `json:"databaseBranchId"`
	DatabaseID                      string                  `json:"databaseId"`
	DatabaseManager                 *DatabaseManager        `json:"-"`
	DatabaseReferenceID             sql.NullInt64           `json:"-"`
	Name                            string                  `json:"name"`
	parentBranch                    *Branch                 `json:"-"`
	ParentDatabaseBranchReferenceID sql.NullInt64           `json:"-"`
	Settings                        *DatabaseBranchSettings `json:"settings"`
	CreatedAt                       time.Time               `json:"createdAt"`
	UpdatedAt                       time.Time               `json:"updatedAt"`

	Exists bool `json:"-"`
}

func NewBranch(databaseManager *DatabaseManager, databaseReferenceID int64, parentName string, name string) (*Branch, error) {
	db, err := databaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	// Ensure there is not a current branch with the same name and parent branch within this database
	var existingBranchCount int64
	var parentBranchID sql.NullInt64

	// Get the parent branch ID if parentName is provided
	if parentName != "" {
		err = db.QueryRow(
			`SELECT id FROM database_branches WHERE name = ? AND database_reference_id = ?`,
			parentName,
			databaseReferenceID,
		).Scan(&parentBranchID.Int64)

		if err != nil {
			return nil, fmt.Errorf("parent branch '%s' not found in this database", parentName)
		}

		parentBranchID.Valid = true
	}

	// Check for existing branch with same name within this database
	err = db.QueryRow(
		`SELECT COUNT(*) FROM database_branches 
			WHERE name = ? AND database_reference_id = ?`,
		name,
		databaseReferenceID,
	).Scan(&existingBranchCount)

	if err != nil {
		return nil, fmt.Errorf("error checking for existing branch: %w", err)
	}

	if existingBranchCount > 0 {
		return nil, ErrBranchAlreadyExists(name)
	}

	return &Branch{
		DatabaseBranchID:                uuid.New().String(),
		DatabaseManager:                 databaseManager,
		DatabaseReferenceID:             sql.NullInt64{Int64: databaseReferenceID, Valid: true},
		Name:                            name,
		ParentDatabaseBranchReferenceID: parentBranchID,
	}, nil
}

func InsertBranch(b *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	result, err := db.Exec(
		`INSERT INTO database_branches (
			database_reference_id,
			parent_database_branch_reference_id,
			database_id, 
			database_branch_id, 
			name, 
			created_at, 
			updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		`,
		b.DatabaseReferenceID,
		b.ParentDatabaseBranchReferenceID,
		b.DatabaseID,
		b.DatabaseBranchID,
		b.Name,
		time.Now().UTC(),
		time.Now().UTC(),
	)

	if err != nil {

		log.Fatal(err)
		return err
	}

	id, err := result.LastInsertId()

	if err != nil {
		return err
	}

	b.ID = id
	b.Exists = true

	// Create branch settings (copy from parent if available)
	err = InsertBranchSettings(b, b.ParentBranch())

	if err != nil {
		return fmt.Errorf("failed to create branch settings: %w", err)
	}

	database, err := b.Database()

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Update the Database's branch cache
	if database != nil {
		database.UpdateBranchCache(b.DatabaseBranchID, true)
	}

	return nil
}

func UpdateBranch(b *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	_, err = db.Exec(
		`UPDATE database_branches
		SET
			name = ?,
			updated_at = ?
		WHERE database_branch_id = ?
		`,
		b.Name,
		time.Now().UTC(),
		b.DatabaseBranchID,
	)

	if err != nil {
		return err
	}

	database, err := b.Database()

	if err != nil && err != sql.ErrNoRows {
		return err
	}

	// Update the Database's branch cache to ensure consistency
	if database != nil {
		database.UpdateBranchCache(b.DatabaseBranchID, true)
	}

	return nil
}

// Retrieve the database that the branch belongs to.
func (b *Branch) Database() (*Database, error) {
	if b.database != nil {
		return b.database, nil
	}

	db, err := b.DatabaseManager.Get(b.DatabaseID)

	if err != nil {
		return nil, err
	}

	b.database = db

	return b.database, nil
}

// Delete the branch
func (b *Branch) Delete() error {
	if b == nil || !b.Exists {
		return fmt.Errorf("branch does not exist or is nil")
	}

	database, err := b.Database()

	if err != nil {
		return fmt.Errorf("failed to load branch's database: %w", err)
	}

	primaryBranch, err := database.PrimaryBranch()

	if err != nil {
		return fmt.Errorf("failed to load primary branch: %w", err)
	}

	if primaryBranch == nil {
		return fmt.Errorf("cannot delete branch: primary branch not found")
	}

	if primaryBranch.DatabaseBranchID == b.DatabaseBranchID {
		return fmt.Errorf("cannot delete the primary branch of a database")
	}

	resources := b.DatabaseManager.Resources(b.DatabaseID, b.DatabaseBranchID)

	// Close all database connections to the database before deleting it
	b.DatabaseManager.ConnectionManager().CloseDatabaseBranchConnections(b.DatabaseID, b.DatabaseBranchID)

	fileSystem := resources.FileSystem()

	// Delete from the system database
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return fmt.Errorf("failed to get database connection: %w", err)
	}

	_, err = db.Exec(
		`DELETE FROM database_branches WHERE id = ?`,
		b.ID,
	)

	if err != nil {
		return fmt.Errorf("failed to delete database branch: %w", err)
	}

	database, err = b.Database()

	if err != nil && err != sql.ErrNoRows {
		return fmt.Errorf("failed to load branch's database: %w", err)
	}

	// Remove the branch from the database's branch cache
	if database != nil {
		database.branchCache.Delete(b.DatabaseBranchID)
		database.InvalidateBranchCache(b.DatabaseBranchID)
	}

	// Invalidate the database branch cache

	if b.DatabaseManager.databaseCache != nil {
		database, _ = b.DatabaseManager.Get(b.DatabaseID)

		database.branchCache.Delete(b.DatabaseBranchID)
	}

	// Delete the database storage.
	// TODO: Removing all database storage may require the removal of a lot of files.
	// How is this going to work with tiered storage? We also need to test that
	// removing a branch stops any operations to the database.
	err = fileSystem.FileSystem().RemoveAll(
		file.GetDatabaseBranchRootDir(
			b.DatabaseID,
			b.DatabaseBranchID,
		),
	)

	if err != nil {
		slog.Error("Error deleting database storage", "error", err)
		return err
	}

	resources.Remove()

	return nil
}

// Load and return the parent branch of the current branch
func (branch *Branch) ParentBranch() *Branch {
	if branch == nil {
		return nil
	}

	if branch.parentBranch == nil {
		// If no primary branch ID is set, return nil
		if !branch.ParentDatabaseBranchReferenceID.Valid || branch.ParentDatabaseBranchReferenceID.Int64 == 0 {
			return nil
		}

		// Load the primary branch from the system database using the foreign key
		if branch.DatabaseManager != nil {
			db, err := branch.DatabaseManager.SystemDatabase().DB()

			if err != nil {
				return nil
			}

			var parentBranch Branch

			err = db.QueryRow(
				`SELECT id, database_reference_id, parent_database_branch_reference_id, database_id, database_branch_id, name, created_at, updated_at FROM database_branches WHERE id = ?`,
				branch.ParentDatabaseBranchReferenceID.Int64,
			).Scan(
				&parentBranch.ID,
				&parentBranch.DatabaseReferenceID,
				&parentBranch.ParentDatabaseBranchReferenceID,
				&parentBranch.DatabaseID,
				&parentBranch.DatabaseBranchID,
				&parentBranch.Name,
				&parentBranch.CreatedAt,
				&parentBranch.UpdatedAt,
			)

			if err == nil {
				parentBranch.DatabaseManager = branch.DatabaseManager
				parentBranch.Exists = true
				branch.parentBranch = &parentBranch
			} else {
				log.Println("Error loading primary branch:", err)
			}
		}
	}

	return branch.parentBranch
}

// Save a database to the system database.
func (b *Branch) Save() error {
	if b.DatabaseID == "" || b.DatabaseBranchID == "" {
		return fmt.Errorf("branch is missing required fields")
	}

	if b.Exists {
		return UpdateBranch(b)
	} else {
		return InsertBranch(b)
	}
}

// InsertBranchSettings creates default settings for a newly created branch.
func InsertBranchSettings(b *Branch, parentBranch *Branch) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	// Copy settings from parent branch if available, otherwise use defaults
	var settings *DatabaseBranchSettings

	if parentBranch != nil {
		settings, err = parentBranch.GetBranchSettings()

		if err != nil {
			return fmt.Errorf("failed to get parent branch settings: %w", err)
		}
	} else {
		settings = NewDefaultBranchSettings()
	}

	// Marshal default pragmas to JSON
	defaultPragmasJSON, err := json.Marshal(settings.DefaultPragmas)

	if err != nil {
		return fmt.Errorf("failed to marshal default pragmas: %w", err)
	}

	now := time.Now().UTC().Unix()

	_, err = db.Exec(`
		INSERT INTO database_branch_settings (
			database_branch_reference_id,
			backups_enabled,
			backups_interval,
			backups_retention_days,
			default_pragmas_json,
			error_logs_enabled,
			error_logs_retention_days,
			incremental_backups_enabled,
			incremental_backups_retention_days,
			query_logs_enabled,
			query_logs_retention_days,
			created_at,
			updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		b.ID,
		utils.BoolToInt(settings.BackupsEnabled),
		settings.BackupInterval,
		settings.BackupsRetentionDays,
		string(defaultPragmasJSON),
		utils.BoolToInt(settings.ErrorLogsEnabled),
		settings.ErrorLogsRetentionDays,
		utils.BoolToInt(settings.IncrementalBackupsEnabled),
		settings.IncrementalBackupsRetentionDays,
		utils.BoolToInt(settings.QueryLogsEnabled),
		settings.QueryLogsRetentionDays,
		now,
		now,
	)

	return err
}

// GetBranchSettings retrieves the settings for this branch.
func (b *Branch) GetBranchSettings() (*DatabaseBranchSettings, error) {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return nil, err
	}

	var (
		backupsCleanedAt                sql.NullInt64
		backupsEnabled                  int
		backupInterval                  string
		backupNextAt                    sql.NullInt64
		backupsRetentionDays            int
		defaultPragmasJSON              string
		errorLogsCleanedAt              sql.NullInt64
		errorLogsEnabled                int
		errorLogsRetentionDays          int
		incrementalBackupsCleanedAt     sql.NullInt64
		incrementalBackupsEnabled       int
		incrementalBackupsRetentionDays int
		queryLogsCleanedAt              sql.NullInt64
		queryLogsEnabled                int
		queryLogsRetentionDays          int
	)

	err = db.QueryRow(`
		SELECT 
			backups_cleaned_at,
			backups_enabled,
			backups_interval,
			backup_next_at,
			backups_retention_days,
			default_pragmas_json,
			error_logs_cleaned_at,
			error_logs_enabled,
			error_logs_retention_days,
			incremental_backups_cleaned_at,
			incremental_backups_enabled,
			incremental_backups_retention_days,
			query_logs_cleaned_at,
			query_logs_enabled,
			query_logs_retention_days
		FROM database_branch_settings 
		WHERE database_branch_reference_id = ?
	`, b.ID).Scan(
		&backupsCleanedAt,
		&backupsEnabled,
		&backupInterval,
		&backupNextAt,
		&backupsRetentionDays,
		&defaultPragmasJSON,
		&errorLogsCleanedAt,
		&errorLogsEnabled,
		&errorLogsRetentionDays,
		&incrementalBackupsCleanedAt,
		&incrementalBackupsEnabled,
		&incrementalBackupsRetentionDays,
		&queryLogsCleanedAt,
		&queryLogsEnabled,
		&queryLogsRetentionDays,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to get branch settings: %w", err)
	}

	var defaultPragmas DatabaseDefaultPragmaSettings

	if err := json.Unmarshal([]byte(defaultPragmasJSON), &defaultPragmas); err != nil {
		return nil, fmt.Errorf("failed to unmarshal default pragmas: %w", err)
	}

	return &DatabaseBranchSettings{
		BackupsCleanedAt:                backupsCleanedAt,
		BackupsEnabled:                  backupsEnabled == 1,
		BackupInterval:                  DatabaseBranchBackupInterval(backupInterval),
		BackupNextAt:                    backupNextAt,
		BackupsRetentionDays:            backupsRetentionDays,
		DefaultPragmas:                  &defaultPragmas,
		ErrorLogsCleanedAt:              errorLogsCleanedAt,
		ErrorLogsEnabled:                errorLogsEnabled == 1,
		ErrorLogsRetentionDays:          errorLogsRetentionDays,
		IncrementalBackupsCleanedAt:     incrementalBackupsCleanedAt,
		IncrementalBackupsEnabled:       incrementalBackupsEnabled == 1,
		IncrementalBackupsRetentionDays: incrementalBackupsRetentionDays,
		QueryLogsCleanedAt:              queryLogsCleanedAt,
		QueryLogsEnabled:                queryLogsEnabled == 1,
		QueryLogsRetentionDays:          queryLogsRetentionDays,
	}, nil
}

// UpdateBranchSettings updates the settings for this branch.
func (b *Branch) UpdateBranchSettings(settings *DatabaseBranchSettings) error {
	db, err := b.DatabaseManager.SystemDatabase().DB()

	if err != nil {
		return err
	}

	defaultPragmasJSON, err := json.Marshal(settings.DefaultPragmas)

	if err != nil {
		return fmt.Errorf("failed to marshal default pragmas: %w", err)
	}

	now := time.Now().UTC().Unix()

	_, err = db.Exec(`
		UPDATE database_branch_settings SET
			backups_enabled = ?,
			backups_interval = ?,
			backups_retention_days = ?,
			default_pragmas_json = ?,
			error_logs_enabled = ?,
			error_logs_retention_days = ?,
			incremental_backups_enabled = ?,
			incremental_backups_retention_days = ?,
			query_logs_enabled = ?,
			query_logs_retention_days = ?,
			updated_at = ?
		WHERE database_branch_reference_id = ?
	`,
		utils.BoolToInt(settings.BackupsEnabled),
		settings.BackupInterval,
		settings.BackupsRetentionDays,
		string(defaultPragmasJSON),
		utils.BoolToInt(settings.ErrorLogsEnabled),
		settings.ErrorLogsRetentionDays,
		utils.BoolToInt(settings.IncrementalBackupsEnabled),
		settings.IncrementalBackupsRetentionDays,
		utils.BoolToInt(settings.QueryLogsEnabled),
		settings.QueryLogsRetentionDays,
		now,
		b.ID,
	)

	return err
}

// MarshalJSON customizes the JSON representation of the Branch struct.
// It includes the parent branch name if available.
func (b *Branch) MarshalJSON() ([]byte, error) {
	type Alias Branch

	result := &struct {
		*Alias
		ParentName *string `json:"parentName,omitempty"`
	}{
		Alias: (*Alias)(b),
	}

	// Add parent name if there's a parent branch
	if parentBranch := b.ParentBranch(); parentBranch != nil {
		result.ParentName = &parentBranch.Name
	}

	return json.Marshal(result)
}
