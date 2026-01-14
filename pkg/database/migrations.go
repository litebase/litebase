package database

import (
	"crypto/sha256"
	"database/sql"
	"fmt"
	"log/slog"
	"sort"

	"github.com/litebase/litebase/pkg/database/migrations"
)

// Migration represents a database migration with a name and up function.
type Migration struct {
	Name string
	Up   func(*sql.DB) error
}

// MigrationRunner handles running migrations on the system database.
type MigrationRunner struct {
	db         *sql.DB
	migrations []Migration
}

// NewMigrationRunner creates a new migration runner.
func NewMigrationRunner(db *sql.DB, migs []migrations.Migration) *MigrationRunner {
	// Convert migrations.Migration to database.Migration
	dbMigrations := make([]Migration, len(migs))

	for i, m := range migs {
		dbMigrations[i] = Migration{
			Name: m.Name,
			Up:   m.Up,
		}
	}

	return &MigrationRunner{
		db:         db,
		migrations: dbMigrations,
	}
}

// createMigrationsTable creates the migrations tracking table if it doesn't exist.
// This table must be created before any other tables.
func (mr *MigrationRunner) createMigrationsTable() error {
	_, err := mr.db.Exec(
		`CREATE TABLE IF NOT EXISTS migrations
		(
			id INTEGER PRIMARY KEY,
			name TEXT UNIQUE NOT NULL,
			applied_at TEXT NOT NULL
		)`,
	)

	if err != nil {
		return fmt.Errorf("failed to create migrations table: %w", err)
	}

	return nil
}

// getAppliedMigrations returns a set of migration names that have already been applied.
func (mr *MigrationRunner) getAppliedMigrations() (map[string]bool, error) {
	rows, err := mr.db.Query("SELECT name FROM migrations")

	if err != nil {
		return nil, fmt.Errorf("failed to query migrations: %w", err)
	}

	defer func() {
		err := rows.Close()

		if err != nil {
			slog.Error("Error closing rows", "error", err)
		}
	}()

	applied := make(map[string]bool)

	for rows.Next() {
		var name string

		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("failed to scan migration name: %w", err)
		}

		applied[name] = true
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migrations: %w", err)
	}

	return applied, nil
}

// recordMigration records that a migration has been applied.
func (mr *MigrationRunner) recordMigration(name string) error {
	_, err := mr.db.Exec(
		"INSERT INTO migrations (name, applied_at) VALUES (?, datetime('now'))",
		name,
	)

	if err != nil {
		return fmt.Errorf("failed to record migration %s: %w", name, err)
	}

	return nil
}

// Run executes all pending migrations in order.
func (mr *MigrationRunner) Run() error {
	// First, create the migrations table
	if err := mr.createMigrationsTable(); err != nil {
		return err
	}

	// Get already applied migrations
	applied, err := mr.getAppliedMigrations()

	if err != nil {
		return err
	}

	// Sort migrations by name to ensure consistent ordering
	sort.Slice(mr.migrations, func(i, j int) bool {
		return mr.migrations[i].Name < mr.migrations[j].Name
	})

	// Apply pending migrations
	var lastAppliedMigration string

	for _, migration := range mr.migrations {
		if applied[migration.Name] {
			slog.Debug("Skipping already applied migration", "name", migration.Name)
			lastAppliedMigration = migration.Name

			continue
		}

		// Execute migration in a transaction
		tx, err := mr.db.Begin()

		if err != nil {
			return fmt.Errorf("failed to begin transaction for migration %s: %w", migration.Name, err)
		}

		// Apply the migration
		if err := migration.Up(mr.db); err != nil {
			err := tx.Rollback()

			if err != nil {
				slog.Error("Error rolling back transaction", "error", err)
			}

			return fmt.Errorf("failed to apply migration %s: %w", migration.Name, err)
		}

		// Record the migration
		if err := mr.recordMigration(migration.Name); err != nil {
			err := tx.Rollback()

			if err != nil {
				slog.Error("Error rolling back transaction", "error", err)
			}

			return err
		}

		// Commit the transaction
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit migration %s: %w", migration.Name, err)
		}

		slog.Debug("Successfully applied migration", "name", migration.Name)
		lastAppliedMigration = migration.Name
	}

	// Update the migrations hash after all migrations are applied
	if lastAppliedMigration != "" {
		if err := mr.updateMigrationsHash(lastAppliedMigration); err != nil {
			return fmt.Errorf("failed to update migrations hash: %w", err)
		}
	}

	return nil
}

// updateMigrationsHash stores a SHA256 hash of the latest migration name in metadata
func (mr *MigrationRunner) updateMigrationsHash(latestMigrationName string) error {
	hash := sha256.Sum256([]byte(latestMigrationName))
	hashStr := fmt.Sprintf("%x", hash)

	_, err := mr.db.Exec(
		`INSERT OR REPLACE INTO metadata (key, value) 
		 VALUES (?, ?)`,
		"migrations_hash",
		hashStr,
	)

	if err != nil {
		return fmt.Errorf("failed to store migrations hash: %w", err)
	}

	slog.Debug("Updated migrations hash", "latest_migration", latestMigrationName, "hash", hashStr)

	return nil
}
