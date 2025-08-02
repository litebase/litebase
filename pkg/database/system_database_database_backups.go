package database

import (
	"fmt"
	"time"

	"github.com/litebase/litebase/pkg/backups"
)

// Delete a database  backup from the system database.
func (s *SystemDatabase) DeleteDatabaseBackup(
	databaseId string,
	branchId string,
	restorePointTimestamp int64,
) error {
	db, err := s.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	_, err = db.Exec(
		"DELETE FROM database_backups WHERE database_id = ? AND database_branch_id = ? AND restore_point_timestamp = ?",
		databaseId,
		branchId,
		restorePointTimestamp,
	)

	if err != nil {
		return fmt.Errorf("failed to delete database backup: %w", err)
	}

	return nil
}

// Retrieve a specific database backup by its restore point timestamp.
func (s *SystemDatabase) GetDatabaseBackup(
	databaseId string,
	branchId string,
	restorePointTimestamp int64,
) (*backups.Backup, error) {
	db, err := s.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database connection: %w", err)
	}

	var backup backups.Backup

	err = db.QueryRow(
		"SELECT database_id, database_branch_id, restore_point_timestamp, restore_point_page_count, size FROM database_backups WHERE database_id = ? AND database_branch_id = ? AND restore_point_timestamp = ?",
		databaseId,
		branchId,
		restorePointTimestamp,
	).Scan(
		&backup.DatabaseID,
		&backup.DatabaseBranchID,
		&backup.RestorePoint.Timestamp,
		&backup.RestorePoint.PageCount,
		&backup.Size,
	)

	if err != nil {
		return nil, err
	}

	return &backup, nil
}

func (s *SystemDatabase) ListDatabaseBackups(
	databaseId string,
	branchId string,
) ([]*backups.Backup, error) {
	db, err := s.DB()

	if err != nil {
		return nil, fmt.Errorf("failed to get system database connection: %w", err)
	}

	rows, err := db.Query(
		"SELECT database_id, database_branch_id, restore_point_timestamp, restore_point_page_count, size FROM database_backups WHERE database_id = ? AND database_branch_id = ? ORDER BY restore_point_timestamp DESC",
		databaseId,
		branchId,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to query database backups: %w", err)
	}
	defer rows.Close()

	var backupsList []*backups.Backup

	for rows.Next() {
		var backup backups.Backup
		err = rows.Scan(
			&backup.DatabaseID,
			&backup.DatabaseBranchID,
			&backup.RestorePoint.Timestamp,
			&backup.RestorePoint.PageCount,
			&backup.Size,
		)

		if err != nil {
			return nil, fmt.Errorf("failed to scan database backup row: %w", err)
		}

		backupsList = append(backupsList, &backup)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating over database backups: %w", err)
	}

	return backupsList, nil
}

func (s *SystemDatabase) StoreDatabaseBackup(
	databaseReferenceID, branchReferenceID int64,
	databaseID, branchID string,
	restorePointTimestamp int64,
	restorePointPageCount int64,
	size int64,
) error {
	db, err := s.DB()

	if err != nil {
		return fmt.Errorf("failed to get system database connection: %w", err)
	}

	_, err = db.Exec(
		`INSERT INTO database_backups 
		(database_reference_id, database_branch_reference_id, database_id, database_branch_id, restore_point_timestamp, restore_point_page_count, size, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		databaseReferenceID,
		branchReferenceID,
		databaseID,
		branchID,
		restorePointTimestamp,
		restorePointPageCount,
		size,
		time.Now().UTC().Format(time.RFC3339),
	)

	if err != nil {
		return fmt.Errorf("failed to store database backup: %w", err)
	}

	return nil
}
