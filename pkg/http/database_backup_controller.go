package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

// Array of database branches for list operations
type DatabaseBackupIndexResponse []*backups.Backup

// List all backups for a specific database and branch
func DatabaseBackupControllerIndex(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	_, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	var response DatabaseBackupIndexResponse

	// List the backups for the specified database and branch
	response, err = request.databaseManager.SystemDatabase().ListDatabaseBackups(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database backups.",
		response,
		200,
	)
}

type DatabaseBackupStoreResponse struct {
	DatabaseBranchID string                     `json:"databaseBranchId"`
	DatabaseID       string                     `json:"databaseId"`
	RestorePoint     DatabaseBackupRestorePoint `json:"restorePoint"`
	Size             int64                      `json:"size"`
}

// Create a new database backup
func DatabaseBackupControllerStore(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	db, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("branch not found"))
		}

		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)

		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeBackup},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	backup, err := backups.Run(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		db.DatabaseID,
		branch.DatabaseBranchID,
		request.databaseManager.Resources(branch).SnapshotLogger(),
		request.databaseManager.Resources(branch).FileSystem(),
		request.databaseManager.Resources(branch).RollbackLogger(),
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	// Store the database backup in the system database.
	err = request.databaseManager.SystemDatabase().StoreDatabaseBackup(
		db.ID,
		branch.ID,
		db.DatabaseID,
		branch.DatabaseBranchID,
		backup.RestorePoint.Timestamp,
		backup.RestorePoint.PageCount,
		backup.GetSize(),
	)

	if err != nil {
		slog.Error("Failed to store database backup", "error", err, "databaseId", db.DatabaseID, "branchId", branch.DatabaseBranchID)
		return ServerErrorResponse(err)
	}

	return SuccessResponse("Database backup created successfully", DatabaseBackupStoreResponse{
		DatabaseBranchID: backup.DatabaseBranchID,
		DatabaseID:       backup.DatabaseID,
		RestorePoint: DatabaseBackupRestorePoint{
			Timestamp: backup.RestorePoint.Timestamp,
			PageCount: backup.RestorePoint.PageCount,
		},
		Size: backup.Size,
	}, 200)
}

// A single restore point within a database backup
type DatabaseBackupRestorePoint struct {
	Timestamp int64 `json:"timestamp,string"`
	PageCount int64 `json:"pageCount"`
}

// A single database branch response
type DatabaseBackupShowResponse struct {
	DatabaseBranchID string                     `json:"databaseBranchId"`
	DatabaseID       string                     `json:"databaseId"`
	RestorePoint     DatabaseBackupRestorePoint `json:"restorePoint"`
	Size             int64                      `json:"size"`
}

// Show a specific database backup
func DatabaseBackupControllerShow(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeBackup},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	timestamp, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return ServerErrorResponse(err)
	}

	backup, err := request.databaseManager.SystemDatabase().GetDatabaseBackup(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
		timestamp,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("backup not found"))
		}

		slog.Error("Failed to retrieve database backup", "error", err, "databaseId", databaseKey.DatabaseID, "branchId", databaseKey.DatabaseBranchID)

		return ServerErrorResponse(err)
	}

	return SuccessResponse("Database backup retrieved successfully", DatabaseBackupShowResponse{
		DatabaseBranchID: backup.DatabaseBranchID,
		DatabaseID:       backup.DatabaseID,
		RestorePoint: DatabaseBackupRestorePoint{
			Timestamp: backup.RestorePoint.Timestamp,
			PageCount: backup.RestorePoint.PageCount,
		},
		Size: backup.Size,
	}, 200)
}

// Delete a specific database backup
func DatabaseBackupControllerDestroy(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeBackup},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	timestamp, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return ServerErrorResponse(err)
	}

	branch, err := request.databaseManager.GetBranch(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return BadRequestResponse(err)
	}

	backup, err := backups.GetBackup(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.databaseManager.Resources(branch).SnapshotLogger(),
		request.databaseManager.Resources(branch).FileSystem(),
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
		timestamp,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	if backup == nil {
		return NotFoundResponse(errors.New("backup not found"))
	}

	err = backup.Delete()

	if err != nil {
		return ServerErrorResponse(err)
	}

	// Delete the backup from the system database.
	err = request.databaseManager.SystemDatabase().DeleteDatabaseBackup(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
		timestamp,
	)

	if err != nil {
		slog.Error("Failed to delete database backup from system database", "error", err, "databaseId", databaseKey.DatabaseID, "branchId", databaseKey.DatabaseBranchID)

		return ServerErrorResponse(err)
	}

	return SuccessResponse("Database backup deleted successfully", nil, 200)
}
