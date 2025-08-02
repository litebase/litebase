package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

func DatabaseBackupIndexController(request *Request) Response {
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

	// List the backups for the specified database and branch
	backupList, err := request.databaseManager.SystemDatabase().ListDatabaseBackups(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database backups.",
		backupList,
		200,
	)
}

type DatabaseBackupStoreRequest struct{}

// Create a new database backup
func DatabaseBackupStoreController(request *Request) Response {
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

	// Parse the input
	input, err := request.Input(&DatabaseBackupStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	// Validate the input
	validationErrors := request.Validate(input, map[string]string{})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	backup, err := backups.Run(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		db.DatabaseID,
		branch.DatabaseBranchID,
		request.databaseManager.Resources(db.DatabaseID, branch.DatabaseBranchID).SnapshotLogger(),
		request.databaseManager.Resources(db.DatabaseID, branch.DatabaseBranchID).FileSystem(),
		request.databaseManager.Resources(db.DatabaseID, branch.DatabaseBranchID).RollbackLogger(),
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
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

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Database backup created successfully",
		"data":    backup,
	}, 200, nil)
}

// Show a specific database backup
func DatabaseBackupShowController(request *Request) Response {
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
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Invalid timestamp",
		}, 500, nil)
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

	return JsonResponse(map[string]any{
		"status": "success",
		"data":   backup,
	}, 200, nil)
}

func DatabaseBackupDestroyController(request *Request) Response {
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
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	backup, err := backups.GetBackup(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.DatabaseBranchID).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.DatabaseBranchID).FileSystem(),
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
		timestamp,
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	if backup == nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Backup not found",
		}, 404, nil)
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

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Database backup deleted successfully",
	}, 200, nil)
}
