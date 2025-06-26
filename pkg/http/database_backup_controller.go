package http

import (
	"fmt"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

type DatabaseBackupStoreRequest struct{}

func DatabaseBackupStoreController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.BranchID)},
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
		databaseKey.DatabaseID,
		databaseKey.BranchID,
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).FileSystem(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).RollbackLogger(),
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Database backup created successfully",
		"data":    backup.ToMap(),
	}, 200, nil)
}

func DatabaseBackupShowController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.BranchID)},
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

	backup, err := backups.GetBackup(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).FileSystem(),
		databaseKey.DatabaseID,
		databaseKey.BranchID,
		timestamp,
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status": "success",
		"data":   backup.ToMap(),
	}, 200, nil)
}

func DatabaseBackupDestroyController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.BranchID)},
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
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseID, databaseKey.BranchID).FileSystem(),
		databaseKey.DatabaseID,
		databaseKey.BranchID,
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

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Database backup deleted successfully",
	}, 200, nil)
}
