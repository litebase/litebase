package http

import (
	"strconv"
	"time"

	"github.com/litebase/litebase/server/backups"
)

func DatabaseBackupStoreController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	backup, err := backups.Run(
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		time.Now().Unix(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).FileSystem(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).RollbackLogger(),
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
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

	timestamp, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid timestamp",
		}, 500, nil)
	}

	timeInstance := time.Unix(timestamp, 0)

	backup, err := backups.GetBackup(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).FileSystem(),
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		timeInstance.Unix(),
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   backup.ToMap(),
	}, 200, nil)
}

func DatabaseBackupDestroyController(request *Request) Response {
	i, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	timeInstance := time.Unix(i, 0)

	databaseKey := request.DatabaseKey()

	backup, err := backups.GetBackup(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).SnapshotLogger(),
		request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).FileSystem(),
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		timeInstance.Unix(),
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	if backup == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Backup not found",
		}, 404, nil)
	}

	backup.Delete()

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Backup deleted successfully",
	}, 200, nil)
}
