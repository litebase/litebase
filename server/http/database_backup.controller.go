package http

import (
	"fmt"
	"litebase/server/backups"
	"litebase/server/database"
	"strconv"
	"time"
)

func DatabaseBackupStoreController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	backup, err := backups.Run(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		time.Now().Unix(),
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).SnapshotLogger(),
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).FileSystem(),
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).RollbackLogger(),
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
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
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
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).SnapshotLogger(),
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).FileSystem(),
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
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
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).SnapshotLogger(),
		database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).FileSystem(),
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
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
