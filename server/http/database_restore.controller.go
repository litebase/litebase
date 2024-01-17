package http

import (
	"litebasedb/server/backups"
)

func DatabaseRestoreController(request *Request) *Response {
	var backupTimestamp int
	var restorePointTimestamp int

	if value := request.Get("backup_timestamp"); value != nil {
		backupTimestamp = int(value.(float64))
	} else {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "backup_timestamp is required",
		}, 400, nil)
	}

	if value := request.Get("restore_point_timestamp"); value != nil {
		restorePointTimestamp = int(value.(float64))
	}

	if backupTimestamp != 0 && restorePointTimestamp == 0 {
		err := backups.RestoreFromDatabaseBackup(
			request.Param("database"),
			request.Param("branch"),
			backupTimestamp,
		)

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			}, 500, nil)
		}
	}

	if backupTimestamp != 0 && restorePointTimestamp != 0 {
		err := backups.RestoreFromDatabaseBackupAtPointInTime(
			request.Param("database"),
			request.Param("branch"),
			backupTimestamp,
			restorePointTimestamp,
		)

		if err != nil {
			return JsonResponse(map[string]interface{}{
				"status":  "error",
				"message": err.Error(),
			}, 500, nil)
		}
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database restored successfully",
	}, 200, nil)
}
