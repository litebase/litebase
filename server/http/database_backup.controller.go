package http

import (
	"litebasedb/server/backups"
	"strconv"
	"time"
)

func DatabaseBackupStoreController(request *Request) *Response {
	backup, err := backups.RunBackup(
		request.Param("database"),
		request.Param("branch"),
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

func DatabaseBackupShowController(request *Request) *Response {
	timeInstance, err := time.Parse(time.UnixDate, request.Param("timestamp"))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	backup := backups.GetBackup(
		request.Param("database"),
		request.Param("branch"),
		timeInstance,
	)

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   backup.ToMap(),
	}, 200, nil)
}

func DatabaseBackupDestroyController(request *Request) *Response {
	i, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	timeInstance := time.Unix(i, 0)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	backup := backups.GetBackup(
		request.Param("database"),
		request.Param("branch"),
		timeInstance,
	)

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
