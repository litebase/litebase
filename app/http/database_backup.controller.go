package http

import (
	"litebasedb/runtime/app/backups"
	"strconv"
)

type DatabaseBackupController struct {
}

func (controller *DatabaseBackupController) Store(request *Request) *Response {
	backup, err := backups.RunFullBackup(
		request.Param("database"),
		request.Param("branch"),
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database backup created successfully",
		"data":    backup.ToMap(),
	}, 200, nil)
}

func (controller *DatabaseBackupController) Show(request *Request) *Response {
	timestamp, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status": "error",
			"error":  err.Error(),
		}, 500, nil)
	}

	backup := backups.GetFullBackup(
		request.Param("database"),
		request.Param("branch"),
		int64(timestamp),
	)

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   backup.ToMap(),
	}, 200, nil)
}
