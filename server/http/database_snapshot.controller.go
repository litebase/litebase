package http

import (
	"litebasedb/server/backups"
	"strconv"
)

func DatabaseSnapshotShowController(request Request) Response {
	timestamp, err := strconv.Atoi(request.Param("timestamp"))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	snapshot := backups.GetSnapShot(
		request.Param("database"),
		request.Param("branch"),
		timestamp,
	)

	if snapshot == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Snapshot not found",
		}, 404, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   snapshot.WithRestorePoints(),
	}, 200, nil)
}
