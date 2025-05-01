package http

import (
	"strconv"

	"github.com/litebase/litebase/server/backups"
)

func DatabaseSnapshotIndexController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	snapshots, err := request.databaseManager.
		Resources(databaseKey.DatabaseId, databaseKey.BranchId).
		SnapshotLogger().
		GetSnapshots()

	values := make([]*backups.Snapshot, 0)

	for _, snapshot := range snapshots {
		values = append(values, snapshot)
	}

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Failed to get snapshots",
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   values,
	}, 200, nil)
}

func DatabaseSnapshotShowController(request *Request) Response {
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

	snapshot, err := request.databaseManager.
		Resources(databaseKey.DatabaseId, databaseKey.BranchId).
		SnapshotLogger().
		GetSnapshot(timestamp)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Failed to get snapshot",
		}, 404, nil)
	}

	if snapshot.IsEmpty() {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Snapshot not found",
		}, 404, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   snapshot,
	}, 200, nil)
}
