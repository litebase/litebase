package http

import (
	"fmt"
	"litebase/server/backups"
	"litebase/server/database"
	"strconv"
)

func DatabaseSnapshotIndexController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.Subdomains()[0],
	)

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
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
	databaseKey, err := database.GetDatabaseKey(
		request.cluster.Config,
		request.cluster.ObjectFS(),
		request.Subdomains()[0],
	)

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
