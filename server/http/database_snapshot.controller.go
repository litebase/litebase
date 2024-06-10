package http

import (
	"fmt"
	"litebasedb/server/backups"
	"litebasedb/server/database"
	"strconv"
)

func DatabaseSnapshotIndexController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	snapshots, err := backups.GetSnapshots(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Failed to get snapshots",
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   snapshots,
	}, 200, nil)
}

func DatabaseSnapshotShowController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	timestamp, err := strconv.ParseUint(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid timestamp",
		}, 500, nil)
	}

	snapshot, err := backups.GetSnapshot(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		timestamp,
	)

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
