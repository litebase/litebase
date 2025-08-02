package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

func DatabaseSnapshotIndexController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	db, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("branch not found"))
		}

		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)

		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeBackup},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	snapshots, err := request.databaseManager.
		Resources(db.DatabaseID, branch.DatabaseBranchID).
		SnapshotLogger().
		GetSnapshots()

	values := make([]*backups.Snapshot, 0)

	for _, snapshot := range snapshots {
		values = append(values, snapshot)
	}

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Failed to get snapshots",
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status": "success",
		"data":   values,
	}, 200, nil)
}

func DatabaseSnapshotShowController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	db, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("branch not found"))
		}

		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)

		return BadRequestResponse(err)
	}

	timestamp, err := strconv.ParseInt(request.Param("timestamp"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Invalid timestamp",
		}, 500, nil)
	}

	snapshot, err := request.databaseManager.
		Resources(db.DatabaseID, branch.DatabaseBranchID).
		SnapshotLogger().
		GetSnapshot(timestamp)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Failed to get snapshot",
		}, 404, nil)
	}

	if snapshot.IsEmpty() {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "Snapshot not found",
		}, 404, nil)
	}

	return JsonResponse(map[string]any{
		"status": "success",
		"data":   snapshot,
	}, 200, nil)
}
