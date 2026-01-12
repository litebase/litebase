package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

// Array of database snapshots for list operations
type DatabaseSnapshotIndexResponse []*backups.Snapshot

// List all snapshots for a specific database and branch
func DatabaseSnapshotControllerIndex(ctx context.Context, request *Request) Response {
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

	branch, err = request.databaseManager.GetBranch(db.DatabaseID, branch.DatabaseBranchID)

	if err != nil {
		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branch.DatabaseBranchID)

		return BadRequestResponse(err)
	}

	snapshots, err := request.databaseManager.
		Resources(branch).
		SnapshotLogger().
		GetSnapshots()

	var response DatabaseSnapshotIndexResponse

	for _, snapshot := range snapshots {
		response = append(response, snapshot)
	}

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database snapshots.",
		response,
		200,
	)
}

func DatabaseSnapshotControllerShow(ctx context.Context, request *Request) Response {
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
		return BadRequestResponse(errors.New("invalid timestamp"))
	}

	branch, err = request.databaseManager.GetBranch(db.DatabaseID, branch.DatabaseBranchID)

	if err != nil {
		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branch.DatabaseBranchID)

		return BadRequestResponse(err)
	}

	snapshot, err := request.databaseManager.
		Resources(branch).
		SnapshotLogger().
		GetSnapshot(timestamp)

	if err != nil {
		return NotFoundResponse(errors.New("failed to get snapshot"))
	}

	if snapshot.IsEmpty() {
		return NotFoundResponse(errors.New("snapshot not found"))
	}

	return SuccessResponse("Successfully retrieved snapshot.", snapshot, 200)
}
