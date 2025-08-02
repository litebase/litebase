package http

import (
	"fmt"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

type DatabaseRestoreRequest struct {
	TargetDatabase       string `json:"target_database" validate:"required" `
	TargetDatabaseBranch string `json:"target_database_branch" validate:"required"`
	Timestamp            string `json:"timestamp" validate:"required"`
}

func DatabaseRestoreController(request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	database, err := request.databaseManager.Get(databaseKey.DatabaseID)

	if err != nil {
		return BadRequestResponse(err)
	}

	branch, err := database.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeRestore},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&DatabaseRestoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"target_database.required":        "The target database field is required.",
		"target_database_branch.required": "The target database branch field is required.",
		"timestamp.required":              "The timestamp field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	timestamp, err := strconv.ParseInt(input.(*DatabaseRestoreRequest).Timestamp, 10, 64)

	if err != nil {
		return BadRequestResponse(err)
	}

	targetDatabaseName := request.Get("target_database").(string)
	targetBranchName := request.Get("target_database_branch").(string)

	targetDatabase, err := request.databaseManager.GetByName(targetDatabaseName)

	if err != nil {
		return BadRequestResponse(err)
	}

	if !targetDatabase.HasBranch(targetBranchName) {
		return BadRequestResponse(fmt.Errorf("target branch '%s' does not exist in target database '%s'", targetBranchName, targetDatabaseName))
	}

	targetBranch, err := targetDatabase.Branch(targetBranchName)

	if err != nil {
		return BadRequestResponse(err)
	}

	snapshotLogger := request.databaseManager.Resources(database.DatabaseID, branch.DatabaseBranchID).SnapshotLogger()
	checkpointer, err := request.databaseManager.Resources(database.DatabaseID, branch.DatabaseBranchID).Checkpointer()

	if err != nil {
		return ServerErrorResponse(err)
	}

	sourceDfs := request.databaseManager.Resources(database.DatabaseID, branch.DatabaseBranchID).FileSystem()
	targetDfs := request.databaseManager.Resources(targetDatabase.DatabaseID, targetBranch.DatabaseBranchID).FileSystem()

	err = backups.RestoreFromTimestamp(
		request.cluster.Config,
		request.cluster.TieredFS(),
		database.DatabaseID,
		branch.DatabaseBranchID,
		targetDatabase.DatabaseID,
		targetBranch.DatabaseBranchID,
		timestamp,
		snapshotLogger,
		sourceDfs,
		targetDfs,
		checkpointer,
		func(restoreFunc func() error) error {
			return restoreFunc()
		},
	)

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]any{
		"status":  "success",
		"message": "Database restored successfully",
	}, 200, nil)
}
