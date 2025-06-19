package http

import (
	"fmt"
	"log"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
)

type DatabaseRestoreRequest struct {
	TargetDatabaseId       string `json:"target_database_id" validate:"required" `
	TargetDatabaseBranchId string `json:"target_database_branch_id" validate:"required"`
	Timestamp              int64  `json:"timestamp" validate:"required,number"`
}

func DatabaseRestoreController(request *Request) Response {
	databaseKey := request.DatabaseKey()

	if databaseKey == nil {
		return ErrValidDatabaseKeyRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseId, databaseKey.BranchId)},
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
		"target_database_id.required":        "The target database field is required.",
		"target_database_branch_id.required": "The target database branch field is required.",
		"timestamp.required":                 "The timestamp field is required.",
		"timestamp.number":                   "The timestamp field must be a number.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	log.Println("test", input.(*DatabaseRestoreRequest).Timestamp)
	timestamp := float64(input.(*DatabaseRestoreRequest).Timestamp)
	targetDatabaseUuid := request.Get("target_database_id").(string)
	targetBranchUuid := request.Get("target_database_branch_id").(string)

	db, err := request.databaseManager.Get(targetDatabaseUuid)

	if err != nil {
		return BadRequestResponse(err)
	}

	if !db.HasBranch(targetBranchUuid) {
		return BadRequestResponse(fmt.Errorf("target branch '%s' does not exist in target database '%s'", targetBranchUuid, targetDatabaseUuid))
	}

	snapshotLogger := request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).SnapshotLogger()
	sourceDfs := request.databaseManager.Resources(databaseKey.DatabaseId, databaseKey.BranchId).FileSystem()
	targetDfs := request.databaseManager.Resources(targetDatabaseUuid, targetBranchUuid).FileSystem()
	log.Println("Starting restore from timestamp", timestamp)
	err = backups.RestoreFromTimestamp(
		request.cluster.Config,
		request.cluster.TieredFS(),
		databaseKey.DatabaseId,
		databaseKey.BranchId,
		targetDatabaseUuid,
		targetBranchUuid,
		int64(timestamp),
		snapshotLogger,
		sourceDfs,
		targetDfs,
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
