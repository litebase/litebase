package http

import (
	"fmt"
	"litebase/server/backups"
	"litebase/server/database"
	"log"
)

type DatabaseRestoreRequest struct {
	TargetDatabaseId       string `json:"target_database_id"`
	TargetDatabaseBranchId string `json:"target_database_branch_id"`
	Timestamp              int64  `json:"timestamp"`
}

func DatabaseRestoreController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	input, err := request.Input(&DatabaseRestoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"target_database_id.required":        "The target database field is required.",
		"target_database_branch_id.required": "The target database branch field is required.",
		"timestamp.required":                 "The timestamp field is required.",
		"timestamp.number":                   "The timestamp field is required.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	timestamp := int64(request.Get("timestamp").(float64))
	targetDatabaseUuid := request.Get("target_database_id").(string)
	targetBranchUuid := request.Get("target_database_branch_id").(string)

	db, err := database.Get(targetDatabaseUuid)

	if err != nil {
		return BadRequestResponse(err)
	}

	if !db.HasBranch(targetBranchUuid) {
		return BadRequestResponse(fmt.Errorf("target branch '%s' does not exist in target database '%s'", targetBranchUuid, targetDatabaseUuid))
	}

	sourceDfs := database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).FileSystem()
	targetDfs := database.Resources(targetDatabaseUuid, targetBranchUuid).FileSystem()

	err = backups.RestoreFromTimestamp(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		targetDatabaseUuid,
		targetBranchUuid,
		timestamp,
		sourceDfs,
		targetDfs,
		func(completed func() error) error {
			return database.ConnectionManager().Drain(databaseKey.DatabaseUuid, databaseKey.BranchUuid, func() error {
				log.Println("Database connections drained")
				return completed()
			})
		},
	)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": err.Error(),
		}, 500, nil)
	}

	return JsonResponse(map[string]interface{}{
		"status":  "success",
		"message": "Database restored successfully",
	}, 200, nil)
}
