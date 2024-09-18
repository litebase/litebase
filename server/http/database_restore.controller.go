package http

import (
	"fmt"
	"litebase/server/backups"
	"litebase/server/database"
	"log"
)

func DatabaseRestoreController(request *Request) Response {
	databaseKey, err := database.GetDatabaseKey(request.Subdomains()[0])

	if err != nil {
		return BadRequestResponse(fmt.Errorf("a valid database is required to make this request"))
	}

	if request.Get("timestamp") == nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "restore timestamp is required",
		}, 400, nil)
	}

	timestamp := int64(request.Get("timestamp").(float64))

	dfs := database.Resources(databaseKey.DatabaseUuid, databaseKey.BranchUuid).FileSystem()

	err = backups.RestoreFromTimestamp(
		databaseKey.DatabaseUuid,
		databaseKey.BranchUuid,
		timestamp,
		dfs,
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
