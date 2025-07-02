package http

import (
	"fmt"
	"log"
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

func DatabaseIndexController(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	dbs, err := request.databaseManager.All()

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved databases.",
		dbs,
		200,
	)
}

func DatabaseShowController(request *Request) Response {
	databaseId := request.Param("databaseId")

	if databaseId == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s", databaseId)},
		[]auth.Privilege{auth.DatabasePrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.Get(databaseId)

	if err != nil {
		log.Println(err)
		return BadRequestResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database.",
		db,
		200,
	)
}

type DatabaseStoreRequest struct {
	Name database.DatabaseName `json:"name" validate:"required,validateFn"`
}

func DatabaseStoreController(request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeCreate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&DatabaseStoreRequest{})

	if err != nil {
		log.Println(err)
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"name.required":   "The name field is required.",
		"name.validateFn": "The name field can only contain alpha numeric characters, hyphens, or underscores.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var databaseName = input.(*DatabaseStoreRequest).Name

	// check if the database exists
	exists, err := request.databaseManager.Exists(string(databaseName))

	if err != nil {
		return BadRequestResponse(err)
	}

	if exists {
		return BadRequestResponse(fmt.Errorf("database '%s' already exists", databaseName))
	}

	db, err := request.databaseManager.Create(
		string(databaseName),
		request.cluster.Config.DefaultBranchName,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database created successfully.",
		db,
		200,
	)
}

func DatabaseDestroyController(request *Request) Response {
	databaseId := request.Param("databaseId")

	if databaseId == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s", databaseId)},
		[]auth.Privilege{auth.DatabasePrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.Get(databaseId)

	if err != nil {
		return BadRequestResponse(err)
	}

	err = request.databaseManager.Delete(db)

	if err != nil {
		slog.Error("Failed to delete database", "error", err, "databaseId", db.DatabaseID)
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database deleted successfully.",
		map[string]any{},
		200,
	)
}
