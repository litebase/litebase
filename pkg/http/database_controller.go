package http

import (
	"database/sql"
	"errors"
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
	databaseName := request.Param("databaseName")

	if databaseName == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s", databaseName)},
		[]auth.Privilege{auth.DatabasePrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.GetByName(databaseName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database.",
		db,
		200,
	)
}

type DatabaseStoreRequest struct {
	Name          database.DatabaseName `json:"name" validate:"required,validateFn"`
	PrimaryBranch string                `json:"primary_branch,omitempty" validate:"omitempty,lowercase,alphanum"`
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
		"name.required":            "The name field is required.",
		"name.validateFn":          "The name field can only contain alpha numeric characters, hyphens, or underscores.",
		"primary_branch.lowercase": "The primary branch name must be lowercase.",
		"primary_branch.alphanum":  "The primary branch name can only contain alphanumeric characters.",
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

	branchName := request.cluster.Config.DefaultBranchName

	if input.(*DatabaseStoreRequest).PrimaryBranch != "" {
		branchName = input.(*DatabaseStoreRequest).PrimaryBranch
	}

	db, err := request.databaseManager.Create(string(databaseName), branchName)

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
	databaseName := request.Param("databaseName")

	if databaseName == "" {
		return ErrValidDatabaseNameRequiredResponse
	}

	db, err := request.databaseManager.GetByName(databaseName)

	if err != nil {
		if err == sql.ErrNoRows {
			return NotFoundResponse(errors.New("database not found"))
		}

		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{fmt.Sprintf("database:%s", db.DatabaseID)},
		[]auth.Privilege{auth.DatabasePrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
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
