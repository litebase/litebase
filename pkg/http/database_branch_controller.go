package http

import (
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

// List all branches for a specific database
func DatabaseBranchIndexController(request *Request) Response {
	databaseName := request.Param("databaseName")

	if databaseName == "" {
		return ErrValidDatabaseNameRequiredResponse
	}

	db, err := request.databaseManager.GetByName(databaseName)

	if err != nil {
		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{"database:*", fmt.Sprintf("database:%s", db.DatabaseID)},
		[]auth.Privilege{auth.DatabaseBranchPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Get all branches for the database
	branches, err := db.Branches()

	if err != nil {
		slog.Error("Failed to retrieve database branches", "error", err, "databaseName", db.Name)
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database branches.",
		branches,
		200,
	)
}

// Show a specific database branch by ID
func DatabaseBranchShowController(request *Request) Response {
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

	// Get the branch by ID
	branch, err := db.Branch(databaseKey.DatabaseBranchName)

	if err != nil {
		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchName", databaseKey.DatabaseBranchName)
		return BadRequestResponse(err)
	}

	// Authorize the request
	err = request.Authorize(
		[]string{
			"database:*",
			fmt.Sprintf("database:%s:branch:*", db.DatabaseID),
			fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID),
		},
		[]auth.Privilege{auth.DatabasePrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved database branch.",
		branch,
		200,
	)
}

type DatabaseBranchStoreRequest struct {
	Name       database.DatabaseBranchName `json:"name" validate:"required,validateFn"`
	ParentName string                      `json:"parent_name,omitempty"`
}

// Create a new database branch
func DatabaseBranchStoreController(request *Request) Response {
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
		[]string{"database:*", fmt.Sprintf("database:%s", db.DatabaseID)},
		[]auth.Privilege{auth.DatabaseBranchPrivilegeCreate},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&DatabaseBranchStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	validationErrors := request.Validate(input, map[string]string{
		"name.required":   "The name field is required.",
		"name.validateFn": "The name field can only contain alpha numeric characters, hyphens, or underscores.",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var branchName = input.(*DatabaseBranchStoreRequest).Name

	branch, err := db.CreateBranch(
		string(branchName),
		request.cluster.Config.DefaultBranchName,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database branch created successfully.",
		branch,
		200,
	)
}

// Delete a specific database branch
func DatabaseBranchDestroyController(request *Request) Response {
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
		[]string{"database:*", fmt.Sprintf("database:%s:branch:*", db.DatabaseID), fmt.Sprintf("database:%s:branch:%s", db.DatabaseID, branch.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	err = branch.Delete()

	if err != nil {
		slog.Error("Failed to delete database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branch.DatabaseBranchID)

		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database branch deleted successfully.",
		map[string]any{},
		200,
	)
}
