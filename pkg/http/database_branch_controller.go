package http

import (
	"fmt"
	"log/slog"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

// List all branches for a specific database
func DatabaseBranchIndexController(request *Request) Response {
	databaseId := request.Param("databaseId")

	if databaseId == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{"database:*", fmt.Sprintf("database:%s", databaseId)},
		[]auth.Privilege{auth.DatabaseBranchPrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.Get(databaseId)

	if err != nil {
		return BadRequestResponse(err)
	}

	// Get all branches for the database
	branches, err := db.Branches()

	if err != nil {
		slog.Error("Failed to retrieve database branches", "error", err, "databaseId", db.DatabaseID)
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
	databaseID := request.Param("databaseId")

	if databaseID == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	branchID := request.Param("branchId")

	if branchID == "" {
		return ErrValidBranchIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{
			"database:*",
			fmt.Sprintf("database:%s:branch:*", databaseID),
			fmt.Sprintf("database:%s:branch:%s", databaseID, branchID),
		},
		[]auth.Privilege{auth.DatabasePrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.Get(databaseID)

	if err != nil {
		return BadRequestResponse(err)
	}

	// Get the branch by ID
	branch, err := db.Branch(branchID)

	if err != nil {
		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branchID)
		return BadRequestResponse(err)
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
	databaseID := request.Param("databaseId")

	if databaseID == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{"database:*", fmt.Sprintf("database:%s", databaseID)},
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

	// Get the database by ID
	db, err := request.databaseManager.Get(databaseID)

	if err != nil {
		slog.Error("Failed to retrieve database", "error", err, "databaseId", databaseID)
		return BadRequestResponse(err)
	}

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
	databaseID := request.Param("databaseId")

	if databaseID == "" {
		return ErrValidDatabaseIdRequiredResponse
	}

	branchID := request.Param("branchId")

	if branchID == "" {
		return ErrValidBranchIdRequiredResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{"database:*", fmt.Sprintf("database:%s:branch:*", databaseID), fmt.Sprintf("database:%s:branch:%s", databaseID, branchID)},
		[]auth.Privilege{auth.DatabasePrivilegeManage},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	db, err := request.databaseManager.Get(databaseID)

	if err != nil {
		return BadRequestResponse(err)
	}

	branch, err := db.Branch(branchID)

	if err != nil {
		slog.Error("Failed to retrieve database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branchID)
		return BadRequestResponse(err)
	}

	err = branch.Delete()

	if err != nil {
		slog.Error("Failed to delete database branch", "error", err, "databaseId", db.DatabaseID, "branchId", branchID)
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database branch deleted successfully.",
		map[string]any{},
		200,
	)
}
