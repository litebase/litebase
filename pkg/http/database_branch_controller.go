package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/backups"
	"github.com/litebase/litebase/pkg/database"
)

// Array of database branches for list operations
type DatabaseBranchIndexResponse []*database.Branch

// List all branches for a specific database
func DatabaseBranchControllerIndex(ctx context.Context, request *Request) Response {
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

	var branches DatabaseBranchIndexResponse

	// Get all branches for the database
	branches, err = db.Branches()

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

// A single database branch response
type DatabaseBranchShowResponse *database.Branch

// Show a specific database branch by ID
func DatabaseBranchControllerShow(ctx context.Context, request *Request) Response {
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
		DatabaseBranchShowResponse(branch),
		200,
	)
}

// Request payload for creating a new database branch
type DatabaseBranchStoreRequest struct {
	Name       database.DatabaseBranchName `json:"name" validate:"required,validateFn"`
	ParentName string                      `json:"parentName,omitempty"`
}

type DatabaseBranchStoreResponse struct {
	ID               int64                    `json:"id"`
	DatabaseBranchID string                   `json:"databaseBranchId"`
	DatabaseID       string                   `json:"databaseId"`
	DatabaseName     string                   `json:"databaseName"`
	Name             string                   `json:"name"`
	ParentName       string                   `json:"parentName"`
	Settings         *database.BranchSettings `json:"settings"`
	CreatedAt        time.Time                `json:"createdAt"`
	UpdatedAt        time.Time                `json:"updatedAt"`
}

// Create a new database branch
func DatabaseBranchControllerStore(ctx context.Context, request *Request) Response {
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
		"name.required":   "The name field is required",
		"name.validateFn": "The name field can only contain alpha numeric characters, hyphens, or underscores",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var branchName = input.(*DatabaseBranchStoreRequest).Name
	var parentName = input.(*DatabaseBranchStoreRequest).ParentName

	// If no parent name is specified, use the default branch name
	if parentName == "" {
		parentName = request.cluster.Config.DefaultBranchName
	}

	branch, err := db.CreateBranch(
		string(branchName),
		parentName,
	)

	if err != nil {
		if errors.Is(err, backups.ErrNoSnapshotsFound) {
			return BadRequestResponse(errors.New("no snapshots found for the parent branch"))
		}

		expectedBranchExistsErr := database.ErrBranchAlreadyExists(string(branchName))

		if err.Error() == expectedBranchExistsErr.Error() {
			return BadRequestResponse(err)
		}

		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database branch created successfully",
		DatabaseBranchStoreResponse{
			ID:               branch.ID,
			DatabaseBranchID: branch.DatabaseBranchID,
			DatabaseID:       branch.DatabaseID,
			DatabaseName:     db.Name,
			Name:             branch.Name,
			ParentName:       parentName,
			Settings:         branch.Settings,
			CreatedAt:        branch.CreatedAt,
			UpdatedAt:        branch.UpdatedAt,
		},
		200,
	)
}

// Delete a specific database branch
func DatabaseBranchControllerDestroy(ctx context.Context, request *Request) Response {
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
		"Database branch deleted successfully",
		nil,
		200,
	)
}
