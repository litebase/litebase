package http

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

type DatabaseResponse struct {
	ID           int64     `json:"id"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	Url          string    `json:"url"`
}

// Array of databases for list operations
type DatabaseIndexResponse []DatabaseResponse

// List all databases
func DatabaseControllerIndex(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeList},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	var response DatabaseIndexResponse

	databases, err := request.databaseManager.All()

	if err != nil {
		return ServerErrorResponse(err)
	}

	for _, db := range databases {
		primaryBranch, err := db.PrimaryBranch()

		if err != nil {
			slog.Error("Failed to retrieve primary branch", "databaseId", db.DatabaseID, "error", err)

			return ServerErrorResponse(errors.New("failed to retrieve primary branch"))
		}

		response = append(response, DatabaseResponse{
			ID:           db.ID,
			DatabaseID:   db.DatabaseID,
			DatabaseName: db.Name,
			BranchName:   primaryBranch.Name,
			CreatedAt:    db.CreatedAt,
			UpdatedAt:    db.UpdatedAt,
			Url:          db.Url(primaryBranch.Name),
		})
	}

	return SuccessResponse(
		"Successfully retrieved databases.",
		response,
		200,
	)
}

type DatabaseShowResponse struct {
	ID           int64     `json:"id"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	Url          string    `json:"url"`
}

func DatabaseControllerShow(ctx context.Context, request *Request) Response {
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

	primaryBranch, err := db.PrimaryBranch()

	if err != nil {
		slog.Error("Failed to retrieve primary branch", "databaseId", db.DatabaseID, "error", err)

		return ServerErrorResponse(errors.New("failed to retrieve primary branch"))
	}

	return SuccessResponse(
		"Successfully retrieved database.",
		DatabaseShowResponse{
			ID:           db.ID,
			DatabaseID:   db.DatabaseID,
			DatabaseName: db.Name,
			BranchName:   primaryBranch.Name,
			CreatedAt:    db.CreatedAt,
			UpdatedAt:    db.UpdatedAt,
			Url:          db.Url(primaryBranch.Name),
		},
		200,
	)
}

type DatabaseStoreRequest struct {
	Name          database.DatabaseName `json:"name" validate:"required,validateFn"`
	PrimaryBranch string                `json:"primaryBranch,omitempty" validate:"omitempty,lowercase,alphanum"`
}

type DatabaseStoreResponse struct {
	ID           int64     `json:"id"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	CreatedAt    time.Time `json:"createdAt"`
	UpdatedAt    time.Time `json:"updatedAt"`
	Url          string    `json:"url"`
}

// Create a new database
func DatabaseControllerStore(ctx context.Context, request *Request) Response {
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
		return BadRequestResponse(err)
	}

	req, ok := input.(*DatabaseStoreRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"name.required":           "The name field is required",
		"name.validateFn":         "The name field can only contain alpha numeric characters, hyphens, or underscores",
		"primaryBranch.lowercase": "The primary branch name must be lowercase",
		"primaryBranch.alphanum":  "The primary branch name can only contain alphanumeric characters",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var databaseName = req.Name

	// check if the database exists
	exists, err := request.databaseManager.Exists(string(databaseName))

	if err != nil {
		return BadRequestResponse(err)
	}

	if exists {
		return BadRequestResponse(fmt.Errorf("database '%s' already exists", databaseName))
	}

	branchName := request.cluster.Config.DefaultBranchName

	if req.PrimaryBranch != "" {
		branchName = req.PrimaryBranch
	}

	db, err := request.databaseManager.Create(string(databaseName), branchName)

	if err != nil {
		return ServerErrorResponse(err)
	}

	primaryBranch, err := db.PrimaryBranch()

	if err != nil {
		slog.Error("Failed to retrieve primary branch after database creation", "databaseId", db.DatabaseID, "error", err)

		return ServerErrorResponse(errors.New("failed to retrieve primary branch after database creation"))
	}

	return SuccessResponse(
		"Database created successfully",
		DatabaseStoreResponse{
			ID:           db.ID,
			DatabaseID:   db.DatabaseID,
			DatabaseName: db.Name,
			BranchName:   primaryBranch.Name,
			CreatedAt:    db.CreatedAt,
			UpdatedAt:    db.UpdatedAt,
			Url:          db.Url(primaryBranch.Name),
		},
		200,
	)
}

// Delete a database
func DatabaseControllerDestroy(ctx context.Context, request *Request) Response {
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
		"Database deleted successfully",
		nil,
		200,
	)
}
