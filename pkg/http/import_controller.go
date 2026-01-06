package http

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

type ImportShowResponse struct {
	ImportID       int64      `json:"importId"`
	DatabaseID     string     `json:"databaseId"`
	DatabaseName   string     `json:"databaseName"`
	BranchName     string     `json:"branchName"`
	Status         string     `json:"status"`
	ChunkCount     int64      `json:"chunkCount"`
	UploadedChunks int64      `json:"uploadedChunks"`
	MissingChunks  []int64    `json:"missingChunks"`
	TotalSize      *int64     `json:"totalSize,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	UpdatedAt      time.Time  `json:"updatedAt"`
	CompletedAt    *time.Time `json:"completedAt,omitempty"`
}

func ImportControllerShow(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Get import ID from route params
	importIDStr := request.Param("importId")

	if importIDStr == "" {
		return BadRequestResponse(errors.New("invalid import ID"))
	}

	var importID int64

	if _, err := fmt.Sscanf(importIDStr, "%d", &importID); err != nil {
		return BadRequestResponse(errors.New("invalid import ID"))
	}

	// Get the import
	manager := database.NewDatabaseImportManager(request.databaseManager)

	importRecord, err := manager.Get(importID)

	if err != nil {
		return NotFoundResponse(err)
	}

	// Get the database and branch info by querying with reference IDs
	systemDB, err := request.databaseManager.SystemDatabase().DB()

	if err != nil {
		return ServerErrorResponse(err)
	}

	var databaseID, databaseName, branchID, branchName string

	err = systemDB.QueryRow(`
		SELECT d.database_id, d.name, b.database_branch_id, b.name
		FROM databases d
		JOIN database_branches b ON b.id = ?
		WHERE d.id = ?
	`,
		importRecord.DatabaseBranchReferenceID.Int64,
		importRecord.DatabaseReferenceID.Int64,
	).Scan(
		&databaseID,
		&databaseName,
		&branchID,
		&branchName,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	// Get uploaded chunk count and missing chunks
	uploadedCount, err := importRecord.GetUploadedChunkCount()

	if err != nil {
		return ServerErrorResponse(err)
	}

	missingChunks, err := manager.GetMissingChunks(importID)

	if err != nil {
		return ServerErrorResponse(err)
	}

	response := ImportShowResponse{
		ImportID:       importRecord.ID,
		DatabaseID:     databaseID,
		DatabaseName:   databaseName,
		BranchName:     branchName,
		Status:         string(importRecord.Status),
		ChunkCount:     importRecord.ChunkCount,
		UploadedChunks: uploadedCount,
		MissingChunks:  missingChunks,
		CreatedAt:      importRecord.CreatedAt,
		UpdatedAt:      importRecord.UpdatedAt,
	}

	if importRecord.TotalSize.Valid {
		response.TotalSize = &importRecord.TotalSize.Int64
	}

	if importRecord.CompletedAt.Valid {
		response.CompletedAt = &importRecord.CompletedAt.Time
	}

	return SuccessResponse(
		"Database import retrieved successfully",
		response,
		200,
	)
}

type ImportStoreRequest struct {
	DatabaseName database.DatabaseName `json:"databaseName" validate:"required,validateFn"`
	BranchName   string                `json:"branchName,omitempty"`
	ChunkCount   int64                 `json:"chunkCount" validate:"required,min=1"`
}

type ImportStoreResponse struct {
	ImportID     int64     `json:"importId"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	ChunkCount   int64     `json:"chunkCount"`
	Status       string    `json:"status"`
	CreatedAt    time.Time `json:"createdAt"`
}

type ImportUpdateRequest struct {
	DatabaseName database.DatabaseName `json:"databaseName" validate:"required,validateFn"`
	BranchName   string                `json:"branchName,omitempty"`
	ChunkCount   int64                 `json:"chunkCount" validate:"required,min=1"`
}

type ImportUpdateResponse struct {
	ImportID     int64     `json:"importId"`
	DatabaseID   string    `json:"databaseId"`
	DatabaseName string    `json:"databaseName"`
	BranchName   string    `json:"branchName"`
	ChunkCount   int64     `json:"chunkCount"`
	Status       string    `json:"status"`
	UpdatedAt    time.Time `json:"updatedAt"`
}

func ImportControllerStore(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&ImportStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	req, ok := input.(*ImportStoreRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"databaseName.required":   "The database name field is required",
		"databaseName.validateFn": "The database name can only contain alpha numeric characters, hyphens, or underscores",
		"branchName.lowercase":    "The branch name must be lowercase",
		"branchName.alphanum":     "The branch name can only contain alphanumeric characters",
		"chunkCount.required":     "The chunk count field is required",
		"chunkCount.min":          "The chunk count must be at least 1",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	var databaseName = string(req.DatabaseName)

	// Check if the database exists
	exists, err := request.databaseManager.Exists(databaseName)

	if err != nil {
		return ServerErrorResponse(err)
	}

	if exists {
		return BadRequestResponse(fmt.Errorf("database '%s' already exists", databaseName))
	}

	branchName := request.cluster.Config.DefaultBranchName

	if req.BranchName != "" {
		branchName = req.BranchName
	}

	// Create the database and branch first
	db, err := request.databaseManager.Create(databaseName, branchName)

	if err != nil {
		return ServerErrorResponse(err)
	}

	primaryBranch, err := db.PrimaryBranch()

	if err != nil {
		slog.Error("Failed to retrieve primary branch after database creation", "databaseId", db.DatabaseID, "error", err)

		return ServerErrorResponse(errors.New("failed to retrieve primary branch after database creation"))
	}

	// Create the import record
	manager := database.NewDatabaseImportManager(request.databaseManager)

	importRecord, err := manager.Create(db.ID, primaryBranch.ID, req.ChunkCount)

	if err != nil {
		slog.Error("Failed to create import record", "databaseId", db.DatabaseID, "error", err)

		return ServerErrorResponse(errors.New("failed to create import record"))
	}

	return SuccessResponse(
		"Database import created successfully",
		ImportStoreResponse{
			ImportID:     importRecord.ID,
			DatabaseID:   db.DatabaseID,
			DatabaseName: db.Name,
			BranchName:   primaryBranch.Name,
			ChunkCount:   importRecord.ChunkCount,
			Status:       string(importRecord.Status),
			CreatedAt:    importRecord.CreatedAt,
		},
		201,
	)
}

func ImportControllerUpdate(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	input, err := request.Input(&ImportUpdateRequest{})

	if err != nil {
		return BadRequestResponse(err)
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

	return SuccessResponse(
		"Database import updated successfully",
		ImportUpdateResponse{},
		200,
	)
}

func ImportControllerDestroy(ctx context.Context, request *Request) Response {
	// Authorize the request
	err := request.Authorize(
		[]string{"database:*"},
		[]auth.Privilege{auth.DatabasePrivilegeCreate, auth.DatabasePrivilegeImport},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Get import ID from route params
	importIDStr := request.Param("importId")

	if importIDStr == "" {
		return BadRequestResponse(errors.New("invalid import ID"))
	}

	var importID int64

	if _, err := fmt.Sscanf(importIDStr, "%d", &importID); err != nil {
		return BadRequestResponse(errors.New("invalid import ID"))
	}

	// Delete the import
	// Get the import to check if it exists
	manager := database.NewDatabaseImportManager(request.databaseManager)

	_, err = manager.Get(importID)

	if err != nil {
		if err.Error() == "database import not found" {
			return NotFoundResponse(errors.New("import not found"))
		}
		return ServerErrorResponse(err)
	}

	// Delete the import
	err = manager.Delete(importID)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Database import deleted successfully",
		nil,
		200,
	)
}
