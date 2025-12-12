package http

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/database"
)

type ImportChunkControllerStoreRequest struct {
	ChunkData  string `json:"chunkData" validate:"required"`
	ChunkIndex *int64 `json:"chunkIndex" validate:"required,min=0"`
	Checksum   string `json:"checksum,omitempty"`
}

type ImportChunkControllerStoreResponse struct {
	ImportID   int64  `json:"importId"`
	ChunkIndex int64  `json:"chunkIndex"`
	Status     string `json:"status"`
}

// Store a chunk of data for database import
func ImportChunkControllerStore(ctx context.Context, request *Request) Response {
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

	input, err := request.Input(&ImportChunkControllerStoreRequest{})

	if err != nil {
		return BadRequestResponse(err)
	}

	req, ok := input.(*ImportChunkControllerStoreRequest)

	if !ok {
		return ServerErrorResponse(errors.New("invalid request format"))
	}

	validationErrors := request.Validate(input, map[string]string{
		"chunkData.required":  "The chunk data field is required",
		"chunkIndex.required": "The chunk index field is required",
		"chunkIndex.min":      "The chunk index must be at least 0",
	})

	if validationErrors != nil {
		return ValidationErrorResponse(validationErrors)
	}

	// Decode the base64 chunk data
	chunkData, err := base64.StdEncoding.DecodeString(req.ChunkData)

	if err != nil {
		return BadRequestResponse(errors.New("invalid base64 chunk data"))
	}

	// Add the chunk to the import
	manager := database.NewDatabaseImportManager(request.databaseManager)

	chunk, err := manager.AddChunk(importID, *req.ChunkIndex, chunkData, req.Checksum)

	if err != nil {
		// Check for specific error types
		errMsg := err.Error()

		if strings.Contains(errMsg, "database import not found") {
			return NotFoundResponse(errors.New("import not found"))
		}

		if strings.HasPrefix(errMsg, "invalid chunk index") {
			return BadRequestResponse(err)
		}

		return ServerErrorResponse(err)
	}

	// Get the updated import to return status
	importRecord, err := manager.Get(importID)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Import chunk stored successfully",
		ImportChunkControllerStoreResponse{
			ImportID:   chunk.ImportReferenceID,
			ChunkIndex: chunk.ChunkIndex,
			Status:     string(importRecord.Status),
		},
		201,
	)
}
