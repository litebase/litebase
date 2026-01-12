package http

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

type DatabaseExportStoreRequest struct{}

type DatabaseExportStoreResponse struct {
	DatabaseName       string    `json:"databaseName"`
	DatabaseBranchName string    `json:"databaseBranchName"`
	ID                 string    `json:"id"`
	Ranges             []int     `json:"ranges"`
	RangeCount         int       `json:"rangeCount"`
	StartedAt          time.Time `json:"startedAt"`
	ExpiresAt          time.Time `json:"expiresAt"`
}

// Export a database.
func DatabaseExportControllerStore(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	err := request.Authorize(
		[]string{fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID)},
		[]auth.Privilege{auth.DatabasePrivilegeExport},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	branch, err := request.databaseManager.GetBranch(
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return BadRequestResponse(err)
	}

	exportManager, err := request.databaseManager.Resources(branch).ExportManager()

	if err != nil {
		return ServerErrorResponse(err)
	}

	// Check if an export is already active before doing expensive operations
	existingExport, _ := exportManager.Get()

	if existingExport != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": "an export is already active",
		}, http.StatusConflict, nil)
	}

	// Checkpoint the database before export
	err = request.databaseManager.ConnectionManager().ForceCheckpoint(
		request.databaseKey.DatabaseID,
		request.databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("failed to checkpoint database: %w", err))
	}

	// Get the database file system for compaction
	dfs := request.databaseManager.Resources(branch).FileSystem()

	// Compact the database before export (must be done BEFORE creating export to avoid deadlock)
	err = dfs.Compact()

	if err != nil {
		return ServerErrorResponse(fmt.Errorf("failed to compact database: %w", err))
	}

	// Create the export - only one export can run at a time for a given database
	// This acquires a compaction barrier to prevent compaction during export
	export, err := exportManager.Create()

	if err != nil {
		return JsonResponse(map[string]any{
			"status":  "error",
			"message": err.Error(),
		}, http.StatusConflict, nil)
	}

	// Return export metadata
	response := DatabaseExportStoreResponse{
		DatabaseName:       request.databaseKey.DatabaseName,
		DatabaseBranchName: request.databaseKey.DatabaseBranchName,
		ID:                 export.ID,
		Ranges:             export.Ranges(),
		RangeCount:         export.RangeCount(),
		StartedAt:          export.StartedAt,
		ExpiresAt:          export.StartedAt.Add(60 * time.Second),
	}

	return SuccessResponse("Database export started", response, http.StatusCreated)
}
