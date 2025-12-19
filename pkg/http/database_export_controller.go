package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/litebase/litebase/pkg/auth"
)

type DatabaseExportControllerStoreRequest struct{}

type DatabaseExportControllerStoreResponse struct{}

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

	exportManager, err := request.databaseManager.Resources(
		request.databaseKey.DatabaseID,
		request.databaseKey.DatabaseBranchID,
	).ExportManager()

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
	dfs := request.databaseManager.Resources(
		request.databaseKey.DatabaseID,
		request.databaseKey.DatabaseBranchID,
	).FileSystem()

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
	response := map[string]any{
		"id":         export.ID,
		"rangeCount": export.RangeCount(),
		"startedAt":  export.StartedAt.Format(time.RFC3339),
		"expiresAt":  export.StartedAt.Add(60 * time.Second).Format(time.RFC3339),
	}

	return SuccessResponse("Database export started", response, http.StatusCreated)
}

type DatabaseExportControllerEndRequest struct{}

type DatabaseExportControllerEndResponse struct{}

// End an active database export session.
func DatabaseExportControllerEnd(ctx context.Context, request *Request) Response {
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

	// Get the export ID from the route parameters
	exportIDStr := request.Param("exportId")

	if exportIDStr == "" {
		return BadRequestResponse(errors.New("export ID is required"))
	}

	// Get the export manager
	exportManager, err := request.databaseManager.Resources(
		request.databaseKey.DatabaseID,
		request.databaseKey.DatabaseBranchID,
	).ExportManager()

	if err != nil {
		return ServerErrorResponse(err)
	}

	// Get the active export
	export, err := exportManager.Get()

	if err != nil {
		return NotFoundResponse(errors.New("no active export found"))
	}

	// Verify the export ID matches
	if export.ID != exportIDStr {
		return NotFoundResponse(errors.New("export ID does not match active export"))
	}

	// Clear the export
	exportManager.Clear()

	return Response{
		StatusCode: http.StatusNoContent,
	}
}
