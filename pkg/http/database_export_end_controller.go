package http

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/litebase/litebase/pkg/auth"
)

type DatabaseExportEndStoreRequest struct{}

type DatabaseExportEndStoreResponse struct{}

// End an active database export session.
func DatabaseExportEndControllerStore(ctx context.Context, request *Request) Response {
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
