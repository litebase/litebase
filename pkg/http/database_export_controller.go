package http

import (
	"context"
	"encoding/json"
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

	return Response{
		StatusCode: http.StatusOK,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Transfer-Encoding", "chunked")

			// Create the export - only one export can run at a time for a given database
			export, err := exportManager.Create()

			if err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			// Checkpoint the database before export
			err = request.databaseManager.ConnectionManager().ForceCheckpoint(
				request.databaseKey.DatabaseID,
				request.databaseKey.DatabaseBranchID,
			)

			if err != nil {
				http.Error(w, fmt.Sprintf("failed to checkpoint database: %v", err), http.StatusInternalServerError)
				return
			}

			// Get the database file system for compaction
			dfs := request.databaseManager.Resources(
				request.databaseKey.DatabaseID,
				request.databaseKey.DatabaseBranchID,
			).FileSystem()

			// Compact the database before export
			err = dfs.Compact()

			if err != nil {
				http.Error(w, fmt.Sprintf("failed to compact database: %v", err), http.StatusInternalServerError)
				return
			}

			// Create context for managing the export lifecycle
			ctx, cancel := context.WithCancel(request.BaseRequest.Context())

			// Start the export stream handler
			go func() {
				defer func() {
					cancel()
					exportManager.Clear()
					export.End()
				}()

				// Use compaction barrier to prevent compaction during export
				_ = dfs.CompactionBarrier(func() error {
					// Write export metadata to response
					rangeCount := export.RangeCount()

					response := map[string]any{
						"id":         export.ID,
						"rangeCount": rangeCount,
						"startedAt":  export.StartedAt.Format(time.RFC3339),
					}

					// Write the response
					if err := json.NewEncoder(w).Encode(response); err != nil {
						return fmt.Errorf("failed to write response: %w", err)
					}

					// Flush the response to the client
					if flusher, ok := w.(http.Flusher); ok {
						flusher.Flush()
					}

					// Keep the connection alive by periodically writing data
					// This maintains the compaction barrier while the client fetches ranges
					ticker := time.NewTicker(1 * time.Second)
					defer ticker.Stop()

					for {
						select {
						case <-ctx.Done():
							// Client disconnected
							return nil
						case <-ticker.C:
						}
					}
				})
			}()

			// Wait for the export to complete or client to disconnect
			<-ctx.Done()
		},
	}
}
