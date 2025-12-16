package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/storage"
)

// DatabaseExportPartControllerShow retrieves a specific range from an active database export
func DatabaseExportPartControllerShow(ctx context.Context, request *Request) Response {
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

	// Get the range number from the route parameters
	rangeNumberStr := request.Param("rangeNumber")

	if rangeNumberStr == "" {
		return BadRequestResponse(errors.New("range number is required"))
	}

	rangeNumber, err := strconv.ParseInt(rangeNumberStr, 10, 64)

	if err != nil {
		return BadRequestResponse(errors.New("invalid range number"))
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

	// Get the range from the export
	rangeFile, err := export.GetRange(rangeNumber)

	if err != nil {
		return NotFoundResponse(err)
	}

	return Response{
		StatusCode: http.StatusOK,
		Stream: func(w http.ResponseWriter) {
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=\"range_%d\"", rangeNumber))

			// Calculate the size of the range
			rangeSize, err := rangeFile.Size()

			if err != nil {
				http.Error(w, fmt.Sprintf("failed to get range size: %v", err), http.StatusInternalServerError)
				return
			}

			w.Header().Set("Content-Length", fmt.Sprintf("%d", rangeSize))

			// Stream the range data in 1MB chunks for better streaming performance
			// Buffer size: 256 pages * 4KB = 1MB (optimal for HTTP streaming)
			pageSize := int64(4096)                      // SQLite page size
			bufferPages := int64(256)                    // Number of pages per buffer (1MB chunks)
			buffer := make([]byte, pageSize*bufferPages) // 1MB buffer
			pageCount := rangeFile.PageCount()

			for pageIdx := int64(1); pageIdx <= pageCount; pageIdx += bufferPages {
				// Calculate how many pages to read in this iteration
				pagesToRead := bufferPages

				if pageIdx+bufferPages-1 > pageCount {
					pagesToRead = pageCount - pageIdx + 1
				}

				// Read multiple pages at once
				bytesRead := int64(0)

				for i := int64(0); i < pagesToRead; i++ {
					currentPageIdx := pageIdx + i

					// Calculate the absolute page number for this page in the range
					pageNumber := (rangeNumber-1)*storage.RangeMaxPages + currentPageIdx

					n, err := rangeFile.ReadAt(pageNumber, buffer[bytesRead:bytesRead+pageSize])

					if err != nil && err != io.EOF {
						http.Error(w, fmt.Sprintf("failed to read page %d: %v", pageNumber, err), http.StatusInternalServerError)

						return
					}

					if n == 0 {
						break
					}

					bytesRead += int64(n)
				}

				if bytesRead == 0 {
					break
				}

				// Write the buffered data to the response
				_, writeErr := w.Write(buffer[:bytesRead])

				if writeErr != nil {
					// Client disconnected or error writing
					return
				}

				// Flush after each buffer to stream data progressively
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
			}
		},
	}
}
