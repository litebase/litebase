package http

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/logs"
)

// Response payload for error logs
type ErrorLogIndexResponse []*logs.ErrorEntry

type ErrorLogIndexQueryParameters struct {
	// The start timestamp for the error logs to retrieve (in seconds since epoch).
	Start uint64 `json:"start,string"`

	// The end timestamp for the error logs to retrieve (in seconds since epoch).
	End uint64 `json:"end,string"`
}

// List error logs for a specific database and branch
func ErrorLogControllerIndex(ctx context.Context, request *Request) Response {
	databaseKey, errResponse := request.DatabaseKey()

	if !errResponse.IsEmpty() {
		return errResponse
	}

	// Authorize the request
	err := request.Authorize(
		[]string{
			fmt.Sprintf("database:%s:branch:*", databaseKey.DatabaseID),
			fmt.Sprintf("database:%s:branch:%s", databaseKey.DatabaseID, databaseKey.DatabaseBranchID),
		},
		[]auth.Privilege{auth.DatabaseBranchPrivilegeShow},
	)

	if err != nil {
		return ForbiddenResponse(err)
	}

	// Validate and map the request query parameters
	queryParams, err := request.QueryParams(&ErrorLogIndexQueryParameters{})

	if err != nil {
		return BadRequestResponse(errors.New("the request query parameters are invalid"))
	}

	startTimestamp := queryParams.(*ErrorLogIndexQueryParameters).Start

	if startTimestamp == 0 {
		startTimestamp = uint64(time.Now().UTC().Truncate(time.Hour).Unix())
	}

	endTimestamp := queryParams.(*ErrorLogIndexQueryParameters).End

	if endTimestamp == 0 {
		endTimestamp = uint64(time.Now().UTC().Unix())
	}

	if endTimestamp < startTimestamp {
		return BadRequestResponse(errors.New("end timestamp must be greater than or equal to start timestamp"))
	}

	// Use request.GetErrorLog which automatically configures encryption if needed
	errorLog, err := request.GetErrorLog(
		databaseKey.DatabaseHash,
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	if err != nil {
		return ServerErrorResponse(err)
	}

	uint32StartTimestamp, err := utils.SafeUint64ToUint32(startTimestamp)

	if err != nil {
		return BadRequestResponse(errors.New("invalid start timestamp"))
	}

	uint32EndTimestamp, err := utils.SafeUint64ToUint32(endTimestamp)

	if err != nil {
		return BadRequestResponse(errors.New("invalid end timestamp"))
	}

	// Read error entries from storage
	errorEntries, err := errorLog.Read(uint32StartTimestamp, uint32EndTimestamp)

	if err != nil {
		return ServerErrorResponse(err)
	}

	return SuccessResponse(
		"Successfully retrieved error logs.",
		ErrorLogIndexResponse(errorEntries),
		200,
	)
}
