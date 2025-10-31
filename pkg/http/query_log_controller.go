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

// Response payload for query logs
type QueryLogIndexResponse []logs.QueryMetric

type QueryLogIndexQueryParameters struct {
	// The start timestamp for the query logs to retrieve (in seconds since epoch).
	Start uint64 `json:"start,string"`

	// The end timestamp for the query logs to retrieve (in seconds since epoch).
	End uint64 `json:"end,string"`

	// The step interval (in seconds) to combine query metrics. For example, if
	// step is 60, then all query metrics that occur within the same minute will
	// be combined into a single metric. This is useful for reducing the number
	// of query metrics returned when there are many queries executed within a
	// short period of time.
	Step int `json:"step" default:"1"`
}

// List query logs for a specific database and branch
func QueryLogControllerIndex(ctx context.Context, request *Request) Response {
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

	// Validate and map the request query parameters to the QueryLogIndexQueryParameters struct
	// (we don't need the resulting value here because the controller extracts
	// specific params manually below; this call will validate/decode types)
	queryParams, err := request.QueryParams(&QueryLogIndexQueryParameters{})

	if err != nil {
		return BadRequestResponse(errors.New("the request query parameters are invalid"))
	}

	step := queryParams.(*QueryLogIndexQueryParameters).Step

	if step < 1 {
		return BadRequestResponse(errors.New("invalid step value"))
	}

	startTimestamp := queryParams.(*QueryLogIndexQueryParameters).Start

	if startTimestamp == 0 {
		startTimestamp = uint64(time.Now().UTC().Truncate(time.Hour).Unix())
	}

	endTimestamp := queryParams.(*QueryLogIndexQueryParameters).End

	if endTimestamp == 0 {
		endTimestamp = uint64(time.Now().UTC().Unix())
	}

	if endTimestamp < startTimestamp {
		return BadRequestResponse(errors.New("end timestamp must be greater than or equal to start timestamp"))
	}

	queryLog := request.logManager.GetQueryLog(
		request.cluster,
		databaseKey.DatabaseHash,
		databaseKey.DatabaseID,
		databaseKey.DatabaseBranchID,
	)

	uint32StartTimestamp, err := utils.SafeUint64ToUint32(startTimestamp)

	if err != nil {
		return BadRequestResponse(errors.New("invalid start timestamp"))
	}

	uint32EndTimestamp, err := utils.SafeUint64ToUint32(endTimestamp)

	if err != nil {
		return BadRequestResponse(errors.New("invalid end timestamp"))
	}

	metrics, err := queryLog.Read(uint32StartTimestamp, uint32EndTimestamp)

	if err != nil {
		return ServerErrorResponse(err)
	}

	response := combineQueryMeticsByStep(metrics, step)

	return SuccessResponse(
		"Successfully retrieved query logs.",
		response,
		200,
	).WithMeta("keys", logs.QueryMetricKeys())
}

// Combine query metrics by step, which is the number of seconds to combine.
// Start from the first metric and any subsequent metrics that are within the
// step interval into a single metric.
func combineQueryMeticsByStep(metrics []logs.QueryMetric, step int) QueryLogIndexResponse {
	if step == 1 {
		return metrics
	}

	combinedMetrics := make([]logs.QueryMetric, 0)
	combinedMetric := logs.QueryMetric{}

	for i, metric := range metrics {
		if i == 0 {
			combinedMetric = metric
			continue
		}

		uint32Step, err := utils.SafeIntToUint32(step)

		if err != nil {
			return nil
		}

		if metric.Timestamp >= combinedMetric.Timestamp+uint32Step {
			combinedMetrics = append(combinedMetrics, combinedMetric)
			combinedMetric = metric
			continue
		}

		combinedMetric = combinedMetric.Combine(metric)
	}

	combinedMetrics = append(combinedMetrics, combinedMetric)

	return combinedMetrics
}
