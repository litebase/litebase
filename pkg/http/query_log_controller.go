package http

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/litebase/litebase/internal/utils"
	"github.com/litebase/litebase/pkg/auth"
	"github.com/litebase/litebase/pkg/logs"
)

// Response payload for query logs
type QueryLogIndexResponse []logs.QueryMetric

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

	step, err := strconv.ParseInt(request.QueryParam("step", "1"), 10, 64)

	if err != nil || step < 1 {
		return BadRequestResponse(errors.New("invalid step value"))
	}

	startTimestamp, err := strconv.ParseUint(request.QueryParam("start"), 10, 64)

	if err != nil {
		return BadRequestResponse(errors.New("invalid start timestamp"))
	}

	endTimestamp, err := strconv.ParseUint(request.QueryParam("end"), 10, 64)

	if err != nil {
		return BadRequestResponse(errors.New("invalid end timestamp"))
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
func combineQueryMeticsByStep(metrics []logs.QueryMetric, step int64) QueryLogIndexResponse {
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

		uint32Step, err := utils.SafeInt64ToUint32(step)

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
