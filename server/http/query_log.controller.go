package http

import (
	"litebase/server/logs"
	"strconv"
)

func QueryLogController(request *Request) Response {
	step, err := strconv.ParseInt(request.QueryParam("step", "1"), 10, 64)

	if err != nil || step < 1 {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid step value",
		}, 400, nil)
	}

	startTimestamp, err := strconv.ParseUint(request.QueryParam("start"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid start timestamp",
		}, 400, nil)
	}

	endTimestamp, err := strconv.ParseUint(request.QueryParam("end"), 10, 64)

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid end timestamp",
		}, 400, nil)
	}

	queryLog := logs.GetQueryLog(
		request.DatabaseKey().DatabaseHash,
		request.DatabaseKey().DatabaseUuid,
		request.DatabaseKey().BranchUuid,
	)

	metrics := queryLog.Read(uint32(startTimestamp), uint32(endTimestamp))
	metrics = combineQueryMeticsByStep(metrics, step)

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"meta": map[string]interface{}{
			"keys": logs.QueryMetricKeys(),
		},
		"data": metrics,
	}, 200, nil)
}

// Combine query metrics by step, which is the number of seconds to combine.
// Start from the first metric and any subsequent metrics that are within the
// step interval into a single metric.
func combineQueryMeticsByStep(metrics []logs.QueryMetric, step int64) []logs.QueryMetric {
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

		if metric.Timestamp >= combinedMetric.Timestamp+uint32(step) {
			combinedMetrics = append(combinedMetrics, combinedMetric)
			combinedMetric = metric
			continue
		}

		combinedMetric = combinedMetric.Combine(metric)
	}

	combinedMetrics = append(combinedMetrics, combinedMetric)

	return combinedMetrics
}
