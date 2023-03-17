package http

import (
	"litebasedb/app/logging"
	"strconv"
)

func QueryLogController(request *Request) *Response {
	startTimestamp, err := strconv.Atoi(request.QueryParam("start"))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid start timestamp",
		}, 400, nil)
	}

	endTimestamp, err := strconv.Atoi(request.QueryParam("end"))

	if err != nil {
		return JsonResponse(map[string]interface{}{
			"status":  "error",
			"message": "Invalid end timestamp",
		}, 400, nil)
	}

	queryLog := logging.GetQueryLog(
		request.Param("database"),
		request.Param("branch"),
	)

	data := queryLog.Read(startTimestamp, endTimestamp)

	return JsonResponse(map[string]interface{}{
		"status": "success",
		"data":   data,
	}, 200, nil)
}
