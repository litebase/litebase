package http

import (
	"encoding/json"
	"errors"
	"litebasedb/internal/config"
	"litebasedb/router/auth"
	"litebasedb/router/connections"
	"litebasedb/router/runtime"

	"log"
	"time"
)

type RuntimeResponse struct {
	StatusCode int                    `json:"statusCode"`
	Body       map[string]interface{} `json:"body"`
}

func ForwardRequest(request *Request, databaseUuid string, branchUuid string, accessKeyId string, fn string) *RuntimeResponse {
	var err error

	if fn == "" {
		fn, err = auth.SecretsManager().GetFunctionName(databaseUuid, branchUuid)

		if fn == "" || err != nil {
			log.Fatal(errors.New("this database is not properly configured"))
		}
	}

	if config.Get("env") == "local" {
		fn = "function"
	}

	payload, err := runtime.PrepareRequest(&runtime.RuntimeRequestObject{
		AccessKeyId:  accessKeyId,
		Body:         request.Body,
		DatabaseUuid: databaseUuid,
		BranchUuid:   branchUuid,
		Headers:      request.Headers().All(),
		Method:       request.Method,
		Path:         request.Path,
		Query:        request.QueryParams,
	}, accessKeyId == "")

	if err != nil {
		log.Println(err)

		return &RuntimeResponse{
			StatusCode: 500,
			Body: map[string]interface{}{
				"message": "Internal server error",
				"status":  "error",
			},
		}
	}

	startTime := time.Now()
	var result []byte
	executionContext := 1
	connectionResult := connections.SendThroughConnection(databaseUuid, branchUuid, fn, payload)

	if len(connectionResult) > 0 {
		executionContext = 2
		result = connectionResult
	} else {
		result = connections.Send(databaseUuid, branchUuid, fn, payload)
	}

	return PrepareResponse(
		startTime,
		executionContext,
		result,
	)
}

func PrepareResponse(startTime time.Time, executionContext int, res []byte) *RuntimeResponse {
	if res == nil {
		return &RuntimeResponse{
			StatusCode: 500,
			Body: map[string]interface{}{
				"Message": "Internal server error",
				"Status":  "error",
			},
		}
	}

	response := &RuntimeResponse{}
	err := json.Unmarshal([]byte(res), response)

	if err != nil {
		log.Println(err)
		return &RuntimeResponse{
			StatusCode: 500,
			Body: map[string]interface{}{
				"message": "Internal server error",
				"status":  "error",
			},
		}
	}

	if response.StatusCode >= 400 {
		return &RuntimeResponse{
			StatusCode: response.StatusCode,
			Body: map[string]interface{}{
				"message": response.Body["message"],
				"status":  "error",
			},
		}
	}

	response.Body["_execution_context"] = executionContext
	response.Body["_execution_latency"] = float64(time.Since(startTime)) / float64(time.Millisecond)

	return response

}
