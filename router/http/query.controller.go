package http

import (
	"litebasedb/router/auth"
)

func QueryController(request *Request) *Response {
	// start := time.Now()

	databaseKey := request.Subdomains()[0]

	if databaseKey == "" || len(request.Subdomains()) != 2 {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	databaseUuid := auth.SecretsManager().GetDatabaseUuid(databaseKey)

	if databaseUuid == "" {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	accessKey := request.RequestToken("Authorization").AccessKey(databaseUuid)

	if accessKey == nil {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	branchUuid := accessKey.GetBranchUuid()

	if branchUuid == "" {
		return &Response{
			StatusCode: 400,
			Body: map[string]interface{}{
				"status":  "error",
				"message": "Bad request",
			},
		}
	}

	response := ForwardRequest(
		request,
		databaseUuid,
		branchUuid,
		accessKey.AccessKeyId,
		"",
	)

	// log.Println(time.Since(start))

	return &Response{
		StatusCode: response.StatusCode,
		Body:       response.Body,
	}
}
